// program/permissions_test.go
// Tests for PermissionsFor, the manifest-to-NATS-permissions derivation.
// Methodology: two layers. A table names every crossing the worker SDK
// and a micro service make, one row each, and checks it against the
// generated lists with NATS wildcard rules. Then the same program runs
// for real (worker, micro service, own stream and bucket, a full
// workflow run) as an NKey user carrying exactly those permissions, and
// the server's violation log must stay empty. The negative tests drive
// the denials through the same server and assert each one is logged.
package program_test

import (
	"context"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// sharedBuckets is the engine's coarse-shared bucket list (design §2.1).
var sharedBuckets = []string{
	"workers", "services", "triggers", "trigger_types", "checkpoints",
	"signals", "workflow_runs", "worker_status", "submissions", "leases",
}

type crossing struct {
	name    string
	publish bool // false: subscribe
	subject string
}

// crossingTable is every subject a toy program reaches, one row per
// crossing. Consumer names are the worker SDK's durables for toy.echo
// and toy.send.email.
func crossingTable() []crossing {
	rows := []crossing{
		{"task stream info", true, "$JS.API.STREAM.INFO.TASK_QUEUES"},
		{"task consumer list", true, "$JS.API.CONSUMER.LIST.TASK_QUEUES"},
		{"task consumer create", true,
			"$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-echo.task.toy.echo.*"},
		{"pause marker write", true, "$KV.checkpoints.run1.wait"},
		{"task consumer create dotted", true,
			"$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-send-email.task.toy.send.email.*"},
		{"task consumer info", true, "$JS.API.CONSUMER.INFO.TASK_QUEUES.workers-toy-echo"},
		{"task consumer delete", true, "$JS.API.CONSUMER.DELETE.TASK_QUEUES.workers-toy-echo"},
		{"task consumer next", true, "$JS.API.CONSUMER.MSG.NEXT.TASK_QUEUES.workers-toy-echo"},
		{"task ack", true, "$JS.ACK.TASK_QUEUES.workers-toy-echo.1.2.3.4.5.6"},
		{"sticky stream info", true, "$JS.API.STREAM.INFO.STICKY_TASKS"},
		{"sticky consumer create", true,
			"$JS.API.CONSUMER.CREATE.STICKY_TASKS.abc.sticky.toy.echo.wid.>"},
		{"sticky consumer next", true, "$JS.API.CONSUMER.MSG.NEXT.STICKY_TASKS.abc"},
		{"sticky ack", true, "$JS.ACK.STICKY_TASKS.abc.1.2.3.4.5.6"},
		{"history", true, "history.run1"},
		{"stream", true, "stream.run1.step1"},
		{"logs", true, "logs.run1.step1.stdout"},
		{"journal", true, "journal.run1.step1"},
		{"journal stream info", true, "$JS.API.STREAM.INFO.AGENT_JOURNAL"},
		{"journal tail", true, "$JS.API.STREAM.MSG.GET.AGENT_JOURNAL"},
		{"history stream info", true, "$JS.API.STREAM.INFO.WORKFLOW_HISTORY"},
		{"history tail", true, "$JS.API.STREAM.MSG.GET.WORKFLOW_HISTORY"},
		{"control plane", true, "api.p.toy.workflows.register"},
		{"run cancel", false, "dagnats.cancel.run1"},
		{"account info", true, "$JS.API.INFO"},
		{"micro ping", false, "$SRV.PING"},
		{"micro ping name", false, "$SRV.PING.toy"},
		{"micro ping id", false, "$SRV.PING.toy.id1"},
		{"micro info", false, "$SRV.INFO"},
		{"micro info name", false, "$SRV.INFO.toy"},
		{"micro info id", false, "$SRV.INFO.toy.id1"},
		{"micro stats", false, "$SRV.STATS"},
		{"micro stats name", false, "$SRV.STATS.toy"},
		{"micro stats id", false, "$SRV.STATS.toy.id1"},
		{"own subjects publish", true, "toy.svc.echo"},
		{"own subjects subscribe", false, "toy.svc.echo"},
		{"protocol publish", true, "toyproto.ping"},
		{"protocol subscribe", false, "toyproto.ping"},
		{"protocol subtree", false, "toyproto.ping.deep"},
		{"own inbox", false, "_INBOX.toy.abc"},
	}
	for _, b := range sharedBuckets {
		rows = append(rows, kvRows("shared "+b, b)...)
	}
	rows = append(rows, kvRows("own bucket", "toy_state")...)
	return append(rows, streamRows("own stream", "TOY_EVENTS")...)
}

func kvRows(label, bucket string) []crossing {
	s := "KV_" + bucket
	return []crossing{
		{label + " stream info", true, "$JS.API.STREAM.INFO." + s},
		{label + " direct get", true, "$JS.API.DIRECT.GET." + s + ".$KV." + bucket + ".k"},
		{label + " watch", true, "$JS.API.CONSUMER.CREATE." + s + ".c.$KV." + bucket + ".>"},
		{label + " watch stop", true, "$JS.API.CONSUMER.DELETE." + s + ".c"},
		{label + " put", true, "$KV." + bucket + ".k"},
		{label + " flow control", true, "$JS.FC." + s + ".c.x"},
	}
}

func streamRows(label, stream string) []crossing {
	return []crossing{
		{label + " info", true, "$JS.API.STREAM.INFO." + stream},
		{label + " msg get", true, "$JS.API.STREAM.MSG.GET." + stream},
		{label + " consumer create", true, "$JS.API.CONSUMER.CREATE." + stream + ".c.toy.events.>"},
		{label + " consumer info", true, "$JS.API.CONSUMER.INFO." + stream + ".c"},
		{label + " consumer delete", true, "$JS.API.CONSUMER.DELETE." + stream + ".c"},
		{label + " consumer list", true, "$JS.API.CONSUMER.LIST." + stream},
		{label + " consumer next", true, "$JS.API.CONSUMER.MSG.NEXT." + stream + ".c"},
		{label + " ack", true, "$JS.ACK." + stream + ".c.1.2.3.4.5.6"},
		{label + " publish", true, "toy.events.x"},
	}
}

func TestPermissions_CrossingListComplete(t *testing.T) {
	m := validManifest()
	perms := program.PermissionsFor(m)
	for _, row := range crossingTable() {
		sp, kind := perms.Subscribe, "subscribe"
		if row.publish {
			sp, kind = perms.Publish, "publish"
		}
		// Positive: every crossing the SDK makes is granted.
		if !allowedBy(sp, row.subject) {
			t.Errorf("%s: %s %q not granted", row.name, kind, row.subject)
		}
	}
	// Positive: micro replies reach a caller's inbox, any program's.
	if !allowedBy(perms.Publish, "_INBOX.other.reply1") {
		t.Error("publish to a caller's inbox not granted")
	}
	if t.Failed() {
		t.FailNow()
	}
	runToyEndToEnd(t, m)
}

// runToyEndToEnd runs the toy program as its restricted user through a
// whole workflow run and every crossing, then asserts no violation.
func runToyEndToEnd(t *testing.T, m program.Manifest) {
	f, svc := startEngineFixture(t, m)
	adoptLegacyDurable(t, f)
	nc := f.dialProgram(t, m.Name)
	prog := &toyProgram{manifest: m, service: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop, err := program.Attach(ctx, nc, prog, toyGrant())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Cleanup(func() { _ = stop(context.Background()) })

	// Positive: the run, including a Pause and its resume, completed
	// over the restricted user.
	runToyWorkflow(t, svc, dag.RunStatusCompleted)
	if n := prog.resumed.Load(); n != 1 {
		t.Fatalf("toy.wait resumed %d times from its pause marker, want 1", n)
	}
	exerciseToyCrossings(ctx, t, f, nc)
	if err := stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Negative: not one subject the program used was denied.
	if v := f.violations.snapshot(); len(v) > 0 {
		t.Fatalf("permission violations:\n%s", strings.Join(v, "\n"))
	}
}

// exerciseToyCrossings drives the crossings a workflow run does not
// reach on its own: shared buckets, own stores, micro, control plane.
func exerciseToyCrossings(ctx context.Context, t *testing.T, f *fixture, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	for _, b := range append(slices.Clone(sharedBuckets), "toy_state") {
		exerciseBucket(ctx, t, js, b)
	}
	exerciseOwnStream(ctx, t, js)
	_, err = nc.Request("api.p.toy.attach", nil, 500*time.Millisecond)
	if err != nats.ErrNoResponders {
		t.Fatalf("api.p.toy.attach: %v, want no responders (D2 serves it)", err)
	}
	for _, subj := range []string{
		"$SRV.PING.toy", "$SRV.INFO", "$SRV.STATS.toy", "toyproto.ping", "toy.svc.echo",
	} {
		if _, err := f.admin.Request(subj, []byte("x"), 5*time.Second); err != nil {
			t.Fatalf("admin request %s: %v", subj, err)
		}
	}
}

// adoptLegacyDurable leaves a pre-#674 durable (trailing ">" filter) for
// the SDK to inspect and replace, the path that needs consumer info and
// delete on the program's own durable.
func adoptLegacyDurable(t *testing.T, f *fixture) {
	t.Helper()
	js, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	_, err = js.CreateConsumer(context.Background(), "TASK_QUEUES", jetstream.ConsumerConfig{
		Durable: "workers-toy-echo", FilterSubject: "task.toy.echo.>",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("legacy durable: %v", err)
	}
}

func exerciseBucket(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string) {
	t.Helper()
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		t.Fatalf("open %s: %v", bucket, err)
	}
	if _, err := kv.Put(ctx, "toy-probe", []byte("v")); err != nil {
		t.Fatalf("put %s: %v", bucket, err)
	}
	if _, err := kv.Get(ctx, "toy-probe"); err != nil {
		t.Fatalf("get %s: %v", bucket, err)
	}
	w, err := kv.Watch(ctx, "toy-probe")
	if err != nil {
		t.Fatalf("watch %s: %v", bucket, err)
	}
	select {
	case <-w.Updates():
	case <-ctx.Done():
		t.Fatalf("watch %s: no initial value", bucket)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("watch stop %s: %v", bucket, err)
	}
	if err := kv.Delete(ctx, "toy-probe"); err != nil {
		t.Fatalf("delete %s: %v", bucket, err)
	}
}

func exerciseOwnStream(ctx context.Context, t *testing.T, js jetstream.JetStream) {
	t.Helper()
	if _, err := js.Publish(ctx, "toy.events.x", []byte("e")); err != nil {
		t.Fatalf("publish toy.events.x: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "TOY_EVENTS", jetstream.ConsumerConfig{
		Durable: "reader", FilterSubject: "toy.events.>", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("own consumer: %v", err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	got := 0
	for msg := range batch.Messages() {
		got++
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if got != 1 {
		t.Fatalf("fetched %d messages from TOY_EVENTS, want 1", got)
	}
}

func TestPermissions_FilteredConsumerAllowedForeignDenied(t *testing.T) {
	m := validManifest()
	f := startFixture(t, m)
	js, err := jetstream.New(f.dialProgram(t, m.Name))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()
	create := func(cfg jetstream.ConsumerConfig) error {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg)
		return err
	}
	// Positive: the SDK's own durable with its own filter is created.
	own := jetstream.ConsumerConfig{
		Durable: "workers-toy-echo", FilterSubject: "task.toy.echo.*",
		AckPolicy: jetstream.AckExplicitPolicy,
	}
	if err := create(own); err != nil {
		t.Fatalf("own filtered consumer: %v", err)
	}
	denied := []struct {
		name    string
		cfg     jetstream.ConsumerConfig
		subject string
	}{
		{"foreign filter", jetstream.ConsumerConfig{
			Durable: "workers-toy-echo", FilterSubject: "task.notify.send.*",
		}, "$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-echo.task.notify.send.*"},
		{"undeclared own type", jetstream.ConsumerConfig{
			Durable: "workers-toy-other", FilterSubject: "task.toy.other.*",
		}, "$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-other.task.toy.other.*"},
		{"foreign durable name", jetstream.ConsumerConfig{
			Durable: "workers-notify-send", FilterSubject: "task.toy.echo.*",
		}, "$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-notify-send.task.toy.echo.*"},
		{"unfiltered", jetstream.ConsumerConfig{Durable: "workers-toy-echo"},
			"$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-echo"},
		{"multi filter", jetstream.ConsumerConfig{
			Durable: "workers-toy-echo", FilterSubjects: []string{"task.toy.echo.*", "task.toy.x.*"},
		}, "$JS.API.CONSUMER.CREATE.TASK_QUEUES.workers-toy-echo"},
	}
	for _, d := range denied {
		// Negative: every other consumer shape on TASK_QUEUES is refused.
		if err := create(d.cfg); err == nil {
			t.Errorf("%s: consumer created, want denial", d.name)
		}
		if !f.violations.contains(d.subject) {
			t.Errorf("%s: no violation logged for %q", d.name, d.subject)
		}
	}
	assertForeignResourcesDenied(t, f, js)
}

// assertForeignResourcesDenied replays the rest of the §2.3 prototype:
// every store create (own ones are the host's job), foreign subject and
// foreign subscription fails.
func assertForeignResourcesDenied(t *testing.T, f *fixture, js jetstream.JetStream) {
	t.Helper()
	// Each call gets its own deadline: a denial burns its whole timeout.
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	if _, err := js.CreateStream(short(), jetstream.StreamConfig{
		Name: "TOY_EVENTS", Subjects: []string{"toy.events.>"}, MaxBytes: 1 << 20,
	}); err == nil {
		t.Error("own stream created by the program, want the host to provision it")
	}
	if _, err := js.CreateStream(short(), jetstream.StreamConfig{
		Name: "NOTIFY_FEED", Subjects: []string{"notify.feed.>"},
	}); err == nil {
		t.Error("foreign stream created, want denial")
	}
	inbox := jetstream.KeyValueConfig{Bucket: "notify_inbox"}
	if _, err := js.CreateKeyValue(short(), inbox); err == nil {
		t.Error("foreign bucket created, want denial")
	}
	if _, err := js.KeyValue(short(), "workflow_defs"); err == nil {
		t.Error("engine-private bucket workflow_defs opened, want denial")
	}
	nc := js.Conn()
	if err := nc.Publish("task.notify.send.run9", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := nc.SubscribeSync("task.>"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, subj := range []string{
		"$JS.API.STREAM.CREATE.TOY_EVENTS",
		"$JS.API.STREAM.CREATE.NOTIFY_FEED", "$JS.API.STREAM.CREATE.KV_notify_inbox",
		"$JS.API.STREAM.INFO.KV_workflow_defs", "task.notify.send.run9", "task.>",
	} {
		// Negative: each foreign crossing reached the violation log.
		if !waitFor(func() bool { return f.violations.contains(subj) }) {
			t.Errorf("no violation logged for %q", subj)
		}
	}
}

func TestPermissions_InboxConfinedToOwnPrefix(t *testing.T) {
	toy, other := validManifest(), validManifest()
	other.Name, other.TaskTypes, other.Workflows = "other", nil, nil
	other.Streams, other.Buckets, other.Protocols = nil, nil, nil
	for _, a := range program.PermissionsFor(toy).Subscribe.Allow {
		// Negative: no inbox grant reaches beyond the program's own.
		if strings.HasPrefix(a, "_INBOX") && a != "_INBOX.toy.>" {
			t.Fatalf("subscribe grant %q, want only _INBOX.toy.>", a)
		}
		if subjectMatches(a, "_INBOX.other.x") {
			t.Fatalf("subscribe grant %q covers program other's inbox", a)
		}
	}
	f := startFixture(t, toy, other)
	toyJS, err := jetstream.New(f.dialProgram(t, "toy"))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	spy := f.dialProgram(t, "other")
	sniffed, err := spy.SubscribeSync("_INBOX.toy.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := spy.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	fetchOneTask(t, f, toyJS)
	// Negative: the other program saw nothing of toy's fetch.
	if msg, err := sniffed.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("other read toy's inbox: %s %q", msg.Subject, msg.Data)
	}
	// Positive: the server refused the subscription outright.
	if !waitFor(func() bool { return f.violations.contains("_INBOX.toy.>") }) {
		t.Fatal("no violation logged for other's _INBOX.toy.> subscription")
	}
}

// fetchOneTask publishes a toy.echo task as the engine and fetches it
// as toy, so a task body crosses toy's inbox.
func fetchOneTask(t *testing.T, f *fixture, js jetstream.JetStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
		Durable: "workers-toy-echo", FilterSubject: "task.toy.echo.*",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if err := f.admin.Publish("task.toy.echo.run1", []byte("secret")); err != nil {
		t.Fatalf("publish task: %v", err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	got := 0
	for range batch.Messages() {
		got++
	}
	if got != 1 {
		t.Fatalf("toy fetched %d tasks, want 1", got)
	}
}

func TestPermissions_PlainStructNoNatsServerImport(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/danmestas/dagnats/program").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	deps := strings.Fields(string(out))
	// Positive: the list is real (program links the client).
	if !slices.Contains(deps, "github.com/nats-io/nats.go") {
		t.Fatalf("deps lack nats.go; go list output unexpected:\n%s", out)
	}
	for _, d := range deps {
		// Negative: nothing under nats-server is linked.
		if strings.HasPrefix(d, "github.com/nats-io/nats-server") {
			t.Fatalf("program links %s", d)
		}
	}
}

func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestPermissions_ReadsGrantReadSubjectsOnly(t *testing.T) {
	m := validManifest()
	m.Reads = []string{"tools_registry"}
	perms := program.PermissionsFor(m)
	for _, subject := range []string{
		"$JS.API.STREAM.INFO.KV_tools_registry",
		"$JS.API.DIRECT.GET.KV_tools_registry.$KV.tools_registry.k",
		"$JS.API.CONSUMER.CREATE.KV_tools_registry.c.$KV.tools_registry.>",
		"$JS.API.CONSUMER.INFO.KV_tools_registry.c",
		"$JS.FC.KV_tools_registry.c.x",
	} {
		// Positive: each read crossing is granted.
		if !allowedBy(perms.Publish, subject) {
			t.Errorf("publish %q not granted", subject)
		}
	}
	for _, subject := range []string{
		"$KV.tools_registry.k",
		"$JS.API.STREAM.PURGE.KV_tools_registry",
		"$JS.API.STREAM.DELETE.KV_tools_registry",
		"$JS.API.STREAM.UPDATE.KV_tools_registry",
		"$JS.API.STREAM.MSG.DELETE.KV_tools_registry",
		"$JS.API.STREAM.INFO.KV_tools_roles",
		"$JS.API.CONSUMER.DELETE.KV_tools_registry.c",
	} {
		// Negative: no write, purge or ungranted bucket.
		if allowedBy(perms.Publish, subject) {
			t.Errorf("publish %q granted, want denied", subject)
		}
	}
}

func consumerCount(ctx context.Context, t *testing.T, js jetstream.JetStream, stream string) int {
	t.Helper()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	n := 0
	names := s.ListConsumers(ctx)
	for range names.Info() {
		n++
	}
	if err := names.Err(); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	return n
}

// TestPermissions_ReaderCannotDeleteOwnersWatcher: the owner's watch
// consumer survives a reader's delete, which would otherwise be a DoS.
func TestPermissions_ReaderCannotDeleteOwnersWatcher(t *testing.T) {
	owner := program.Manifest{
		Name: "tools", Dagnats: ">=0.0.35 <0.1.0",
		Buckets: []program.BucketSpec{{Name: "tools_registry", History: 1, MaxBytes: 1 << 20}},
	}
	reader := validManifest()
	reader.Reads = []string{"tools_registry"}
	f := startFixture(t, owner, reader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminJS, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := adminJS.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "tools_registry", History: 1, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		t.Fatalf("owner watch: %v", err)
	}
	defer w.Stop()
	stream, err := adminJS.Stream(ctx, "KV_tools_registry")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var name string
	for ci := range stream.ListConsumers(ctx).Info() {
		name = ci.Name
	}
	if name == "" {
		t.Fatal("owner watcher consumer not found")
	}
	js, err := jetstream.New(f.dialProgram(t, reader.Name))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	short, cancelShort := context.WithTimeout(ctx, time.Second)
	defer cancelShort()
	// Negative: the reader's delete is refused and logged.
	if err := js.DeleteConsumer(short, "KV_tools_registry", name); err == nil {
		t.Error("reader deleted the owner's watcher, want denial")
	}
	if !f.violations.contains("$JS.API.CONSUMER.DELETE.KV_tools_registry." + name) {
		t.Error("no violation logged for the denied consumer delete")
	}
	// Positive: the owner's consumer is still there.
	if got := consumerCount(ctx, t, adminJS, "KV_tools_registry"); got != 1 {
		t.Errorf("consumers after denied delete = %d, want 1", got)
	}
}

func TestPermissions_ReaderGetsWatchesButCannotWrite(t *testing.T) {
	owner := program.Manifest{
		Name: "tools", Dagnats: ">=0.0.35 <0.1.0",
		Buckets: []program.BucketSpec{{Name: "tools_registry", History: 1, MaxBytes: 1 << 20}},
	}
	reader := validManifest()
	reader.Reads = []string{"tools_registry"}
	f := startFixture(t, owner, reader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminJS, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	seed, err := adminJS.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "tools_registry", History: 1, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("create owner bucket: %v", err)
	}
	if _, err := seed.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	js, err := jetstream.New(f.dialProgram(t, reader.Name))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := js.KeyValue(ctx, "tools_registry")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Positive: Get and Watch work.
	if e, err := kv.Get(ctx, "k"); err != nil || string(e.Value()) != "v1" {
		t.Fatalf("Get = %v, %v, want v1", e, err)
	}
	w, err := kv.Watch(ctx, "k")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || string(e.Value()) != "v1" {
			t.Fatalf("watch first = %v, want v1", e)
		}
	case <-ctx.Done():
		t.Fatal("watch: no initial value")
	}
	// Stop's own consumer delete is denied (the grant is withheld), so
	// it returns an error after nats.go's timeout; the server reaps the
	// ephemeral consumer once its interest is gone.
	if err := w.Stop(); err == nil {
		t.Error("watch stop deleted the consumer, want the delete denied")
	}
	if !waitFor(func() bool { return consumerCount(ctx, t, adminJS, "KV_tools_registry") == 0 }) {
		t.Error("stopped watcher's consumer was not reaped")
	}
	// Negative: Put and Delete are refused and the key is untouched.
	short, cancelShort := context.WithTimeout(ctx, time.Second)
	defer cancelShort()
	if _, err := kv.Put(short, "k", []byte("v2")); err == nil {
		t.Error("Put on read-only bucket succeeded, want denial")
	}
	if err := kv.Delete(short, "k"); err == nil {
		t.Error("Delete on read-only bucket succeeded, want denial")
	}
	if e, err := seed.Get(ctx, "k"); err != nil || string(e.Value()) != "v1" {
		t.Fatalf("owner key after denied writes = %v, %v, want v1", e, err)
	}
	if !f.violations.contains("$KV.tools_registry.k") {
		t.Error("no violation logged for the denied write")
	}
}
