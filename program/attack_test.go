// program/attack_test.go
// Escape attempts by a hostile program against its generated
// permissions. Program "alpha" tries to reach program "beta" and the
// engine; every attempt must be refused.
// Methodology: the permission fixture with two programs. Each attack is
// what a malicious program can send with its own credentials. The test
// asserts the attack's effect is absent (seen as admin) and, where the
// server can name it, that the denial reached the violation log.
package program_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func attackManifests() (program.Manifest, program.Manifest) {
	alpha := program.Manifest{
		Name: "alpha", TaskTypes: []string{"alpha.work"},
		Streams: []program.StreamSpec{{
			Name: "ALPHA_EVENTS", Subjects: []string{"alpha.events.>"}, MaxBytesFloor: 1 << 20,
		}},
		Buckets: []program.BucketSpec{{Name: "alpha_state", MaxBytes: 1 << 20}},
	}
	beta := program.Manifest{Name: "beta", TaskTypes: []string{"beta.work"}}
	return alpha, beta
}

// attackFixture starts both programs' users and provisions alpha's
// stores as the host does, over the admin connection.
func attackFixture(t *testing.T) (*fixture, jetstream.JetStream) {
	t.Helper()
	alpha, beta := attackManifests()
	f := startFixture(t, alpha, beta)
	admin, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	grant := program.Grant{Replicas: 1, StoreBudgetBytes: 8 << 20,
		StreamMaxBytes: map[string]int64{"ALPHA_EVENTS": 1 << 20}}
	if err := program.Provision(context.Background(), admin, alpha, grant); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	js, err := jetstream.New(f.dialProgram(t, "alpha"))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return f, js
}

func shortCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

// betaTaskWatch subscribes, as admin, to what an injected beta task
// would look like, both on the wire and in TASK_QUEUES.
func betaTaskWatch(t *testing.T, f *fixture) *nats.Subscription {
	t.Helper()
	sub, err := f.admin.SubscribeSync("task.beta.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := f.admin.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return sub
}

func assertNoBetaTask(t *testing.T, f *fixture, sub *nats.Subscription) {
	t.Helper()
	if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf("alpha injected a beta task: %s %q", msg.Subject, msg.Data)
	}
	js, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(context.Background(), "TASK_QUEUES")
	if err != nil {
		t.Fatalf("TASK_QUEUES: %v", err)
	}
	_, err = stream.GetLastMsgForSubject(context.Background(), "task.beta.>")
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("TASK_QUEUES holds a beta task (err %v)", err)
	}
}

func TestAttack_ReplyReflectionCannotPublishAnywhere(t *testing.T) {
	f, js := attackFixture(t)
	nc := js.Conn()
	watch := betaTaskWatch(t, f)
	mirror, err := nc.SubscribeSync("alpha.reflect")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for _, target := range []string{"$KV.workflow_defs.pwned", "task.beta.work.run9"} {
		if err := nc.PublishMsg(&nats.Msg{Subject: "alpha.reflect", Reply: target}); err != nil {
			t.Fatalf("publish: %v", err)
		}
		msg, err := mirror.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("reflected message: %v", err)
		}
		if err := msg.Respond([]byte("pwned")); err != nil {
			t.Fatalf("respond: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		// Negative: the response to a forged reply subject was refused.
		if !waitFor(func() bool { return f.violations.contains(target) }) {
			t.Errorf("no violation for a response to %q", target)
		}
	}
	defs, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := defs.KeyValue(context.Background(), "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	// Positive: the engine's bucket was not written.
	if _, err := kv.Get(context.Background(), "pwned"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("workflow_defs.pwned exists (err %v)", err)
	}
	assertNoBetaTask(t, f, watch)
}

func TestAttack_ProgramCannotCreateOrReconfigureStreams(t *testing.T) {
	f, js := attackFixture(t)
	attempts := []struct {
		name string
		cfg  jetstream.StreamConfig
	}{
		{"capture beta inbox", jetstream.StreamConfig{
			Name: "ALPHA_SPY", Subjects: []string{"_INBOX.beta.>"}}},
		{"capture admin api", jetstream.StreamConfig{
			Name: "ALPHA_API", Subjects: []string{"api.>"}}},
		{"mirror engine bucket", jetstream.StreamConfig{
			Name: "ALPHA_DEFS", Mirror: &jetstream.StreamSource{Name: "KV_workflow_defs"}}},
		{"republish into beta", jetstream.StreamConfig{
			Name: "ALPHA_FWD", Subjects: []string{"alpha.fwd.>"},
			RePublish: &jetstream.RePublish{Source: ">", Destination: "task.beta.work.run9"}}},
	}
	for _, a := range attempts {
		// Negative: no stream config is accepted from a program.
		if _, err := js.CreateStream(shortCtx(t), a.cfg); err == nil {
			t.Errorf("%s: stream created", a.name)
		}
	}
	update := jetstream.StreamConfig{Name: "ALPHA_EVENTS", Subjects: []string{"api.>"}}
	if _, err := js.UpdateStream(shortCtx(t), update); err == nil {
		t.Error("alpha re-pointed its own stream at api.>")
	}
	bucket := jetstream.KeyValueConfig{Bucket: "alpha_state", History: 64}
	if _, err := js.UpdateKeyValue(shortCtx(t), bucket); err == nil {
		t.Error("alpha reconfigured its own bucket")
	}
	// Positive: the refusals are permission violations, not config errors.
	for _, subj := range []string{
		"$JS.API.STREAM.CREATE.ALPHA_SPY", "$JS.API.STREAM.UPDATE.ALPHA_EVENTS",
		"$JS.API.STREAM.UPDATE.KV_alpha_state",
	} {
		if !waitFor(func() bool { return f.violations.contains(subj) }) {
			t.Errorf("no violation for %q", subj)
		}
	}
}

// TestAttack_PushConsumerCannotDeliverIntoForeignSubjects: a consumer's
// deliver subject travels in the request body, out of reach of subject
// permissions, and nats-server does not check it against the creator's
// permissions. Any consumer-create grant (own streams, shared buckets for
// KV watches) therefore lets a program push stream contents to any
// subject that has interest, here a stand-in for beta's service.
//
// Not closable with user permissions in one account: KV watches need
// push consumers. Accepted for trusted programs; see "Known limitations /
// threat model" in the package doc. The test stays as the proof.
func TestAttack_PushConsumerCannotDeliverIntoForeignSubjects(t *testing.T) {
	t.Skip("accepted gap for trusted programs: see \"Known limitations / threat model\" " +
		"in the program package doc; closed by accounts or a server check before untrusted programs")
	f, js := attackFixture(t)
	victim, err := f.admin.SubscribeSync("beta.svc.x")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := f.admin.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	req := pushConsumerRequest("ALPHA_EVENTS", "push", "alpha.events.>", "beta.svc.x")
	_, err = js.Conn().Request(
		"$JS.API.CONSUMER.CREATE.ALPHA_EVENTS.push.alpha.events.>", req, time.Second)
	if err != nil {
		t.Fatalf("consumer create: %v", err)
	}
	if _, err := js.Publish(shortCtx(t), "alpha.events.x", []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Negative: nothing alpha stored reaches beta's subject.
	if msg, err := victim.NextMsg(time.Second); err == nil {
		t.Fatalf("alpha delivered into beta.svc.x: %q", msg.Data)
	}
	// Positive: the subscription was live, so silence is a refusal.
	if !victim.IsValid() {
		t.Fatal("victim subscription closed")
	}
}

func TestAttack_ConsumerNamesCannotBeEnumerated(t *testing.T) {
	f, js := attackFixture(t)
	nc := js.Conn()
	for _, subj := range []string{
		"$JS.API.CONSUMER.LIST.STICKY_TASKS", "$JS.API.CONSUMER.NAMES.STICKY_TASKS",
		"$JS.API.CONSUMER.LIST.KV_workflow_runs", "$JS.API.CONSUMER.NAMES.KV_workflow_runs",
		"$JS.API.CONSUMER.INFO.KV_workflow_runs.any",
	} {
		// Negative: listing the consumers whose names gate deletes and
		// sticky pulls is refused.
		if _, err := nc.Request(subj, nil, time.Second); err == nil {
			t.Errorf("%s answered", subj)
		}
		// Positive: as a permission violation.
		if !waitFor(func() bool { return f.violations.contains(subj) }) {
			t.Errorf("no violation for %q", subj)
		}
	}
}

// pushConsumerRequest is a raw consumer create with a deliver subject;
// the jetstream package only builds pull consumers.
func pushConsumerRequest(stream, name, filter, deliver string) []byte {
	req, err := json.Marshal(map[string]any{
		"stream_name": stream,
		"config": map[string]any{
			"name": name, "durable_name": name, "filter_subject": filter,
			"deliver_subject": deliver, "deliver_policy": "all", "ack_policy": "none",
		},
	})
	if err != nil {
		panic(err)
	}
	return req
}
