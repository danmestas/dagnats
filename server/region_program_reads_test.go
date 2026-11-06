// region_program_reads_test.go: a program in the us region reading a KV
// bucket that lives in the eu JetStream domain (#949), over the real leaf
// link and allow-list the runbook deploys. Each reader is an NKey user on
// a us node carrying exactly PermissionsFor its manifest, loaded the way
// a registered program's is (userSet), and dials with its confined inbox
// prefix. Positive: a read naming "eu:" gets, watches and stops its
// watcher through jetstream.NewWithDomain with no permission violation.
// Negative: the same bucket read bare grants only "$JS.API.*", so the
// domain-prefixed open is denied. Every wait is bounded.
package server

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/programs"
	"github.com/danmestas/dagnats/program"
)

// violationLog collects a connection's asynchronous errors.
type violationLog struct {
	mu     sync.Mutex
	errors []string
}

func (v *violationLog) record(_ *nats.Conn, _ *nats.Subscription, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.errors = append(v.errors, err.Error())
}

// mentioning returns the recorded errors that contain substring.
func (v *violationLog) mentioning(substring string) []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []string
	for _, e := range v.errors {
		if strings.Contains(e, substring) {
			out = append(out, e)
		}
	}
	return out
}

// dialReader makes name an NKey program user on node with reads, and
// connects as it.
func dialReader(
	t *testing.T, users *userSet, url, name string, reads []string,
) (*nats.Conn, *violationLog) {
	t.Helper()
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), name+".seed"))
	if err != nil {
		t.Fatalf("key for %s: %v", name, err)
	}
	record := programs.Record{
		Manifest: program.Manifest{
			Name: name, TaskTypes: []string{name + ".work"}, Reads: reads,
		},
		PublicKey: key.PublicKey(), RegisteredAt: time.Now(),
	}
	if err := users.put(name, record, false); err != nil {
		t.Fatalf("load program user %s: %v", name, err)
	}
	log := &violationLog{}
	conn := connectOrFail(t, url, key.ConnectOption(),
		nats.CustomInboxPrefix(program.InboxPrefix(name)), nats.ErrorHandler(log.record))
	return conn, log
}

func TestRegion_ProgramReadsBucketInAnotherDomain(t *testing.T) {
	eu, us := startRehearsalPair(t)
	ctx := regionCtx(t)
	putter, err := eu.jetStream(t, 0, "").CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "chat_cards", Replicas: 3,
	})
	if err != nil {
		t.Fatalf("create chat_cards in eu: %v", err)
	}
	putCard(t, putter, "card.before")
	users := newUserSet(us.nodes[0])
	url := us.nodes[0].ClientURL()
	bare, bareLog := dialReader(t, users, url, "legacy", []string{"chat_cards"})
	reader, readerLog := dialReader(t, users, url, "notify", []string{"eu:chat_cards"})

	// Negative: a bare read grants no "$JS.eu.API" subject.
	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := newJetStream(t, bare, "eu").KeyValue(openCtx, "chat_cards"); err == nil {
		t.Fatal("a bare read opened eu chat_cards by domain")
	}
	if denied := bareLog.mentioning("$JS.eu.API.STREAM.INFO.KV_chat_cards"); len(denied) == 0 {
		t.Fatalf("bare reader errors %v, want a violation on the eu stream info", bareLog.errors)
	}

	// Positive: the domain-qualified read gets, watches and stops.
	kv, err := newJetStream(t, reader, "eu").KeyValue(ctx, "chat_cards")
	if err != nil {
		t.Fatalf("open eu chat_cards as a program: %v", err)
	}
	entry, err := kv.Get(ctx, "card.before")
	if err != nil || string(entry.Value()) != "card.before" {
		t.Fatalf("get card.before: %v", err)
	}
	watcher, err := kv.WatchAll(ctx, jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("watch eu chat_cards as a program: %v", err)
	}
	putCard(t, putter, "card.after")
	waitWatched(t, watcher, "card.after", rehearsalWait)
	if err := watcher.Stop(); err != nil {
		t.Fatalf("stop the watcher: %v", err)
	}
	if errs := readerLog.mentioning(""); len(errs) != 0 {
		t.Fatalf("domain reader hit async errors: %v", errs)
	}
}
