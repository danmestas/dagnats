// program/promotion_internal_test.go
// Tests for waitPromoted's timeout diagnosis: a probe the server refused
// is told apart from one it allowed but nobody answered, and each gets
// its own message. Methodology: a real embedded server with one user
// denied the probe subject and one allowed it, with a responder that
// never answers.
package program

import (
	"errors"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestDeniedSinceTellsARefusalFromSilence(t *testing.T) {
	const subject = "api.p.toy.runs.get"
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1,
		Users: []*natsserver.User{
			{Username: "pending", Password: "p", Permissions: &natsserver.Permissions{
				Publish: &natsserver.SubjectPermission{Deny: []string{subject}},
			}},
			{Username: "live", Password: "l"},
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	connect := func(user, password string) *nats.Conn {
		nc, err := nats.Connect(ns.ClientURL(), nats.UserInfo(user, password),
			nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		if err != nil {
			t.Fatalf("connect %s: %v", user, err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	live := connect("live", "l")
	if _, err := live.Subscribe(subject, func(*nats.Msg) {}); err != nil {
		t.Fatalf("silent responder: %v", err)
	}
	if err := live.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	probe := func(nc *nats.Conn) bool {
		before := nc.LastError()
		if _, err := nc.Request(subject, nil, 300*time.Millisecond); err == nil {
			t.Fatal("probe was answered")
		}
		return deniedSince(nc, before, subject)
	}
	pending := connect("pending", "p")
	for attempt := 1; attempt <= 2; attempt++ {
		// Twice: each refusal is a new last error, not the first one again.
		if !probe(pending) {
			t.Fatalf("refused probe %d was not seen as denied", attempt)
		}
	}
	if deniedSince(pending, pending.LastError(), subject) {
		t.Fatal("an earlier refusal was counted for a later probe")
	}
	if probe(live) {
		t.Fatal("an allowed, unanswered probe was seen as denied")
	}
	cause := errors.New("timeout")
	if msg := promotionTimeout("toy", subject, false, cause).Error(); !strings.Contains(msg,
		"engine API is not answering") {
		t.Fatalf("silent-engine message = %q", msg)
	}
	if msg := promotionTimeout("toy", subject, true, cause).Error(); !strings.Contains(msg,
		"permissions did not reach") {
		t.Fatalf("denied message = %q", msg)
	}
}
