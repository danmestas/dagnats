// internal/api/consumer_release_test.go
// Every journal and log read the API serves opens an ordered consumer
// and must release it when the request ends, including a follow whose
// client simply disconnects: a consumer left behind counts toward the
// server's 1000-per-stream cap.
// Methodology: real embedded NATS + orchestrator + parked worker (the
// journal fixture), driven over httptest. The stream's consumer count
// from stream Info is the observable: pages must leave it at baseline,
// and a follow must show a consumer while connected and none after the
// client closes. Every wait is bounded.
package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

func streamConsumers(t *testing.T, js jetstream.JetStream, name string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("Stream %s: %v", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Info %s: %v", name, err)
	}
	return info.State.Consumers
}

// waitConsumers polls until the stream has want consumers. The bound
// covers the log follow, which notices a gone client only when its
// in-flight fetch ends (one keepalive interval, 15s).
func waitConsumers(t *testing.T, js jetstream.JetStream, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	got := -1
	for time.Now().Before(deadline) {
		if got = streamConsumers(t, js, name); got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s consumers = %d, want %d", name, got, want)
}

func TestJournalPage_ReleasesItsConsumer(t *testing.T) {
	f := newJournalFixture(t, "jrn-rel-page")
	f.append(t, "one")
	baseline := streamConsumers(t, f.svc.js, natsutil.AgentJournalStream)
	for i := 0; i < 20; i++ {
		page, status := getJournalPage(t, f.url("a", ""))
		if status != http.StatusOK || len(page.Records) == 0 {
			t.Fatalf("page status=%d records=%d, want 200 with records", status, len(page.Records))
		}
	}
	waitConsumers(t, f.svc.js, natsutil.AgentJournalStream, baseline)
}

func TestJournalFollow_ReleasesItsConsumerOnDisconnect(t *testing.T) {
	f := newJournalFixture(t, "jrn-rel-follow")
	f.open(t)
	baseline := streamConsumers(t, f.svc.js, natsutil.AgentJournalStream)
	resp, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	nextRecordEvent(t, events)
	waitConsumers(t, f.svc.js, natsutil.AgentJournalStream, baseline+1)
	resp.Body.Close()
	waitConsumers(t, f.svc.js, natsutil.AgentJournalStream, baseline)
}

func TestLogsPageAndFollow_ReleaseTheirConsumer(t *testing.T) {
	release := make(chan struct{})
	svc, server, _, _ := newLogsRestFixture(t, "logs-rel", "logs-rel-task",
		func(tc worker.TaskContext) error {
			<-release
			return tc.Complete(nil)
		}, false)
	t.Cleanup(func() { close(release) })
	runID := startLogsTestRun(t, svc, "logs-rel")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)
	baseline := streamConsumers(t, svc.js, "BUILD_LOGS")
	pageURL := fmt.Sprintf("%s/runs/%s/logs?step=a", server.URL, runID)
	for i := 0; i < 10; i++ {
		resp, err := http.Get(pageURL)
		if err != nil {
			t.Fatalf("GET logs: %v", err)
		}
		resp.Body.Close()
	}
	waitConsumers(t, svc.js, "BUILD_LOGS", baseline)

	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(pageURL + "&follow=1")
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	waitConsumers(t, svc.js, "BUILD_LOGS", baseline+1)
	resp.Body.Close()
	waitConsumers(t, svc.js, "BUILD_LOGS", baseline)
}
