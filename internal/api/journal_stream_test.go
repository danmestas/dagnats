// internal/api/journal_stream_test.go
// Tests for GET /runs/{run}/steps/{step}/journal (#808): paging and
// resuming by journal sequence, SSE follow with Last-Event-ID resume,
// the shared concurrency cap and max-duration bound, the empty-journal
// page, the eof signal, and the run/step gate shared with run logs.
// Methodology: real embedded NATS + orchestrator + a worker whose
// handler parks until the test releases it (so the step stays running),
// with journal records written through the real journal package, all
// driven over httptest.Server. Every wait is bounded.
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// journalFixture is a running step "a" with a journal writer on it.
type journalFixture struct {
	svc     *Service
	nc      *nats.Conn
	baseURL string
	runID   string
	release chan struct{}
	once    sync.Once
	jrn     *journal.Journal
}

func newJournalFixture(t *testing.T, name string) *journalFixture {
	t.Helper()
	release := make(chan struct{})
	svc, server, nc, _ := newLogsRestFixture(t, name, name+"-task",
		func(tc worker.TaskContext) error {
			<-release
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, name)
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	f := &journalFixture{
		svc: svc, nc: nc, baseURL: server.URL, runID: runID, release: release,
	}
	t.Cleanup(f.finish)
	return f
}

// finish lets the parked handler complete; safe to call repeatedly.
func (f *journalFixture) finish() { f.once.Do(func() { close(f.release) }) }

// open starts the step's journal (its claim record is the first
// record); a test that never calls it, directly or through append,
// exercises a step with no journal at all.
func (f *journalFixture) open(t *testing.T) {
	t.Helper()
	pub := natsutil.NewTracingPublisher(f.nc, f.svc.js)
	jrn, err := journal.Open(context.Background(), f.svc.js, pub, journal.Task{
		RunID: f.runID, StepID: "a", Iteration: 0, Attempt: 1, Delivered: 1,
	})
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	f.jrn = jrn
}

func (f *journalFixture) append(t *testing.T, body string) uint64 {
	t.Helper()
	if f.jrn == nil {
		f.open(t)
	}
	seq, err := f.jrn.Append(context.Background(), journal.TypeEntry, []byte(`"`+body+`"`))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return seq
}

func (f *journalFixture) url(step, query string) string {
	return fmt.Sprintf("%s/runs/%s/steps/%s/journal%s", f.baseURL, f.runID, step, query)
}

func getJournalPage(t *testing.T, url string) (journalResponse, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET journal: %v", err)
	}
	defer resp.Body.Close()
	var out journalResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode journal page: %v", err)
		}
	}
	return out, resp.StatusCode
}

// sseEvent is one parsed SSE frame.
type sseEvent struct{ id, event, data string }

// readSSE parses frames off body into a channel, closed at EOF.
func readSSE(body *bufio.Scanner) <-chan sseEvent {
	out := make(chan sseEvent, 64)
	go func() {
		defer close(out)
		var cur sseEvent
		for body.Scan() {
			line := body.Text()
			switch {
			case strings.HasPrefix(line, "id: "):
				cur.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.data = strings.TrimPrefix(line, "data: ")
			case line == "" && cur.event != "":
				out <- cur
				cur = sseEvent{}
			}
		}
	}()
	return out
}

func openJournalFollow(
	t *testing.T, url, lastEventID string,
) (*http.Response, <-chan sseEvent) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp, readSSE(bufio.NewScanner(resp.Body))
}

func nextRecordEvent(t *testing.T, ch <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("stream ended before the expected record")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a record event")
	}
	return sseEvent{}
}

func TestJournalPage_PagesFromSequence(t *testing.T) {
	f := newJournalFixture(t, "jrn-page")
	f.append(t, "one")
	second := f.append(t, "two")
	third := f.append(t, "three")

	all, status := getJournalPage(t, f.url("a", ""))
	if status != http.StatusOK || len(all.Records) != 4 {
		t.Fatalf("status=%d records=%d, want 200 and 4 (claim + 3)", status, len(all.Records))
	}
	if all.NextFrom != third+1 || all.EOF {
		t.Fatalf("next_from=%d eof=%v, want %d and false (step running)",
			all.NextFrom, all.EOF, third+1)
	}
	from, _ := getJournalPage(t, f.url("a", fmt.Sprintf("?from=%d", second)))
	if len(from.Records) != 2 || from.Records[0].Seq != second {
		t.Fatalf("from page = %+v, want records starting at seq %d", from.Records, second)
	}
	past, _ := getJournalPage(t, f.url("a", fmt.Sprintf("?from=%d", third+1)))
	if len(past.Records) != 0 || past.NextFrom != third+1 {
		t.Fatalf("past-the-end page = %+v, want empty at %d", past, third+1)
	}
}

func TestJournalPage_StepWithoutJournalIsEmptyNotError(t *testing.T) {
	f := newJournalFixture(t, "jrn-empty")
	page, status := getJournalPage(t, f.url("a", ""))
	if status != http.StatusOK || page.Records == nil || len(page.Records) != 0 {
		t.Fatalf("status=%d records=%v, want 200 and an empty non-nil array",
			status, page.Records)
	}
	if page.NextFrom != 0 || page.EOF {
		t.Fatalf("next_from=%d eof=%v, want 0 and false", page.NextFrom, page.EOF)
	}
}

func TestJournalPage_UnknownRunAndStepAre404(t *testing.T) {
	f := newJournalFixture(t, "jrn-404")
	if _, status := getJournalPage(t, f.url("nope", "")); status != http.StatusNotFound {
		t.Fatalf("unknown step status = %d, want 404", status)
	}
	url := fmt.Sprintf("%s/runs/%s/steps/a/journal", f.baseURL, "no-such-run")
	if _, status := getJournalPage(t, url); status != http.StatusNotFound {
		t.Fatalf("unknown run status = %d, want 404", status)
	}
	resp, err := http.Post(f.url("a", ""), "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", resp.StatusCode)
	}
}

func TestJournalFollow_DeliversRecordsAppendedAfterConnect(t *testing.T) {
	f := newJournalFixture(t, "jrn-follow")
	f.open(t)
	_, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	claim := nextRecordEvent(t, events)
	if claim.event != "record" || claim.id == "" {
		t.Fatalf("first event = %+v, want a record with an id", claim)
	}
	seq := f.append(t, "late")
	late := nextRecordEvent(t, events)
	var rec journal.Record
	if err := json.Unmarshal([]byte(late.data), &rec); err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if rec.Seq != seq || late.id != fmt.Sprint(seq) || rec.Type != journal.TypeEntry {
		t.Fatalf("late record = %+v id=%s, want entry at seq %d", rec, late.id, seq)
	}
}

func TestJournalFollow_ResumesFromLastEventID(t *testing.T) {
	f := newJournalFixture(t, "jrn-resume")
	f.append(t, "one")
	resp, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	nextRecordEvent(t, events) // claim
	one := nextRecordEvent(t, events)
	resp.Body.Close()

	two := f.append(t, "two")
	f.append(t, "three")
	_, resumed := openJournalFollow(t, f.url("a", "?follow=1"), one.id)
	got := nextRecordEvent(t, resumed)
	if got.id != fmt.Sprint(two) {
		t.Fatalf("resumed at id %s, want %d (the record after %s)", got.id, two, one.id)
	}
	if next := nextRecordEvent(t, resumed); next.id == one.id || next.id == got.id {
		t.Fatalf("duplicate record after resume: %+v", next)
	}
}

func TestJournalFollow_FromParamStartsAtSequence(t *testing.T) {
	f := newJournalFixture(t, "jrn-fromparam")
	f.append(t, "one")
	two := f.append(t, "two")
	_, events := openJournalFollow(t, f.url("a", fmt.Sprintf("?follow=1&from=%d", two)), "")
	if got := nextRecordEvent(t, events); got.id != fmt.Sprint(two) {
		t.Fatalf("first id = %s, want %d (from is inclusive)", got.id, two)
	}
}

func TestJournalFollow_ConcurrencyCapReturns503(t *testing.T) {
	f := newJournalFixture(t, "jrn-cap")
	f.open(t)
	orig := journalFollowConcurrentMax
	journalFollowConcurrentMax = 1
	t.Cleanup(func() { journalFollowConcurrentMax = orig })

	openJournalFollow(t, f.url("a", "?follow=1"), "")
	resp, err := http.Get(f.url("a", "?follow=1"))
	if err != nil {
		t.Fatalf("second follow: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second follow status = %d, want 503", resp.StatusCode)
	}
	// The log lane has its own counter: a full journal cap leaves it open.
	logCtx, stopLogs := context.WithCancel(context.Background())
	defer stopLogs()
	logReq, err := http.NewRequestWithContext(logCtx, "GET",
		fmt.Sprintf("%s/runs/%s/logs?step=a&follow=1", f.baseURL, f.runID), nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	logsResp, err := http.DefaultClient.Do(logReq)
	if err != nil {
		t.Fatalf("log follow: %v", err)
	}
	defer logsResp.Body.Close()
	if logsResp.StatusCode != http.StatusOK {
		t.Fatalf("log follow status = %d, want 200 (separate cap)", logsResp.StatusCode)
	}
}

func TestJournalFollow_DrainsRecordAppendedAsStepEnds(t *testing.T) {
	f := newJournalFixture(t, "jrn-drain")
	f.open(t)
	_, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	nextRecordEvent(t, events) // claim; the follow is now idle-waiting
	var lastSeq atomic.Uint64
	var once sync.Once
	hook := func() {
		once.Do(func() {
			lastSeq.Store(f.append(t, "final"))
			f.finish()
			waitForStepTerminal(t, f.svc, f.runID, "a", 5*time.Second)
		})
	}
	journalIdleHook.Store(&hook)
	t.Cleanup(func() { journalIdleHook.Store(nil) })
	rec := nextRecordEvent(t, events)
	if rec.event != "record" || rec.id != fmt.Sprint(lastSeq.Load()) {
		t.Fatalf("got %+v, want the final record %d before eof", rec, lastSeq.Load())
	}
	if eof := nextRecordEvent(t, events); eof.event != "eof" {
		t.Fatalf("after the final record got %+v, want eof", eof)
	}
}

func TestJournal_LastEventIDWithoutSuccessorIs400(t *testing.T) {
	f := newJournalFixture(t, "jrn-maxid")
	for _, query := range []string{"", "?follow=1"} {
		req, err := http.NewRequest("GET", f.url("a", query), nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Last-Event-ID", "18446744073709551615")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, want 400", query, resp.StatusCode)
		}
	}
}

func TestJournalFollow_EndsAtMaxDuration(t *testing.T) {
	f := newJournalFixture(t, "jrn-maxdur")
	f.open(t)
	orig := followDurationMax
	followDurationMax = 400 * time.Millisecond
	t.Cleanup(func() { followDurationMax = orig })

	_, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	nextRecordEvent(t, events) // claim
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("follow outlived the max duration")
		}
	}
}

func TestJournalFollow_EndsWithEOFWhenStepTerminal(t *testing.T) {
	f := newJournalFixture(t, "jrn-eof")
	f.open(t)
	_, events := openJournalFollow(t, f.url("a", "?follow=1"), "")
	nextRecordEvent(t, events) // claim
	f.finish()
	waitForStepTerminal(t, f.svc, f.runID, "a", 5*time.Second)
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed without an eof event")
			}
			if ev.event == "eof" {
				return
			}
		case <-deadline:
			t.Fatal("no eof event after the step completed")
		}
	}
}
