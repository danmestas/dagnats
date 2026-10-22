// Methodology: resume of the journal SSE follow across the loss of a
// node (#808). Builds the real binary and forms the same three-process
// cluster as multiprocess_test.go (opt-in: DAGNATS_E2E_CLUSTER=1). A run
// with one parked step gets journal records written through the cluster;
// a client follows GET /runs/{run}/steps/a/journal on node 2, receives
// the records so far, then node 2 is SIGKILLed. Records are appended
// while the client is disconnected, and the client reconnects to node 0
// with Last-Event-ID set to the last id it saw. It must receive exactly
// the records after that id, plus one appended after it reconnected:
// the union of both connections equals the journal read back from node
// 0, with no gap and no duplicate. Every wait is bounded.
package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/worker"
)

const (
	journalReadTimeout   = 30 * time.Second
	journalAppendTimeout = 60 * time.Second
	openWait             = 8 * time.Second
	reconnectsMax        = 5
)

// journalFollow is a client following one step's journal on one node.
// Like a browser EventSource it reconnects, with Last-Event-ID set to
// the last id it saw, when the stream ends before the caller is done:
// the server closes with event: error on a transient failure (a
// consumer that could not be created mid-election).
type journalFollow struct {
	addr, runID string
	lastID      string
	seqs        []uint64
	scan        *bufio.Scanner
}

func openJournalFollow(t *testing.T, addr, runID, lastEventID string) *journalFollow {
	t.Helper()
	f := &journalFollow{addr: addr, runID: runID, lastID: lastEventID}
	f.connect(t)
	return f
}

// connect (re)opens the stream, retrying until it answers 200.
func (f *journalFollow) connect(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*journalReadTimeout)
	t.Cleanup(cancel)
	url := fmt.Sprintf("http://%s/runs/%s/steps/a/journal?follow=1", f.addr, f.runID)
	// Headers arrive before any record, so a connection that has not
	// answered within openWait is stuck and is retried.
	client := &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: openWait}}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if f.lastID != "" {
			req.Header.Set("Last-Event-ID", f.lastID)
		}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			t.Cleanup(func() { resp.Body.Close() })
			f.scan = bufio.NewScanner(resp.Body)
			return
		}
		if err != nil {
			t.Logf("follow %s: %v", f.addr, err)
		} else {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			t.Logf("follow %s: status %d: %s", f.addr, resp.StatusCode, body)
		}
		if ctx.Err() != nil {
			t.Fatalf("follow %s did not open: %v", f.addr, ctx.Err())
		}
		time.Sleep(time.Second)
	}
}

// readUntil returns every record sequence received so far once stop
// holds for the latest, reconnecting (bounded) when the stream ends.
func (f *journalFollow) readUntil(t *testing.T, stop func(seq uint64) bool) []uint64 {
	t.Helper()
	for reconnects := 0; reconnects <= reconnectsMax; reconnects++ {
		for f.scan.Scan() {
			line := f.scan.Text()
			if strings.HasPrefix(line, "event: error") {
				t.Logf("server ended the stream with an error; reconnecting")
			}
			if !strings.HasPrefix(line, "id: ") {
				continue
			}
			f.lastID = strings.TrimPrefix(line, "id: ")
			seq, err := strconv.ParseUint(f.lastID, 10, 64)
			if err != nil {
				t.Fatalf("bad id line %q: %v", line, err)
			}
			f.seqs = append(f.seqs, seq)
			if stop(seq) {
				return f.seqs
			}
		}
		f.connect(t)
	}
	t.Fatalf("stream kept ending after %v", f.seqs)
	return nil
}

// appendRetry appends an entry, retrying through the re-election that
// follows a node loss; the stable Nats-Msg-Id makes a retry safe.
func appendRetry(t *testing.T, jrn *journal.Journal, body string) uint64 {
	t.Helper()
	deadline := time.Now().Add(journalAppendTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		seq, err := jrn.Append(ctx, journal.TypeEntry, []byte(`"`+body+`"`))
		cancel()
		if err == nil {
			return seq
		}
		if time.Now().After(deadline) {
			t.Fatalf("append %s: %v", body, err)
		}
		time.Sleep(time.Second)
	}
}

// fetchJournalSeqs reads the journal page from addr, retrying through
// the transient 500s a cluster answers right after a node loss.
func fetchJournalSeqs(t *testing.T, addr, runID string) []uint64 {
	t.Helper()
	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf("http://%s/runs/%s/steps/a/journal", addr, runID)
	deadline := time.Now().Add(journalReadTimeout)
	for {
		seqs, err := fetchJournalPage(client, url)
		if err == nil {
			return seqs
		}
		if time.Now().After(deadline) {
			t.Fatalf("page %s: %v", url, err)
		}
		time.Sleep(time.Second)
	}
}

func fetchJournalPage(client *http.Client, url string) ([]uint64, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var page struct {
		Records []struct {
			Seq uint64 `json:"seq"`
		} `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, err
	}
	seqs := make([]uint64, 0, len(page.Records))
	for _, r := range page.Records {
		seqs = append(seqs, r.Seq)
	}
	return seqs, nil
}

func TestMultiProcessCluster_JournalFollowResumesAcrossNodeLoss(t *testing.T) {
	if os.Getenv("DAGNATS_E2E_CLUSTER") != "1" {
		t.Skip("set DAGNATS_E2E_CLUSTER=1 to run the 3-process cluster test")
	}
	if testing.Short() {
		t.Skip("multi-process cluster test skipped in -short mode")
	}
	if dl, ok := t.Deadline(); ok && time.Until(dl) < totalBudget {
		t.Fatalf("go test -timeout leaves %s; this test needs %s "+
			"(use -timeout 600s)", time.Until(dl).Round(time.Second), totalBudget)
	}
	nodes := formCluster(t, buildBinary(t))
	urls := ""
	for _, n := range nodes {
		urls += fmt.Sprintf("nats://127.0.0.1:%d,", n.natsPort)
	}
	nc, err := nats.Connect(urls, nats.MaxReconnects(-1),
		nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	release := make(chan struct{})
	harness.SubscribeWorker(t, nc, "journal-park-task",
		func(tc worker.TaskContext) error {
			select {
			case <-release:
			case <-time.After(5 * time.Minute):
			}
			return tc.Complete([]byte(`"ok"`))
		})
	t.Cleanup(func() { close(release) })

	svc := harness.NewTestService(t, nc)
	wb := dag.NewWorkflow(harness.UniqueName(t, "journal-resume"))
	wb.Task("a", "journal-park-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runID := harness.RegisterAndStart(t, svc, def, nil)
	harness.WaitForPrecondition(t, "step a known", 30*time.Second, func() bool {
		run, err := svc.GetRun(context.Background(), runID)
		if err != nil {
			return false
		}
		_, ok := run.Steps["a"]
		return ok
	})

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	jrn, err := journal.Open(context.Background(), js,
		natsutil.NewTracingPublisher(nc, js), journal.Task{
			RunID: runID, StepID: "a", Attempt: 1, Delivered: 1,
		})
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	var lastBefore uint64
	for _, body := range []string{"one", "two", "three"} {
		lastBefore = appendRetry(t, jrn, body)
	}

	// Claim + 3 entries are followed through node 2, which is then lost.
	first := openJournalFollow(t, nodes[2].httpAddr, runID, "")
	before := first.readUntil(t, func(seq uint64) bool { return seq >= lastBefore })
	killNode(nodes[2])

	// Written while the client has no connection.
	var lastMissed uint64
	for _, body := range []string{"four", "five"} {
		lastMissed = appendRetry(t, jrn, body)
	}

	second := openJournalFollow(t, nodes[0].httpAddr, runID, strconv.FormatUint(lastBefore, 10))
	lastLive := appendRetry(t, jrn, "six")
	after := second.readUntil(t, func(seq uint64) bool { return seq >= lastLive })

	want := fetchJournalSeqs(t, nodes[0].httpAddr, runID)
	have := append(append([]uint64{}, before...), after...)
	if fmt.Sprint(have) != fmt.Sprint(want) {
		t.Fatalf("followed %v (before %v, after %v), journal holds %v",
			have, before, after, want)
	}
	if len(after) == 0 || after[0] <= lastBefore || lastMissed >= lastLive {
		t.Fatalf("resume must start past %d and include the missed records: %v",
			lastBefore, after)
	}
}
