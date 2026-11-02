// worker/parallel_setup_test.go
// Tests for Start setting up its task types' consumers concurrently. A
// program with many task types reaches a stream leader across a slow
// link, where every JetStream API call is a round trip; serial setup
// cost each type several of them, about 30 s for 16 types on the mesh.
// Methodology: a TCP proxy between the worker and a real embedded
// server delays every server-to-client write, so each round trip costs
// at least proxyDelay. The same 16-type setup runs with one consumer
// set up at a time (the serial baseline, measured in the same test) and
// with the default bound, and the ratio is asserted, not an absolute
// time, so a loaded runner slows both alike. The consumers created and
// a failing type's error are checked too.
package worker

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	proxyDelay     = 50 * time.Millisecond
	setupTypeCount = 16
)

// startDelayProxy forwards a local port to target, delaying each write
// from the server by proxyDelay. Returns the proxy's nats:// URL.
func startDelayProxy(t *testing.T, target string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			go func() { io.Copy(server, client); server.Close() }() //nolint:errcheck // proxy
			go func() { delayedCopy(client, server); client.Close() }()
		}
	}()
	return "nats://" + listener.Addr().String()
}

func delayedCopy(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(proxyDelay)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func setupTypes() []string {
	types := make([]string, setupTypeCount)
	for i := range types {
		types[i] = fmt.Sprintf("setup.t%02d", i)
	}
	return types
}

// timedStart starts a worker over url with types and the given
// in-flight bound, and returns how long Start took.
func timedStart(
	t *testing.T, url string, inFlight int, types []string,
) (*Worker, time.Duration) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	w := NewWorker(nc)
	w.setupInFlight = inFlight
	for _, taskType := range types {
		w.Handle(taskType, func(TaskContext) error { return nil })
	}
	began := time.Now()
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	took := time.Since(began)
	t.Cleanup(w.Stop)
	return w, took
}

// medianStart is the median of three timedStarts, each worker stopped
// after it is timed.
func medianStart(t *testing.T, url string, inFlight int, types []string) time.Duration {
	t.Helper()
	var took []time.Duration
	for range 3 {
		w, d := timedStart(t, url, inFlight, types)
		w.Stop()
		took = append(took, d)
	}
	slices.Sort(took)
	return took[1]
}

func consumerNames(t *testing.T, js jetstream.JetStream, stream string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	var names []string
	lister := s.ListConsumers(ctx)
	for info := range lister.Info() {
		names = append(names, info.Name+" "+info.Config.FilterSubject)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list %s: %v", stream, err)
	}
	slices.Sort(names)
	return names
}

func TestStart_SetsUpTaskTypesConcurrently(t *testing.T) {
	ns, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	if err := natsutil.SetupStickyStream(mustJS(t, nc), 1); err != nil {
		t.Fatalf("SetupStickyStream: %v", err)
	}
	url := startDelayProxy(t, ns.Addr().String())
	// Start's cost that does not grow with task types (KV binds, the
	// directory, the shared listings), taken from a one-type worker and
	// left out of both sides of the comparison. Fixed and parallel are
	// medians of three, so one slow sample on a loaded runner cannot
	// decide the ratio.
	fixed := medianStart(t, url, 1, setupTypes()[:1])
	serialWorker, serial := timedStart(t, url, 1, setupTypes())
	serialWorker.Stop()
	parallel := medianStart(t, url, setupInFlightMax, setupTypes())
	parallelWorker, _ := timedStart(t, url, setupInFlightMax, setupTypes())
	t.Logf("fixed %s, serial %s, parallel %s", fixed, serial, parallel)
	// Positive: the per-type part is well under its serial cost (about a
	// fifth on an idle machine; a half leaves room for a loaded one).
	if (parallel-fixed)*2 > serial-fixed {
		t.Fatalf("parallel setup took %s, serial %s, fixed %s: want the per-type "+
			"part under half", parallel, serial, fixed)
	}
	js := mustJS(t, nc)
	var wantQueue []string
	for _, taskType := range setupTypes() {
		wantQueue = append(wantQueue,
			consumerNameFor(taskType, "")+" "+consumerFilterFor(taskType, ""))
	}
	slices.Sort(wantQueue)
	// Negative: concurrency changed nothing about what was created.
	if got := consumerNames(t, js, "TASK_QUEUES"); !slices.Equal(got, wantQueue) {
		t.Fatalf("TASK_QUEUES consumers = %q, want %q", got, wantQueue)
	}
	sticky := 0
	for _, name := range consumerNames(t, js, "STICKY_TASKS") {
		if strings.Contains(name, "."+parallelWorker.workerID+".") {
			sticky++
		}
	}
	if sticky != setupTypeCount {
		t.Fatalf("sticky consumers for the worker = %d, want %d", sticky, setupTypeCount)
	}
}

// TestStart_ConcurrentSetupReportsAFailingType: types whose consumers
// cannot be updated fail Start. The first in task-type order is named
// and leads however the setups finished; the second is named after it
// if it was started before the first failed (no type starts after a
// failure); no other type is blamed.
func TestStart_ConcurrentSetupReportsAFailingType(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js := mustJS(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	types := setupTypes()
	bad := []string{types[5], types[11]}
	for _, taskType := range bad {
		// ReplayPolicy cannot change on update, so the worker's create fails.
		if _, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
			Durable: consumerNameFor(taskType, ""), FilterSubject: consumerFilterFor(taskType, ""),
			AckPolicy: jetstream.AckExplicitPolicy, ReplayPolicy: jetstream.ReplayOriginalPolicy,
		}); err != nil {
			t.Fatalf("seed consumer: %v", err)
		}
	}
	w := NewWorker(nc)
	for _, taskType := range types {
		w.Handle(taskType, func(TaskContext) error { return nil })
	}
	err := w.Start()
	if err == nil {
		t.Fatal("Start succeeded, want the seeded types to fail it")
	}
	first := strings.Index(err.Error(), consumerNameFor(bad[0], "")+" ")
	second := strings.Index(err.Error(), consumerNameFor(bad[1], "")+" ")
	// Positive: the earlier type is reported, and leads.
	if first < 0 || (second >= 0 && second < first) {
		t.Fatalf("Start = %v, want %s, leading %s if that ran", err, bad[0], bad[1])
	}
	for _, taskType := range types {
		// Negative: no other type is blamed.
		if !slices.Contains(bad, taskType) &&
			strings.Contains(err.Error(), consumerNameFor(taskType, "")+" ") {
			t.Fatalf("Start blamed %s too: %v", taskType, err)
		}
	}
}

func mustJS(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// TestStart_StopsStartingTypesAfterAFailure: with one setup at a time,
// a failing first type leaves every later type unstarted, as serial
// setup did; no durable is created for them.
func TestStart_StopsStartingTypesAfterAFailure(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js := mustJS(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	types := setupTypes()
	if _, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
		Durable: consumerNameFor(types[0], ""), FilterSubject: consumerFilterFor(types[0], ""),
		AckPolicy: jetstream.AckExplicitPolicy, ReplayPolicy: jetstream.ReplayOriginalPolicy,
	}); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
	w := NewWorker(nc)
	w.setupInFlight = 1
	for _, taskType := range types {
		w.Handle(taskType, func(TaskContext) error { return nil })
	}
	// Positive: the first type's failure fails Start.
	if err := w.Start(); err == nil {
		t.Fatal("Start succeeded, want the seeded type to fail it")
	}
	want := []string{consumerNameFor(types[0], "") + " " + consumerFilterFor(types[0], "")}
	// Negative: no later type's durable was created.
	if got := consumerNames(t, js, "TASK_QUEUES"); !slices.Equal(got, want) {
		t.Fatalf("TASK_QUEUES consumers = %q, want only the seeded %q", got, want)
	}
}

// TestSubscribeTasks_PanicReachesTheCaller: an assertion that fires in a
// setup goroutine panics on the caller's goroutine, after the setups
// started have finished, instead of ending the process. The healthy type
// comes first, so it always starts.
func TestSubscribeTasks_PanicReachesTheCaller(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)
	t.Cleanup(w.Stop)
	w.Handle("setup.ok", func(TaskContext) error { return nil })
	w.handlers["setup.broken"] = nil // setupTask asserts a handler
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		w.subscribeTasks([]string{"setup.ok", "setup.broken"}) //nolint:errcheck // panics
	}()
	// Positive: the setup's own assertion surfaced here.
	if msg, ok := recovered.(string); !ok || !strings.Contains(msg, "handler must not be nil") {
		t.Fatalf("recovered %v, want setupTask's handler assertion", recovered)
	}
	// Negative: not before the healthy setup ended, which left its lanes.
	w.setupMu.Lock()
	defer w.setupMu.Unlock()
	if len(w.lanes) == 0 {
		t.Fatal("re-panicked before the healthy type's setup finished")
	}
}
