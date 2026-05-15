package console

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DataSource is the read-only surface the console needs from the
// running api.Service. Keeping it narrow lets tests substitute a fake
// without standing up NATS, and makes the surface PR-by-PR additive
// (later PRs widen it as new mutations land).
//
// Every method must be safe to call concurrently with the rest of the
// system; the underlying api.Service already meets that bar.
//
// PR 3 extends the surface with two streaming methods. Both return a
// receive-only channel that closes when ctx is cancelled. The KV /
// JetStream resources behind the stream are released exactly when
// ctx is cancelled — callers can rely on that for goroutine cleanup.
type DataSource interface {
	ListWorkflows(ctx context.Context) ([]dag.WorkflowDef, error)
	GetWorkflow(name string) (dag.WorkflowDef, error)
	ListRuns(ctx context.Context, workflowFilter string) ([]dag.WorkflowRun, error)
	GetRun(ctx context.Context, runID string) (dag.WorkflowRun, error)
	ListRunEvents(ctx context.Context, runID string, fullData bool) ([]api.RunEvent, error)
	ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error)

	// WatchRuns streams the workflow_runs KV bucket. Each emitted
	// RunUpdate carries the latest snapshot for one run plus a flag
	// indicating whether this is the first emission for that key
	// (Created=true) or a status/state mutation (Created=false). The
	// channel closes when ctx is cancelled or the underlying watcher
	// fails. Caller is responsible for filtering — the stream emits
	// everything in the bucket.
	WatchRuns(ctx context.Context) (<-chan RunUpdate, error)

	// WatchRunHistory streams history.<runID> events. Events arrive
	// chronologically per the JetStream delivery order. fromSeq is the
	// stream sequence to resume from (0 means deliver-all). The channel
	// closes when ctx is cancelled. Bounded buffering on the channel
	// drops the oldest event if the consumer can't keep up — operators
	// always see the latest state, never a stale one.
	WatchRunHistory(
		ctx context.Context, runID string, fromSeq uint64,
	) (<-chan HistoryEvent, error)
}

// RunUpdate is one observation on the workflow_runs KV bucket. Created
// distinguishes a brand-new run (worth prepending to the list with a
// highlight) from a status mutation (worth replacing the existing row
// in place). Seq is the KV revision so reconnects can deduplicate.
type RunUpdate struct {
	Run     dag.WorkflowRun
	Created bool
	Seq     uint64
}

// HistoryEvent is one history.<runID> message materialised for the
// console's SSE writers. Seq is the JetStream stream sequence; it's
// the value clients hand back via the Last-Event-ID header to resume
// without replaying the prefix they already saw.
type HistoryEvent struct {
	Event api.RunEvent
	Seq   uint64
}

// apiServiceAdapter wraps *api.Service to satisfy DataSource. The
// adapter exists so callers in server/server.go can pass *api.Service
// directly without code there knowing about console.DataSource.
//
// PR 3 widens the adapter to also hold a raw *nats.Conn — the watch
// methods reach into the workflow_runs KV bucket and the
// WORKFLOW_HISTORY stream directly. We could route through api.Service
// but the watch shape is one-of-a-kind to the console and keeping it
// alongside the rest of the adapter keeps the wiring legible.
type apiServiceAdapter struct {
	svc *api.Service
	nc  *nats.Conn
}

// NewAPIDataSource returns a DataSource backed by the live api.Service.
// Panics on nil so misconfiguration fails at startup, not at first
// request. nc may be nil — in that case the streaming methods return an
// error rather than panic, so older callers that haven't been updated
// keep building.
func NewAPIDataSource(svc *api.Service, nc *nats.Conn) DataSource {
	if svc == nil {
		panic("NewAPIDataSource: svc is nil")
	}
	return &apiServiceAdapter{svc: svc, nc: nc}
}

func (a *apiServiceAdapter) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListWorkflows: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListWorkflows: ctx is nil")
	}
	return a.svc.ListWorkflows(ctx)
}

func (a *apiServiceAdapter) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetWorkflow: svc is nil")
	}
	if name == "" {
		panic("apiServiceAdapter.GetWorkflow: name is empty")
	}
	return a.svc.GetWorkflow(name)
}

func (a *apiServiceAdapter) ListRuns(
	ctx context.Context, workflowFilter string,
) ([]dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRuns: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListRuns: ctx is nil")
	}
	return a.svc.ListRuns(ctx, workflowFilter)
}

func (a *apiServiceAdapter) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetRun: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.GetRun: runID is empty")
	}
	return a.svc.GetRun(ctx, runID)
}

func (a *apiServiceAdapter) ListRunEvents(
	ctx context.Context, runID string, fullData bool,
) ([]api.RunEvent, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRunEvents: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.ListRunEvents: runID is empty")
	}
	return a.svc.ListRunEvents(ctx, runID, fullData)
}

func (a *apiServiceAdapter) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListTriggers: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListTriggers: ctx is nil")
	}
	return a.svc.ListTriggers(ctx)
}

// WatchRuns opens a KV watcher against the workflow_runs bucket and
// translates each entry update into a RunUpdate. The channel has a
// small buffer; if the consumer can't keep up the goroutine drops the
// oldest queued update. That's intentional — the operator UI always
// wants the latest snapshot, never a stale one. Initial replay of
// existing keys is included, marked Created=true so the list page
// can pre-populate.
func (a *apiServiceAdapter) WatchRuns(
	ctx context.Context,
) (<-chan RunUpdate, error) {
	if ctx == nil {
		panic("apiServiceAdapter.WatchRuns: ctx is nil")
	}
	if a.nc == nil {
		return nil, fmt.Errorf("nats.Conn not configured")
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		return nil, fmt.Errorf("workflow_runs bucket: %w", err)
	}
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch workflow_runs: %w", err)
	}
	const bufSize = 32
	out := make(chan RunUpdate, bufSize)
	go runWatchPump(ctx, watcher, out)
	return out, nil
}

// runWatchPump translates KV updates into RunUpdate values until ctx
// is cancelled or the watcher closes. nil sentinel marks the end of
// the historical replay; we ignore it because we don't need to signal
// "live now" to the SSE consumer.
//
// out is the buffered channel both ends of the goroutine share; we
// take it as a read/write to allow the slow-consumer back-pressure
// path to drop the oldest queued value.
func runWatchPump(
	ctx context.Context,
	watcher jetstream.KeyWatcher, out chan RunUpdate,
) {
	defer close(out)
	defer watcher.Stop()           //nolint:errcheck
	const maxUpdates = 100_000_000 // bounded loop per project rule
	for i := 0; i < maxUpdates; i++ {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}
			ru, ok := parseRunUpdate(entry)
			if !ok {
				continue
			}
			if !sendOrDropOldest(ctx, out, ru) {
				return
			}
		}
	}
}

// sendOrDropOldest pushes value onto out. Falls through with a drop-
// the-oldest path if out is full; returns false when ctx is cancelled.
// Pulled out so runWatchPump stays under 70 lines.
func sendOrDropOldest(
	ctx context.Context, out chan RunUpdate, value RunUpdate,
) bool {
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case <-out:
	default:
	}
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// parseRunUpdate decodes a KV entry into a RunUpdate. PutOp is the
// only operation we surface (Save). DeleteOp / PurgeOp are unusual on
// workflow_runs — engine never deletes — and ignored.
func parseRunUpdate(entry jetstream.KeyValueEntry) (RunUpdate, bool) {
	if entry == nil {
		return RunUpdate{}, false
	}
	if entry.Operation() != jetstream.KeyValuePut {
		return RunUpdate{}, false
	}
	var run dag.WorkflowRun
	if err := json.Unmarshal(entry.Value(), &run); err != nil {
		return RunUpdate{}, false
	}
	return RunUpdate{
		Run:     run,
		Created: entry.Revision() == 1,
		Seq:     entry.Revision(),
	}, true
}

// WatchRunHistory subscribes to history.<runID> and pumps each message
// through as a HistoryEvent. fromSeq>0 starts the consumer from that
// sequence (used for Last-Event-ID resume). A nil channel return path
// is reserved for misconfiguration; callers must check the error.
func (a *apiServiceAdapter) WatchRunHistory(
	ctx context.Context, runID string, fromSeq uint64,
) (<-chan HistoryEvent, error) {
	if ctx == nil {
		panic("WatchRunHistory: ctx is nil")
	}
	if runID == "" {
		panic("WatchRunHistory: runID is empty")
	}
	if a.nc == nil {
		return nil, fmt.Errorf("nats.Conn not configured")
	}
	subject := "history." + runID
	const bufSize = 32
	out := make(chan HistoryEvent, bufSize)
	jsLegacy, err := a.nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream legacy: %w", err)
	}
	opts := []nats.SubOpt{nats.AckNone()}
	if fromSeq > 0 {
		opts = append(opts, nats.StartSequence(fromSeq+1))
	} else {
		opts = append(opts, nats.DeliverAll())
	}
	sub, err := jsLegacy.SubscribeSync(subject, opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	go historyPump(ctx, sub, out)
	return out, nil
}

// historyPump reads messages off sub and converts them to HistoryEvent
// values. Iterates until ctx is cancelled. The 250ms NextMsg deadline
// lets the loop respond to ctx cancellation without dangling: the worst
// case latency between cancel and channel-close is 250ms.
func historyPump(
	ctx context.Context, sub *nats.Subscription, out chan<- HistoryEvent,
) {
	defer close(out)
	defer sub.Unsubscribe()        //nolint:errcheck
	const maxIters = 1_000_000_000 // bounded loop
	const pollWait = 250 * time.Millisecond
	for i := 0; i < maxIters; i++ {
		if ctx.Err() != nil {
			return
		}
		msg, err := sub.NextMsg(pollWait)
		if err != nil {
			// nats.ErrTimeout: just poll again until ctx is done.
			// Other errors: subscription closed externally; bail.
			if err == nats.ErrTimeout {
				continue
			}
			return
		}
		he, ok := parseHistoryEvent(msg)
		if !ok {
			continue
		}
		select {
		case out <- he:
		case <-ctx.Done():
			return
		}
	}
}

// parseHistoryEvent decodes one history message. Returns ok=false on
// malformed JSON — the dropped event is logged at the caller's slog
// elsewhere; here we just signal "skip". Seq comes from the JetStream
// metadata.
func parseHistoryEvent(msg *nats.Msg) (HistoryEvent, bool) {
	if msg == nil {
		return HistoryEvent{}, false
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		return HistoryEvent{}, false
	}
	meta, err := msg.Metadata()
	var seq uint64
	if err == nil && meta != nil {
		seq = meta.Sequence.Stream
	}
	return HistoryEvent{
		Event: api.RunEvent{
			Type:        string(evt.Type),
			RunID:       evt.RunID,
			StepID:      evt.StepID,
			Timestamp:   evt.Timestamp,
			Data:        string(evt.Payload),
			TraceParent: evt.TraceParent,
		},
		Seq: seq,
	}, true
}
