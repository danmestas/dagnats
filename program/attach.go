package program

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// replicasMax matches the largest JetStream cluster dagnats supports.
const replicasMax = 5

// Attach is the one path that calls prog.Start. It takes the Grant the
// caller already obtained (registration, the in-process host, or a
// literal in tests), builds a Host, calls Start, and then starts the
// worker that serves the handlers Start registered through Host.Handle.
// Each dispatch runs under a recover, so a panicking handler fails its
// task permanently instead of killing the process.
//
// nc must have been dialed with nats.CustomInboxPrefix(InboxPrefix(name));
// Attach panics otherwise, because the program's permissions grant no
// other inbox and every reply would time out.
//
// The returned stop func stops the worker (draining in-flight tasks),
// then calls prog.Stop once; later calls return the first result. When
// Start fails, Attach returns its error without calling Stop. When the
// worker fails to start, Attach calls Stop and returns both errors.
func Attach(
	ctx context.Context, nc *nats.Conn, prog Program, g Grant,
) (stop func(context.Context) error, err error) {
	if ctx == nil || nc == nil || prog == nil {
		panic("program.Attach: ctx, nc and prog must not be nil")
	}
	m := prog.Manifest()
	if err := Validate(m); err != nil {
		return nil, err
	}
	if want := InboxPrefix(m.Name); nc.Opts.InboxPrefix != want {
		panic(fmt.Sprintf(
			"program.Attach: %s: connection inbox prefix is %q; dial with "+
				"nats.CustomInboxPrefix(%q)", m.Name, nc.Opts.InboxPrefix, want))
	}
	if err := checkGrant(m, g); err != nil {
		return nil, err
	}
	a := &attachment{
		manifest: m, prog: prog, worker: worker.NewWorker(nc),
		logger: slog.Default().With("program", m.Name),
	}
	host := Host{Conn: nc, Grant: g, Logger: a.logger, Handle: a.handle}
	if err := prog.Start(ctx, host); err != nil {
		a.seal()
		return nil, fmt.Errorf("program %s: start: %w", m.Name, err)
	}
	if a.seal() == 0 {
		return a.stop, nil
	}
	if err := a.startWorker(); err != nil {
		return nil, errors.Join(err, prog.Stop(ctx))
	}
	return a.stop, nil
}

// checkGrant refuses a Grant that does not cover the manifest. A
// mismatch is a dagnats bug, but it is reported, not panicked on,
// because the Grant crosses a process boundary.
func checkGrant(m Manifest, g Grant) error {
	if g.Replicas < 1 || g.Replicas > replicasMax {
		return fmt.Errorf("program %s: grant replicas %d outside [1, %d]",
			m.Name, g.Replicas, replicasMax)
	}
	var total int64
	for _, s := range m.Streams {
		size := g.StreamMaxBytes[s.Name]
		if size <= 0 {
			return fmt.Errorf("program %s: grant does not size stream %s", m.Name, s.Name)
		}
		total += size
	}
	for _, b := range m.Buckets {
		total += b.MaxBytes
	}
	if total > g.StoreBudgetBytes {
		return fmt.Errorf("program %s: grant budget %d bytes is below the %d its stores need",
			m.Name, g.StoreBudgetBytes, total)
	}
	return nil
}

// attachment is one Attach's state: the worker Start registers on, and
// the once that makes stop idempotent.
type attachment struct {
	manifest Manifest
	prog     Program
	worker   *worker.Worker
	logger   *slog.Logger

	mu      sync.Mutex
	sealed  bool
	handled []string

	started  bool
	stopOnce sync.Once
	stopErr  error
}

// handle is Host.Handle.
func (a *attachment) handle(
	taskType string, fn worker.HandlerFunc, opts ...worker.HandlerOption,
) {
	if fn == nil {
		panic("program.Host.Handle: fn must not be nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sealed {
		panic("program.Host.Handle: called after Start returned")
	}
	if !slices.Contains(a.manifest.TaskTypes, taskType) {
		panic(fmt.Sprintf("program.Host.Handle: %s does not declare task type %q",
			a.manifest.Name, taskType))
	}
	if slices.Contains(a.handled, taskType) {
		panic(fmt.Sprintf("program.Host.Handle: task type %q handled twice", taskType))
	}
	a.handled = append(a.handled, taskType)
	a.worker.Handle(taskType, a.recovering(taskType, fn), opts...)
}

// seal closes Handle and reports how many task types were registered.
func (a *attachment) seal() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sealed = true
	if len(a.handled) > len(a.manifest.TaskTypes) {
		panic("program.attachment.seal: more handlers than declared task types")
	}
	return len(a.handled)
}

// recovering turns a handler panic into a permanent task failure. A
// panic is a bug in the program; retrying would repeat it.
func (a *attachment) recovering(taskType string, fn worker.HandlerFunc) worker.HandlerFunc {
	if taskType == "" || fn == nil {
		panic("program.attachment.recovering: taskType and fn are required")
	}
	return func(tc worker.TaskContext) (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			a.logger.Error("task handler panicked", "task_type", taskType,
				"run_id", tc.RunID(), "panic", r, "stack", string(debug.Stack()))
			err = worker.NewNonRetryableError(
				fmt.Errorf("program %s: %s handler panicked: %v", a.manifest.Name, taskType, r))
		}()
		return fn(tc)
	}
}

// startWorker starts consuming. worker.Start panics on a setup failure
// (a denied or timed-out consumer create); here that is an operational
// error the caller should see, so it is returned instead.
func (a *attachment) startWorker() (err error) {
	if a.started {
		panic("program.attachment.startWorker: already started")
	}
	defer func() {
		if r := recover(); r != nil {
			a.worker.Stop()
			err = fmt.Errorf("program %s: start worker: %v", a.manifest.Name, r)
		}
	}()
	a.worker.Start()
	a.started = true
	return nil
}

func (a *attachment) stop(ctx context.Context) error {
	if ctx == nil {
		panic("program: stop: ctx must not be nil")
	}
	a.stopOnce.Do(func() {
		if a.started {
			a.worker.Stop()
		}
		a.stopErr = a.prog.Stop(ctx)
	})
	return a.stopErr
}
