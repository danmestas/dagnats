package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// storeBudgetShrinkHeadroomBytes is the slack added on top of what existing
// streams reserve when #688's auto-shrink temporarily raises the embedded
// server's JetStreamMaxStore to admit them. Matches the convention
// natsutil.testServerMaxStoreHeadroomBytes already uses for the same
// purpose: JetStream's own bookkeeping needs a little more than the exact
// sum of stream MaxBytes, so an exact match still risks err 10047.
const storeBudgetShrinkHeadroomBytes int64 = 64 << 20

// storeShrinkListTimeout bounds the JetStream ListStreams round trips the
// shrink pass makes to snapshot MaxBytes before and after -- single
// request/reply calls against an already-connected server, so seconds are
// generous.
const storeShrinkListTimeout = 5 * time.Second

// startNATSAndSetupAll starts the embedded NATS server for cfg and applies
// setupOpts, auto-recovering when cfg.MaxStoreBytes is lower than what
// existing streams on disk already reserve (#688, following #685/#686).
//
// The ordinary path is unchanged: start once, SetupAll once, done. Only when
// SetupAll fails with *natsutil.StoreBudgetExceededError -- the operator
// lowered max_store_bytes below the established store's reservations -- does
// this restart the NATS process at a temporarily larger limit that admits
// the existing streams, then retry SetupAll at the ORIGINAL configured
// budget so every stream still shrinks to the operator's target ceilings.
// The configured budget takes full effect (as JetStreamMaxStore) on the
// NEXT restart, once nothing is left to shrink.
//
// onReady, when non-nil, is called as soon as each embedded NATS process
// (the initial one, and the recovery restart if one happens) is up and
// accepting connections, before SetupAll runs against it -- so a caller
// logging progress does not go silent for the duration of a setup hang.
func startNATSAndSetupAll(
	cfg Config, setupOpts []natsutil.SetupOption, onReady func(*natsserver.Server),
) (*natsserver.Server, *nats.Conn, error) {
	if cfg.DataDir == "" {
		panic("startNATSAndSetupAll: cfg.DataDir is empty")
	}
	if len(setupOpts) == 0 {
		panic("startNATSAndSetupAll: setupOpts is empty")
	}

	ns, nc, err := startNATSAndConnect(cfg, onReady)
	if err != nil {
		return nil, nil, err
	}

	err = natsutil.SetupAll(nc, setupOpts...)
	if err == nil {
		return ns, nc, nil
	}

	var budgetErr *natsutil.StoreBudgetExceededError
	if !errors.As(err, &budgetErr) {
		nc.Close()
		ns.Shutdown()
		return nil, nil, fmt.Errorf("setup NATS resources: %w", err)
	}

	nc.Close()
	ns.Shutdown()
	ns.WaitForShutdown()
	return recoverFromStoreBudgetExceeded(cfg, setupOpts, budgetErr, onReady)
}

// startNATSAndConnect starts the embedded NATS server and connects a client
// to it, cleaning up the server on a failed connect. onReady, when non-nil,
// fires once the process is accepting connections, before the client
// connect attempt.
func startNATSAndConnect(
	cfg Config, onReady func(*natsserver.Server),
) (*natsserver.Server, *nats.Conn, error) {
	if cfg.DataDir == "" {
		panic("startNATSAndConnect: cfg.DataDir is empty")
	}
	if cfg.MaxStoreBytes <= 0 {
		panic(fmt.Sprintf(
			"startNATSAndConnect: cfg.MaxStoreBytes <= 0: %d", cfg.MaxStoreBytes))
	}

	ns, err := startNATS(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("start NATS: %w", err)
	}
	if onReady != nil {
		onReady(ns)
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return ns, nc, nil
}

// effectiveStoreBudget is the JetStreamMaxStore a #688 recovery restart
// uses: the configured budget when it already covers what is reserved (the
// budgetErr should not have fired, but stay conservative), otherwise enough
// to admit the existing reservation plus headroom.
func effectiveStoreBudget(configured, reserved int64) int64 {
	if configured <= 0 {
		panic("effectiveStoreBudget: configured must be positive")
	}
	if reserved <= 0 {
		panic("effectiveStoreBudget: reserved must be positive")
	}
	effective := reserved + storeBudgetShrinkHeadroomBytes
	if effective < reserved {
		panic(fmt.Sprintf(
			"effectiveStoreBudget: reserved+headroom overflowed int64: "+
				"reserved=%d headroom=%d", reserved, storeBudgetShrinkHeadroomBytes))
	}
	if effective < configured {
		return configured
	}
	return effective
}

// recoverFromStoreBudgetExceeded restarts the NATS process at
// effectiveStoreBudget(cfg.MaxStoreBytes, budgetErr.Reserved) and retries
// SetupAll there, telling it via WithStoreAdmissionCeiling that the process
// now admits more than cfg.MaxStoreBytes while still sizing every stream's
// ceiling from cfg.MaxStoreBytes -- so the shrink pass actually shrinks.
func recoverFromStoreBudgetExceeded(
	cfg Config, setupOpts []natsutil.SetupOption,
	budgetErr *natsutil.StoreBudgetExceededError, onReady func(*natsserver.Server),
) (*natsserver.Server, *nats.Conn, error) {
	if budgetErr == nil {
		panic("recoverFromStoreBudgetExceeded: budgetErr is nil")
	}
	if len(setupOpts) == 0 {
		panic("recoverFromStoreBudgetExceeded: setupOpts is empty")
	}

	effective := effectiveStoreBudget(cfg.MaxStoreBytes, budgetErr.Reserved)
	slog.Warn(
		"max_store_bytes is below what existing streams already reserve; "+
			"starting this boot at a temporarily higher limit so the "+
			"configured budget can shrink them -- shrinking a stream's "+
			"MaxBytes discards its oldest messages to comply, and the "+
			"configured limit takes full effect on the next restart",
		"configured_max_store_bytes", cfg.MaxStoreBytes,
		"reserved_bytes", budgetErr.Reserved,
		"effective_max_store_bytes", effective,
	)

	shrinkCfg := cfg
	shrinkCfg.MaxStoreBytes = effective
	ns, nc, err := startNATSAndConnect(shrinkCfg, onReady)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"restart NATS at the effective store budget %d: %w", effective, err)
	}

	shrinkOpts := append(
		append([]natsutil.SetupOption{}, setupOpts...),
		natsutil.WithStoreAdmissionCeiling(effective),
	)
	resized, err := runShrinkPass(nc, shrinkOpts, cfg.MaxStoreBytes)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, nil, fmt.Errorf("shrink stream ceilings: %w", err)
	}
	slog.Info(
		"shrink pass complete: stream ceilings resized to the configured "+
			"max_store_bytes",
		"max_store_bytes", cfg.MaxStoreBytes,
		"streams_resized", resized,
	)
	return ns, nc, nil
}

// runShrinkPass snapshots every stream's MaxBytes, runs setupOpts through
// natsutil.SetupAll, and reports how many streams came out with a smaller
// MaxBytes than they went in with.
//
// SetupAll only shrinks the streams IT manages. A stream outside that set --
// operator-created, a mirror, an orphan left by a removed feature -- can
// still hold the aggregate reservation over configuredBudget after the
// managed streams shrink, in which case every future boot would silently
// re-enter this recovery path forever at an inflated limit. So after
// SetupAll succeeds, this rechecks the aggregate against configuredBudget
// (via natsutil.CheckStoreBudget, the same preflight SetupAll itself runs)
// and returns that *natsutil.StoreBudgetExceededError, naming the offenders,
// if it still does not fit -- the caller must not report success in that
// case.
func runShrinkPass(
	nc *nats.Conn, setupOpts []natsutil.SetupOption, configuredBudget int64,
) (int, error) {
	if nc == nil {
		panic("runShrinkPass: nc is nil")
	}
	if len(setupOpts) == 0 {
		panic("runShrinkPass: setupOpts is empty")
	}
	if configuredBudget <= 0 {
		panic("runShrinkPass: configuredBudget must be positive")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return 0, fmt.Errorf("jetstream.New: %w", err)
	}

	beforeCtx, beforeCancel := context.WithTimeout(
		context.Background(), storeShrinkListTimeout,
	)
	before, err := snapshotStreamMaxBytes(beforeCtx, js)
	beforeCancel()
	if err != nil {
		return 0, fmt.Errorf("snapshot stream MaxBytes before shrink: %w", err)
	}

	if err := natsutil.SetupAll(nc, setupOpts...); err != nil {
		return 0, err
	}

	afterCtx, afterCancel := context.WithTimeout(
		context.Background(), storeShrinkListTimeout,
	)
	resized, err := countShrunkStreams(afterCtx, js, before)
	afterCancel()
	if err != nil {
		return 0, err
	}

	recheckCtx, recheckCancel := context.WithTimeout(
		context.Background(), storeShrinkListTimeout,
	)
	defer recheckCancel()
	if err := natsutil.CheckStoreBudget(recheckCtx, js, configuredBudget); err != nil {
		return resized, err
	}
	return resized, nil
}

// snapshotStreamMaxBytes lists every stream's current MaxBytes, keyed by
// stream name.
func snapshotStreamMaxBytes(
	ctx context.Context, js jetstream.JetStream,
) (map[string]int64, error) {
	if js == nil {
		panic("snapshotStreamMaxBytes: js is nil")
	}
	if ctx == nil {
		panic("snapshotStreamMaxBytes: ctx is nil")
	}

	snapshot := make(map[string]int64)
	lister := js.ListStreams(ctx)
	for info := range lister.Info() {
		if info == nil {
			continue
		}
		snapshot[info.Config.Name] = info.Config.MaxBytes
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// countShrunkStreams reports how many streams present in before now carry a
// smaller MaxBytes than their snapshotted value.
func countShrunkStreams(
	ctx context.Context, js jetstream.JetStream, before map[string]int64,
) (int, error) {
	if js == nil {
		panic("countShrunkStreams: js is nil")
	}
	if before == nil {
		panic("countShrunkStreams: before is nil")
	}

	resized := 0
	lister := js.ListStreams(ctx)
	for info := range lister.Info() {
		if info == nil {
			continue
		}
		prev, ok := before[info.Config.Name]
		if ok && info.Config.MaxBytes < prev {
			resized++
		}
	}
	if err := lister.Err(); err != nil {
		return 0, err
	}
	return resized, nil
}
