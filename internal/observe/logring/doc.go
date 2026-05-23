// Package logring exposes a bounded slog.Handler that retains recent
// log records in memory so the console can render a live-tail Logs
// page (#342, R2) without re-shipping logs through a separate
// transport.
//
// The handler is a pass-through: every record is forwarded to an inner
// slog.Handler so existing stdout/JSON sinks keep working unchanged.
// In addition the record is appended to a fixed-capacity ring that is
// also bounded by age — the oldest entries are dropped first whenever
// either bound trips. The drop policy is intentionally lossy: a wedged
// SSE subscriber or a runaway log producer can never block the engine.
//
// Eviction has two independent triggers:
//
//   - Capacity: once cap_entries records are held, the next append
//     drops the oldest. The default cap is 10_000 — chosen to match
//     a few minutes of busy engine output without holding the JSON
//     payloads in memory indefinitely.
//   - Age: records older than max_age (default 30 minutes) are pruned
//     on every append and on every Snapshot()/Subscribe() snapshot
//     copy. The operator's intent in the Logs page is "show me what
//     just happened"; older entries are noise compared to the metric
//     and DLQ surfaces.
//
// Either trigger may fire first, and they apply independently — a
// burst that fills the buffer in seconds keeps only the most recent
// 10_000 entries; a long-idle process accumulates < 10_000 entries
// but still drops anything past 30 minutes the next time something
// is logged.
//
// The interface is deliberately narrow:
//
//   - slog.Handler — so the type can be installed via slog.SetDefault.
//   - Snapshot() — a single immediate copy of the buffer, used by the
//     initial pageload to bootstrap the table.
//   - Subscribe(ctx) — a live-tail channel that receives every new
//     record from the moment the subscription is created. The returned
//     cleanup func cancels the subscription and frees the channel.
//
// Filtering (severity, trace-ID, free-text) and aggregation
// ("top sources" footer) live in the page handler, not in the ring —
// the ring stays a dumb transport so the audit reviewer can reason
// about it in isolation. The page handler may walk the slice returned
// by Snapshot() with any filter shape it likes.
package logring
