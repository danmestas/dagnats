// Package prom renders metrics from the in-house aggregator into
// Prometheus text exposition format (version 0.0.4). It is the only
// package in the repository allowed to know about Prometheus naming
// rules — the rest of the codebase speaks the provider-agnostic
// internal/observe/metrics types.
//
// Output conforms to https://prometheus.io/docs/instrumenting/exposition_formats/:
//
//   - Each metric prefaced by `# HELP <name> <description>` and
//     `# TYPE <name> <kind>` lines.
//   - Counters carry a `_total` suffix; their internal canonical name
//     (e.g. "workflow.runs.completed") is translated to a Prometheus-
//     legal underscore-separated form ("workflow_runs_completed_total").
//   - Histograms render the `_bucket{le="..."}`, `_sum`, `_count`
//     companion series.
//   - Labels are sorted alphabetically inside `{}` so output is
//     byte-stable across rebuilds — useful for diff-watchers and
//     test snapshots.
//
// The package exposes a single Handler builder that satisfies
// http.HandlerFunc and a NoData sentinel so callers can render a
// useful response when the aggregator hasn't received its first ingest
// yet.
package prom
