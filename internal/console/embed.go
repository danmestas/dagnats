// Package console implements the dagnats embedded operator UI.
//
// The package mounts under `/console/` (sibling of `/docs` from the
// openapi package). Everything is server-rendered HTML driven by
// Datastar + Basecoat; assets are vendored locally and never fetched
// from a CDN at runtime. See ADR-014 for the architecture.
package console

import "embed"

// assetsFS holds the gzipped bundles and the small uncompressed app.css.
// Files arrive verbatim from the deploy-time bundling pipeline
// documented in `assets/README.md`.
//
//go:embed assets/console.js.gz
//go:embed assets/basecoat.css.gz
//go:embed assets/uplot.min.js.gz
//go:embed assets/app.css
//go:embed assets/sources/connection-state.js
//go:embed assets/sources/toast.js
//go:embed assets/sources/count-chip.js
//go:embed assets/sources/metrics.js
//go:embed assets/sources/onboarding.js
//go:embed assets/sources/sparkline.js
//go:embed assets/sources/sheet.js
//go:embed assets/fonts/ibm-plex-sans-latin.woff2
var assetsFS embed.FS

// templatesFS carries every Go html/template file the console renders.
// Layouts live at the root; per-region fragments live under fragments/.
//
//go:embed templates/layout.html
//go:embed templates/dashboard.html
//go:embed templates/disabled.html
//go:embed templates/workflows_list.html
//go:embed templates/workflow_detail.html
//go:embed templates/runs_list.html
//go:embed templates/run_detail.html
//go:embed templates/triggers_list.html
//go:embed templates/trigger_detail.html
//go:embed templates/dlq_list.html
//go:embed templates/dlq_detail.html
//go:embed templates/audit_log.html
//go:embed templates/ops_index.html
//go:embed templates/ops_workers.html
//go:embed templates/ops_leases.html
//go:embed templates/ops_kv.html
//go:embed templates/metrics_dashboard.html
//go:embed templates/not_found.html
//go:embed templates/fragments/heartbeat.html
//go:embed templates/fragments/workflows_tbody.html
//go:embed templates/fragments/runs_tbody.html
//go:embed templates/fragments/status_badge.html
//go:embed templates/fragments/pager.html
//go:embed templates/fragments/run_row.html
//go:embed templates/fragments/run_event_row.html
//go:embed templates/fragments/run_step_row.html
//go:embed templates/fragments/triggers_tbody.html
//go:embed templates/fragments/dlq_tbody.html
//go:embed templates/fragments/audit_tbody.html
//go:embed templates/fragments/connection_pill.html
//go:embed templates/fragments/trigger_row.html
//go:embed templates/fragments/dlq_row.html
//go:embed templates/fragments/metric_tile.html
//go:embed templates/fragments/metrics_chart.html
//go:embed templates/components/step_list.html
//go:embed templates/components/run_error_banner.html
//go:embed templates/components/run_tab_panels.html
//go:embed templates/components/tile_live.html
//go:embed templates/components/recent_panels.html
//go:embed templates/components/dlq_action_modal.html
//go:embed templates/components/command_palette.html
//go:embed templates/components/tooltip.html
//go:embed templates/components/side_sheet.html
//go:embed templates/components/page_header.html
//go:embed templates/components/empty_state.html
var templatesFS embed.FS
