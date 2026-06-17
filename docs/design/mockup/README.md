# Console design mockup (authoritative reference)

This directory is the preserved source of the **dagnats operator-console design
mockup** — the authoritative visual/structural reference the console
(`internal/console/`) was built to match.

## Provenance

- Tool: **MagicPath** (`magicpath-ai` CLI), component **`kindly-stream-5966`**,
  project **"Dagnats UI"**.
- Captured: **2026-06** (during the console-fidelity campaign).
- **The MagicPath copy was later deleted**, so this is the only surviving
  source. Treat it as canonical and do not delete.

## Contents

| File | What it is |
|---|---|
| `ConsoleRedesign.tsx` | The shell: nav rail, topbar (title + Read-only/Destructive toggles + status tiles), view router, footer. |
| `ConsoleViewsObserve.tsx` | DashboardView, WorkflowsView, WorkflowDetailView, RunsView, RunDetailView, TriggersView. |
| `ConsoleViewsInventory.tsx` | Functions, Function detail, Workers, Worker detail, Streams. |
| `ConsoleViewsOperate.tsx` | DLQ, Concurrency (admission-control). |
| `ConsoleViewsSystem.tsx` | Server health, Connections, Consumers, KV, Config, Services. |
| `ConsoleViewsTrace.tsx` | Traces (gantt/span detail), Logs. |
| `Spark.tsx` | The inline sparkline component. |
| `consoleData.ts` | Fixtures + `NAV` definition + status text/colour maps. |
| `mockup-source.json`, `mockup-inspect.json` | Raw MagicPath API captures (complete dump; the `.tsx` above are extracted from these). |
| `console-dashboard-preview.png` | Rendered preview of the dashboard (the source the muted palette hexes were sampled from: failed `#FF8A93`, completed `#62C875`, accent teal `#4EC9B0`). |
| `AUDIT.md` | Original page-by-page audit of the console vs this mockup. |
| `audit-2026-06-15.md` | Consolidated re-audit (gap list: shipped / honest-omit / engine-gated). |

## Note

The `--dn-*` design tokens (e.g. `--dn-teal`, `--dn-green`) referenced by the
`.tsx` were defined in MagicPath's global stylesheet, which was **not** part of
the component export — they are not in these files. The actual rendered colours
were recovered by pixel-sampling `console-dashboard-preview.png`.
