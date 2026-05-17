# console asset sources

Unbundled inputs the deploy-time toolchain consumes. See
`../README.md` for the refresh procedure and pinned upstream versions.

## Basecoat component inventory (2026-05-17, Phase 2 T01 audit)

Upstream Basecoat (https://github.com/hunvreus/basecoat) ships JS for:
`tabs, command, popover, dropdown-menu, select, sidebar, toast`. The
upstream CSS bundle (`basecoat.cdn.min.css`) ships matching selectors
for each. **Upstream does NOT ship `sheet` or `tooltip`** — neither in
JS nor CSS. The Phase 2 plan called these out as needed; we author
minimal in-house implementations that conform to Basecoat's
`window.basecoat.register(name, selector, init)` contract so consumers
(T03 tabs, T10 tooltips, T11 cmd+k, T12 sheets) treat them identically
to native Basecoat components.

| Component | Source | Status |
|---|---|---|
| `tabs`     | upstream basecoat   | already vendored (PR 1) |
| `command`  | upstream basecoat   | already vendored (PR 1) |
| `sheet`    | in-house (shadcn-ish) | added 2026-05-17 (Phase 2 T01) |
| `tooltip`  | in-house (shadcn-ish) | added 2026-05-17 (Phase 2 T01) |

Both in-house components use HTML's native `<dialog>` + `[popover]`
where appropriate, register on `window.basecoat`, and emit
`basecoat:initialized` to mirror the upstream lifecycle event.

Section markers (`/* === <name> (added YYYY-MM-DD, Phase 2) === */`)
delimit the in-house blocks in `basecoat-raw.css` and `basecoat.js` so
a future Basecoat refresh can drop in upstream replacements without
hand-merging.
