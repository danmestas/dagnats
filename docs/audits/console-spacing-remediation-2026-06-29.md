# Console spacing & typography remediation — 2026-06-29

Audit driven by two spacing references ([uxlab](https://www.uxlab.academy/blogs/the-ultimate-spacing-guide-for-ui-designers),
[Phogat](https://mohitphogat.medium.com/the-spacing-system-that-makes-every-ui-look-more-intentional-52251ae61c2f))
+ Norman, against the console's own `--space-*` scale. Found via a grid-conformance
scan of `app.css` + a live computed-style/Norman visual sweep.

## The system (the rubric)

- **Grid:** every spacing value is a `--space-N` token — `--space-1..6` = **4 / 8 / 12 / 16 / 24 / 32 px** (4px grid). No arbitrary literals (`0.55rem`, `0.7rem`, `14px`, `18px`, `22.5px` are all off-grid).
- **Proximity / internal-to-external:** spacing *inside* a group < spacing *between* groups. A section header must sit close to its content; unrelated groups get more air.
- **Density, not "more is better":** padding scales with content. A short row in a tall, over-padded card reads *hollow*, not spacious.
- **Consistency:** the same component role uses the same tokens everywhere.
- **Hierarchy (Norman):** a row's *value* identifier (step name, run id, instance name) must read **primary** (`--text-primary`). *Labels* (uppercase section captions) are intentionally muted — leave those.

## Tier 1 — broken text color (primary content rendering dim/invisible)

Selectors that style primary **value** content but omit `color`, so they inherit
basecoat's light-mode foreground (`oklch(0.145 0 0)`) → dim-to-invisible on dark.
**Fix: add `color: var(--text-primary)`.** (This is the flagged "text color wrong in the steps section" — and its bug class across the run timeline.)

| selector | app.css | role |
|---|---|---|
| `.console-step-name` | 805 | step name (flagged) |
| `.console-step-id` | 745 | step id |
| `.timeline-name` | 2708 | run-timeline step label |
| `.timeline-name .step-name` | 2714 | step name in timeline |
| `.console-name` / `.console-brand .console-name` | 233 / 945 | instance/brand name |
| `.console-metric-tile-title` | 1728 | KPI tile name |
| `.dashboard-tile .dashboard-tile-title` | 4164 | dashboard tile name |
| `.logs-top-source-name`, `.task-type-name` | 4078, 2087 | source / task-type name |

**Leave alone (intentional muted *label* aesthetic, not a bug):** `.card-header h2`,
`.kv-section-title`, `.config-section-title` and other uppercase section captions.
The console deliberately uses muted uppercase labels — changing those is a taste call, not a defect.

## Tier 2 — card rhythm: the ~44px header→content gap (the flagged "too much spacing")

Every `.card` stacks `padding 22.5px` + flex `gap 22.5px` (header↔body) + `card-body
padding-top 12px` → **~44px between a card title and its first content**. For a
one-row STEPS/TRIGGERS card this dominates and reads hollow. `22.5px` is itself
off-grid.
- **Fix:** normalize `.card` padding to `--space-5` (24px) and the header↔body gap to `--space-3` (12px) (and/or drop `card-body padding-top` to 0), bringing the title→content gap to ~24px (`--space-5`). Verify across all detail cards (steps, triggers, stream, run, server).

## Tier 3 — hollow cards (CSS-grid stretch)

Two-column card grids use `align-items: normal` (stretch), inflating the shorter
card to the taller sibling:
- `.config-jetstream-grid` — left pane **~473px empty** (worst on the site).
- `.stream-detail-cards` — State card **~78px empty**.
- Dashboard "Recent operator actions" — fixed-height symmetry hollows the sparse side.
- **Fix:** `align-items: start` on the grid containers (and per-card `align-self: start`).

## Tier 4 — off-grid spacing migration (the systemic sweep)

**~118 off-grid values** + **~130 hardcoded on-grid literals** → migrate to `--space-*`
tokens. Dominant offenders: `0.55rem`/`0.7rem`/`0.65rem`/`0.85rem`/`14px`/`18px`/`22.5px`.
The known clusters that affect the flagged views first:
- `.console-step` (774): `0.55rem 0.7rem` pad, `0.6rem` gap, `1.1rem` margin → `--space-2`/`--space-3`/`--space-2`/`--space-4`.
- `.console-step-card` (732/736/741), `.console-tile` (395: `1.1rem 1.2rem`), `.ops-tile` (1437: 18px), `.console-chart` (1786: 14px) — align all card-like containers to one padding pattern (`--space-3` v / `--space-4` h standard; `--space-2`/`--space-3` compact rows).
- Full per-line table: see the grid-conformance audit output (248 sites).

## Tier 5 — minor hierarchy / consistency

- Trigger / run **ID cells** render `font-weight:400` like metadata — bump primary-id cells to `600` for row hierarchy.
- Section-header `margin-bottom` inconsistent (dashboard/config 12px vs list pages 16px) → one token (`--space-4`).
- Off-grid filter-bar padding (`11.25px`) → `--space-3`.

## Remediation order (ship as batches / "the loop")

1. **Batch 1 (flagged + critical):** Tier 1 colors + Tier 2 card rhythm + the `.console-step` off-grid spacing. Fixes everything in the screenshot.
2. **Batch 2:** Tier 3 hollow-card `align-items: start`.
3. **Batch 3+:** Tier 4 token migration (mechanical, large — do in reviewable slices) + Tier 5.

Each batch: edit `app.css`, rebuild + reload the seeded console, verify computed values + screenshot, PR → CI → merge.
