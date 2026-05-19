# ADR-015 — Operator-first console re-skin (supersedes ADR-014 aesthetic)

**Status:** Accepted
**Date:** 2026-05-19
**Supersedes:** ADR-014's aesthetic decisions only (the architecture, IA, auth model, four-verifier dispatch discipline, and 8-PR arc structure remain in force).

## Context

ADR-014 (2026-05-03) chose **"e-ink editorial"** as the console aesthetic — soft warm cream + warm near-black palette, Fraunces (display serif) for headings, IBM Plex Sans for body, decorative atmospheric chrome (gradient body backgrounds, cross-hatch DAG canvas, hand-drawn-feel imperfections, dotted-underline tooltips). Inspiration was bump.sh, craftdesign.group, and editorial paper-feel reading rooms.

The 8-PR arc shipped that aesthetic. Subsequent audits (ui-ux-pro-max, two rounds of ui-honest-qa, four user-driven feedback sessions with screenshots) surfaced a recurring complaint pattern:

- "DAG view is ridiculously large and totally worthless"
- "Many pages are unreadable" (even after WCAG-AA contrast fixes shipped)
- "Hover on dark mode is illegible"
- "Background thing going on" (the body gradient + noise composition)
- Operators repeatedly noted the UI feels like a portfolio piece, not a tool

Each round we shipped tactical contrast bumps, removed decorative chrome, adjusted spacing. Each round closed some bugs and the next round found more in the same class. The aggregate scoring suggested progress (Norman delta +0.93 across 16 pages after PR #264). User perception did not match the scores.

The persona — confirmed 2026-05-19 — is **production SRE/engineer getting paged**. The UI's primary jobs are 5-second triage, 2-minute root-cause diagnosis, and 1-click recovery actions. Not 5-minute exploration of a reading-paper interface.

## The fundamental tension

| Editorial aesthetic wants | SRE operator needs |
|---|---|
| Calm reading rhythm | Fast scanability — info-rich rows |
| Generous whitespace | Information density — more per screen |
| Soft, warm, low-saturation colors | High-contrast functional colors (status pops) |
| Serif typography for personality | Monospace numerals for IDs / timestamps / durations |
| Decorative atmosphere | Function only — no decoration |
| One thing at a time | Many things at once, well-organized |
| Subtle interactions | Bold affordances |
| Section margins 2rem+ | Section margins 0.5-1rem |
| Body 16-17px serif | Body 13-14px sans, mono for data |

These conflict at the foundational level. Tactical fixes can soften individual symptoms but cannot reconcile the two.

When `ui-ux-pro-max --design-system "observability dashboard developer tools admin operator"` was run early in the arc, it recommended **Data-Dense Dashboard** style (minimal padding, grid layout, max data visibility, Fira Code + Fira Sans). The recommendation matches the persona. We rejected it in ADR-014. This ADR reverses that decision.

## Decision

**Re-skin the console with Data-Dense Dashboard priorities while preserving the architecture and infrastructure ADR-014 + the 8-PR arc + subsequent fix arcs already shipped.**

### What changes

- **Palette family**: switch from warm cream / warm near-black to a functional dark-first palette. Operators tend to prefer dark mode for monitoring tools; we'll dark-first the design and ensure light-mode parity.
- **Typography**: 
  - Body face: **IBM Plex Mono** (already loaded as a font asset) becomes the default for data — IDs, timestamps, durations, counts, code, table cells.
  - **IBM Plex Sans** for prose, labels, headings, longer descriptions.
  - **Fraunces** retained ONLY for `/docs` (the Scalar API explorer where editorial reading IS the appropriate aesthetic).
- **Status colors**: bump saturation to functional levels. Replace warm-shifted desaturated tones with clear operator semantics — red means urgent, amber means watch, green means fine, blue means in-progress.
- **Spacing scale**: 4/8/12/16 instead of 8/16/24/32. Row padding tightens 30-40%. Section margins shrink. More info per screen.
- **DAG view**: vertical step list (GitHub Actions style) becomes the default for the workflow detail page. The SVG flowchart becomes a secondary tab for genuinely non-linear workflows.
- **Status badges**: replace `[● running]` chip-style badges with a single status icon (colored) + plain text. Saves horizontal space, reads faster.
- **Navigation**: sidebar on desktop (≥1024px) freeing 60-80 vertical pixels for data. Top nav stays on mobile.
- **Decorative chrome**: zero. No gradients, noise textures, cross-hatch backgrounds, hand-drawn-feel imperfections, dotted underlines as "atmospheric touches". Every pixel earns its place.
- **Cmd+K + always-visible search**: cmd+K is power-user; an always-visible search input above list pages helps first-time operators.

### What is preserved

- **All Phase 2 infrastructure**: tabs, sheets, cmd+K palette, glossary tooltips (terms only, no atmospheric dotted underlines), audit emitter, read-only middleware, CSRF, dev-mode env var, root redirect, dagnats config show env vars.
- **The architecture**: server-side rendered HTML + Datastar live updates, NATS-native primitives (KV watches, JetStream consumers), event bus for cross-handler signaling.
- **The four-verifier dispatch discipline**: dx-audit + norman + frontend-design (now ui-ux-pro-max with Data-Dense Dashboard target) + agent-browser-per-iteration. Plus the ui-honest-qa skill for verification.
- **The IA**: route structure, page boundaries, what's under Ops vs Triggers vs DLQ. None of those change.
- **The auth + read-only models**.
- **The locked engine telemetry assumptions**: workers/leases buckets pending, metrics aggregator path. Re-skin doesn't change what data is shown — only how.
- **All tests, audits, plans, hand-off docs** stay in `docs/` as historical reference.

### `/docs` is special

The `/docs` route renders the Scalar OpenAPI explorer. That's a reading-context use case where editorial typography (Fraunces) and calm rhythm work. `/docs` keeps the prior aesthetic. Console keeps re-skin. Clear seam.

## Consequences

### Positive

- Operator perception aligns with what the UI actually IS. The recurring "this looks unprofessional / unreadable / worthless" feedback should resolve at the foundation, not via tactical patches.
- Audit scores become trustworthy because the principles being measured match the use case the persona has. Currently Norman scores 8+/10 against a UI the operator finds 4/10.
- DAG view becomes useful (vertical step list with timing + status + log expansion) instead of a portfolio illustration.
- Information density supports the 5-second triage job.

### Negative

- Substantial re-skin effort. Estimated 2-4K LOC across templates + CSS, multi-PR arc.
- Visual continuity with prior screenshots breaks. Anyone with the e-ink aesthetic in mental cache will need to reorient.
- The decision to invest in editorial aesthetic in ADR-014 is now explicitly written off. We learned something; the cost is real.

### Neutral

- Light mode is still supported (operator preference varies; some run dark on monitors, some run light).
- Some users may genuinely prefer the editorial feel for casual use; that's why `/docs` preserves it.

## Implementation plan (subsequent ADR / PR arcs)

This ADR documents the strategic decision. Implementation will follow as a deliberate arc, similar to the original 8-PR arc:

1. **Tier A bug fixes** (in flight): close the 5 visible bugs from latest screenshots within the current aesthetic. Buys time to plan the re-skin properly without operator pain compounding.
2. **Skin design brief**: define palette, type ramp, spacing scale, component variants. Reference Data-Dense Dashboard from ui-ux-pro-max. ~1-2 days planning.
3. **Foundation re-skin** (1 PR): swap palette tokens, type tokens, spacing tokens, base component classes. Visual breaking change confined to one PR.
4. **DAG → step list as default** (1 PR): restructure workflow detail.
5. **Sidebar nav on desktop** (1 PR): restructure layout.html for ≥1024px.
6. **Status badge simplification** (1 PR): replace chip badges with icon + text.
7. **Audit pass with updated skill targets** (1 PR): verify principle scoring matches operator perception.

Total estimated: 5-7 PRs, 2-4K LOC, similar arc length to Phase 2.

## References

- ADR-014 (the aesthetic decision this supersedes)
- `/tmp/console-first-principles.md` (the analysis that led here)
- `~/.claude/skills/ui-honest-qa/SKILL.md` (the QA methodology that surfaced the persistent gap)
- `ui-ux-pro-max --design-system "observability dashboard developer tools admin operator"` (the recommendation we should have taken in ADR-014)
- Reference UIs: GitHub Actions (step list), Dagger.io (TUI-like progress), Inngest (gantt + traces), PagerDuty (incident density), k9s (keyboard-first ops)
