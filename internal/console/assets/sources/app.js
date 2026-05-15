/*
 * dagnats console — entry point.
 *
 * Imports Datastar (auto-initializes `data-*` attributes on load),
 * pulls in Basecoat's interactive components (dialogs, dropdowns, etc.),
 * and exposes any global hooks the console templates rely on.
 *
 * The build pipeline (esbuild) bundles this with `datastar.js` and
 * `basecoat.js` into a single `console.js`, which is gzipped and
 * embedded into the dagnats binary via `//go:embed`.
 */

import "./datastar.js";
import "./basecoat.js";

// No additional wiring yet — heartbeat tile is driven by data-on-load
// + Datastar's SSE PatchElements support. Future PRs add chart init,
// keyboard shortcuts, and theme toggle here.
