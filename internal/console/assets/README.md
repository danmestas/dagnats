# dagnats console — vendored assets

This directory holds the bundled, gzipped, and ready-to-embed asset
artifacts that ship inside the dagnats binary. Every asset the console
serves is **vendored locally** — the browser makes zero outbound
requests to any CDN or third-party endpoint at runtime. See ADR-014's
"Local-first asset policy" section for the rationale.

## What's in here

| File | Source | Size (raw → gzipped) |
|---|---|---|
| `console.js.gz` | esbuild bundle of `sources/app.js` + `sources/datastar.js` + `sources/basecoat.js` | ~46 KB → ~16 KB |
| `basecoat.css.gz` | Basecoat CSS (CDN distribution) | ~134 KB → ~13 KB |
| `uplot.min.js.gz` | µPlot upstream `dist/uPlot.iife.min.js` | ~50 KB → ~22 KB |
| `app.css` | dagnats custom styles + e-ink palette as CSS custom properties | ~4 KB (served uncompressed) |

`app.css` is small enough that gzipping it would yield diminishing
returns. Every other asset ships gzipped on disk and is served with
`Content-Encoding: gzip` directly so any compression-aware client
(every modern browser) decompresses transparently. This mirrors the
Scalar pattern at `internal/openapi/scalar/standalone.js.gz`.

## sources/

The `sources/` directory holds the unbundled inputs the deploy-time
toolchain consumes:

| File | Upstream | Pinned version |
|---|---|---|
| `sources/datastar.js` | https://github.com/starfederation/datastar | `v1.0.0-RC.6` |
| `sources/basecoat.js` | https://github.com/hunvreus/basecoat (the `all.min.js` distribution) | `latest` (refresh procedure below) |
| `sources/basecoat-raw.css` | Basecoat CDN distribution | `latest` |
| `sources/app.js` | hand-written | n/a |

A contributor inspecting how the console works reads the gzipped
artifacts in this directory. A maintainer regenerating the bundles
reads `sources/` and the refresh procedure below.

## Refresh procedure

The release-time toolchain consists of two single-binary tools — no
npm, no Node toolchain at deploy time.

### Tooling

- **Tailwind standalone CLI** — single Mach-O / Linux binary from
  https://github.com/tailwindlabs/tailwindcss/releases. Pinned to
  `v4.1.16`.
- **esbuild** — single binary from
  https://github.com/evanw/esbuild/releases. Pinned to `v0.28.0`.

Neither tool is invoked during per-commit CI. They run at release time
when a maintainer regenerates the bundles. CI stays Node-free.

### Refresh commands

Run from the repository root:

```bash
# 1. Refresh upstream sources (bump pins above accordingly)
curl -fsSL -o internal/console/assets/sources/datastar.js \
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0-RC.6/bundles/datastar.js"
curl -fsSL -o internal/console/assets/sources/basecoat.js \
  "https://cdn.jsdelivr.net/npm/basecoat-css/dist/js/all.min.js"
curl -fsSL -o internal/console/assets/sources/basecoat-raw.css \
  "https://cdn.jsdelivr.net/npm/basecoat-css/dist/basecoat.cdn.min.css"
curl -fsSL -o internal/console/assets/uplot.min.js \
  "https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/uPlot.iife.min.js"

# 2. Bundle JS
esbuild internal/console/assets/sources/app.js \
  --bundle --minify \
  --target=es2020 --format=esm \
  --outfile=internal/console/assets/console.js

# 3. Copy Basecoat CSS (currently used as-is from upstream)
cp internal/console/assets/sources/basecoat-raw.css \
   internal/console/assets/basecoat.css

# 4. Gzip
gzip -9 -f internal/console/assets/console.js
gzip -9 -f internal/console/assets/basecoat.css
gzip -9 -f internal/console/assets/uplot.min.js
```

### v1 simplification: Basecoat CSS shipped as-is

PR 1 ships the Basecoat CSS as published by upstream (the
`basecoat.cdn.min.css` distribution). A later PR will re-introduce a
true Tailwind purge step that scans the live `templates/*.html`
inventory and emits only the classes actually used. Even un-purged the
bundle is ~13 KB gzipped, which is comfortably within the design
budget. Once the template inventory grows beyond what Basecoat's
default bundle covers — or once the gzipped budget tightens — the
maintainer regenerates with:

```bash
tailwindcss \
  -i internal/console/assets/sources/basecoat-entry.css \
  -o internal/console/assets/basecoat.css \
  --minify
```

(driven by `@source` directives inside `basecoat-entry.css` pointing
at `internal/console/templates/**/*.html`).

## Why gzipped on disk

Embedding the gzipped form keeps the binary size down and avoids the
runtime cost of compressing each response. The handler sets
`Content-Encoding: gzip` directly. A non-gzip client is not supported —
every modern browser handles gzip, and the operator console targets
modern browsers anyway.
