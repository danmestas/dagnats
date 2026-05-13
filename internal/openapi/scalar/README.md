# Vendored Scalar API Reference

`standalone.js.gz` is the gzipped standalone bundle from
`@scalar/api-reference`, pinned to **v1.55.3**.

Fetched from:

```
https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.55.3/dist/browser/standalone.js
```

The gzipped variant is what we ship because the raw bundle is ~3.5 MB
and the gzipped form is ~1 MB — small enough to embed via `embed.FS`
without bloating the binary, and we serve it with
`Content-Encoding: gzip` directly.

## Refresh procedure

```bash
curl --globoff -fsSL -o /tmp/scalar.js \
  'https://cdn.jsdelivr.net/npm/@scalar/api-reference@<VER>/dist/browser/standalone.js'
gzip -9 -c /tmp/scalar.js > internal/openapi/scalar/standalone.js.gz
```

Bump the `<VER>` and update this README. Keep the existing version line
in sync with the embed comment in `internal/openapi/docs.go`.
