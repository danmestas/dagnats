// openapi/docs.go
//
// HTTP surface for the OpenAPI spec and the Scalar-rendered API
// explorer. Three routes total:
//
//	GET /openapi.json — synthesised spec, one allocation per request
//	GET /docs         — HTML shell that loads Scalar
//	GET /docs/scalar.js — vendored Scalar standalone bundle, gzipped
//
// Spec generation is on-demand because the KV scan + struct synth is
// sub-millisecond for any realistic workflow count and the freshness
// guarantee outweighs the cache complexity. The Scalar bundle is
// embedded at build time so /docs survives in offline / firewalled
// environments without a CDN dependency.
//
// Vendored Scalar version: @scalar/[email protected]
// — fetched from https://cdn.jsdelivr.net/npm/@scalar/api-reference
// see internal/openapi/scalar/README.md for refresh instructions.
package openapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

//go:embed scalar.html
var scalarHTML []byte

//go:embed scalar/standalone.js.gz
var scalarBundleGz []byte

// ProviderFunc returns the inputs needed to synthesise the spec at
// request time. The host package supplies a closure over its own
// trigger / workflow loaders. Keeping the dependency direction this
// way (caller-supplied, not openapi-imports-api) means the openapi
// package does not need to import internal/api — which would cycle
// since api wants to mount these routes.
type ProviderFunc func(
	ctx context.Context,
) (triggers []trigger.TriggerDef, defs map[string]dag.WorkflowDef, err error)

// Handler returns an http.Handler that serves the OpenAPI JSON and
// the Scalar UI. title and version are the spec's Info.Title and
// Info.Version. provider must not be nil — callers connect it to the
// engine's KV-backed trigger / workflow stores.
//
// The handler routes:
//
//	GET /openapi.json    → spec
//	GET /docs            → Scalar shell HTML
//	GET /docs/scalar.js  → Scalar standalone bundle
//
// Any other path under the returned handler 404s.
func Handler(
	title, version string, provider ProviderFunc,
) http.Handler {
	if provider == nil {
		panic("openapi.Handler: provider must not be nil")
	}
	if len(scalarBundleGz) == 0 {
		panic("openapi.Handler: scalar bundle not embedded")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/openapi.json", func(
		w http.ResponseWriter, r *http.Request,
	) {
		serveSpec(w, r, title, version, provider)
	})
	mux.HandleFunc("/docs", serveScalarHTML)
	mux.HandleFunc("/docs/scalar.js", serveScalarBundle)
	return mux
}

// serveSpec writes the synthesised OpenAPI document as
// application/json. Errors from the provider degrade to a 500.
func serveSpec(
	w http.ResponseWriter,
	r *http.Request,
	title, version string,
	provider ProviderFunc,
) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	triggers, defs, err := provider(r.Context())
	if err != nil {
		http.Error(w, "spec build: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	spec := Build(title, version, triggers, defs)
	body, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		http.Error(w, "spec marshal: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

// serveScalarHTML returns the Scalar UI shell. Static content, so
// the response is safely cached short-term.
func serveScalarHTML(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control",
		"public, max-age=60, must-revalidate")
	_, _ = w.Write(scalarHTML)
}

// serveScalarBundle returns the vendored Scalar standalone JS. The
// bundle is stored gzipped on disk; we serve with
// Content-Encoding: gzip directly so any compression-aware client
// (every browser) decompresses transparently. A non-gzip client
// would need a separate code path — we choose not to support that
// because Scalar itself requires a modern browser anyway.
func serveScalarBundle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Cache-Control",
		"public, max-age=31536000, immutable")
	_, _ = w.Write(scalarBundleGz)
}

// SortedPathKeys returns the path keys in the spec sorted lexically.
// Exported so callers (tests, debug surfaces) can enumerate the
// stable order without re-implementing the comparator.
func SortedPathKeys(s Spec) []string {
	out := make([]string, 0, len(s.Paths))
	for k := range s.Paths {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
