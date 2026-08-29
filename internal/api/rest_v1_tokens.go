// rest_v1_tokens.go
// Admin-only /v1/tokens routes (#627): mint, list, revoke worker
// tokens. All three require the DAGNATS_BRIDGE_TOKEN admin bearer;
// with no admin token configured, token management is unavailable
// (503) rather than open -- the same fail-closed contract the bridge
// applies to its own auth (bridge/bridge.go).
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/workertoken"
)

// tokenAdminCreatedBy is the CreatedBy value stamped on every token
// minted via this REST surface. The admin bearer has no finer-grained
// identity today -- a future per-operator credential could replace
// this, but that is out of scope for #627.
const tokenAdminCreatedBy = "admin"

// tokenRoutes closes over the admin bearer (captured once at mount
// time, mirroring Bridge's snapshot-at-construction behavior in
// bridge.NewBridge) and the shared workertoken.Store.
type tokenRoutes struct {
	adminToken string
	store      *workertoken.Store
}

// mountTokenRoutes registers the /v1/tokens routes on mux. Called only
// from MountV1, which has already asserted mux and store are non-nil.
func mountTokenRoutes(mux *http.ServeMux, store *workertoken.Store) {
	if mux == nil {
		panic("mountTokenRoutes: mux must not be nil")
	}
	if store == nil {
		panic("mountTokenRoutes: store must not be nil")
	}
	tr := &tokenRoutes{
		adminToken: os.Getenv("DAGNATS_BRIDGE_TOKEN"),
		store:      store,
	}
	mux.HandleFunc("POST /v1/tokens", tr.handleMint)
	mux.HandleFunc("GET /v1/tokens", tr.handleList)
	mux.HandleFunc("DELETE /v1/tokens/{id}", tr.handleRevoke)
}

// requireAdmin enforces the fail-closed admin gate. Writes the
// response and returns false when the caller must stop; the caller
// proceeds only when this returns true.
func (tr *tokenRoutes) requireAdmin(
	w http.ResponseWriter, r *http.Request,
) bool {
	if tr == nil {
		panic("requireAdmin: tr must not be nil")
	}
	if w == nil {
		panic("requireAdmin: w must not be nil")
	}
	if tr.adminToken == "" {
		http.Error(w,
			"token management requires DAGNATS_BRIDGE_TOKEN as the "+
				"admin credential",
			http.StatusServiceUnavailable,
		)
		return false
	}
	bearer, hasBearer := strings.CutPrefix(
		r.Header.Get("Authorization"), "Bearer ",
	)
	if !hasBearer || subtle.ConstantTimeCompare(
		[]byte(bearer), []byte(tr.adminToken),
	) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// mintTokenRequest is the JSON body for POST /v1/tokens.
type mintTokenRequest struct {
	Label            string   `json:"label"`
	TaskTypePrefixes []string `json:"task_type_prefixes"`
}

// mintTokenResponse is the JSON body for a successful POST /v1/tokens.
// Token (the bearer) is shown exactly once here -- it is not
// recoverable from any other endpoint afterward.
type mintTokenResponse struct {
	ID               string    `json:"id"`
	Token            string    `json:"token"`
	Label            string    `json:"label"`
	TaskTypePrefixes []string  `json:"task_type_prefixes"`
	CreatedAt        time.Time `json:"created_at"`
}

// handleMint serves POST /v1/tokens.
func (tr *tokenRoutes) handleMint(w http.ResponseWriter, r *http.Request) {
	if tr == nil {
		panic("handleMint: tr must not be nil")
	}
	if r == nil {
		panic("handleMint: r must not be nil")
	}
	if !tr.requireAdmin(w, r) {
		return
	}
	var req mintTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	id, bearer, err := tr.store.Mint(
		r.Context(), req.Label, req.TaskTypePrefixes, tokenAdminCreatedBy,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := mintTokenResponse{
		ID:               id,
		Token:            bearer,
		Label:            req.Label,
		TaskTypePrefixes: req.TaskTypePrefixes,
		CreatedAt:        time.Now().UTC(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode mint token response failed", "error", err)
	}
}

// listTokensResponse is the JSON body for GET /v1/tokens.
type listTokensResponse struct {
	Tokens []workertoken.Token `json:"tokens"`
}

// handleList serves GET /v1/tokens.
func (tr *tokenRoutes) handleList(w http.ResponseWriter, r *http.Request) {
	if tr == nil {
		panic("handleList: tr must not be nil")
	}
	if r == nil {
		panic("handleList: r must not be nil")
	}
	if !tr.requireAdmin(w, r) {
		return
	}
	tokens, err := tr.store.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tokens == nil {
		tokens = []workertoken.Token{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(
		listTokensResponse{Tokens: tokens},
	); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleRevoke serves DELETE /v1/tokens/{id}.
func (tr *tokenRoutes) handleRevoke(w http.ResponseWriter, r *http.Request) {
	if tr == nil {
		panic("handleRevoke: tr must not be nil")
	}
	if r == nil {
		panic("handleRevoke: r must not be nil")
	}
	if !tr.requireAdmin(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	err := tr.store.Revoke(r.Context(), id)
	if errors.Is(err, workertoken.ErrTokenNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
