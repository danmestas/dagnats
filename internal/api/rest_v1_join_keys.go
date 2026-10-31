// rest_v1_join_keys.go
// Admin-only /v1/programs/join-keys routes (#885): create, list and
// revoke program join keys. Gated by the same fail-closed
// DAGNATS_BRIDGE_TOKEN admin bearer as /v1/tokens.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
)

// joinKeyBodyBytesMax bounds a create body; a real one is under 300
// bytes.
const joinKeyBodyBytesMax = 4096

type joinKeyRoutes struct {
	adminToken string
	store      *joinkey.Store
}

// MountJoinKeys registers the /v1/programs/join-keys routes on mux.
// The admin bearer is captured once, at mount, like MountV1's token
// routes.
func MountJoinKeys(mux *http.ServeMux, store *joinkey.Store) {
	if mux == nil {
		panic("MountJoinKeys: mux must not be nil")
	}
	if store == nil {
		panic("MountJoinKeys: store must not be nil")
	}
	routes := &joinKeyRoutes{
		adminToken: os.Getenv("DAGNATS_BRIDGE_TOKEN"),
		store:      store,
	}
	mux.HandleFunc("POST /v1/programs/join-keys", routes.handleCreate)
	mux.HandleFunc("GET /v1/programs/join-keys", routes.handleList)
	mux.HandleFunc("DELETE /v1/programs/join-keys/{id}", routes.handleRevoke)
}

func (jr *joinKeyRoutes) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if jr == nil {
		panic("joinKeyRoutes.requireAdmin: jr must not be nil")
	}
	return requireAdminBearer(w, r, jr.adminToken, "join key management")
}

// createJoinKeyRequest is the POST body. Zero values mean unbounded.
type createJoinKeyRequest struct {
	Label     string     `json:"label"`
	NameGlob  string     `json:"name_glob"`
	UsesMax   int        `json:"uses_max"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// createJoinKeyResponse is the stored record plus Key, the plaintext,
// which no other endpoint ever returns.
type createJoinKeyResponse struct {
	joinkey.JoinKey
	Key string `json:"key"`
}

type listJoinKeysResponse struct {
	JoinKeys []joinkey.JoinKey `json:"join_keys"`
}

func (jr *joinKeyRoutes) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !jr.requireAdmin(w, r) {
		return
	}
	var req createJoinKeyRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, joinKeyBodyBytesMax))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	key, plaintext, err := jr.store.Create(r.Context(), joinkey.Spec{
		Label:     req.Label,
		NameGlob:  req.NameGlob,
		UsesMax:   req.UsesMax,
		ExpiresAt: req.ExpiresAt,
	}, tokenAdminCreatedBy)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := createJoinKeyResponse{JoinKey: key, Key: plaintext}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode create join key response failed", "error", err)
	}
}

func (jr *joinKeyRoutes) handleList(w http.ResponseWriter, r *http.Request) {
	if !jr.requireAdmin(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := listJoinKeysResponse{JoinKeys: jr.store.List()}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode list join keys response failed", "error", err)
	}
}

func (jr *joinKeyRoutes) handleRevoke(w http.ResponseWriter, r *http.Request) {
	if !jr.requireAdmin(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	err := jr.store.Revoke(r.Context(), id)
	if errors.Is(err, joinkey.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
