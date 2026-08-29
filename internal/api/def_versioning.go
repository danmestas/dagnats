// api/def_versioning.go
// Retention and eviction for the immutable name.v.hash def-version
// keys RegisterWorkflow writes alongside the mutable name -> latest
// pointer in workflow_defs (#637). Keeps a run pinned to the def it
// started under (see loadRunAndDef in internal/engine/orchestrator.go)
// without letting the version population grow unbounded under a
// caller that re-registers on every trigger.
package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
)

// DefVersionsMax bounds the number of immutable def versions retained
// per workflow name. Bounded per TigerStyle: persistDefVersion evicts
// the oldest unreferenced version before writing past this cap, and
// refuses the register outright (ErrTooManyLiveWorkflowVersions) when
// every retained version is still pinned by a non-terminal run.
const DefVersionsMax = 32

// defVersionScanMax bounds the workflow_defs key population
// persistDefVersion is willing to scan when counting name's retained
// versions. Mirrors the bound runtimes.go's countDefsForRoot applies
// to the same bucket.
const defVersionScanMax = 100_000

// ErrTooManyLiveWorkflowVersions is returned by RegisterWorkflow when
// name already retains DefVersionsMax def versions and every one of
// them is still referenced by a non-terminal run -- none is safe to
// evict to make room for the new version. The REST handler maps this
// to 409. LiveVersions is the number of referenced versions found.
type ErrTooManyLiveWorkflowVersions struct {
	Name         string
	LiveVersions int
}

func (e *ErrTooManyLiveWorkflowVersions) Error() string {
	if e == nil {
		panic("ErrTooManyLiveWorkflowVersions.Error: receiver is nil")
	}
	return fmt.Sprintf(
		"workflow %q retains %d live def versions (max %d); "+
			"every retained version is still referenced by a "+
			"non-terminal run, so none can be evicted",
		e.Name, e.LiveVersions, DefVersionsMax,
	)
}

// persistDefVersion writes the immutable name.v.hash snapshot for def
// (content data, already marshaled by the caller so the bytes match
// exactly what registerWorkflowInner is about to Put under the plain
// name too). A no-op when that exact content hash is already stored
// -- re-registering byte-identical content across an idempotent
// forge poll must not grow the retained-version count or touch
// retention accounting.
func (s *Service) persistDefVersion(
	ctx context.Context, def dag.WorkflowDef, data []byte,
) error {
	if ctx == nil {
		panic("persistDefVersion: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("persistDefVersion: defKV must not be nil")
	}
	versionKey := dag.DefVersionKey(def.Name, dag.DefHash(def))
	if _, err := s.defKV.Get(ctx, versionKey); err == nil {
		return nil // already retained -- no growth, no eviction needed
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err := s.reserveDefVersionSlot(ctx, def.Name); err != nil {
		return err
	}
	_, err := s.defKV.Create(ctx, versionKey, data)
	if err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	return nil
}

// reserveDefVersionSlot makes room for one new, not-yet-written
// version of name when name is already at DefVersionsMax retained
// versions: it evicts the oldest version key that no non-terminal
// run currently pins via WorkflowRun.DefHash. If every retained
// version is still referenced, it refuses with
// ErrTooManyLiveWorkflowVersions instead of silently exceeding the
// cap or evicting a version a running run still needs.
func (s *Service) reserveDefVersionSlot(
	ctx context.Context, name string,
) error {
	if ctx == nil {
		panic("reserveDefVersionSlot: ctx must not be nil")
	}
	if name == "" {
		panic("reserveDefVersionSlot: name must not be empty")
	}
	versionKeys, err := s.defVersionKeysForName(ctx, name)
	if err != nil {
		return err
	}
	if len(versionKeys) < DefVersionsMax {
		return nil
	}
	liveHashes, err := s.liveDefHashes(ctx, name)
	if err != nil {
		return err
	}
	evictKey, err := s.oldestEvictableVersion(ctx, name, versionKeys, liveHashes)
	if err != nil {
		return err
	}
	if evictKey == "" {
		return &ErrTooManyLiveWorkflowVersions{
			Name: name, LiveVersions: len(versionKeys),
		}
	}
	return s.defKV.Delete(ctx, evictKey)
}

// defVersionKeysForName returns the workflow_defs KV keys that are
// version keys for name, via one bounded (defVersionScanMax) whole-
// bucket key scan -- the KV client exposes no server-side prefix
// filter, so this mirrors the scan-and-filter pattern countDefsForRoot
// and defCountsByRoot already use over the same bucket.
func (s *Service) defVersionKeysForName(
	ctx context.Context, name string,
) ([]string, error) {
	if ctx == nil {
		panic("defVersionKeysForName: ctx must not be nil")
	}
	if name == "" {
		panic("defVersionKeysForName: name must not be empty")
	}
	keys, err := s.defKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	if len(keys) > defVersionScanMax {
		return nil, fmt.Errorf(
			"workflow_defs key scan exceeded bound (%d > %d)",
			len(keys), defVersionScanMax)
	}
	prefix := dag.DefVersionKeyPrefix(name)
	versionKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) && dag.IsDefVersionKey(key) {
			versionKeys = append(versionKeys, key)
		}
	}
	return versionKeys, nil
}

// liveDefHashes returns the set of DefHash values referenced by a
// non-terminal run of workflow name, scanned via the bounded
// ListRecent(MaxRunsLimitCeiling) window the REST run-list endpoints
// already accept as "the genuinely most-recent window" for this
// bucket (#637 reuses that bound rather than introducing a new one).
func (s *Service) liveDefHashes(
	ctx context.Context, name string,
) (map[string]bool, error) {
	if ctx == nil {
		panic("liveDefHashes: ctx must not be nil")
	}
	if name == "" {
		panic("liveDefHashes: name must not be empty")
	}
	runs, err := s.store.ListRecent(ctx, MaxRunsLimitCeiling)
	if err != nil {
		return nil, err
	}
	live := make(map[string]bool)
	for _, run := range runs {
		if run.WorkflowID != name || run.DefHash == "" {
			continue
		}
		if !run.Status.IsTerminal() {
			live[run.DefHash] = true
		}
	}
	return live, nil
}

// oldestEvictableVersion returns the version key among versionKeys
// with the lowest KV revision (i.e. oldest) whose content hash is not
// in liveHashes, or "" if every version is referenced. Revisions,
// not wall-clock timestamps, decide "oldest" -- monotonic within a
// single KV bucket and immune to clock skew.
func (s *Service) oldestEvictableVersion(
	ctx context.Context, name string,
	versionKeys []string, liveHashes map[string]bool,
) (string, error) {
	if ctx == nil {
		panic("oldestEvictableVersion: ctx must not be nil")
	}
	if name == "" {
		panic("oldestEvictableVersion: name must not be empty")
	}
	evictKey := ""
	var evictRevision uint64
	for _, key := range versionKeys {
		hash, ok := dag.DefHashFromVersionKey(key, name)
		if !ok || liveHashes[hash] {
			continue
		}
		entry, err := s.defKV.Get(ctx, key)
		if err != nil {
			return "", err
		}
		if evictKey == "" || entry.Revision() < evictRevision {
			evictKey, evictRevision = key, entry.Revision()
		}
	}
	return evictKey, nil
}
