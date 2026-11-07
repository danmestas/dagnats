// api/service_runs_idempotency.go
// Atomic runs.start idempotency (#946). A workflow with an IdempotencyKey
// path claims its run ID with KV Create in the idempotency_keys bucket
// (24h TTL) BEFORE WorkflowStarted is published, exactly as keyed child
// spawns do (runtimes_spawn_key.go). The event's Nats-Msg-Id embeds the
// run ID, so a call that finds an existing claim whose run does not exist
// yet (first start still in flight, or a crash between claim and publish)
// republishes for the stored ID and the stream drops the duplicate if the
// original landed.
//
// Failure choices: every KV error on the claim or the read fails the
// start (fail closed; no run starts without dedupe). A publish failure
// after a successful claim returns the error and KEEPS the claim: the
// retry finds it, sees no run, and republishes for the stored ID.
package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/nats-io/nats.go/jetstream"
)

// resolveStartRunID picks the run ID for a start and says whether the
// caller must publish WorkflowStarted for it. Unkeyed starts (no
// IdempotencyKey, no input, or the key path absent from the input) get a
// fresh ID and publish.
func (s *Service) resolveStartRunID(
	ctx context.Context,
	workflowName string, def dag.WorkflowDef, input []byte,
) (runID string, publish bool, err error) {
	if workflowName == "" {
		panic("resolveStartRunID: workflowName must not be empty")
	}
	fresh := runid.New()
	if fresh == "" {
		panic("resolveStartRunID: runid.New returned empty")
	}
	if def.IdempotencyKey == "" || input == nil {
		return fresh, true, nil
	}
	val, extractErr := dag.ExtractDotPath(def.IdempotencyKey, input)
	if extractErr != nil {
		// The input carries no key: nothing to dedupe on.
		return fresh, true, nil
	}
	if s.idempotencyKV == nil {
		return "", false, errors.New(
			"idempotency_keys bucket unavailable")
	}
	slot := idempotencyHash(workflowName, fmt.Sprintf("%v", val))
	return s.claimStartSlot(ctx, slot, fresh)
}

// claimStartSlot claims slot for fresh. A lost race adopts the winner's
// run ID, and publishes only when that run does not exist yet.
func (s *Service) claimStartSlot(
	ctx context.Context, slot, fresh string,
) (string, bool, error) {
	if slot == "" {
		panic("claimStartSlot: slot must not be empty")
	}
	if fresh == "" {
		panic("claimStartSlot: fresh must not be empty")
	}
	_, err := s.idempotencyKV.Create(ctx, slot, []byte(fresh))
	if err == nil {
		return fresh, true, nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return "", false, fmt.Errorf("claim idempotency key: %w", err)
	}
	entry, err := s.idempotencyKV.Get(ctx, slot)
	if err != nil {
		return "", false, fmt.Errorf("read idempotency claim: %w", err)
	}
	claimed := string(entry.Value())
	if claimed == "" {
		return "", false, errors.New("idempotency claim holds no run ID")
	}
	_, err = s.store.Load(ctx, claimed)
	if err == nil {
		return claimed, false, nil
	}
	if !errors.Is(err, engine.ErrRunNotFound) {
		return "", false, fmt.Errorf("check claimed run: %w", err)
	}
	return claimed, true, nil
}
