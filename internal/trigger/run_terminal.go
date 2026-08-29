// internal/trigger/run_terminal.go
// run_terminal is a trigger type that starts a target workflow when
// ANOTHER workflow's run reaches a terminal state (#634). It is
// general workflow chaining; the reference log-offload workflow
// (examples/log-offload/) is the first user, reacting to a build
// workflow's completion to copy its BUILD_LOGS chunks into long-term
// storage — a concern dagnats itself stays deliberately ignorant of.
//
// Source: a durable JetStream consumer on the EVENTS stream, filtered
// to event.run.{workflowToken}.*.* (internal/engine/run_event.go's
// RunEvent publish contract, docs/wire-protocol.md "Consumer
// contract: run lifecycle events"). Filtering by subject rather than
// consuming event.run.> and checking WorkflowID in-process means a
// busy engine with many concurrently-running workflows never delivers
// this trigger a message it has to immediately discard — the server
// does the filtering.
//
// Loop guards (design in, not patch after — see the issue):
//   - Register-time (validateRunTerminalConfig, wired into
//     validate.go): a trigger whose filter workflow equals its own
//     target workflow is rejected, naming both. A wildcard/empty
//     filter is rejected too — it must name exactly one workflow, so
//     the subject filter above is always a concrete, matchable
//     subject rather than an accidental catch-all.
//   - Runtime (registrar_run_terminal.go): dag.WorkflowRun.TriggerDepth
//     bounds cross-workflow cycles (A→B→A) that the register-time
//     check above cannot see, since the trigger graph is mutable
//     after registration.
package trigger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// TriggerDepthMax bounds how many run_terminal hops a chain of
// triggered runs may take before the engine refuses to start the
// next one (#634). A var, not a const, so tests can lower it to
// exercise the cap without constructing an 8-deep trigger chain.
// Exported (capitalized) only because this package's own e2e tests
// live in the same package and need to reassign it directly — this
// lives under internal/, so it is not part of any documented public
// API surface, and no production caller has a reason to set it to
// anything but the default.
var TriggerDepthMax = 8

// runTerminalStatuses is the full set of terminal RunEventType
// strings a run_terminal trigger may filter on — the same three
// buckets protocol.RunEventType coarsens dag.RunStatus into. Kept as
// plain strings (not protocol.RunEventType) here so this file stays
// free of a dependency the config's JSON `statuses` field doesn't
// otherwise need.
var runTerminalStatuses = []string{"completed", "failed", "cancelled"}

// RunTerminalConfig selects a source workflow and a subset of
// terminal statuses to fire on. Workflow is a FILTER — the trigger
// starts TriggerDef.WorkflowID (the target) when a run of Workflow
// (the source) reaches one of Statuses. Distinct fields, not reused,
// because "which workflow do I watch" and "which workflow do I start"
// are different questions and conflating them is exactly the
// self-trigger loop this type must reject at validation time.
type RunTerminalConfig struct {
	Workflow string   `json:"workflow"`
	Statuses []string `json:"statuses,omitempty"`
}

// EffectiveStatuses returns c.Statuses, or all three terminal
// statuses when c.Statuses is empty (the documented default). Kept
// separate from validateRunTerminalConfig so defaulting stays a pure
// function of the config, callable from both the registrar (deciding
// whether to fire) and tests, without re-running validation.
func (c RunTerminalConfig) EffectiveStatuses() []string {
	if len(c.Statuses) == 0 {
		out := make([]string, len(runTerminalStatuses))
		copy(out, runTerminalStatuses)
		return out
	}
	return c.Statuses
}

// matchesStatus reports whether status is one of c's effective
// statuses. Small helper so the registrar's message handler reads as
// one condition instead of an inline loop.
func (c RunTerminalConfig) matchesStatus(status string) bool {
	for _, s := range c.EffectiveStatuses() {
		if s == status {
			return true
		}
	}
	return false
}

// validateRunTerminalConfig enforces the register-time loop guard and
// the structural rules for a run_terminal trigger. def.ID and
// def.WorkflowID are assumed non-empty — validateCommon (validate.go)
// checks those before dispatching to a type-specific validator, the
// same contract validateCronConfig/validateWebhookConfig rely on.
func validateRunTerminalConfig(def TriggerDef) error {
	if def.ID == "" {
		panic("validateRunTerminalConfig: def.ID must not be empty")
	}
	if def.RunTerminal == nil {
		panic("validateRunTerminalConfig: RunTerminal must not be nil")
	}
	rt := def.RunTerminal

	if rt.Workflow == "" {
		return fmt.Errorf(
			"trigger %q: run_terminal workflow filter must not be empty",
			def.ID,
		)
	}
	if strings.ContainsAny(rt.Workflow, "*>") {
		return fmt.Errorf(
			"trigger %q: run_terminal workflow filter %q must name "+
				"exactly one workflow, not a wildcard",
			def.ID, rt.Workflow,
		)
	}
	if rt.Workflow == def.WorkflowID {
		return fmt.Errorf(
			"trigger %q: run_terminal workflow filter %q must not "+
				"equal its own target workflow %q — this would "+
				"re-trigger the target every time it finishes",
			def.ID, rt.Workflow, def.WorkflowID,
		)
	}
	for _, status := range rt.Statuses {
		if !isKnownRunTerminalStatus(status) {
			return fmt.Errorf(
				"trigger %q: run_terminal unknown status %q "+
					"(want one of completed, failed, cancelled)",
				def.ID, status,
			)
		}
	}
	return nil
}

// isKnownRunTerminalStatus reports whether status is one of the three
// values RunTerminalConfig.Statuses accepts.
func isKnownRunTerminalStatus(status string) bool {
	for _, s := range runTerminalStatuses {
		if s == status {
			return true
		}
	}
	return false
}

// runTerminalTokenMaxLen mirrors engine's subjectTokenMaxLen
// (internal/engine/run_event.go) — see runTerminalSubject for why the
// sanitizer is duplicated here rather than shared.
const runTerminalTokenMaxLen = 128

// runTerminalSubject builds the event.run.{token}.*.* filter subject
// for workflow. Duplicates internal/engine's subjectToken sanitizer
// byte-for-byte rather than importing it: internal/trigger already
// imports internal/engine (debounce.go, for the sleep-timer), so no
// import cycle forces this, but subjectToken is unexported and
// engine-package-private by design (it is a detail of how engine
// builds ITS publish subject, not a public contract). A trigger
// filtering on a name it does not control (an operator-typed
// workflow string) must derive the exact same subject token engine
// derives when it PUBLISHES, so the two copies must stay identical —
// enforced by TestRunTerminalSubject asserting the same non-obvious
// case (a space AND a dot in one name) documented in engine's test.
func runTerminalSubject(workflow string) string {
	token := sanitizeSubjectToken(workflow)
	if token == "" {
		token = "_"
	}
	return "event.run." + token + ".*.*"
}

// sanitizeSubjectToken makes s safe for use as a NATS subject token:
// any byte outside [A-Za-z0-9_-] becomes '_', capped to
// runTerminalTokenMaxLen. See runTerminalSubject's doc for why this
// duplicates internal/engine's subjectToken instead of importing it.
func sanitizeSubjectToken(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > runTerminalTokenMaxLen {
		out = out[:runTerminalTokenMaxLen]
	}
	return out
}

// runTerminalDurableName derives the EVENTS-stream durable consumer
// name for trigger triggerID. Hashed (not sanitizeSubjectToken)
// because two distinct IDs can sanitize to the identical token (#634
// review, Major 5) — e.g. "x.y" and "x_y" both sanitize to "x_y". A
// name collision there would make the second trigger's
// CreateOrUpdateConsumer call silently REWRITE the first trigger's
// consumer (same durable name = same consumer identity to
// JetStream), so only one of the two triggers would ever actually
// receive events. SHA-256 makes a collision between two operator-
// chosen IDs practically impossible; 16 hex chars (64 bits) is ample
// for a durable-name suffix while keeping consumer names short.
func runTerminalDurableName(triggerID string) string {
	if triggerID == "" {
		panic("runTerminalDurableName: triggerID must not be empty")
	}
	sum := sha256.Sum256([]byte(triggerID))
	return "run-terminal-" + hex.EncodeToString(sum[:])[:16]
}

// runTerminalChainRunID derives the deterministic run ID a
// run_terminal trigger starts in reaction to sourceRunID (#634
// review, Blocker 2). MUST be deterministic — not runid.New() — so
// that firing the SAME (triggerID, sourceRunID) pair twice (a
// redelivered source RunEvent, whether within JetStream's short
// Nats-Msg-Id dedup window or hours later after a crash/restart)
// always names the SAME target run. That determinism is what lets
// the engine's atomic run-ID claim (SnapshotStore.CreateSnapshot) reject
// the second attempt outright, instead of relying on a dedup window
// that cannot survive a restart gap. sha256 (not e.g. FNV) because
// this ID crosses a trust boundary into a value operators and other
// systems will see (run IDs are exposed via the API/CLI) and a
// cryptographic hash means two different (triggerID, sourceRunID)
// pairs colliding is not something an adversary or bad luck can
// engineer.
func runTerminalChainRunID(triggerID, sourceRunID string) string {
	if triggerID == "" {
		panic("runTerminalChainRunID: triggerID must not be empty")
	}
	if sourceRunID == "" {
		panic("runTerminalChainRunID: sourceRunID must not be empty")
	}
	sum := sha256.Sum256([]byte(triggerID + "|" + sourceRunID))
	return "trg" + hex.EncodeToString(sum[:])[:20]
}
