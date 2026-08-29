package dag

import (
	"fmt"
	"strings"
)

// taskTypeLenMax bounds a StepDef.Task's length. A task type feeds
// directly into a NATS subject token (internal/engine/task_publisher.go's
// StepSubject builds "task.{Task}.{runID}" verbatim), so it inherits the
// same practical ceiling NATS subject tokens carry elsewhere in this
// codebase (see internal/natsutil.subjectTokenMaxLen).
const taskTypeLenMax = 128

// ValidTaskType reports whether s is safe to publish verbatim as one or
// more NATS subject tokens. StepSubject builds
// "task.{Task}.{runID}" (optionally "task.{Task}.{WorkerGroup}.{runID}")
// from a StepDef's Task with no sanitization — unlike a step ID, which
// passes through natsutil.SubjectToken first — because a worker's
// registered task_types must match the value byte-for-byte to poll it.
// consumername.FilterFor anchors a worker's consumer filter on the same
// value. An unsafe Task (whitespace, a NATS wildcard, a stray leading,
// trailing, or empty dotted token) would register cleanly today and only
// break at dispatch time, either minting a malformed subject or silently
// never getting picked up — see issue #674.
//
// Dots ARE allowed within a token: "dagger.call" is a production task
// type, and rejecting dots outright would break it. What keeps a dotted
// task type from leaking into another worker's poll is FilterFor's exact
// token-count anchor, not a charset restriction here — see
// internal/consumername.FilterFor's doc comment.
func ValidTaskType(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("task type must not be empty")
	}
	if len(s) > taskTypeLenMax {
		return fmt.Errorf(
			"task type %q is %d bytes (max %d)",
			s, len(s), taskTypeLenMax,
		)
	}
	// Indexing s[0] and s[len(s)-1] unguarded here is safe only because
	// the len(s) == 0 check above already returned — s is guaranteed
	// non-empty from this point on.
	if s[0] == '.' || s[len(s)-1] == '.' {
		return fmt.Errorf(
			"task type %q must not start or end with '.'", s,
		)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
			continue
		case c == '.':
			if s[i-1] == '.' {
				return fmt.Errorf(
					"task type %q has an empty token (\"..\")", s,
				)
			}
			continue
		default:
			return fmt.Errorf(
				"task type %q contains disallowed byte %q at position %d",
				s, string(c), i,
			)
		}
	}
	return nil
}

// ValidWorkerGroup reports whether s is safe to use as a StepDef.WorkerGroup.
// StepSubject appends WorkerGroup as its OWN subject token
// ("task.{Task}.{WorkerGroup}.{runID}"), so it must satisfy ValidTaskType's
// charset/length/leading-trailing-dot rule AND additionally contain no dots
// at all — unlike Task, WorkerGroup is never a dotted namespace.
//
// Dots are banned here (not merely anchored around, the way FilterFor
// isolates a dotted Task) because a dot in WorkerGroup is indistinguishable
// from the separator FilterFor/StepSubject place between Task and
// WorkerGroup: FilterFor("render", "gpu.fast") and FilterFor("render.gpu",
// "fast") both derive "task.render.gpu.fast.*", and NameFor collapses both
// to "workers-render-gpu-fast" — a dotted group silently collides with an
// unrelated dotted-task/group split. Banning dots in WorkerGroup outright
// (rather than case-by-case) closes that for a single step's own Task,
// regardless of what that step's Task looks like; see
// validateStepDispatch's combination check for the remaining case where
// THAT SAME step's Task is dotted and WorkerGroup is set. Neither rule
// reaches across steps or workflow defs: step A {Task:"render.gpu"} and
// step B {Task:"render", WorkerGroup:"gpu"} in different workflows still
// derive the identical filter subject and durable name, and the
// cross-process collision check treats that as ordinary idempotent
// durable reuse, not a conflict — see docs/wire-protocol.md "Task
// Subjects" for that limitation.
func ValidWorkerGroup(s string) error {
	if err := ValidTaskType(s); err != nil {
		return err
	}
	if strings.Contains(s, ".") {
		return fmt.Errorf(
			"worker group %q must not contain '.': worker_group is "+
				"always a single subject token, never a dotted namespace",
			s,
		)
	}
	return nil
}
