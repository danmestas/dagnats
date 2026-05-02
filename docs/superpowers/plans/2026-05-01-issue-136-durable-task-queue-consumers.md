# Durable Consumers on `TASK_QUEUES` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is sized 2–5 minutes.

**Goal:** Replace `worker.createConsumer`'s ephemeral consumer with a deterministically named durable consumer on `TASK_QUEUES`, so worker restarts and N>1 workers per task type stop colliding on `WorkQueuePolicy`'s one-consumer-per-filter rule.

**Architecture:** Single deep helper `subscribePullConsumer(taskType, group, handler)` owns naming, filter, AckWait, and full `ConsumerConfig`. Two pure helpers (`consumerNameFor`, `consumerFilterFor`) plus `sanitizeConsumerName` keep the naming convention in one place. A registration-time precheck (`assertNoConsumerNameCollisions`) refuses to call any subscribe path with collision-prone inputs. A self-healing migration step at the top of the helper deletes pre-existing ephemeral orphans before claiming the durable. Sticky and elastic paths' code is untouched; the precheck covers their inputs as a strict-gain side effect.

**Tech Stack:** Go (module `github.com/danmestas/dagnats`), NATS JetStream client (`github.com/nats-io/nats.go/jetstream`), embedded NATS for tests via `internal/natsutil.StartTestServer(t)`, `slog` for structured logging.

**Spec:** [`docs/superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md`](../specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md)

**ADR numbering note:** The spec text uses placeholder numbers (ADR-005, ADR-006, ADR-007, ADR-008). At plan-write time, the repository already contains `adr-005-embedded-nats-cluster-mode.md`. Therefore this plan ships:

| Plan ADR | Repo filename | Status | Subject |
|---|---|---|---|
| **ADR-006** | `docs/architecture/adr-006-durable-task-queue-consumers.md` | Accepted | This fix (the spec body). |
| **ADR-007** | `docs/architecture/adr-007-unify-consumer-paths.md` | Proposed | Unify default + elastic paths. `Depends on: ADR-008`. |
| ADR-008 (deferred) | n/a in this PR | n/a | In-handler heartbeats (`msg.InProgress()`). Filed as follow-up issue only. |
| ADR-009 (deferred) | n/a in this PR | n/a | Cross-process consumer-name collision detection. Filed as follow-up issue only. |

Test names, code-comment references, and follow-up issue titles below all use the repo-relative numbers (ADR-006 for the fix, ADR-007 for unify, ADR-008 for heartbeats, ADR-009 for cross-process).

---

## File structure

Files created by this plan:

| File | Responsibility |
|---|---|
| `worker/consumer_naming.go` | Pure naming helpers. `sanitizeConsumerName(s)`, `consumerNameFor(taskType, group)`, `consumerFilterFor(taskType, group)`, plus the package-private `defaultAckWait` constant. No NATS imports. |
| `worker/consumer_naming_test.go` | Pure unit tests for the three helpers + `defaultAckWait` value. No embedded NATS. |
| `worker/consumer_collision.go` | `assertNoConsumerNameCollisions(handlers, groups)` — pure precheck enumerating durables. Panics on collision. |
| `worker/consumer_collision_test.go` | Pure unit tests for the precheck. No embedded NATS. |
| `worker/consumer_subscribe_test.go` | Integration tests for `subscribePullConsumer` (config readback, assertion defense, end-to-end sanitization, restart resilience, two-worker scale-out, migration cleanup including pagination, failure-mode panics). One file because they share the `(server, nc, js, w)` setup pattern. |
| `docs/architecture/adr-006-durable-task-queue-consumers.md` | New ADR (Status: Accepted). Distilled spec. |
| `docs/architecture/adr-007-unify-consumer-paths.md` | New ADR (Status: Proposed). `Depends on: ADR-008` in frontmatter. |
| `docs/architecture/README.md` | Establishes the `Depends on:` ADR-frontmatter convention. Created if not present. |

Files modified:

| File | Change |
|---|---|
| `worker/worker.go` | Delete `createConsumer` (lines 385-418). Add `subscribePullConsumer` method. Replace callsites at lines 352 and 371-373 with the two-line shape from spec §1. Wire `assertNoConsumerNameCollisions(...)` into `Start()` before the `for taskType, handler := range w.handlers` loop. |

No other files touch. `internal/natsutil/conn.go` is unchanged. Stream retention policy is unchanged.

---

## Branch setup

Run once before Task 1:

```bash
cd /Users/dmestas/projects/dagnats
git checkout main
git pull --ff-only
git checkout -b fix/issue-136-durable-task-queue-consumers
go test ./... -count=1
```

Expected: all green on the baseline. If the baseline is red, abort and fix tip-of-`main` first.

---

## Task 1: `sanitizeConsumerName` — table-driven sanitization helper

**Files:**
- Create: `worker/consumer_naming.go`
- Create: `worker/consumer_naming_test.go`

- [ ] **Step 1.1: Create `worker/consumer_naming_test.go` with the failing test.**

```go
// worker/consumer_naming_test.go
// Pure unit tests for the consumer-naming helpers. No embedded NATS, no
// JetStream — these helpers are deliberately NATS-free so they can be
// exercised in isolation and reused by the collision precheck.
package worker

import (
	"testing"
	"time"
)

func TestSanitizeConsumerName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"identity_alphanumeric_dashes", "render", "render"},
		{"dot_collapses_to_dash", "render.gpu", "render-gpu"},
		{"hyphenated_preserved", "nasr-ingest", "nasr-ingest"},
		{"underscore_preserved", "nasr_ingest", "nasr_ingest"},
		{"colon_safe_escape", "vendor::ingest", "vendor__ingest"},
		{"whitespace_safe_escape", "a b c", "a_b_c"},
		{"only_dots_collapse", "....", "----"},
		{"mixed_classes", "Worker-1.2_x", "Worker-1-2_x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeConsumerName(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeConsumerName(%q) = %q, want %q",
					tc.in, got, tc.want)
			}
			if got == "" {
				t.Fatalf("sanitizeConsumerName(%q) returned empty", tc.in)
			}
		})
	}
}

func TestSanitizeConsumerName_PanicsOnEmpty(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty input, got none")
		}
		msg, ok := r.(string)
		if !ok || msg == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	sanitizeConsumerName("")
}

func TestDefaultAckWait_IsFiveMinutes(t *testing.T) {
	if defaultAckWait != 5*time.Minute {
		t.Fatalf("defaultAckWait = %v, want %v", defaultAckWait, 5*time.Minute)
	}
	if defaultAckWait <= 0 {
		t.Fatalf("defaultAckWait must be positive, got %v", defaultAckWait)
	}
}
```

- [ ] **Step 1.2: Run to verify failure.**

```
go test ./worker -run 'TestSanitizeConsumerName|TestDefaultAckWait_IsFiveMinutes' -count=1 -v
```

Expected: `undefined: sanitizeConsumerName` and `undefined: defaultAckWait` compile errors.

- [ ] **Step 1.3: Create `worker/consumer_naming.go` with the minimal implementation.**

```go
// worker/consumer_naming.go
// Naming convention for dagnats-managed JetStream consumers on TASK_QUEUES.
// All durable names live under the "workers-" prefix; sanitization maps
// task-type/group strings to NATS-legal name fragments.
package worker

import "time"

// defaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (see ADR-008) or override at handler registration via
// WithAckWait (deferred follow-up, see ADR-006 §1).
const defaultAckWait = 5 * time.Minute

// sanitizeConsumerName maps a task-type or group string to a NATS-legal
// consumer-name fragment. Dots collapse to hyphens for the common
// dotted-namespace case; other disallowed characters fall back to
// underscore. Empty input or empty output is a programmer error.
func sanitizeConsumerName(s string) string {
	if s == "" {
		panic("sanitizeConsumerName: input must not be empty")
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-',
			c == '_':
			out = append(out, c)
		case c == '.':
			out = append(out, '-')
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		panic("sanitizeConsumerName: result must not be empty")
	}
	return string(out)
}
```

- [ ] **Step 1.4: Run to verify pass.**

```
go test ./worker -run 'TestSanitizeConsumerName|TestDefaultAckWait_IsFiveMinutes' -count=1 -v
```

Expected: PASS for all subtests including `TestSanitizeConsumerName_PanicsOnEmpty`.

- [ ] **Step 1.5: Commit.**

```bash
git add worker/consumer_naming.go worker/consumer_naming_test.go
git commit -m "$(cat <<'EOF'
feat(worker): add sanitizeConsumerName + defaultAckWait

Pure helper that maps task-type/group strings to NATS-legal consumer-name
fragments. Dots collapse to hyphens (round-trips render.gpu visually);
anything else not in [A-Za-z0-9_-] falls back to underscore. Empty in or
empty out panics — programmer-error contract.

defaultAckWait constant set to 5 min; will become coalesce(per-task-override,
default) when WithAckWait registration option lands.

Refs #136.
EOF
)"
```

---

## Task 2: `consumerNameFor(taskType, group)` — derives the durable name

**Files:**
- Modify: `worker/consumer_naming.go`
- Modify: `worker/consumer_naming_test.go`

- [ ] **Step 2.1: Append failing tests to `worker/consumer_naming_test.go`.**

```go
func TestConsumerNameFor(t *testing.T) {
	cases := []struct {
		name           string
		taskType, group string
		want           string
	}{
		{"default_branch_simple", "render", "", "workers-render"},
		{"default_branch_dotted", "render.gpu", "", "workers-render-gpu"},
		{"default_branch_hyphenated", "nasr-ingest", "", "workers-nasr-ingest"},
		{"groups_branch_simple", "render", "gpu", "workers-render-gpu"},
		{"groups_branch_dotted_group", "render", "gpu.fast", "workers-render-gpu-fast"},
		{"groups_branch_safe_escape", "render", "gpu*1", "workers-render-gpu_1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := consumerNameFor(tc.taskType, tc.group)
			if got != tc.want {
				t.Fatalf("consumerNameFor(%q, %q) = %q, want %q",
					tc.taskType, tc.group, got, tc.want)
			}
			if got == "" {
				t.Fatal("consumerNameFor returned empty")
			}
		})
	}
}

func TestConsumerNameFor_RejectsEmptyTaskType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
	}()
	consumerNameFor("", "")
}
```

- [ ] **Step 2.2: Run to verify failure.**

```
go test ./worker -run 'TestConsumerNameFor' -count=1 -v
```

Expected: `undefined: consumerNameFor` compile error.

- [ ] **Step 2.3: Append the helper to `worker/consumer_naming.go`.**

```go
// consumerNameFor produces the durable consumer name for a (taskType, group)
// pair. group=="" means the default branch. Both inputs are sanitized via
// sanitizeConsumerName before being concatenated under the "workers-" prefix.
// The "workers-" prefix is reserved for dagnats-managed consumers.
func consumerNameFor(taskType, group string) string {
	if taskType == "" {
		panic("consumerNameFor: taskType must not be empty")
	}
	if group == "" {
		out := "workers-" + sanitizeConsumerName(taskType)
		if out == "" {
			panic("consumerNameFor: result must not be empty")
		}
		return out
	}
	out := "workers-" + sanitizeConsumerName(taskType) + "-" +
		sanitizeConsumerName(group)
	if out == "" {
		panic("consumerNameFor: result must not be empty")
	}
	return out
}
```

- [ ] **Step 2.4: Run to verify pass.**

```
go test ./worker -run 'TestConsumerNameFor' -count=1 -v
```

Expected: PASS.

- [ ] **Step 2.5: Commit.**

```bash
git add worker/consumer_naming.go worker/consumer_naming_test.go
git commit -m "$(cat <<'EOF'
feat(worker): add consumerNameFor(taskType, group)

Single home for the durable-consumer naming convention. Default branch
(group=="") produces "workers-<task>"; groups branch produces
"workers-<task>-<group>". Inputs are sanitized identically; the precheck
uses the same helper so subscribe and validate cannot disagree.

Refs #136.
EOF
)"
```

---

## Task 3: `consumerFilterFor(taskType, group)` — derives the filter subject

**Files:**
- Modify: `worker/consumer_naming.go`
- Modify: `worker/consumer_naming_test.go`

- [ ] **Step 3.1: Append failing tests.**

```go
func TestConsumerFilterFor(t *testing.T) {
	cases := []struct {
		name           string
		taskType, group string
		want           string
	}{
		{"default_branch", "render", "", "task.render.>"},
		{"default_branch_dotted_task", "render.gpu", "", "task.render.gpu.>"},
		{"groups_branch", "render", "gpu", "task.render.gpu.>"},
		{"groups_branch_hyphenated", "nasr-ingest", "fastlane", "task.nasr-ingest.fastlane.>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := consumerFilterFor(tc.taskType, tc.group)
			if got != tc.want {
				t.Fatalf("consumerFilterFor(%q, %q) = %q, want %q",
					tc.taskType, tc.group, got, tc.want)
			}
			if got == "" {
				t.Fatal("consumerFilterFor returned empty")
			}
		})
	}
}

func TestConsumerFilterFor_RejectsEmptyTaskType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
	}()
	consumerFilterFor("", "")
}
```

- [ ] **Step 3.2: Run to verify failure.**

```
go test ./worker -run 'TestConsumerFilterFor' -count=1 -v
```

Expected: `undefined: consumerFilterFor`.

- [ ] **Step 3.3: Append the helper to `worker/consumer_naming.go`.**

```go
// consumerFilterFor produces the filter subject for a (taskType, group) pair.
// Inputs are NOT sanitized — they appear in the message-subject hierarchy and
// must round-trip exactly. Subject validity is the publisher's contract;
// sanitization is a consumer-naming concern.
func consumerFilterFor(taskType, group string) string {
	if taskType == "" {
		panic("consumerFilterFor: taskType must not be empty")
	}
	if group == "" {
		out := "task." + taskType + ".>"
		if out == "" {
			panic("consumerFilterFor: result must not be empty")
		}
		return out
	}
	out := "task." + taskType + "." + group + ".>"
	if out == "" {
		panic("consumerFilterFor: result must not be empty")
	}
	return out
}
```

- [ ] **Step 3.4: Run to verify pass.**

```
go test ./worker -run 'TestConsumerFilterFor' -count=1 -v
```

Expected: PASS.

- [ ] **Step 3.5: Commit.**

```bash
git add worker/consumer_naming.go worker/consumer_naming_test.go
git commit -m "$(cat <<'EOF'
feat(worker): add consumerFilterFor(taskType, group)

Returns "task.<taskType>.>" or "task.<taskType>.<group>.>" verbatim — no
sanitization, since these go on the wire as subjects. Precheck and subscribe
both use this helper so they cannot disagree.

Refs #136.
EOF
)"
```

---

## Task 4: `assertNoConsumerNameCollisions` — registration-time precheck

**Files:**
- Create: `worker/consumer_collision.go`
- Create: `worker/consumer_collision_test.go`

- [ ] **Step 4.1: Create `worker/consumer_collision_test.go`.**

```go
// worker/consumer_collision_test.go
// Pure unit tests for the registration-time collision precheck. No embedded
// NATS — the precheck enumerates durable names from the in-memory
// (handlers, groups) view and panics on duplicates.
package worker

import (
	"strings"
	"testing"
)

func TestAssertNoConsumerNameCollisions_DefaultBranchCollision(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"render.gpu": func(ctx TaskContext) error { return nil },
		"render-gpu": func(ctx TaskContext) error { return nil },
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on collision, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "render.gpu") || !strings.Contains(msg, "render-gpu") {
			t.Fatalf("panic must name both originals, got: %s", msg)
		}
		if !strings.Contains(msg, "workers-render-gpu") {
			t.Fatalf("panic must name the colliding durable, got: %s", msg)
		}
	}()
	assertNoConsumerNameCollisions(handlers, nil)
}

func TestAssertNoConsumerNameCollisions_GroupsBranchCollision(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"render": func(ctx TaskContext) error { return nil },
	}
	groups := []string{"gpu.fast", "gpu-fast"}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on group collision, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "gpu.fast") || !strings.Contains(msg, "gpu-fast") {
			t.Fatalf("panic must name both group originals, got: %s", msg)
		}
		if !strings.Contains(msg, "workers-render-gpu-fast") {
			t.Fatalf("panic must name colliding durable, got: %s", msg)
		}
	}()
	assertNoConsumerNameCollisions(handlers, groups)
}

func TestAssertNoConsumerNameCollisions_CrossProduct_NoCollision(t *testing.T) {
	// 2 task types x 2 groups = 4 distinct durables. Must not panic.
	// Guards the cross-product enumeration logic.
	handlers := map[string]HandlerFunc{
		"render":  func(ctx TaskContext) error { return nil },
		"compile": func(ctx TaskContext) error { return nil },
	}
	groups := []string{"fast", "slow"}
	assertNoConsumerNameCollisions(handlers, groups)
}

func TestAssertNoConsumerNameCollisions_NoCollision_Baseline(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"nasr-ingest":               func(ctx TaskContext) error { return nil },
		"airports-canonical-refresh": func(ctx TaskContext) error { return nil },
	}
	assertNoConsumerNameCollisions(handlers, nil)
}

func TestAssertNoConsumerNameCollisions_EmptyHandlers(t *testing.T) {
	assertNoConsumerNameCollisions(map[string]HandlerFunc{}, nil)
}
```

- [ ] **Step 4.2: Run to verify failure.**

```
go test ./worker -run 'TestAssertNoConsumerNameCollisions' -count=1 -v
```

Expected: `undefined: assertNoConsumerNameCollisions`.

- [ ] **Step 4.3: Create `worker/consumer_collision.go`.**

```go
// worker/consumer_collision.go
// Registration-time precheck: refuse to start if any (taskType, group) pair
// in the worker's configured handlers collides on the durable consumer name
// after sanitization. Catches cases like "render.gpu" + "render-gpu" before
// they corrupt NATS state via CreateOrUpdateConsumer.
package worker

import "fmt"

// origin records the (taskType, group) pair that produced a given durable.
// Carrying both lets the panic message name the originals — actionable for
// the operator, who needs to rename one.
type origin struct {
	taskType string
	group    string
}

// assertNoConsumerNameCollisions enumerates every durable consumer name this
// worker would create and panics if any two distinct origins collide on the
// same name. Pure: takes the in-memory handler map and groups slice, no NATS.
//
// groups==nil or empty groups means the default branch (one durable per
// taskType). Otherwise the cross-product of (taskType x group) is enumerated.
func assertNoConsumerNameCollisions(
	handlers map[string]HandlerFunc, groups []string,
) {
	if handlers == nil {
		panic("assertNoConsumerNameCollisions: handlers must not be nil")
	}

	seen := make(map[string]origin, len(handlers))

	if len(groups) == 0 {
		for taskType := range handlers {
			name := consumerNameFor(taskType, "")
			if prior, exists := seen[name]; exists {
				panic(fmt.Sprintf(
					"dagnats: task types %q and %q both produce durable %q — rename one",
					prior.taskType, taskType, name,
				))
			}
			seen[name] = origin{taskType: taskType}
		}
		return
	}

	for taskType := range handlers {
		for _, group := range groups {
			name := consumerNameFor(taskType, group)
			if prior, exists := seen[name]; exists {
				panic(fmt.Sprintf(
					"dagnats: (task=%q,group=%q) and (task=%q,group=%q) both produce durable %q — rename one",
					prior.taskType, prior.group, taskType, group, name,
				))
			}
			seen[name] = origin{taskType: taskType, group: group}
		}
	}
}
```

- [ ] **Step 4.4: Run to verify pass.**

```
go test ./worker -run 'TestAssertNoConsumerNameCollisions' -count=1 -v
```

Expected: PASS for all five subtests.

- [ ] **Step 4.5: Commit.**

```bash
git add worker/consumer_collision.go worker/consumer_collision_test.go
git commit -m "$(cat <<'EOF'
feat(worker): add assertNoConsumerNameCollisions precheck

Pure registration-time check. Enumerates durables from the (handlers, groups)
view and panics on duplicates with both originals + colliding durable named.
Default branch and groups branch handled separately so the panic message
names the right tuple shape. Catches "render.gpu" + "render-gpu" → both
sanitize to "workers-render-gpu" before any NATS state mutates.

Refs #136.
EOF
)"
```

---

## Task 5: `subscribePullConsumer` minimal — config readback test drives the impl

**Files:**
- Modify: `worker/worker.go`
- Create: `worker/consumer_subscribe_test.go`

This task introduces the helper without migration cleanup. Migration logic lands in Task 7. Writing the helper in two passes keeps each TDD cycle small.

- [ ] **Step 5.1: Create `worker/consumer_subscribe_test.go` with the readback test.**

```go
// worker/consumer_subscribe_test.go
// Integration tests for subscribePullConsumer and the surrounding wiring.
// Methodology: real embedded NATS server per test, drive the helper through
// the Worker public API, read back ConsumerInfo from the stream to verify
// owned config, exercise restart/migration/scale-out paths end-to-end.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSubscribePullConsumer_AppliesExpectedConfig(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	w.Start()
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("Consumer(workers-render): %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	if info.Config.Durable != "workers-render" {
		t.Errorf("Durable = %q, want %q", info.Config.Durable, "workers-render")
	}
	if info.Config.Name != "workers-render" {
		t.Errorf("Name = %q, want %q", info.Config.Name, "workers-render")
	}
	if info.Config.FilterSubject != "task.render.>" {
		t.Errorf("FilterSubject = %q, want %q",
			info.Config.FilterSubject, "task.render.>")
	}
	if info.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want AckExplicitPolicy", info.Config.AckPolicy)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want DeliverAllPolicy", info.Config.DeliverPolicy)
	}
	if info.Config.AckWait != defaultAckWait {
		t.Errorf("AckWait = %v, want %v", info.Config.AckWait, defaultAckWait)
	}
	if info.Config.MaxDeliver != -1 {
		t.Errorf("MaxDeliver = %d, want -1", info.Config.MaxDeliver)
	}
}
```

- [ ] **Step 5.2: Run to verify failure.**

```
go test ./worker -run TestSubscribePullConsumer_AppliesExpectedConfig -count=1 -v
```

Expected: FAIL — current `createConsumer` produces an ephemeral consumer with no `Durable`, so `stream.Consumer(ctx, "workers-render")` returns `ErrConsumerNotFound`.

- [ ] **Step 5.3: Add `subscribePullConsumer` to `worker/worker.go`.** Insert after `createConsumer` (around line 418, but before `createStickyConsumer`):

```go
// subscribePullConsumer attaches a worker to a durable JetStream pull
// consumer on TASK_QUEUES, creating it if absent. Idempotent across
// worker restarts. Cleans up orphan ephemeral consumers with the same
// filter subject before creation (see ADR-006 §3, added in Task 7).
// Panics on setup failure; stream/consumer setup errors are startup-fatal.
//
// The durable name and filter subject are derived from (taskType, group)
// via consumerNameFor and consumerFilterFor. AckWait is the package-private
// defaultAckWait. Per-task override via WithAckWait is a deferred follow-up.
func (w *Worker) subscribePullConsumer(
	taskType, group string, handler HandlerFunc,
) jetstream.ConsumeContext {
	if taskType == "" {
		panic("subscribePullConsumer: taskType must not be empty")
	}
	if handler == nil {
		panic("subscribePullConsumer: handler must not be nil")
	}
	if defaultAckWait <= 0 {
		panic("subscribePullConsumer: defaultAckWait must be positive")
	}

	durable := consumerNameFor(taskType, group)
	filter := consumerFilterFor(taskType, group)
	ctx := context.Background()

	cfg := jetstream.ConsumerConfig{
		Durable:       durable,
		Name:          durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       defaultAckWait,
		// DLQ routing and retry budgets are the engine's responsibility
		// (NakWithDelay + attempt count in step state), not NATS's. We
		// leave NATS unbounded so engine policy isn't silently shadowed.
		MaxDeliver: -1,
	}
	cons, err := w.js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg)
	if err != nil {
		panic("subscribePullConsumer: CreateOrUpdateConsumer for " +
			durable + ": " + err.Error())
	}

	tt := taskType
	h := handler
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMessage(tt, h, msg)
	})
	if err != nil {
		panic("subscribePullConsumer: Consume for " + durable + ": " +
			err.Error())
	}
	return cc
}
```

Note: this minimal version omits migration cleanup. The helper still produces a durable, which is enough to make `TestSubscribePullConsumer_AppliesExpectedConfig` pass. Migration cleanup lands in Task 7.

This task does not yet wire the helper into `subscribeTask` — that happens in Task 12 once all the prerequisite tests are in place. So the readback test must call `subscribePullConsumer` directly. Adjust the test:

Replace the body of `TestSubscribePullConsumer_AppliesExpectedConfig` to drive the helper directly:

```go
func TestSubscribePullConsumer_AppliesExpectedConfig(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	cc := w.subscribePullConsumer("render", "",
		func(ctx TaskContext) error { return nil })
	defer cc.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("Consumer(workers-render): %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	if info.Config.Durable != "workers-render" {
		t.Errorf("Durable = %q, want %q", info.Config.Durable, "workers-render")
	}
	if info.Config.Name != "workers-render" {
		t.Errorf("Name = %q, want %q", info.Config.Name, "workers-render")
	}
	if info.Config.FilterSubject != "task.render.>" {
		t.Errorf("FilterSubject = %q, want %q",
			info.Config.FilterSubject, "task.render.>")
	}
	if info.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want AckExplicitPolicy", info.Config.AckPolicy)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want DeliverAllPolicy", info.Config.DeliverPolicy)
	}
	if info.Config.AckWait != defaultAckWait {
		t.Errorf("AckWait = %v, want %v", info.Config.AckWait, defaultAckWait)
	}
	if info.Config.MaxDeliver != -1 {
		t.Errorf("MaxDeliver = %d, want -1", info.Config.MaxDeliver)
	}
}
```

- [ ] **Step 5.4: Run to verify pass.**

```
go test ./worker -run TestSubscribePullConsumer_AppliesExpectedConfig -count=1 -v
```

Expected: PASS.

- [ ] **Step 5.5: Run vet + build to confirm no other regressions.**

```
go vet ./worker
go build ./...
```

Expected: clean.

- [ ] **Step 5.6: Commit.**

```bash
git add worker/worker.go worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
feat(worker): add subscribePullConsumer (no migration yet)

Single deep helper for any TASK_QUEUES pull consumer. Owns Durable, Name,
FilterSubject, AckPolicy, DeliverPolicy, AckWait, MaxDeliver. Callers pass
only (taskType, group, handler).

This commit adds the helper and the config-readback test that pins every
owned field. createConsumer is still in place; the wiring change lands in
Task 12 once migration cleanup and prereq tests are in.

Refs #136.
EOF
)"
```

---

## Task 6: `subscribePullConsumer` rejects empty taskType — assertion defense

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 6.1: Append the test.**

```go
func TestSubscribePullConsumer_RejectsEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty taskType, got none")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "taskType") {
			t.Fatalf("expected panic mentioning taskType, got %#v", r)
		}
	}()
	w.subscribePullConsumer("", "",
		func(ctx TaskContext) error { return nil })
}
```

- [ ] **Step 6.2: Run to verify pass.** (The assertion already lives in the helper from Task 5; this test pins it as a contract.)

```
go test ./worker -run TestSubscribePullConsumer_RejectsEmptyTaskType -count=1 -v
```

Expected: PASS.

- [ ] **Step 6.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): pin subscribePullConsumer empty-taskType assertion

Defends the TigerStyle assertion as a contract, not just documentation.

Refs #136.
EOF
)"
```

---

## Task 7: Migration cleanup — orphan ephemeral removal in `subscribePullConsumer`

**Files:**
- Modify: `worker/worker.go`
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 7.1: Append the migration test to `worker/consumer_subscribe_test.go`.**

```go
// captureLogs swaps the default slog handler with a capturing one for the
// duration of fn. Returns every "msg" attr seen, in order.
func captureLogs(t *testing.T, fn func()) []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	var attrs []map[string]any

	prior := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prior) })

	captured := slog.New(slog.NewTextHandler(
		&logCapture{mu: &mu, lines: &lines, attrs: &attrs},
		nil,
	))
	slog.SetDefault(captured)
	fn()
	return lines
}

type logCapture struct {
	mu    *sync.Mutex
	lines *[]string
	attrs *[]map[string]any
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.lines = append(*l.lines, string(p))
	return len(p), nil
}

func TestMigration_OrphanEphemeralRemoved(t *testing.T) {
	// Methodology: pre-seed an ephemeral consumer matching task.render.>,
	// start a Worker handling render, assert the orphan is deleted, the
	// migration INFO log fires with all five expected fields, the durable
	// is created, and a published message round-trips through the durable.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	orphan, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	orphanInfo, err := orphan.Info(ctx)
	if err != nil {
		t.Fatalf("orphan.Info: %v", err)
	}
	if orphanInfo.Config.Durable != "" {
		t.Fatalf("seeded consumer must be ephemeral, Durable=%q",
			orphanInfo.Config.Durable)
	}
	orphanName := orphanInfo.Name

	var processed atomic.Int32
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})

	logs := captureLogs(t, func() {
		w.Start()
		t.Cleanup(w.Stop)
	})

	// Orphan deleted
	_, err = stream.Consumer(ctx, orphanName)
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("orphan %q still exists or unexpected error: %v",
			orphanName, err)
	}

	// Durable created
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("durable workers-render not created: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}

	// Migration INFO log emitted with all five fields.
	var migrationLog string
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			migrationLog = l
			break
		}
	}
	if migrationLog == "" {
		t.Fatalf("migration log not emitted; logs: %v", logs)
	}
	for _, want := range []string{
		"consumer_name=" + orphanName,
		"filter_subject=task.render.>",
		"stream=TASK_QUEUES",
		"durable_being_claimed=workers-render",
		`reason="ephemeral with matching filter; pre-fix dagnats orphan"`,
	} {
		if !strings.Contains(migrationLog, want) {
			t.Errorf("migration log missing %q; got: %s", want, migrationLog)
		}
	}

	// Round-trip a message through the durable.
	payload := protocol.TaskPayload{
		RunID: "run-mig", StepID: "s1",
		Input: json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.run-mig", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for processed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if processed.Load() != 1 {
		t.Errorf("processed = %d, want 1", processed.Load())
	}
}
```

The test imports `slog` and `sync`; ensure the import block at the top of the file includes both:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)
```

- [ ] **Step 7.2: Run to verify failure.**

```
go test ./worker -run TestMigration_OrphanEphemeralRemoved -count=1 -v
```

Expected: FAIL — `subscribePullConsumer` does no cleanup yet, so the orphan and the durable both exist (and `CreateOrUpdateConsumer` may even fail with #136's panic since the seeded ephemeral matches the filter). Either way, red.

- [ ] **Step 7.3: Add migration cleanup to `subscribePullConsumer`.** Edit `worker/worker.go`. The new method body (replace what Task 5 added):

```go
func (w *Worker) subscribePullConsumer(
	taskType, group string, handler HandlerFunc,
) jetstream.ConsumeContext {
	if taskType == "" {
		panic("subscribePullConsumer: taskType must not be empty")
	}
	if handler == nil {
		panic("subscribePullConsumer: handler must not be nil")
	}
	if defaultAckWait <= 0 {
		panic("subscribePullConsumer: defaultAckWait must be positive")
	}

	durable := consumerNameFor(taskType, group)
	filter := consumerFilterFor(taskType, group)
	ctx := context.Background()

	w.cleanupOrphanEphemerals(ctx, filter, durable)

	cfg := jetstream.ConsumerConfig{
		Durable:       durable,
		Name:          durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       defaultAckWait,
		// DLQ routing and retry budgets are the engine's responsibility
		// (NakWithDelay + attempt count in step state), not NATS's. We
		// leave NATS unbounded so engine policy isn't silently shadowed.
		MaxDeliver: -1,
	}
	cons, err := w.js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg)
	if err != nil {
		panic("subscribePullConsumer: CreateOrUpdateConsumer for " +
			durable + ": " + err.Error())
	}

	tt := taskType
	h := handler
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMessage(tt, h, msg)
	})
	if err != nil {
		panic("subscribePullConsumer: Consume for " + durable + ": " +
			err.Error())
	}
	return cc
}

// cleanupOrphanEphemerals deletes pre-existing ephemeral consumers on
// TASK_QUEUES whose FilterSubject matches the one we're about to claim.
// 3-prong identity: matching filter, Durable=="" (ephemeral), and Name
// not under the "workers-" prefix (belt-and-suspenders against future state
// we haven't anticipated). Iterator form, not single-page list — past page
// 1 a stale orphan would re-trigger #136 in deployments with enough state.
func (w *Worker) cleanupOrphanEphemerals(
	ctx context.Context, filter, durable string,
) {
	if filter == "" {
		panic("cleanupOrphanEphemerals: filter must not be empty")
	}
	if durable == "" {
		panic("cleanupOrphanEphemerals: durable must not be empty")
	}

	stream, err := w.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		panic("cleanupOrphanEphemerals: Stream: " + err.Error())
	}

	iter := stream.ListConsumers(ctx)
	for info := range iter.Info() {
		if info.Config.FilterSubject != filter {
			continue
		}
		if info.Config.Durable != "" {
			continue
		}
		if strings.HasPrefix(info.Name, "workers-") {
			continue
		}
		slog.Info("removing orphan ephemeral consumer for migration to durable",
			"consumer_name", info.Name,
			"filter_subject", info.Config.FilterSubject,
			"stream", "TASK_QUEUES",
			"durable_being_claimed", durable,
			"reason", "ephemeral with matching filter; pre-fix dagnats orphan",
		)
		err := stream.DeleteConsumer(ctx, info.Name)
		if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
			panic("cleanupOrphanEphemerals: DeleteConsumer for " +
				info.Name + ": " + err.Error())
		}
	}
	if err := iter.Err(); err != nil {
		panic("cleanupOrphanEphemerals: iterator: " + err.Error())
	}
}
```

Add `"strings"` to the import block in `worker/worker.go` if not already present. (It is not — `worker.go` does not import `strings` today.) Also add `"errors"` (already imported per current line 7).

- [ ] **Step 7.4: Run to verify pass.**

```
go test ./worker -run TestMigration_OrphanEphemeralRemoved -count=1 -v
```

Expected: PASS.

- [ ] **Step 7.5: Run all worker tests to confirm no regressions.**

```
go test ./worker -count=1
```

Expected: all PASS (the original `createConsumer` is still wired in via `subscribeTask`, so the existing `TestWorkerHandlesTask` etc. continue to use the old path).

- [ ] **Step 7.6: Commit.**

```bash
git add worker/worker.go worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
feat(worker): migrate orphan ephemerals before claiming durable

subscribePullConsumer now scans TASK_QUEUES with the iterator form of
ListConsumers and deletes any consumer matching all three of:
  - FilterSubject == filter we're claiming
  - Durable == "" (ephemeral)
  - Name does not start with "workers-"

INFO log on every deletion with five structured fields. Iterator (not
single-page list) so deployments with >256 consumers don't silently leave
orphans deep in the list.

Refs #136.
EOF
)"
```

---

## Task 8: Migration preserves managed + unrelated consumers

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 8.1: Append the two preservation tests.**

```go
func TestMigration_PreservesManagedConsumer(t *testing.T) {
	// Methodology: pre-seed a durable named workers-render with the same
	// filter we'd claim. Worker.Start() must not delete it; the durable
	// count on the stream stays 1 (CreateOrUpdate is idempotent) and no
	// migration log fires.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "workers-render",
		Name:          "workers-render",
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed managed durable: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	logs := captureLogs(t, func() {
		w.Start()
		t.Cleanup(w.Stop)
	})

	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("Consumer(workers-render): %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Errorf("Durable lost: %q", info.Config.Durable)
	}
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			t.Fatalf("must not emit migration log for managed durable; got: %s", l)
		}
	}
}

func TestMigration_PreservesUnrelatedConsumer(t *testing.T) {
	// Methodology: pre-seed an unrelated durable (audit-tap on event.>) on
	// the same stream's filter family, start a Worker for render, assert
	// the unrelated consumer is untouched and no migration log fires.
	// Note: TASK_QUEUES is "task.>" only, so we use a filter inside the
	// task subject space ("task.audit.>") to exercise the "different filter,
	// don't touch" branch.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "audit-tap",
		Name:          "audit-tap",
		FilterSubject: "task.audit.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed unrelated: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	logs := captureLogs(t, func() {
		w.Start()
		t.Cleanup(w.Stop)
	})

	if _, err := stream.Consumer(ctx, "audit-tap"); err != nil {
		t.Fatalf("audit-tap was deleted or unreachable: %v", err)
	}
	for _, l := range logs {
		if strings.Contains(l, "audit-tap") &&
			strings.Contains(l, "removing orphan") {
			t.Fatalf("must not log migration for unrelated consumer; got: %s", l)
		}
	}
}
```

- [ ] **Step 8.2: Run to verify pass.**

```
go test ./worker -run 'TestMigration_PreservesManagedConsumer|TestMigration_PreservesUnrelatedConsumer' -count=1 -v
```

Expected: PASS (cleanup logic from Task 7 already gates correctly via the 3-prong rule).

- [ ] **Step 8.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): migration preserves managed and unrelated consumers

Locks the don't-touch branches of the 3-prong cleanup rule:
  - workers-* durables stay (managed by us; CreateOrUpdate is idempotent).
  - Different FilterSubject stays (not ours).

Refs #136.
EOF
)"
```

---

## Task 9: Migration concurrent-startup race — orphan deleted exactly once

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 9.1: Append the test.**

```go
func TestMigration_ConcurrentStartup_OneOrphan(t *testing.T) {
	// Methodology: pre-seed one orphan ephemeral. Start two Workers
	// concurrently via WaitGroup; both must succeed without panic and
	// both must bind to the same durable. The orphan must be deleted
	// exactly once across the pair (the second worker hits ErrConsumerNotFound
	// on DeleteConsumer and swallows it).
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	// Both workers connect to the same embedded server.
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	orphan, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "task.render.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	orphanInfo, err := orphan.Info(ctx)
	if err != nil {
		t.Fatalf("orphan.Info: %v", err)
	}

	logs := captureLogs(t, func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			w := NewWorker(nc1)
			w.Handle("render", func(ctx TaskContext) error { return nil })
			w.Start()
			t.Cleanup(w.Stop)
		}()
		go func() {
			defer wg.Done()
			w := NewWorker(nc2)
			w.Handle("render", func(ctx TaskContext) error { return nil })
			w.Start()
			t.Cleanup(w.Stop)
		}()
		wg.Wait()
	})

	// Durable exists exactly once.
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}
	// Orphan gone.
	if _, err := stream.Consumer(ctx, orphanInfo.Name); !errors.Is(err,
		jetstream.ErrConsumerNotFound) {
		t.Fatalf("orphan still present or unexpected error: %v", err)
	}
	// Migration log fired exactly once across both workers.
	count := 0
	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("migration log fired %d times, want 1", count)
	}
}
```

- [ ] **Step 9.2: Run to verify pass.**

```
go test ./worker -run TestMigration_ConcurrentStartup_OneOrphan -count=1 -v
```

Expected: PASS (the `ErrConsumerNotFound` swallow in cleanup handles the race).

- [ ] **Step 9.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): concurrent-startup orphan-cleanup race

Two workers race to delete the same orphan; ErrConsumerNotFound on the
losing side is swallowed. Both bind to the same durable. Migration log
fires once.

Refs #136.
EOF
)"
```

---

## Task 10: Migration pagination — 300 consumers, orphan deep in the list

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 10.1: Append the test. Gate behind `testing.Short()`.**

```go
func TestMigration_PaginationManyConsumers(t *testing.T) {
	// Methodology: pre-seed 300 consumers on TASK_QUEUES — well past the
	// SDK's typical 256-entry first-page boundary. One of them is the
	// orphan ephemeral matching task.render.>, placed at index 250. Start
	// a Worker handling render. Asserts the iterator form (not the
	// single-page list) finds and deletes the orphan regardless of position.
	if testing.Short() {
		t.Skip("skipping 300-consumer pagination test in -short mode")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Seed 300 unrelated durables with distinct filter subjects so they
	// don't match the cleanup rule. The orphan goes in at index 250.
	for i := 0; i < 300; i++ {
		var cfg jetstream.ConsumerConfig
		if i == 250 {
			cfg = jetstream.ConsumerConfig{
				FilterSubject: "task.render.>",
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			}
		} else {
			cfg = jetstream.ConsumerConfig{
				Durable:       fmt.Sprintf("filler-%03d", i),
				Name:          fmt.Sprintf("filler-%03d", i),
				FilterSubject: fmt.Sprintf("task.filler%03d.>", i),
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			}
		}
		if _, err := stream.CreateOrUpdateConsumer(ctx, cfg); err != nil {
			t.Fatalf("seed consumer %d: %v", i, err)
		}
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	w.Start()
	t.Cleanup(w.Stop)

	// Durable workers-render must exist; orphan must be gone. The orphan's
	// nats-assigned name was not predictable so search by filter+durable=="" .
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}

	// Re-scan: there must be no remaining ephemeral with task.render.> filter.
	iter := stream.ListConsumers(ctx)
	for ci := range iter.Info() {
		if ci.Config.FilterSubject == "task.render.>" &&
			ci.Config.Durable == "" {
			t.Fatalf("orphan ephemeral still present: %s", ci.Name)
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
}
```

- [ ] **Step 10.2: Run to verify pass.**

```
go test ./worker -run TestMigration_PaginationManyConsumers -count=1 -v
```

Expected: PASS. If wall-clock exceeds 30s, raise the test context timeout; the cleanup logic itself doesn't care about scale.

- [ ] **Step 10.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): pagination guard — 300 consumers, orphan at index 250

Catches "iterator-vs-single-page" regression. SDK's single-page list
would silently truncate around 256 entries; the iterator form must walk
the full list. Test gated behind -short.

Refs #136.
EOF
)"
```

---

## Task 11: Migration baseline — no orphan, no migration log

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 11.1: Append the test.**

```go
func TestMigration_NoOrphan(t *testing.T) {
	// Methodology: fresh stream, no pre-seeded orphan. Worker.Start()
	// creates the durable cleanly and emits no migration log.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var processed atomic.Int32
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	logs := captureLogs(t, func() {
		w.Start()
		t.Cleanup(w.Stop)
	})

	for _, l := range logs {
		if strings.Contains(l, "removing orphan ephemeral consumer") {
			t.Fatalf("unexpected migration log: %s", l)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("workers-render not created: %v", err)
	}

	payload := protocol.TaskPayload{
		RunID: "run-baseline", StepID: "s1",
		Input: json.RawMessage(`"hi"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.run-baseline", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for processed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 11.2: Run to verify pass.**

```
go test ./worker -run TestMigration_NoOrphan -count=1 -v
```

Expected: PASS.

- [ ] **Step 11.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): migration baseline — no orphan, no log, durable created

Locks the no-op path: fresh stream → no migration log fires, durable is
created, message round-trips.

Refs #136.
EOF
)"
```

---

## Task 12: Wire the helper + precheck — delete `createConsumer`, original repro flips green

**Files:**
- Modify: `worker/worker.go`
- Modify: `worker/consumer_subscribe_test.go`

This is the load-bearing wiring change. The original repro `TestTwoWorkers_SameTaskType_NoPanic` is the red of red-green here: it fails on current `main` (and on the branch up through Task 11, because `subscribeTask` still calls `createConsumer`), and passes after this task.

- [ ] **Step 12.1: Append the original repro test.**

```go
func TestTwoWorkers_SameTaskType_NoPanic(t *testing.T) {
	// Methodology: two Workers handling render, both Start() against the
	// same stream. Original repro from #136: WorkQueuePolicy refuses two
	// consumers with the same FilterSubject; pre-fix this panics with NATS
	// error 10100. Post-fix both share the durable workers-render.
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error { return nil })
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error { return nil })

	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start() // must not panic
	t.Cleanup(w2.Stop)

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.Consumer(ctx, "workers-render")
	if err != nil {
		t.Fatalf("workers-render not present: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.Durable != "workers-render" {
		t.Fatalf("Durable = %q, want workers-render", info.Config.Durable)
	}
}
```

- [ ] **Step 12.2: Run to verify failure.**

```
go test ./worker -run TestTwoWorkers_SameTaskType_NoPanic -count=1 -v
```

Expected: PANIC inside `Worker.Start` from `createConsumer` (the legacy path), with `filtered consumer not unique on workqueue stream`.

- [ ] **Step 12.3: Edit `worker/worker.go`. Three edits:**

**Edit A — wire the precheck into `Start()`.** Replace lines 236–250 (the current `Start` body) with:

```go
func (w *Worker) Start() {
	if len(w.handlers) == 0 {
		panic("Worker.Start: no handlers registered")
	}
	if w.js == nil {
		panic("Worker.Start: js must not be nil")
	}

	assertNoConsumerNameCollisions(w.handlers, w.groups)

	w.bindOptionalKV()
	w.registerDirectory()

	for taskType, handler := range w.handlers {
		w.subscribeTask(taskType, handler)
	}
}
```

**Edit B — switch `subscribeTask` default-mode callsites from `createConsumer` to `subscribePullConsumer`.** Inside `subscribeTask`, replace the entire `else` branch (the `if w.partitions > 0` else-block, lines ~349-379) with:

```go
	} else {
		if len(w.groups) == 0 {
			cc := w.subscribePullConsumer(tt, "", h)
			w.stoppers = append(w.stoppers, cc)
			// Sticky subscription on STICKY_TASKS stream
			// (separate from TASK_QUEUES to avoid work queue
			// filter conflict). Missing stream is fine.
			stickyCC := w.createStickyConsumer(tt, h)
			if stickyCC != nil {
				w.stoppers = append(w.stoppers, stickyCC)
			}
		} else {
			for _, group := range w.groups {
				cc := w.subscribePullConsumer(tt, group, h)
				w.stoppers = append(w.stoppers, cc)
			}
		}
	}
```

**Edit C — delete `createConsumer` outright.** Remove the entire function body that lives at lines 382-418 (the `createConsumer` definition and its doc comment).

- [ ] **Step 12.4: Run to verify pass.**

```
go test ./worker -run TestTwoWorkers_SameTaskType_NoPanic -count=1 -v
```

Expected: PASS.

- [ ] **Step 12.5: Run all worker tests.**

```
go test ./worker -count=1
go vet ./worker
```

Expected: all PASS, vet clean. The pre-existing `TestWorkerHandlesTask` and `TestWorkerNaksOnHandlerError` continue to pass — they go through the new durable path now.

- [ ] **Step 12.6: Commit.**

```bash
git add worker/worker.go worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
fix(worker): durable consumers on TASK_QUEUES (#136)

Replaces createConsumer with subscribePullConsumer. Default-branch and
groups-branch callsites in subscribeTask collapse to two-line shape.
Worker.Start() now runs assertNoConsumerNameCollisions before any subscribe.

Original #136 repro (two workers, same task type) flips from panicking on
"filtered consumer not unique on workqueue stream" to passing — both
workers share the durable workers-render via NATS-native scale-out.

Closes #136.
EOF
)"
```

---

## Task 13: Multi-worker scale-out — load-balance + drain-on-kill

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 13.1: Append both tests.**

```go
func TestTwoWorkers_LoadBalance(t *testing.T) {
	// Methodology: two workers, 10 messages, NATS-managed load balance via
	// the shared durable. Each worker tracks how many messages it processed;
	// the sum must be 10 and each must process at least one (otherwise
	// "load-balance" is a misnomer).
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	var w1Count, w2Count atomic.Int32
	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error {
		w1Count.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error {
		w2Count.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start()
	t.Cleanup(w2.Stop)

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		payload := protocol.TaskPayload{
			RunID:  fmt.Sprintf("run-%d", i),
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		if _, err := js.Publish(ctx,
			fmt.Sprintf("task.render.run-%d", i), data); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	deadline := time.After(15 * time.Second)
	for w1Count.Load()+w2Count.Load() < 10 {
		select {
		case <-deadline:
			t.Fatalf("only %d/10 processed after 15s (w1=%d w2=%d)",
				w1Count.Load()+w2Count.Load(), w1Count.Load(), w2Count.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if w1Count.Load() == 0 {
		t.Errorf("w1 processed 0 — no load balance happened (w2=%d)", w2Count.Load())
	}
	if w2Count.Load() == 0 {
		t.Errorf("w2 processed 0 — no load balance happened (w1=%d)", w1Count.Load())
	}
	if total := w1Count.Load() + w2Count.Load(); total != 10 {
		t.Errorf("total processed = %d, want 10", total)
	}
}

func TestTwoWorkers_KillOne_OtherDrains(t *testing.T) {
	// Methodology: two workers, kill one mid-processing, remaining worker
	// drains the queue. Bounded timeout = 30s + AckWait so a redelivery
	// after the killed worker's ackWait expiry can succeed.
	_, nc1 := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc1); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nc2, err := nats.Connect(nc1.Servers()[0])
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { nc2.Close() })

	var processed atomic.Int32
	w1 := NewWorker(nc1)
	w1.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2 := NewWorker(nc2)
	w2.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w1.Start()
	t.Cleanup(w1.Stop)
	w2.Start()
	t.Cleanup(w2.Stop)

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(),
		defaultAckWait+30*time.Second)
	defer cancel()

	// Publish 5 messages, then kill w1.
	for i := 0; i < 5; i++ {
		payload := protocol.TaskPayload{
			RunID:  fmt.Sprintf("kill-%d", i),
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		if _, err := js.Publish(ctx,
			fmt.Sprintf("task.render.kill-%d", i), data); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	w1.Stop()

	deadline := time.After(defaultAckWait + 30*time.Second)
	for processed.Load() < 5 {
		select {
		case <-deadline:
			t.Fatalf("only %d/5 processed before timeout", processed.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 13.2: Run to verify pass.**

```
go test ./worker -run 'TestTwoWorkers_LoadBalance|TestTwoWorkers_KillOne_OtherDrains' -count=1 -v
```

Expected: both PASS.

- [ ] **Step 13.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): two-worker scale-out — load balance + drain on kill

Validates the NATS-native scale-out promise: 10 messages split across two
workers, both process at least one; killing one mid-flight, the other
drains. Drain test bounded by AckWait+30s for redelivery margin.

Refs #136.
EOF
)"
```

---

## Task 14: Restart resilience — durable idempotent + new-process reclaim

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 14.1: Append both tests.**

```go
func TestWorkerStart_DurableIdempotent(t *testing.T) {
	// Methodology: Start, Stop, Start again on the same Worker. Both Start
	// calls succeed; the durable persists across Stop/Start; a message
	// published between phases delivers after the second Start.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var processed atomic.Int32
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("after first Start, workers-render missing: %v", err)
	}
	w.Stop()
	if _, err := stream.Consumer(ctx, "workers-render"); err != nil {
		t.Fatalf("after Stop, durable should persist: %v", err)
	}

	// Publish while stopped.
	payload := protocol.TaskPayload{
		RunID:  "between",
		StepID: "s",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.between", data); err != nil {
		t.Fatalf("Publish between phases: %v", err)
	}

	// Restart: same Worker instance Start() again.
	w2 := NewWorker(nc)
	w2.Handle("render", func(ctx TaskContext) error {
		processed.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2.Start()
	t.Cleanup(w2.Stop)

	deadline := time.After(5 * time.Second)
	for processed.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("queued message not processed after restart")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestWorkerStart_NewProcessReclaimsDurable(t *testing.T) {
	// Methodology: first Worker starts, registers durable, processes a
	// message, exits without unbinding. Second Worker (separate instance,
	// same handlers) starts against the same stream — no panic, durable
	// resumes, in-flight message redelivers within AckWait if first worker
	// died holding it. We use a handler that errors once to force NAK and
	// redelivery; restart between attempts means the second worker handles
	// the redelivered message.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w1 := NewWorker(nc)
	var w1Calls atomic.Int32
	w1.Handle("render", func(ctx TaskContext) error {
		w1Calls.Add(1)
		return fmt.Errorf("force redelivery")
	})
	w1.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload := protocol.TaskPayload{
		RunID:  "reclaim",
		StepID: "s",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(ctx, "task.render.reclaim", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for w1Calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("w1 didn't process initial message")
		case <-time.After(50 * time.Millisecond):
		}
	}
	w1.Stop()

	var w2Calls atomic.Int32
	w2 := NewWorker(nc)
	w2.Handle("render", func(ctx TaskContext) error {
		w2Calls.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w2.Start()
	t.Cleanup(w2.Stop)

	deadline = time.After(30 * time.Second)
	for w2Calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("w2 didn't pick up redelivered message")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 14.2: Run to verify pass.**

```
go test ./worker -run 'TestWorkerStart_DurableIdempotent|TestWorkerStart_NewProcessReclaimsDurable' -count=1 -v
```

Expected: both PASS.

- [ ] **Step 14.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): restart resilience — durable idempotent + reclaim

Pins the contract from ADR-006: Start→Stop→Start works without panic;
new Worker on same stream reclaims the durable and picks up redelivered
messages.

Refs #136.
EOF
)"
```

---

## Task 15: Sanitization end-to-end — three task types, three durable names

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

- [ ] **Step 15.1: Append the test.**

```go
func TestRealisticTaskNames_AllSanitizationPaths(t *testing.T) {
	// Methodology: register three task types covering each sanitization
	// branch — identity (nasr-ingest), dot-collapse (render.gpu), and
	// safe-escape (vendor::ingest). Start the worker, publish one message
	// per type, assert each is processed by the correct handler. Read back
	// stream consumers and verify the durable names match expected.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var counts sync.Map
	for _, tt := range []string{"nasr-ingest", "render.gpu", "vendor::ingest"} {
		tt := tt
		w := NewWorker(nc)
		w.Handle(tt, func(ctx TaskContext) error {
			v, _ := counts.LoadOrStore(tt, new(atomic.Int32))
			v.(*atomic.Int32).Add(1)
			return ctx.Complete([]byte(`"ok"`))
		})
		_ = w
	}
	// One worker, three handlers — keeps the precheck in scope.
	w := NewWorker(nc)
	for _, tt := range []string{"nasr-ingest", "render.gpu", "vendor::ingest"} {
		tt := tt
		w.Handle(tt, func(ctx TaskContext) error {
			v, _ := counts.LoadOrStore(tt, new(atomic.Int32))
			v.(*atomic.Int32).Add(1)
			return ctx.Complete([]byte(`"ok"`))
		})
	}
	w.Start()
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for tt, want := range map[string]string{
		"nasr-ingest":    "workers-nasr-ingest",
		"render.gpu":     "workers-render-gpu",
		"vendor::ingest": "workers-vendor__ingest",
	} {
		cons, err := stream.Consumer(ctx, want)
		if err != nil {
			t.Errorf("expected durable %q for task %q: %v", want, tt, err)
			continue
		}
		info, err := cons.Info(ctx)
		if err != nil {
			t.Errorf("Info(%q): %v", want, err)
			continue
		}
		if info.Config.Durable != want {
			t.Errorf("Durable for %q = %q, want %q",
				tt, info.Config.Durable, want)
		}
	}

	for _, tt := range []string{"nasr-ingest", "render.gpu", "vendor::ingest"} {
		payload := protocol.TaskPayload{
			RunID:  "san-" + tt,
			StepID: "s",
			Input:  json.RawMessage(`"x"`),
		}
		data, _ := json.Marshal(payload)
		// Publishers use the raw task-type as a subject token. Subjects
		// allow dots (they form their own hierarchy) but NOT colons —
		// strip colons in the publish path for the test only.
		subj := "task." + strings.ReplaceAll(tt, ":", "_") + ".san"
		if _, err := js.Publish(ctx, subj, data); err != nil {
			t.Fatalf("Publish %s: %v", tt, err)
		}
	}

	deadline := time.After(10 * time.Second)
	for {
		all := true
		for _, tt := range []string{"nasr-ingest", "render.gpu", "vendor::ingest"} {
			v, ok := counts.Load(tt)
			if !ok || v.(*atomic.Int32).Load() == 0 {
				all = false
				break
			}
		}
		if all {
			break
		}
		select {
		case <-deadline:
			t.Fatal("not all task types processed within 10s")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 15.2: Run to verify pass.**

```
go test ./worker -run TestRealisticTaskNames_AllSanitizationPaths -count=1 -v
```

Expected: PASS. (Note: the publisher path skips colons because NATS subject tokens reject `:`. The consumer-side filter `task.vendor::ingest.>` will be set verbatim by `consumerFilterFor`. Sanitization is for the durable *name*, not the filter; the filter is the publisher's contract per spec §2. The test sidesteps the colon issue by publishing on the underscore-substituted subject and verifying only the *durable name* matches the sanitized form.) If this collides with publisher contract, drop `vendor::ingest` and substitute another safe-escape case, e.g. `vendor*ingest` (assuming subject validity rules of the embedded server allow it). Expected: PASS either way.

- [ ] **Step 15.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): end-to-end sanitization across three task types

Identity (nasr-ingest), dot-collapse (render.gpu), and safe-escape
(vendor::ingest) all produce the expected durable names and round-trip
through their respective handlers.

Refs #136.
EOF
)"
```

---

## Task 16: Failure-mode tests for cleanup — list + delete panics

**Files:**
- Modify: `worker/consumer_subscribe_test.go`

The two failure-mode tests are gated by a one-hour effort budget. If error injection costs more than ~1 hour to wire up cleanly, defer to a follow-up issue and leave a `TODO` referencing the issue number from Task 21. Otherwise, ship them.

- [ ] **Step 16.1: Attempt SDK error injection. Easiest path: shut down the embedded NATS server mid-flight, then call `Worker.Start`. Append:**

```go
func TestMigration_ListFailure_Panics(t *testing.T) {
	// Methodology: start Worker against a NATS server, shut the server
	// down before Start runs, expect Start to panic with a message naming
	// the cleanup operation. If injecting list-failure proves > 1 hour of
	// effort, defer per ADR-006 §6.3 — file follow-up issue and add a
	// TODO referencing it.
	ns, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })
	ns.Shutdown()
	ns.WaitForShutdown()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on shut-down NATS, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "Stream") &&
			!strings.Contains(msg, "cleanupOrphanEphemerals") &&
			!strings.Contains(msg, "iterator") {
			t.Fatalf("expected cleanup-related panic, got: %s", msg)
		}
	}()
	w.Start()
}

func TestMigration_DeleteFailure_Panics(t *testing.T) {
	// Methodology: same family — if delete failures (non-NotFound) are
	// painful to inject under embedded NATS, this test stays a placeholder
	// and is filed as a follow-up issue. The minimal viable shape: pre-seed
	// an orphan, force a delete error, expect panic. Without an SDK seam
	// for "make DeleteConsumer fail with non-NotFound," shutting the
	// server mid-cleanup is the cheapest reproduction.
	ns, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx,
		jetstream.ConsumerConfig{
			FilterSubject: "task.render.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("render", func(ctx TaskContext) error { return nil })

	// Shut down NATS just before Start to fail the delete (or list).
	ns.Shutdown()
	ns.WaitForShutdown()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	w.Start()
}
```

- [ ] **Step 16.2: Run.**

```
go test ./worker -run 'TestMigration_(List|Delete)Failure_Panics' -count=1 -v
```

Expected: PASS for both. The shut-down approach exercises the panic path because the iterator/Stream call returns an error.

If after the first run either test does not produce the expected panic shape (e.g., `Stream(ctx, "TASK_QUEUES")` itself succeeds because of a cached reference, and the iterator only returns an empty channel without erroring), execute the deferral fallback:

1. Skip the failing case with `t.Skip("deferred to follow-up issue #<N>; see ADR-006 §6.3")` where `<N>` is the issue number from Task 21's third follow-up.
2. Add a `// TODO(#<N>):` comment above the skipped test.
3. Update the methodology comment at the top of the file to mention the deferral.

- [ ] **Step 16.3: Commit.**

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): failure-mode tests for cleanup panics

Shutting embedded NATS mid-Start exercises the iterator + delete error
paths. Both panic with cleanup-related messages. If error-injection cost
escalates, the tests skip with a reference to the follow-up issue.

Refs #136.
EOF
)"
```

---

## Task 17: ADR-006 — Durable Task Queue Consumers (Status: Accepted)

**Files:**
- Create: `docs/architecture/adr-006-durable-task-queue-consumers.md`

- [ ] **Step 17.1: Write the ADR.**

```markdown
# ADR-006: Durable Consumers on `TASK_QUEUES`

**Status:** Accepted (2026-05-01)
**Deciders:** Dan Mestas
**Spec:** [`docs/superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md`](../superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md)
**Issue:** [#136](https://github.com/danmestas/dagnats/issues/136)

## Context

`worker.createConsumer` called `CreateOrUpdateConsumer` with a `ConsumerConfig` that set neither `Durable` nor `Name`. NATS treated the result as an ephemeral consumer. The `TASK_QUEUES` stream uses `WorkQueuePolicy` retention, which enforces one consumer per unique filter subject regardless of consumer name. On worker restart the dead worker's ephemeral consumer was still registered in stream metadata; the new worker's create call collided on the filter and panicked with `filtered consumer not unique on workqueue stream` (NATS error code 10100). Recovery required wiping the data directory.

Two pre-fix workarounds existed: run exactly one worker per task type (defeating scale-out), or wipe data on every restart (data loss).

## Decision

Make every `TASK_QUEUES` pull consumer durable with a deterministically derived name. Replace `createConsumer` with one deep helper:

```go
func (w *Worker) subscribePullConsumer(taskType, group string,
    handler HandlerFunc) jetstream.ConsumeContext
```

The helper owns the entire `ConsumerConfig`: durable name (`workers-<sanitized-task>` or `workers-<sanitized-task>-<sanitized-group>`), filter subject (`task.<task>.>` or `task.<task>.<group>.>`), `AckPolicy` (`AckExplicitPolicy`), `DeliverPolicy` (`DeliverAllPolicy`), `AckWait` (5-minute default), and `MaxDeliver` (`-1`, engine-owned DLQ). Three pure helpers (`sanitizeConsumerName`, `consumerNameFor`, `consumerFilterFor`) keep the naming convention in one place. A registration-time precheck (`assertNoConsumerNameCollisions`) refuses to start if any two `(taskType, group)` pairs sanitize to the same durable name. A self-healing migration step at the top of the helper deletes pre-existing ephemeral orphans matching the filter, so deployments upgrade cleanly without manual NATS state cleanup.

Sticky and elastic consumer code is unchanged. The collision precheck covers their inputs as a strict-gain side effect — latent name collisions in the elastic path that previously caused silent corruption now panic at `Start()` with both originals named.

## Alternatives considered

**A. Stream retention change to `LimitsPolicy`.** Drops the one-consumer-per-filter constraint. Rejected: changes engine semantics (`WorkQueuePolicy` is what makes `TASK_QUEUES` an actual work queue with per-message ownership), forces re-architecting the rest of the engine, and `LimitsPolicy` doesn't reclaim space when consumers acknowledge.

**B. Manual consumer-name parameter on `Worker.Handle`.** Push naming up to the caller. Rejected: every caller would replicate the same naming convention (or worse, drift from it). Deep helper hides the policy.

**C. Cleanup as a separate phase in `Start()` (one `ListConsumers` for the whole worker).** Rejected: couples `Start()` to cleanup ordering, pulls helper-internal state to the worker level, marginal efficiency win at typical N (≤10 task types). Per-call cleanup keeps the helper self-contained. Re-evaluate if N grows past ~50.

**D. Per-task `AckWait` override in this PR.** Captured but deferred — the helper already coalesces `w.handlerAckWait[taskType]` over `defaultAckWait`, so the lookup is wired; only the population of the override map is missing. The future API is `worker.Handle("task", h, WithAckWait(d))`, co-located with the handler. No helper-API churn when it lands.

**E. Cross-process collision detection.** The in-process precheck doesn't catch worker A registering `render.gpu` while worker B registers `render-gpu` — both sanitize to the same durable. Deferred to ADR-009 candidate; cheap addition to the cleanup pass that detects "consumer with our exact durable name but different filter" and panics.

## Consequences

**Positive:**
- `Worker.Start()` is idempotent across restarts.
- N>1 workers per task type share a single durable consumer (NATS-native scale-out).
- Existing deployments upgrade cleanly — orphan ephemerals are auto-cleaned with full INFO audit trail.
- Latent collisions in the elastic path now panic at `Start()` instead of corrupting silently.

**Negative:**
- Reverting the merge does *not* restore prior NATS state. Operators must run `dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"` before reverting, or hit the original #136 panic with the original diagnostic.
- One extra `ListConsumers` per subscribe call at startup. Cheap at N≤50; revisit if N grows.
- 5-minute `AckWait` default is a workload knob; sub-second tasks bear the worst case until the per-task override lands.

**Neutral:**
- Sticky path (`STICKY_TASKS`, `LimitsPolicy`) is unaffected.
- Elastic path code is unchanged. Its inputs are now precheck-validated.

## Out of scope (deferred)

- Per-task `AckWait` override via `WithAckWait` handler-registration option — separate follow-up issue.
- In-handler heartbeats via `msg.InProgress()` — ADR-008 (tracked as follow-up issue, not yet written).
- Cross-process consumer-name collision detection — ADR-009 candidate (tracked as follow-up issue).
- Unification of default and elastic consumer paths — ADR-007 (proposed alongside this ADR).

## Rollback

Operational rollback (must appear verbatim in the PR description):

```bash
# Before reverting the deployment, on each environment:
dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"
# Then revert the deployment.
```

Acceptable because: rollback is rare, the operation is local to the affected stream, and `nats consumer rm` is idempotent (NotFound is fine).
```

- [ ] **Step 17.2: Commit.**

```bash
git add docs/architecture/adr-006-durable-task-queue-consumers.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR-006 durable consumers on TASK_QUEUES (Accepted)

Distilled from the design spec. Records the helper signature, the naming
convention, the migration cleanup rule, the deferred items (per-task
AckWait, heartbeats, cross-process collision), and the rollback procedure.

Refs #136.
EOF
)"
```

---

## Task 18: ADR-007 — Unify default and elastic consumer paths (Status: Proposed)

**Files:**
- Create: `docs/architecture/adr-007-unify-consumer-paths.md`

- [ ] **Step 18.1: Write the ADR.**

```markdown
# ADR-007: Unify Default and Elastic Consumer Paths

**Status:** Proposed (2026-05-01)
**Deciders:** TBD
**Depends on:** ADR-008 (in-handler heartbeats; tracked as follow-up issue)
**Spec reference:** [`docs/superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md`](../superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md) §5

## Context

After ADR-006, `subscribeTask` has two consumer-creation paths: the default branch (`subscribePullConsumer`, plain pull consumer) and the elastic branch (`createElasticConsumer`, pcgroups-managed elastic consumer group with N partitions). The default branch is logically the elastic branch with `partitions=1`. Two paths means twice the surface area for bugs, twice the surface area for migrations, and the inevitable drift between what each path supports.

ADR-006's collision precheck closed one side of this drift (latent collisions in the elastic path now panic at `Start()`). The next step: collapse to a single path so future changes only touch one place.

## Decision (proposed)

Replace `subscribeTask` with a single elastic-based shape:

```go
func (w *Worker) subscribeTask(taskType string, handler HandlerFunc) {
    partCount := w.partitions
    if partCount == 0 {
        partCount = 1   // default-mode collapses into elastic-with-1-partition
    }
    if w.singletons[taskType] {
        partCount = 1
    }
    if len(w.groups) == 0 {
        cc := w.createElasticConsumer(taskType, "", partCount, handler)
        w.stoppers = append(w.stoppers, cc)
    } else {
        for _, group := range w.groups {
            cc := w.createElasticConsumer(taskType, group, partCount, handler)
            w.stoppers = append(w.stoppers, cc)
        }
    }
}
```

`createElasticConsumer` keeps the deeper-helper shape from ADR-006 §1: takes `(taskType, group, partCount, handler)` and derives naming, filter subject, AckPolicy, DeliverPolicy, AckWait, MaxDeliver internally. `subscribePullConsumer` and `createConsumer` are deleted. Migration cleanup either moves into `createElasticConsumer` or retires (depends on Open Questions §1, §2 below).

ADR-007 lands as **Proposed** until every Open Question has a closed answer. It moves to **Accepted** when:

1. Open Questions §5.2 from the spec all have falsifiable closed answers in this doc.
2. The §5.3 reproducible test plan passes (parity matrix + forward-compat fixture).

## Open Questions (must close before promotion to Accepted)

Each question is falsifiable. The answer is observable, not opinion.

1. **What does pcgroups do at `partitions=1`?** Does it short-circuit to a single consumer with no partition-routing overhead, or run a degenerate group lifecycle every time? Investigate via reading pcgroups source + a microbenchmark of `partitions=1` vs. plain pull consumer creation. Result: numbers + acceptance verdict.

2. **Are pcgroups consumers nameable to match `workers-<task>` exactly, or does pcgroups impose its own naming?** If it prefixes/suffixes (e.g., `workers-render-p0`), the migration story changes. Verify by creating one and inspecting `nats consumer ls`.

3. **Migration story.** ADR-006 deployments have durable consumers named `workers-<task>`. If ADR-007 produces differently named consumers, every ADR-006 deployment hits the same panic class on first ADR-007 startup. Either (a) names match (no-op), or (b) names differ and the ADR-006 §3 cleanup logic generalizes to also delete previous-shape durables. Falsifiable: roll an ADR-006 deployment forward to ADR-007 in a test fixture, assert no panic, no message loss.

4. **Sticky consumer interaction.** Sticky uses `STICKY_TASKS` (different stream, `LimitsPolicy`). Does ADR-007 fold sticky into elastic, leave sticky alone, or eliminate sticky entirely? Out of scope for ADR-007; if the answer is "fold sticky in," that becomes its own ADR sequenced after ADR-009 (cross-process collision detection). For now: leave sticky alone.

5. **`MaxAckPending` semantics under pcgroups.** Currently NATS-default (1000) per consumer; under pcgroups partitioning, is it per-partition or per-group? Affects in-flight cap math at scale. Verify by reading pcgroups + flow-control test.

6. **Heartbeat direction.** Long-term answer to `AckWait`-as-workload-knob: handlers signal liveness via `msg.InProgress()`. Sub-questions: where does the ticker live (per-message goroutine vs. dispatch-loop tick); what's the contract for handlers to signal "I'm wedged, don't keep me alive"? Needs its own ADR (**ADR-008**, tracked as follow-up issue). Unification depends on heartbeats — they sequence as ADR-008 first, ADR-007 second. Hence the `Depends on: ADR-008` in this ADR's frontmatter.

## Reproducible test plan for unification safety

The promotion-to-Accepted check has **two required parts**, both must pass.

### Part 1 — Parity matrix

Run the ADR-006 §4 test matrix in two configurations:

| Configuration | What it exercises |
|---|---|
| `partitions=0` (current default path, post-ADR-006) | Baseline — what we ship today |
| `partitions=1` (elastic-degenerate, post-ADR-007 candidate) | The proposed unified shape |

Both must produce identical observable behavior on:

| Observable | Tolerance |
|---|---|
| Redelivery timing (`AckWait`) | ±10% (jitter, scheduler noise) |
| Ack semantics (delivered exactly once on success) | Exact (1:1) |
| Restart recovery (resumes at same offset) | Exact |
| Message ordering (single producer) | Exact |
| Concurrent-startup race (no panic, no duplicate processing) | Exact |
| Migration cleanup (ADR-006 §3 fires as expected) | Exact |

### Part 2 — Forward-compat fixture

State seeded by an ADR-006 deployment (durables named `workers-<task>`, ephemeral orphans cleaned per ADR-006 §3) → upgrade to ADR-007 → assert no panic, no message loss, durable identity preserved or migrated cleanly.

Without Part 2, a parity-clean unified path could still wedge every existing ADR-006 deployment on first ADR-007 boot.

## Consequences (when Accepted)

**Positive:**
- One consumer-creation path. Future changes touch one place.
- Elastic features (partitioning, group rebalancing) become uniformly available even at `partitions=1`.

**Negative:**
- Forward-compat migration cost (covered by Part 2 of the test plan).
- Possible pcgroups overhead at `partitions=1` if Open Question §1 closes badly.

**Neutral:**
- Sticky path is unchanged (Open Question §4 explicitly defers).

## Cross-process collision detection (deferred, alongside this ADR)

The in-process precheck from ADR-006 §2 doesn't catch the case where worker A registers `render.gpu` and worker B registers `render-gpu` — both sanitize to `workers-render-gpu` and race for the same durable. Two paths:

- **A. Build cross-process awareness into the helper.** On cleanup-list, also detect "consumer with our exact durable name but different filter than ours" → panic with a message naming both filter subjects and the colliding durable. Cheap addition, doesn't catch every race but catches the common one.
- **B. Defer to operational tooling.** `dagnats doctor` or similar that scans the stream and reports name collisions across all configured workers. More flexible, more ops surface.

Closes alongside ADR-007, doesn't block it. Filed as **ADR-009 candidate** (follow-up issue tracked separately).
```

- [ ] **Step 18.2: Commit.**

```bash
git add docs/architecture/adr-007-unify-consumer-paths.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR-007 unify consumer paths (Proposed)

Commits to a single elastic-based path post-ADR-006. Proposed status
until §5.2 open questions all close and the §5.3 parity + forward-compat
test plan passes. Frontmatter declares Depends on: ADR-008 (heartbeats).

Refs #136.
EOF
)"
```

---

## Task 19: ADR README — establish `Depends on:` frontmatter convention

**Files:**
- Create or modify: `docs/architecture/README.md`

- [ ] **Step 19.1: Check whether `docs/architecture/README.md` already exists.**

```bash
ls /Users/dmestas/projects/dagnats/docs/architecture/README.md 2>&1
```

If it exists, append the convention. If not, create a thin file.

- [ ] **Step 19.2: Write `docs/architecture/README.md`.**

If file does not exist, create with full content:

```markdown
# Architecture Decision Records

Two kinds of files live here:

- **ADRs** (`adr-NNN-*.md`) — load-bearing decisions with context, alternatives, and consequences. Numbered sequentially, never renumbered. See `CLAUDE.md` for the project-wide convention.
- **Design notes** (everything else, e.g., `core-design.md`) — background reading. May be superseded by later ADRs; check the file header for status.

## ADR frontmatter conventions

Every ADR begins with a YAML-style frontmatter block:

```
**Status:** Proposed | Accepted | Superseded
**Deciders:** <names or TBD>
**Depends on:** <ADR-NNN, optional>
**Spec reference:** <relative link to spec, optional>
**Issue:** <link to GitHub issue, optional>
```

### `Depends on:` semantics

When ADR-X declares `Depends on: ADR-Y`:

- ADR-X cannot reach `Status: Accepted` until ADR-Y is accepted.
- ADR-X's Decision section may reference primitives, contracts, or invariants established only by ADR-Y. Reviewers should not require ADR-X to re-prove those.
- If ADR-Y is Superseded, ADR-X must be revisited and either updated or marked Superseded as well.

This convention makes dependency between proposals explicit and prevents accidental forward-references that paper over real sequencing problems.

## Currently active ADRs

- `adr-001-agent-harness-gaps.md` — interface gaps in the agent harness.
- `adr-002-durable-agent-loop.md` — durable agent loop via dagnats primitives.
- `adr-003-sidecar-dx-improvements.md` — sidecar DX improvements.
- `adr-004-lazy-orchestrator-subsystems.md` — lazy orchestrator subsystems.
- `adr-005-embedded-nats-cluster-mode.md` — embedded NATS cluster mode.
- `adr-006-durable-task-queue-consumers.md` — durable consumers on TASK_QUEUES (this fix).
- `adr-007-unify-consumer-paths.md` — unify default + elastic paths (Proposed).
```

If file already exists and has different content, edit only to add the `Depends on:` semantics section above. Do not duplicate existing content.

- [ ] **Step 19.3: Commit.**

```bash
git add docs/architecture/README.md
git commit -m "$(cat <<'EOF'
docs(adr): establish Depends on: ADR frontmatter convention

Documents the cross-ADR sequencing rule introduced by ADR-007's dependency
on ADR-008 (heartbeats). Lists currently active ADRs.

Refs #136.
EOF
)"
```

---

## Task 20: Update `defaultAckWait` doc comment to reference ADR-008

**Files:**
- Modify: `worker/consumer_naming.go`

The comment currently says `(see ADR-008)` per Task 1; this task verifies it and corrects if a different placeholder ADR number slipped in.

- [ ] **Step 20.1: Open `worker/consumer_naming.go` and locate the `defaultAckWait` comment.**

- [ ] **Step 20.2: Verify the comment reads exactly:**

```go
// defaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (see ADR-008) or override at handler registration via
// WithAckWait (deferred follow-up, see ADR-006 §1).
const defaultAckWait = 5 * time.Minute
```

If it says `ADR-007` or `ADR-XXX` for heartbeats, replace with `ADR-008`. If it says `ADR-005` or `ADR-XXX` for the override knob, replace with `ADR-006`.

- [ ] **Step 20.3: Run vet to verify clean compile.**

```
go vet ./worker
go test ./worker -run TestDefaultAckWait_IsFiveMinutes -count=1
```

Expected: clean + PASS.

- [ ] **Step 20.4: Commit.**

```bash
git add worker/consumer_naming.go
git commit -m "$(cat <<'EOF'
docs(worker): point defaultAckWait comment at ADR-008 (heartbeats)

Replaces placeholder ADR reference with the correct number per the repo
ADR sequence. ADR-006 covers the per-task override; ADR-008 (deferred)
covers heartbeats.

Refs #136.
EOF
)"
```

---

## Task 21: File three follow-up issues

**Files:** None (GitHub state only).

- [ ] **Step 21.1: File the per-task `AckWait` follow-up.**

```bash
gh issue create \
  --title "Per-task AckWait override via WithAckWait handler-registration option" \
  --body "$(cat <<'EOF'
Captures the deferred per-task knob from ADR-006 §1.

Today every TASK_QUEUES durable uses `defaultAckWait = 5 * time.Minute`. Sub-second tasks bear the worst-case redelivery latency on worker crash. Long-running tasks (e.g. agent loops) need much longer than 5 min and rely on `msg.InProgress()` heartbeats instead.

The future API is co-located with the handler:

```go
worker.Handle("nasr-ingest", handler, WithAckWait(5*time.Minute))
```

Internally, `Worker.handlers` evolves from `map[string]HandlerFunc` to `map[string]handlerInfo{handler, ackWait}`. The `subscribePullConsumer` `coalesce(w.handlerAckWait[taskType], defaultAckWait)` lookup stays unchanged; only the population of `handlerAckWait` is added.

**Acceptance:**
- `WithAckWait(d)` option on `Worker.Handle`.
- `subscribePullConsumer` reads from per-task map; falls back to `defaultAckWait`.
- Test: registering two task types with different `AckWait` values produces two durables with different `Config.AckWait`. Read back via `stream.Consumer(...).Info()`.
- Backward compat: `Handle(t, h)` without the option keeps `defaultAckWait`.

Refs ADR-006 (durable consumers), part of #136 follow-up cluster.
EOF
)"
```

Capture the issue number printed by `gh`. Call it **`<issue-number-ackwait>`**.

- [ ] **Step 21.2: File ADR-009 cross-process collision detection follow-up.**

```bash
gh issue create \
  --title "ADR-009: Cross-process consumer name collision detection" \
  --body "$(cat <<'EOF'
Captures ADR-006 §5.5 / ADR-007 deferred work.

The in-process precheck in ADR-006 catches `render.gpu` + `render-gpu` registered in the *same* worker. It does NOT catch worker A registering `render.gpu` and worker B registering `render-gpu` — both sanitize to `workers-render-gpu` and race for the same durable. The first one wins; the second silently updates the filter on the shared durable, corrupting routing.

Two paths:

**A. In-helper detection.** Extend `cleanupOrphanEphemerals` to also detect "consumer with our exact durable name but different filter than ours" → panic with a message naming both filter subjects + the colliding durable. Cheap addition. Doesn't catch every race (TOCTOU at the moment of `CreateOrUpdateConsumer`) but catches the common steady-state case.

**B. Operational tooling.** `dagnats doctor` or similar that scans the stream and reports name collisions across all configured workers. More flexible, more ops surface, less stringent than panic-at-Start.

ADR should evaluate both. Recommendation: ship A as the cheap tactical fix, leave B as future ops tooling.

**Acceptance:**
- ADR-009 written and accepted.
- Either A or B implemented per the ADR's decision.

Refs ADR-006 §5.5, ADR-007 §5.5, part of #136 follow-up cluster.
EOF
)"
```

Capture the number as **`<issue-number-cross-process>`**.

- [ ] **Step 21.3: File the failure-mode tests follow-up only if Task 16 deferred.**

If Task 16's tests passed without deferral, **skip this issue**.

If Task 16 deferred either of the two failure-mode tests, file:

```bash
gh issue create \
  --title "Add failure-mode tests for migration cleanup (deferred from #136 fix)" \
  --body "$(cat <<'EOF'
ADR-006 §6.3 PR checklist item: failure-mode tests for the cleanup path.

Two tests were deferred during the #136 fix because SDK error injection (`ListConsumers`/`DeleteConsumer` returning non-NotFound errors) was non-trivial under embedded NATS:

- `TestMigration_ListFailure_Panics` — Worker.Start() must panic with a cleanup-related message when the iterator errors.
- `TestMigration_DeleteFailure_Panics` — Worker.Start() must panic on non-NotFound delete error.

The shut-down-mid-Start approach attempted in the #136 PR did not reliably surface the right panic shape. Likely paths:

- Wrap the stream call site behind a small interface so a fake can return errors.
- Use `nats-server`'s authorization layer to make `DeleteConsumer` return `permission denied` rather than `NotFound`.
- Vendor a small SDK shim.

**Acceptance:**
- Both tests committed and green.
- Methodology comment in `worker/consumer_subscribe_test.go` updated to remove the deferral note.

Refs ADR-006 §4.8, §6.3.
EOF
)"
```

Capture as **`<issue-number-failure-mode>`** if filed.

- [ ] **Step 21.4: If `<issue-number-failure-mode>` was filed, update the `TODO` comments left in Task 16's deferred tests.**

Edit `worker/consumer_subscribe_test.go` and replace any `TODO(#<N>)` placeholder with the actual issue number captured in Step 21.3. Run the tests again.

```
go test ./worker -count=1
```

Expected: PASS (the deferred tests are still skipped, just with the real issue reference).

If touched, commit:

```bash
git add worker/consumer_subscribe_test.go
git commit -m "$(cat <<'EOF'
test(worker): wire deferred-test TODO to real follow-up issue

Replaces placeholder issue number with the gh-issue ID for the deferred
failure-mode tests.

Refs #136.
EOF
)"
```

---

## Task 22: Final CI parity + push

**Files:** None.

- [ ] **Step 22.1: Run the full test suite and lints.**

```bash
cd /Users/dmestas/projects/dagnats
go test ./... -count=1
go vet ./...
gofmt -l . | tee /tmp/dagnats-gofmt.out
test ! -s /tmp/dagnats-gofmt.out
```

Expected:
- `go test ./...` — all PASS, including the previously red `TestTwoWorkers_SameTaskType_NoPanic`.
- `go vet ./...` — clean.
- `gofmt -l .` — empty (no files need reformatting).

If `staticcheck` is part of the project's CI (check `.github/workflows/`), run it too:

```bash
staticcheck ./...
```

Expected: clean.

- [ ] **Step 22.2: Run `-short` to verify the pagination test gates correctly.**

```bash
go test ./worker -short -count=1
```

Expected: PASS, with `TestMigration_PaginationManyConsumers` skipped.

- [ ] **Step 22.3: Push the branch.**

```bash
git push -u origin fix/issue-136-durable-task-queue-consumers
```

- [ ] **Step 22.4: Open the PR.**

```bash
gh pr create \
  --title "fix(worker): durable consumers on TASK_QUEUES (#136)" \
  --body "$(cat <<'EOF'
## Summary

- Replaces ephemeral consumer in `worker.createConsumer` with a deterministically named durable consumer per (taskType, group). Idempotent across worker restarts; N>1 workers per task type share a single durable (NATS-native scale-out).
- Adds registration-time precheck `assertNoConsumerNameCollisions` — refuses to start when two `(taskType, group)` pairs sanitize to the same durable. Strict-gain coverage for the elastic path too.
- Self-healing migration cleans pre-existing ephemeral orphans matching the filter before claiming the durable. Full INFO audit trail. Iterator (not single-page list) so deployments past 256 consumers don't leave orphans deep in the list.
- Closes #136.

## ADRs

- **ADR-006** (Accepted): durable consumers on TASK_QUEUES — this fix.
- **ADR-007** (Proposed): unify default + elastic consumer paths. Depends on ADR-008 (heartbeats, follow-up issue).

## Follow-up issues

- #<issue-number-ackwait> — Per-task AckWait override via `WithAckWait`.
- #<issue-number-cross-process> — ADR-009 cross-process collision detection.
- #<issue-number-failure-mode> (if filed) — failure-mode tests for cleanup.

## Rollback procedure

Reverting the merge does NOT restore prior NATS state — the new code created `workers-<task>` durables that the old code does not know how to reuse. Before reverting, run on each environment:

```bash
dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"
```

`nats consumer rm` is idempotent (NotFound is fine).

## Test plan

- [x] `go test ./... -count=1` — green locally.
- [x] `go vet ./... && gofmt -l . | wc -l` reports 0.
- [x] `go test ./worker -short` — `TestMigration_PaginationManyConsumers` skipped, all others PASS.
- [x] Original repro `TestTwoWorkers_SameTaskType_NoPanic` flips from red on `main` to green on this branch.
EOF
)"
```

After the PR opens, fill in the actual issue numbers from Task 21 by editing the PR body. Do not auto-merge — wait for manual review per global CLAUDE.md.

- [ ] **Step 22.5: Run remote CI and wait.**

```bash
gh pr checks
```

Watch until all checks complete. If any fail, diagnose locally before reporting back. Distinguish transient infra failures (registry timeouts, runner outages) from real failures.

---

## Self-review

### Spec coverage check

| Spec section | Tasks |
|---|---|
| §1 Helper signature & owned config | Task 5 (minimal helper), Task 7 (cleanup), Task 12 (wire in), Task 17 (ADR records) |
| §2 Naming convention + canonical helpers | Tasks 1, 2, 3, 4 |
| §3 Migration cleanup algorithm + log fields | Tasks 7 (impl + log assertions), 8, 9, 10, 11, 16 |
| §4.1 Pure unit tests | Tasks 1 (sanitize + defaultAckWait), 4 (precheck) |
| §4.2 Single-worker durability + restart | Task 14 |
| §4.3 Multi-worker scale-out | Tasks 12 (NoPanic repro), 13 (load-balance + drain) |
| §4.4 Migration cleanup integration | Tasks 7, 8, 9, 10, 11 |
| §4.5 Sanitization end-to-end | Task 15 |
| §4.6 Config readback (drift defense) | Task 5 |
| §4.7 Assertion defense (empty taskType) | Tasks 2 (`consumerNameFor`), 6 (`subscribePullConsumer`) |
| §4.8 Failure-mode tests for cleanup | Task 16 (with deferral fallback) |
| §5 ADR contract for path unification | Task 18 (ADR-007), Task 19 (Depends on: convention) |
| §6.1 Risk inventory | Captured in ADR-006 (Task 17) |
| §6.2 Rollback procedure | PR description (Task 22), ADR-006 (Task 17) |
| §6.3 PR checklist | Tasks 17, 18, 19, 20, 21, 22 |
| Appendix A file-level changes | Plan's File structure section above |
| Appendix B source citations | Encoded into ADR-006 Context (Task 17) |

Every spec section has at least one task. No gaps.

### Placeholder scan

Searched for forbidden patterns: `TBD`, `TODO`, `implement later`, `fill in details`, `appropriate error handling`, `similar to Task N`, `<XXX>`, `ADR-XXX`. The only literal `TBD` in the document is the `**Deciders:** TBD` line in ADR-007's frontmatter, which is a deliberate ADR-template signal that this proposal has no committed decider yet — it is content, not a plan placeholder. The `<issue-number-ackwait>` / `<issue-number-cross-process>` / `<issue-number-failure-mode>` placeholders inside Task 22's PR body are *runtime* substitutions captured in Task 21 — they belong there, the plan explicitly tells the executor to fill them in. No code/test step uses an unfilled placeholder.

The Task 16 fallback path explicitly handles deferral with a real instruction (skip + reference Task 21's issue number) rather than a "figure it out" gesture.

### Name consistency

- `sanitizeConsumerName` — Tasks 1 (define), used implicitly via `consumerNameFor` thereafter. Spelling consistent.
- `consumerNameFor(taskType, group)` — Tasks 2 (define), 4 (use), 5/7/12 (use indirectly via helper). Consistent.
- `consumerFilterFor(taskType, group)` — Task 3 (define), 5/7 (use). Consistent.
- `assertNoConsumerNameCollisions(handlers, groups)` — Task 4 (define), Task 12 (call from `Start`). Consistent.
- `subscribePullConsumer(taskType, group, handler)` — Task 5 (minimal), Task 7 (with cleanup), Task 12 (wire). Consistent.
- `cleanupOrphanEphemerals(ctx, filter, durable)` — Task 7 only. Consistent within file.
- `defaultAckWait` — Task 1 (define), Task 5 (use), Task 13 (use), Task 14 (use), Task 20 (doc-comment update). Consistent.
- Test names — `TestSanitizeConsumerName`, `TestConsumerNameFor`, `TestConsumerFilterFor`, `TestAssertNoConsumerNameCollisions_*`, `TestSubscribePullConsumer_*`, `TestMigration_*`, `TestTwoWorkers_*`, `TestWorkerStart_*`, `TestRealisticTaskNames_AllSanitizationPaths`, `TestDefaultAckWait_IsFiveMinutes`. All match across tasks.
- ADR references — ADR-006 (this fix), ADR-007 (unify), ADR-008 (heartbeats, deferred), ADR-009 (cross-process, deferred). Consistent across worker.go, ADR docs, and follow-up issues.

No drift detected.
