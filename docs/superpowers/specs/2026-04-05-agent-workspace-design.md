# Agent Workspace

**Status:** Design
**Date:** 2026-04-05
**Depends on:** orbit.go/kvcodec

## Problem

DagNats workflow steps communicate through DAG edges — step A's output becomes
step B's input. This works for linear data flow but fails for **collaborative
agent workflows** where:

- Multiple agents need to read and write shared files concurrently
- An agent needs persistent memory that survives across steps and runs
- Large artifacts (code, conversations, test output) bloat step I/O
- Secrets passed through step inputs are plaintext in NATS
- Agents can't react to each other's work in real time

Every agentic coding framework (Claude Code, Cursor, Codex) has this same gap:
agents can't collaborate on shared mutable state without passing everything
through prompts or step inputs. The DAG is a pipeline, not a workspace.

## Insight

NATS KV + orbit.go/kvcodec gives us the missing primitive. kvcodec wraps
JetStream KV with transparent key and value encoding:

- **PathCodec** — `/run/abc/files/main.go` ↔ `run.abc.files.main_go`
- **ValueCodec** — compress (zstd), encrypt (AES-GCM), or chain both
- **KV Watch** — real-time notification on path prefix changes

Combined with DagNats's existing KV watch infrastructure (used for signals and
event correlation), this creates a **hierarchical, watchable, codec-layered
workspace** that agents share during workflow execution. No other workflow engine
has this — Temporal, Inngest, and Hatchet all use external databases for state.

## Design

### 1. Concept

A **Workspace** is a codec-wrapped NATS KV bucket scoped to a workflow run. It
provides path-style keys, optional compression, optional encryption, and real-
time watches. Agents use it as a shared scratchpad — reading and writing files,
context, memory, and artifacts.

```go
// In a task handler
func codeAgent(ctx worker.TaskContext) error {
    ws := ctx.Workspace()

    // Write a file
    ws.Put(ctx, "files/main.go", sourceCode)

    // Read a review from another agent
    review, _ := ws.Get(ctx, "reviews/main.go")

    // Watch for changes from collaborators
    watcher, _ := ws.Watch(ctx, "files/>")
    for entry := range watcher.Updates() {
        // React to file changes in real time
    }

    return ctx.Complete(nil)
}
```

### 2. Workspace Scoping

Every workspace is scoped by **run ID** — agents in the same workflow run share
one workspace, different runs are isolated. The run ID is the root of all paths:

```
Internal KV key: run.{runID}.files.main_go
User-facing path: files/main.go
```

The PathCodec handles translation. The run ID prefix is injected automatically
by the Workspace — handlers never see it.

Optional **cross-run workspaces** use a named scope instead of run ID:

```go
// Persistent agent memory (survives across runs)
memory := ctx.NamedWorkspace("agent-memory")
memory.Put(ctx, "coder-1/patterns/go-idioms", data)
```

### 3. Codec Layers

Three codec layers, stacked transparently:

**Path encoding (always on):**
PathCodec converts `path/style/keys` to NATS-safe subjects. Handles special
characters via Base64Codec chain when needed.

**Compression (always on):**
ZstdCodec at level 3. Agent workloads are text-heavy (code, conversations,
reviews) — compression is always beneficial with negligible CPU cost.
No option needed; the right default is always-compress.

**Encryption (workflow-level opt-in):**
AES-256-GCM ValueCodec. Enabled via `WorkflowDef.WorkspaceEncrypted`. Key
from `DAGNATS_WORKSPACE_KEY` environment variable or Doppler. Handlers don't
decide security policy — it's a definition-time decision.

**Resulting codec chain:**
```go
// Built internally by workspace constructor:
// path encoding → zstd compression → optional AES encryption
keyCodec := kvcodec.NewPathCodec()
valueCodec := kvcodec.NewValueChainCodec(
    workspace.ZstdCodec{Level: 3},
    // + workspace.AESCodec{Key: key} if encrypted
)
```

### 4. API Surface

#### Workspace Interface

4 methods. Deep and focused.

```go
type Workspace interface {
    // Put writes a value at the given path. Path is relative to
    // the scope (e.g., "files/main.go", not "/run/abc/...").
    // Compression is always on (zstd). Encryption is transparent
    // if configured at the workflow level.
    Put(ctx context.Context, path string, value []byte) error

    // Get reads a value. Returns ErrNotFound if path doesn't exist.
    // Decompression and decryption are transparent.
    Get(ctx context.Context, path string) ([]byte, error)

    // Delete removes a value.
    Delete(ctx context.Context, path string) error

    // Watch returns a channel of changes for paths matching a
    // pattern. Uses NATS KV watch with PathCodec-encoded filter.
    // Pattern supports NATS wildcards: "files/>" for all files.
    // Initial values are delivered first, then live updates —
    // this subsumes List (no separate List method needed).
    Watch(
        ctx context.Context, pattern string,
    ) (Watcher, error)
}

type Watcher interface {
    Updates() <-chan Entry
    Stop() error
}

type Entry struct {
    Path      string
    Value     []byte
    Revision  uint64
    Timestamp time.Time
    Operation Operation // Put, Delete
}
```

No `List` — Watch with initial values covers discovery without hiding an O(n)
scan. No `History` — can be added later if a use case emerges (NATS KV supports
it natively).

#### TaskContext Integration

Both per-run and named workspaces are accessed from TaskContext. Handlers never
manage NATS connections or codec setup — complexity is pulled downward.

```go
type TaskContext interface {
    // ... existing methods ...
    Workspace() Workspace                  // per-run scope
    NamedWorkspace(name string) Workspace  // cross-run scope
}
```

Per-run uses prefix `run.{runID}.`. Named uses prefix `named.{name}.`. Both
live in the same KV bucket (`workspaces`) — one bucket, one place to look.

#### Codec Configuration

**Compression is always on.** Agent workloads are text-heavy (code, conversations,
reviews). Zstd at level 3 compresses well with negligible CPU cost. No option
needed — the right default is always-compress.

**Encryption is a workflow-level decision**, not a per-handler decision. Handlers
shouldn't decide security policy:

```go
type WorkflowDef struct {
    // ... existing fields ...
    WorkspaceEncrypted bool `json:"workspace_encrypted,omitempty"`
}
```

When `WorkspaceEncrypted` is true, the engine configures the AES-256-GCM
ValueCodec using a key from environment (`DAGNATS_WORKSPACE_KEY`) or Doppler.
Handlers read and write plaintext — the codec layer is invisible.

```go
// No options needed for the common case:
ws := ctx.Workspace()
ws.Put(ctx, "files/main.go", code) // compressed + maybe encrypted
```

### 5. TaskContext Integration

Described in Section 4 above. Both `Workspace()` and `NamedWorkspace(name)`
are on `TaskContext`. The engine provisions the KV bucket and codec stack.
Workers that don't call either method pay nothing — lazy-initialized on
first access.

### 6. Use Cases

#### Multi-Agent Code Review Pipeline

```
coder-agent   → writes to   files/src/handler.go
                             files/src/handler_test.go
reviewer-agent → watches    files/> (real-time)
               → reads      files/src/handler.go
               → writes to  reviews/src/handler.go
coder-agent   → watches    reviews/> (real-time)
               → reads      reviews/src/handler.go
               → updates    files/src/handler.go (iterate)
```

The workspace replaces the DAG edge for collaboration. The DAG still orchestrates
the overall flow (who runs when), but the workspace is where the work happens.

#### Persistent Agent Memory

```go
func coderAgent(ctx worker.TaskContext) error {
    // Load memory from a named workspace (cross-run)
    memory := ctx.NamedWorkspace("agent-memory")
    patterns, _ := memory.Get(ctx, "coder/go-patterns")

    // Use patterns in coding...

    // Save new patterns learned
    memory.Put(ctx, "coder/go-patterns", updatedPatterns)
    return ctx.Complete(output)
}
```

Memory persists across runs. TTL can auto-expire stale memories. Named
workspaces are shared across all runs of all workflows — true persistent state.

#### Encrypted Secret Passing

```go
ws := ctx.Workspace() // encryption enabled via workflow config
ws.Put(ctx, "secrets/api-key", []byte(apiKey))
// Stored encrypted in NATS KV — only agents in this run can read
key, _ := ws.Get(ctx, "secrets/api-key")
```

No plaintext secrets in step inputs or NATS message payloads.

#### Large Artifact Handling

```go
ws := ctx.Workspace() // compression enabled
// 400KB conversation history → ~50KB compressed in KV
ws.Put(ctx, "context/conversation", largeHistory)
// Transparently decompressed on read
history, _ := ws.Get(ctx, "context/conversation")
```

### 7. NATS Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `workspaces` | KV bucket (configurable TTL, history: 5) | All workspace state |

One bucket for both per-run and named workspaces, differentiated by key prefix
(`run.{runID}.` vs `named.{name}.`). One place to look during debugging, one
TTL configuration, one setup call. History: 5 enables future revision access
if needed. No new streams — workspaces are KV-only.

### 8. Implementation Structure

```
workspace/
    workspace.go       — Workspace interface + scoped implementation
    codec_zstd.go      — ZstdCodec (ValueCodec)
    codec_aes.go       — AESCodec (ValueCodec)
```

Per-run and named workspaces are the same implementation with different key
prefixes — no separate file needed. Options are gone (compression always on,
encryption at workflow level).

**Dependencies:**
- `github.com/synadia-io/orbit.go/kvcodec` — PathCodec, codec chaining
- `github.com/klauspost/compress/zstd` — compression (already NATS dep)
- `crypto/aes` + `crypto/cipher` — encryption (stdlib)

### 9. Validation

- Path must not be empty, must not start with `/` (relative to scope)
- Path must not contain `..` (path traversal)
- Encryption key must be 32 bytes (AES-256)
- Named workspace names: alphanumeric + hyphens, max 64 chars

### 10. Bounds

- Maximum value size: 1MB (NATS KV default, configurable)
- Maximum paths per workspace: 100,000 (bounded by KV entry count)
- Compression: zstd level 3 (fast, reasonable ratio)
- Watch: max 1,000 concurrent watchers per workspace

### 11. What Makes This Unique

No other workflow engine has this combination:

| Feature | Temporal | Inngest | Hatchet | DagNats |
|---------|----------|---------|---------|---------|
| Typed step I/O | ✅ | ✅ | ✅ | ✅ |
| Shared mutable state | External DB | ❌ | ❌ | **Native workspace** |
| Real-time reactivity | ❌ | ❌ | ❌ | **KV Watch** |
| Path-style navigation | ❌ | ❌ | ❌ | **PathCodec** |
| Transparent encryption | ❌ | ❌ | ❌ | **ValueCodec** |
| Transparent compression | ❌ | ❌ | ❌ | **ValueCodec** |
| Cross-run persistence | External DB | ❌ | ❌ | **Named workspaces** |
| Agent collaboration | ❌ | ❌ | ❌ | **Watch + workspace** |

The workspace is a **first-class primitive** in the execution substrate, not a
bolted-on external dependency. It's watchable, encrypted, compressed, and
hierarchical — because NATS KV + kvcodec makes it possible.

### 12. Interaction with Existing Primitives

- **Signals** remain for direct, targeted inter-step communication (signal by
  name to a specific run). Workspaces are for shared mutable state.
- **Checkpoints** remain for per-step handler state (pause/resume). Workspaces
  are for cross-step shared data.
- **Step I/O** remains for structured DAG data flow. Workspaces are for
  unstructured collaboration.
- **PutStream** remains for real-time streaming to external consumers.
  Workspaces are for persistent, revisioned data.

Each primitive has a distinct purpose. Workspaces don't replace anything — they
fill the collaboration gap that the others can't.

### 13. Build Order

1. `workspace/` package: Workspace interface + scoped KV with PathCodec + ZstdCodec
2. TaskContext.Workspace() + TaskContext.NamedWorkspace() integration
3. AESCodec + WorkflowDef.WorkspaceEncrypted wiring
4. CLI: `dagnats workspace get <run-id> <path>`

Step 1 is the MVP — path-style, compressed KV access scoped to a run. Step 2
wires it into the worker. Step 3 adds encryption. Step 4 adds observability.
Named workspaces come free with step 2 (same implementation, different prefix).
