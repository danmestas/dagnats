# dagnats-agents Foundation — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold the `dagnats-agents` repo with Go tool services (NATS micro), role/tool registry (NATS KV), and a TypeScript tool bridge that forwards Agent SDK tool calls to Go services via NATS request/reply.

**Architecture:** Mixed Go + TypeScript monorepo. Go owns tool execution and registry writes. TypeScript owns role resolution, tool bridge, and Agent SDK integration. NATS is the only communication channel. Deep modules: `ToolBridge` hides NATS request/reply + error translation; `RoleResolver` hides KV lookups + bundle expansion; each Go tool service hides its implementation behind a JSON request/response contract on a NATS subject.

**Tech Stack:** Go 1.22+, TypeScript 5+, Node.js 20+, NATS JetStream, `@anthropic-ai/claude-agent-sdk`, `nats` (npm), `github.com/nats-io/nats.go`, `github.com/nats-io/nats.go/micro`

**Spec:** `docs/superpowers/specs/2026-03-30-agent-sdk-integration-design.md` (in dagnats repo)

---

## File Structure

```
dagnats-agents/
├── go.mod                          # github.com/Craft-Design-Group/dagnats-agents
├── go.sum
├── package.json                    # TypeScript workspace root
├── tsconfig.json
├── CLAUDE.md
│
├── tools/                          # Go tool services (NATS micro)
│   ├── registry/                   # Tool & role registry (KV read/write)
│   │   ├── types.go                # ToolDef, RoleDef, BundleDef types
│   │   ├── register.go             # RegisterTool, RegisterRole, RegisterBundle
│   │   ├── register_test.go
│   │   └── resolve.go              # ResolveTool (KV get by name)
│   │
│   ├── filetools/                  # File operation tools
│   │   ├── filetools.go            # Register(micro.Service) — read, write, edit, glob, grep
│   │   └── filetools_test.go
│   │
│   ├── gittools/                   # Git operation tools
│   │   ├── gittools.go             # Register(micro.Service) — status, diff, commit, log
│   │   └── gittools_test.go
│   │
│   ├── shelltools/                 # Bounded shell execution
│   │   ├── shelltools.go           # Register(micro.Service) — exec with timeout + output limits
│   │   └── shelltools_test.go
│   │
│   └── cmd/
│       └── dagnats-tools/
│           └── main.go             # Tool service binary (registers all tools, connects NATS)
│
├── ts/                             # TypeScript packages
│   ├── package.json
│   ├── tsconfig.json
│   │
│   ├── src/
│   │   ├── bridge.ts               # ToolBridge — builds Agent SDK tool() handlers from registry
│   │   ├── resolver.ts             # RoleResolver — reads role + tool defs from NATS KV
│   │   ├── types.ts                # ToolDef, RoleDef TypeScript types (mirrors Go types)
│   │   └── nats.ts                 # NATS connection helper (connect, get JetStream, get KV)
│   │
│   └── test/
│       ├── bridge.test.ts          # ToolBridge tests (mock NATS, verify request/reply)
│       └── resolver.test.ts        # RoleResolver tests (mock KV, verify resolution)
│
└── testutil/                       # Shared test helpers (Go)
    └── testserver.go               # Embedded NATS server for integration tests
```

### Design Rationale (Ousterhout)

**Deep modules:**
- `ToolBridge` — small interface: `build(toolDefs) → sdkTools[]`. Hides: NATS connection, request/reply, timeout, error-to-tool-result translation, schema validation.
- `RoleResolver` — small interface: `resolve(roleName) → ResolvedRole`. Hides: KV bucket access, JSON deserialization.
- Each Go tool package — small interface: `Register(service)`. Hides: all tool implementations, input parsing, error formatting.

**Information hiding:**
- TypeScript never knows how tools are implemented. It only knows: name, schema, NATS subject.
- Go tools never know about Agent SDK, roles, or TypeScript. They just handle NATS requests.

**Pull complexity down:**
- Error handling lives in `ToolBridge`, not pushed up to the agent worker. NATS timeouts, no-responders, and Go errors all become tool error results.
- Bundle expansion happens in `RegisterRole`, not at resolution time. Stored roles contain only concrete tool names.

**Define errors out of existence:**
- Go tool response format: always `{"result": ...}` or `{"error": "..."}`. No separate error channel.
- Missing tool at resolution time → error before agent starts. Never a runtime "tool not found" during agent execution.

---

## Chunk 1: Repo scaffold and registry types

### Task 1: Initialize repo with Go module and TypeScript package

**Files:**
- Create: `go.mod`
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `CLAUDE.md`

- [ ] **Step 1: Create the repo directory**

```bash
mkdir -p ~/projects/dagnats-agents
cd ~/projects/dagnats-agents
git init
```

- [ ] **Step 2: Initialize Go module**

```bash
go mod init github.com/Craft-Design-Group/dagnats-agents
```

- [ ] **Step 3: Create CLAUDE.md**

Create `CLAUDE.md`:

```markdown
# dagnats-agents

Agent SDK integration for DagNats. Runs Claude agents as DAG workflow steps
with role-based tooling and NATS-native communication.

## Design Philosophy

Same as DagNats core:
- **Ousterhout:** Deep modules, small interfaces, pull complexity down.
- **TigerStyle:** Safety > Performance > DX. Bounded everything.

## Language & Tools

- Go for tool services and registry
- TypeScript for agent worker, tool bridge, workflow SDK
- NATS JetStream for all communication
- No MCP. Tools use NATS request/reply.

## Coding Rules

Go: same as dagnats core (see dagnats/CLAUDE.md).
TypeScript: strict mode, no any, explicit return types, vitest for tests.

## Testing

- Go: TDD with embedded NATS server
- TypeScript: vitest, mock NATS for unit tests, real NATS for integration
- Each test file has a methodology comment
```

- [ ] **Step 4: Initialize TypeScript workspace**

```bash
mkdir -p ts/src ts/test
```

Create `package.json`:

```json
{
  "name": "dagnats-agents",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc",
    "test": "vitest run"
  }
}
```

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "Node16",
    "moduleResolution": "Node16",
    "strict": true,
    "esModuleInterop": true,
    "outDir": "dist",
    "rootDir": ".",
    "declaration": true,
    "sourceMap": true
  },
  "include": ["ts/src/**/*.ts", "ts/test/**/*.ts"]
}
```

- [ ] **Step 5: Install TypeScript dependencies**

```bash
npm install --save-dev typescript vitest @types/node
npm install nats @anthropic-ai/claude-agent-sdk
```

- [ ] **Step 6: Add .gitignore**

Create `.gitignore`:

```
node_modules/
dist/
.worktrees/
.superpowers/
```

- [ ] **Step 7: Commit scaffold**

```bash
git add -A
git commit -m "chore: scaffold dagnats-agents repo with Go module and TypeScript workspace"
```

---

### Task 2: Define registry types (Go)

**Files:**
- Create: `tools/registry/types.go`
- Create: `tools/registry/types_test.go`

- [ ] **Step 1: Write failing test for ToolDef serialization**

Create `tools/registry/types_test.go`:

```go
// tools/registry/types_test.go
// Tests for registry types: ToolDef and RoleDef serialization.
// Methodology: construct types, marshal to JSON, verify field names
// match the NATS KV schema from the spec.
package registry

import (
	"encoding/json"
	"testing"
)

func TestToolDefJSONRoundTrip(t *testing.T) {
	td := ToolDef{
		Name:        "file_read",
		Description: "Read file contents",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
		Subject:     "tool.exec.file_read",
	}

	data, err := json.Marshal(td)
	if err != nil {
		t.Fatalf("Marshal ToolDef: %v", err)
	}

	var got ToolDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal ToolDef: %v", err)
	}
	if got.Name != "file_read" {
		t.Fatalf("Name = %q, want file_read", got.Name)
	}
	if got.Subject != "tool.exec.file_read" {
		t.Fatalf("Subject = %q, want tool.exec.file_read", got.Subject)
	}
}

func TestRoleDefJSONRoundTrip(t *testing.T) {
	rd := RoleDef{
		Name:         "coder",
		Model:        "opus",
		SystemPrompt: "You are an expert software engineer.",
		Skills:       []string{"superpowers:test-driven-development"},
		Tools:        []string{"file_read", "file_write", "git_status"},
		MaxTurns:     50,
		Effort:       "high",
	}

	data, err := json.Marshal(rd)
	if err != nil {
		t.Fatalf("Marshal RoleDef: %v", err)
	}

	var got RoleDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal RoleDef: %v", err)
	}
	if got.Name != "coder" {
		t.Fatalf("Name = %q, want coder", got.Name)
	}
	if len(got.Tools) != 3 {
		t.Fatalf("Tools count = %d, want 3", len(got.Tools))
	}
	if got.MaxTurns != 50 {
		t.Fatalf("MaxTurns = %d, want 50", got.MaxTurns)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/registry/ -v`
Expected: FAIL — types undefined

- [ ] **Step 3: Implement registry types**

Create `tools/registry/types.go`:

```go
// tools/registry/types.go
// Types for the tool and role registry stored in NATS KV.
// These define the wire format — field names match the KV JSON schema.
package registry

import "encoding/json"

// ToolDef describes a single tool in the registry. The InputSchema is
// raw JSON so it can be forwarded to the Agent SDK without parsing.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	Subject     string          `json:"subject"`
	TimeoutMS   int             `json:"timeout_ms,omitempty"`
}

// RoleDef describes a reusable agent profile. Tools is a list of
// concrete tool names (bundles are expanded at registration time).
type RoleDef struct {
	Name         string   `json:"name"`
	Model        string   `json:"model"`
	SystemPrompt string   `json:"system_prompt"`
	Skills       []string `json:"skills,omitempty"`
	Tools        []string `json:"tools"`
	MaxTurns     int      `json:"max_turns"`
	Effort       string   `json:"effort"`
}

// BundleDef groups related tools under one name. Used only at
// registration time — stored roles contain concrete tool names.
type BundleDef struct {
	Name  string   `json:"name"`
	Tools []string `json:"tools"`
}

// ToolResponse is the standard response format from Go tool services.
// Either Result or Error is set, never both. This defines errors out
// of existence — every response is structurally valid.
type ToolResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/registry/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/registry/types.go tools/registry/types_test.go
git commit -m "feat(registry): define ToolDef, RoleDef, BundleDef, ToolResponse types"
```

---

### Task 3: Implement registry write operations (Go)

**Files:**
- Create: `tools/registry/register.go`
- Create: `tools/registry/register_test.go`
- Create: `testutil/testserver.go`

- [ ] **Step 1: Create test server helper**

Create `testutil/testserver.go`:

```go
// testutil/testserver.go
// Embedded NATS server for integration tests. Copied from dagnats core
// to avoid a dependency on the core repo for tests.
package testutil

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// StartTestServer starts an embedded NATS server with JetStream and
// returns the server and a connected client. Cleaned up via t.Cleanup.
func StartTestServer(
	t *testing.T,
) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("create test NATS server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5_000_000_000) {
		t.Fatal("NATS server not ready after 5s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect to test NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return ns, nc
}
```

- [ ] **Step 2: Write failing test for RegisterTool and RegisterRole**

Create `tools/registry/register_test.go`:

```go
// tools/registry/register_test.go
// Tests for registry write operations: storing tool defs, role defs,
// and bundle-expanded roles in NATS KV.
// Methodology: write to KV via Register*, read back via raw KV get,
// verify the stored JSON matches the expected structure.
package registry

import (
	"encoding/json"
	"testing"

	"github.com/Craft-Design-Group/dagnats-agents/testutil"
)

func TestRegisterToolAndRetrieve(t *testing.T) {
	_, nc := testutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	reg, err := NewRegistry(js)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	td := ToolDef{
		Name:        "file_read",
		Description: "Read file contents",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Subject:     "tool.exec.file_read",
	}

	if err := reg.RegisterTool(td); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}

	// Positive: retrieve by name
	got, err := reg.GetTool("file_read")
	if err != nil {
		t.Fatalf("GetTool: %v", err)
	}
	if got.Name != "file_read" {
		t.Fatalf("Name = %q, want file_read", got.Name)
	}
	if got.Subject != "tool.exec.file_read" {
		t.Fatalf("Subject = %q", got.Subject)
	}

	// Negative: missing tool
	_, err = reg.GetTool("nonexistent")
	if err == nil {
		t.Fatalf("expected error for missing tool")
	}
}

func TestRegisterRoleExpandsBundles(t *testing.T) {
	_, nc := testutil.StartTestServer(t)
	js, _ := nc.JetStream()
	reg, _ := NewRegistry(js)

	// Register a bundle
	bundle := BundleDef{
		Name:  "file-ops",
		Tools: []string{"file_read", "file_write"},
	}
	reg.RegisterBundle(bundle)

	// Register a role that references the bundle
	role := RoleDef{
		Name:         "coder",
		Model:        "opus",
		SystemPrompt: "You are a coder.",
		Tools:        []string{"file-ops", "git_status"},
		MaxTurns:     50,
		Effort:       "high",
	}
	if err := reg.RegisterRole(role); err != nil {
		t.Fatalf("RegisterRole: %v", err)
	}

	// Retrieve — tools should be expanded
	got, err := reg.GetRole("coder")
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	if len(got.Tools) != 3 {
		t.Fatalf("Tools = %v, want 3 items (expanded)", got.Tools)
	}

	// Verify expansion: file_read, file_write, git_status
	toolSet := make(map[string]bool)
	for _, name := range got.Tools {
		toolSet[name] = true
	}
	if !toolSet["file_read"] {
		t.Fatalf("missing file_read after bundle expansion")
	}
	if !toolSet["git_status"] {
		t.Fatalf("missing git_status (non-bundle tool)")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./tools/registry/ -v`
Expected: FAIL — `NewRegistry`, `RegisterTool`, `GetTool` undefined

- [ ] **Step 4: Implement registry**

Create `tools/registry/register.go`:

```go
// tools/registry/register.go
// Registry manages tool and role definitions in NATS KV. Bundles are
// expanded at registration time so runtime resolution is a single
// KV lookup — no multi-step expansion needed.
package registry

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

// Registry reads and writes tool/role definitions in NATS KV.
type Registry struct {
	toolKV nats.KeyValue
	roleKV nats.KeyValue
	// In-memory bundle index — bundles are registration-time only,
	// not persisted. They exist to expand role tool lists.
	bundles map[string]BundleDef
}

// NewRegistry creates a Registry backed by NATS KV buckets.
// Creates the buckets if they don't exist.
func NewRegistry(js nats.JetStreamContext) (*Registry, error) {
	if js == nil {
		panic("registry: js must not be nil")
	}
	toolKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "tool_registry",
	})
	if err != nil {
		return nil, fmt.Errorf("create tool_registry: %w", err)
	}
	roleKV, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "roles",
	})
	if err != nil {
		return nil, fmt.Errorf("create roles: %w", err)
	}
	return &Registry{
		toolKV:  toolKV,
		roleKV:  roleKV,
		bundles: make(map[string]BundleDef),
	}, nil
}

// RegisterBundle stores a bundle definition in memory for role
// expansion. Bundles are not persisted — they're a convenience.
func (r *Registry) RegisterBundle(b BundleDef) {
	if b.Name == "" {
		panic("registry: bundle name must not be empty")
	}
	r.bundles[b.Name] = b
}

// RegisterTool writes a tool definition to KV.
func (r *Registry) RegisterTool(td ToolDef) error {
	if td.Name == "" {
		panic("registry: tool name must not be empty")
	}
	data, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("marshal tool %q: %w", td.Name, err)
	}
	_, err = r.toolKV.Put("tool."+td.Name, data)
	return err
}

// GetTool retrieves a tool definition by name.
func (r *Registry) GetTool(name string) (ToolDef, error) {
	entry, err := r.toolKV.Get("tool." + name)
	if err != nil {
		return ToolDef{}, fmt.Errorf("get tool %q: %w", name, err)
	}
	var td ToolDef
	if err := json.Unmarshal(entry.Value(), &td); err != nil {
		return ToolDef{}, fmt.Errorf("unmarshal tool %q: %w", name, err)
	}
	return td, nil
}

// RegisterRole writes a role definition to KV after expanding any
// bundle references in the Tools list to concrete tool names.
func (r *Registry) RegisterRole(rd RoleDef) error {
	if rd.Name == "" {
		panic("registry: role name must not be empty")
	}
	expanded := r.expandTools(rd.Tools)
	rd.Tools = expanded
	data, err := json.Marshal(rd)
	if err != nil {
		return fmt.Errorf("marshal role %q: %w", rd.Name, err)
	}
	_, err = r.roleKV.Put("role."+rd.Name, data)
	return err
}

// GetRole retrieves a role definition by name. Tools are already
// expanded — no further resolution needed.
func (r *Registry) GetRole(name string) (RoleDef, error) {
	entry, err := r.roleKV.Get("role." + name)
	if err != nil {
		return RoleDef{}, fmt.Errorf("get role %q: %w", name, err)
	}
	var rd RoleDef
	if err := json.Unmarshal(entry.Value(), &rd); err != nil {
		return RoleDef{}, fmt.Errorf("unmarshal role %q: %w", name, err)
	}
	return rd, nil
}

// expandTools replaces bundle names with their constituent tool names.
// Non-bundle names pass through unchanged. Bounded: max 100 tools.
func (r *Registry) expandTools(tools []string) []string {
	const maxTools = 100
	expanded := make([]string, 0, len(tools))
	for _, name := range tools {
		if b, ok := r.bundles[name]; ok {
			expanded = append(expanded, b.Tools...)
		} else {
			expanded = append(expanded, name)
		}
		if len(expanded) > maxTools {
			panic(fmt.Sprintf(
				"registry: expanded tool count %d exceeds max %d",
				len(expanded), maxTools,
			))
		}
	}
	return expanded
}
```

- [ ] **Step 5: Add Go dependencies**

```bash
go get github.com/nats-io/nats.go
go get github.com/nats-io/nats-server/v2/server
go mod tidy
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./tools/registry/ -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add tools/ testutil/ go.mod go.sum
git commit -m "feat(registry): implement tool/role/bundle registration with KV storage"
```

---

## Chunk 2: Go tool services

### Task 4: Implement file tools (Go NATS micro service)

**Files:**
- Create: `tools/filetools/filetools.go`
- Create: `tools/filetools/filetools_test.go`

- [ ] **Step 1: Write failing test for file_read tool**

Create `tools/filetools/filetools_test.go`:

```go
// tools/filetools/filetools_test.go
// Tests for file operation tools served via NATS micro.
// Methodology: start tool service, send NATS request, verify response.
// Uses real embedded NATS server.
package filetools

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Craft-Design-Group/dagnats-agents/testutil"
	"github.com/Craft-Design-Group/dagnats-agents/tools/registry"
)

func TestFileReadTool(t *testing.T) {
	_, nc := testutil.StartTestServer(t)

	// Create a temp file to read
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	// Start file tools service
	svc, err := Register(nc)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer svc.Stop()

	// Send request to file_read
	input, _ := json.Marshal(map[string]interface{}{
		"path": path,
	})
	resp, err := nc.Request("tool.exec.file_read", input, 5*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}

	var result registry.ToolResponse
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}

	// Positive: got content
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if string(result.Result) == "" {
		t.Fatalf("result should not be empty")
	}

	// Negative: read nonexistent file
	badInput, _ := json.Marshal(map[string]interface{}{
		"path": "/nonexistent/file.txt",
	})
	badResp, err := nc.Request("tool.exec.file_read", badInput, 5*time.Second)
	if err != nil {
		t.Fatalf("Request bad path: %v", err)
	}
	var badResult registry.ToolResponse
	json.Unmarshal(badResp.Data, &badResult)
	if badResult.Error == "" {
		t.Fatalf("expected error for nonexistent file")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/filetools/ -v`
Expected: FAIL — `Register` undefined

- [ ] **Step 3: Implement file tools**

Create `tools/filetools/filetools.go`:

```go
// tools/filetools/filetools.go
// File operation tools exposed as NATS micro service endpoints.
// Each tool listens on tool.exec.{name} and returns ToolResponse.
package filetools

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Craft-Design-Group/dagnats-agents/tools/registry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Register creates a NATS micro service with file operation endpoints.
func Register(nc *nats.Conn) (micro.Service, error) {
	if nc == nil {
		panic("filetools: nc must not be nil")
	}

	svc, err := micro.AddService(nc, micro.Config{
		Name:    "file-tools",
		Version: "1.0.0",
	})
	if err != nil {
		return nil, fmt.Errorf("add service: %w", err)
	}

	grp := svc.AddGroup("tool.exec")
	grp.AddEndpoint("file_read", micro.HandlerFunc(handleFileRead))

	return svc, nil
}

func handleFileRead(req micro.Request) {
	var input struct {
		Path   string `json:"path"`
		Offset int    `json:"offset,omitempty"`
		Limit  int    `json:"limit,omitempty"`
	}
	if err := json.Unmarshal(req.Data(), &input); err != nil {
		respondError(req, "invalid input: "+err.Error())
		return
	}
	if input.Path == "" {
		respondError(req, "path is required")
		return
	}

	// Resolve to absolute path for safety
	absPath, err := filepath.Abs(input.Path)
	if err != nil {
		respondError(req, "invalid path: "+err.Error())
		return
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		respondError(req, err.Error())
		return
	}

	// Apply offset/limit if specified
	if input.Offset > 0 && input.Offset < len(data) {
		data = data[input.Offset:]
	}
	if input.Limit > 0 && input.Limit < len(data) {
		data = data[:input.Limit]
	}

	respondResult(req, json.RawMessage(
		fmt.Sprintf("%q", string(data)),
	))
}

func respondResult(req micro.Request, result json.RawMessage) {
	resp := registry.ToolResponse{Result: result}
	data, _ := json.Marshal(resp)
	req.Respond(data)
}

func respondError(req micro.Request, msg string) {
	resp := registry.ToolResponse{Error: msg}
	data, _ := json.Marshal(resp)
	req.Respond(data)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/filetools/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/filetools/
git commit -m "feat(filetools): file_read tool as NATS micro service"
```

---

### Task 5: Implement shell tool (Go NATS micro service)

**Files:**
- Create: `tools/shelltools/shelltools.go`
- Create: `tools/shelltools/shelltools_test.go`

- [ ] **Step 1: Write failing test for shell_exec tool**

Create `tools/shelltools/shelltools_test.go`:

```go
// tools/shelltools/shelltools_test.go
// Tests for bounded shell execution tool.
// Methodology: execute known commands, verify output and error handling.
// Bounded: all commands must complete within 5s test timeout.
package shelltools

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Craft-Design-Group/dagnats-agents/testutil"
	"github.com/Craft-Design-Group/dagnats-agents/tools/registry"
)

func TestShellExec(t *testing.T) {
	_, nc := testutil.StartTestServer(t)

	svc, err := Register(nc)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer svc.Stop()

	// Positive: simple echo command
	input, _ := json.Marshal(map[string]interface{}{
		"command":    "echo hello",
		"timeout_ms": 5000,
	})
	resp, err := nc.Request("tool.exec.shell_exec", input, 5*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}

	var result registry.ToolResponse
	json.Unmarshal(resp.Data, &result)
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if string(result.Result) == "" {
		t.Fatalf("result should contain output")
	}

	// Negative: command that fails
	badInput, _ := json.Marshal(map[string]interface{}{
		"command":    "false",
		"timeout_ms": 5000,
	})
	badResp, _ := nc.Request("tool.exec.shell_exec", badInput, 5*time.Second)
	var badResult registry.ToolResponse
	json.Unmarshal(badResp.Data, &badResult)
	if badResult.Error == "" {
		t.Fatalf("expected error for failed command")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/shelltools/ -v`
Expected: FAIL — `Register` undefined

- [ ] **Step 3: Implement shell tool**

Create `tools/shelltools/shelltools.go`:

```go
// tools/shelltools/shelltools.go
// Bounded shell execution tool. Commands run with a timeout and
// output size limit to prevent runaway processes.
package shelltools

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/Craft-Design-Group/dagnats-agents/tools/registry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const (
	defaultTimeoutMS  = 30_000
	maxTimeoutMS      = 300_000
	maxOutputBytes    = 1 << 20 // 1MB
)

// Register creates a NATS micro service with the shell_exec endpoint.
func Register(nc *nats.Conn) (micro.Service, error) {
	if nc == nil {
		panic("shelltools: nc must not be nil")
	}
	svc, err := micro.AddService(nc, micro.Config{
		Name:    "shell-tools",
		Version: "1.0.0",
	})
	if err != nil {
		return nil, fmt.Errorf("add service: %w", err)
	}
	grp := svc.AddGroup("tool.exec")
	grp.AddEndpoint("shell_exec", micro.HandlerFunc(handleShellExec))
	return svc, nil
}

func handleShellExec(req micro.Request) {
	var input struct {
		Command   string `json:"command"`
		TimeoutMS int    `json:"timeout_ms,omitempty"`
		Cwd       string `json:"cwd,omitempty"`
	}
	if err := json.Unmarshal(req.Data(), &input); err != nil {
		respondError(req, "invalid input: "+err.Error())
		return
	}
	if input.Command == "" {
		respondError(req, "command is required")
		return
	}

	timeoutMS := input.TimeoutMS
	if timeoutMS <= 0 {
		timeoutMS = defaultTimeoutMS
	}
	if timeoutMS > maxTimeoutMS {
		timeoutMS = maxTimeoutMS
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(timeoutMS)*time.Millisecond,
	)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", input.Command)
	if input.Cwd != "" {
		cmd.Dir = input.Cwd
	}

	output, err := cmd.CombinedOutput()

	// Truncate output if too large
	if len(output) > maxOutputBytes {
		output = output[:maxOutputBytes]
	}

	if err != nil {
		respondError(req, fmt.Sprintf(
			"exit error: %v\noutput: %s", err, string(output),
		))
		return
	}

	result, _ := json.Marshal(map[string]interface{}{
		"stdout":    string(output),
		"exit_code": 0,
	})
	respondResult(req, result)
}

func respondResult(req micro.Request, result json.RawMessage) {
	resp := registry.ToolResponse{Result: result}
	data, _ := json.Marshal(resp)
	req.Respond(data)
}

func respondError(req micro.Request, msg string) {
	resp := registry.ToolResponse{Error: msg}
	data, _ := json.Marshal(resp)
	req.Respond(data)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/shelltools/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/shelltools/
git commit -m "feat(shelltools): bounded shell_exec tool as NATS micro service"
```

---

## Chunk 3: TypeScript tool bridge and role resolver

### Task 6: Implement TypeScript types (mirror Go types)

**Files:**
- Create: `ts/src/types.ts`

- [ ] **Step 1: Create types**

Create `ts/src/types.ts`:

```typescript
// ts/src/types.ts
// TypeScript mirrors of Go registry types. Field names match
// the JSON wire format stored in NATS KV.

export interface ToolDef {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
  subject: string;
  timeout_ms?: number;
}

export interface RoleDef {
  name: string;
  model: "opus" | "sonnet" | "haiku";
  system_prompt: string;
  skills?: string[];
  tools: string[];
  max_turns: number;
  effort: "low" | "medium" | "high" | "max";
}

export interface ToolResponse {
  result?: unknown;
  error?: string;
}

export interface ResolvedRole {
  role: RoleDef;
  toolDefs: ToolDef[];
}
```

- [ ] **Step 2: Commit**

```bash
git add ts/src/types.ts
git commit -m "feat(ts): add TypeScript type definitions mirroring Go registry types"
```

---

### Task 7: Implement RoleResolver (TypeScript)

**Files:**
- Create: `ts/src/resolver.ts`
- Create: `ts/test/resolver.test.ts`

- [ ] **Step 1: Write failing test**

Create `ts/test/resolver.test.ts`:

```typescript
// ts/test/resolver.test.ts
// Tests for RoleResolver: reads role + tool defs from NATS KV.
// Methodology: set up KV with known data, resolve role, verify
// all tool defs are returned with the role config.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { connect, NatsConnection, JetStreamClient } from "nats";
import { RoleResolver } from "../src/resolver.js";
import type { RoleDef, ToolDef } from "../src/types.js";

// These tests require a running NATS server.
// Run: nats-server -js -p 14222 -sd /tmp/nats-test
// Or skip with: SKIP_NATS=1

describe("RoleResolver", () => {
  let nc: NatsConnection;
  let js: JetStreamClient;

  beforeAll(async () => {
    if (process.env.SKIP_NATS) return;
    nc = await connect({ servers: "nats://localhost:14222" });
    js = nc.jetstream();

    // Seed KV with test data
    const roleKV = await js.views.kv("roles", { history: 1 });
    const toolKV = await js.views.kv("tool_registry", { history: 1 });

    const role: RoleDef = {
      name: "test-coder",
      model: "opus",
      system_prompt: "You are a test coder.",
      tools: ["file_read", "shell_exec"],
      max_turns: 10,
      effort: "high",
    };
    await roleKV.put("role.test-coder", JSON.stringify(role));

    const fileTool: ToolDef = {
      name: "file_read",
      description: "Read file",
      input_schema: { type: "object" },
      subject: "tool.exec.file_read",
    };
    await toolKV.put("tool.file_read", JSON.stringify(fileTool));

    const shellTool: ToolDef = {
      name: "shell_exec",
      description: "Run command",
      input_schema: { type: "object" },
      subject: "tool.exec.shell_exec",
    };
    await toolKV.put("tool.shell_exec", JSON.stringify(shellTool));
  });

  afterAll(async () => {
    if (nc) await nc.close();
  });

  it("resolves role with all tool defs", async () => {
    if (process.env.SKIP_NATS) return;

    const resolver = new RoleResolver(js);
    const resolved = await resolver.resolve("test-coder");

    // Positive: role fields correct
    expect(resolved.role.name).toBe("test-coder");
    expect(resolved.role.model).toBe("opus");

    // Positive: all tools resolved
    expect(resolved.toolDefs).toHaveLength(2);
    const names = resolved.toolDefs.map((t) => t.name);
    expect(names).toContain("file_read");
    expect(names).toContain("shell_exec");
  });

  it("throws on missing role", async () => {
    if (process.env.SKIP_NATS) return;

    const resolver = new RoleResolver(js);
    await expect(resolver.resolve("nonexistent")).rejects.toThrow();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run ts/test/resolver.test.ts`
Expected: FAIL — `RoleResolver` not found

- [ ] **Step 3: Implement RoleResolver**

Create `ts/src/resolver.ts`:

```typescript
// ts/src/resolver.ts
// RoleResolver reads a role and all its tool definitions from NATS KV.
// One call: resolve(name) → complete config. Hides KV access and
// JSON deserialization — caller gets a typed ResolvedRole.
import type { JetStreamClient, KV } from "nats";
import type { RoleDef, ToolDef, ResolvedRole } from "./types.js";

const MAX_TOOLS = 100;

export class RoleResolver {
  private js: JetStreamClient;

  constructor(js: JetStreamClient) {
    this.js = js;
  }

  async resolve(roleName: string): Promise<ResolvedRole> {
    const roleKV = await this.js.views.kv("roles");
    const entry = await roleKV.get(`role.${roleName}`);
    if (!entry || !entry.value) {
      throw new Error(`role "${roleName}" not found`);
    }

    const role: RoleDef = JSON.parse(
      new TextDecoder().decode(entry.value),
    );

    if (role.tools.length > MAX_TOOLS) {
      throw new Error(
        `role "${roleName}" has ${role.tools.length} tools, max ${MAX_TOOLS}`,
      );
    }

    const toolKV = await this.js.views.kv("tool_registry");
    const toolDefs: ToolDef[] = [];

    for (const toolName of role.tools) {
      const toolEntry = await toolKV.get(`tool.${toolName}`);
      if (!toolEntry || !toolEntry.value) {
        throw new Error(
          `tool "${toolName}" referenced by role "${roleName}" not found`,
        );
      }
      const td: ToolDef = JSON.parse(
        new TextDecoder().decode(toolEntry.value),
      );
      toolDefs.push(td);
    }

    return { role, toolDefs };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run ts/test/resolver.test.ts`
Expected: PASS (or SKIP if SKIP_NATS is set)

- [ ] **Step 5: Commit**

```bash
git add ts/src/resolver.ts ts/test/resolver.test.ts
git commit -m "feat(ts): RoleResolver reads role + tool defs from NATS KV"
```

---

### Task 8: Implement ToolBridge (TypeScript)

**Files:**
- Create: `ts/src/bridge.ts`
- Create: `ts/test/bridge.test.ts`

- [ ] **Step 1: Write failing test**

Create `ts/test/bridge.test.ts`:

```typescript
// ts/test/bridge.test.ts
// Tests for ToolBridge: converts tool definitions into Agent SDK
// tool() handlers that forward calls to Go services via NATS.
// Methodology: mock NATS connection, verify request/reply mechanics,
// error translation, and timeout handling.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { connect, NatsConnection } from "nats";
import { ToolBridge } from "../src/bridge.js";
import type { ToolDef } from "../src/types.js";

describe("ToolBridge", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    if (process.env.SKIP_NATS) return;
    nc = await connect({ servers: "nats://localhost:14222" });

    // Set up a mock tool responder
    const sub = nc.subscribe("tool.exec.mock_tool");
    (async () => {
      for await (const msg of sub) {
        const input = JSON.parse(new TextDecoder().decode(msg.data));
        if (input.fail) {
          msg.respond(
            new TextEncoder().encode(
              JSON.stringify({ error: "intentional failure" }),
            ),
          );
        } else {
          msg.respond(
            new TextEncoder().encode(
              JSON.stringify({ result: { echo: input.value } }),
            ),
          );
        }
      }
    })();
  });

  afterAll(async () => {
    if (nc) await nc.close();
  });

  it("builds tool handlers that forward to NATS", async () => {
    if (process.env.SKIP_NATS) return;

    const toolDefs: ToolDef[] = [
      {
        name: "mock_tool",
        description: "A mock tool",
        input_schema: {
          type: "object",
          properties: { value: { type: "string" } },
        },
        subject: "tool.exec.mock_tool",
      },
    ];

    const bridge = new ToolBridge(nc);
    const tools = bridge.build(toolDefs);

    // Positive: one tool created
    expect(tools).toHaveLength(1);
    expect(tools[0].name).toBe("mock_tool");
  });

  it("translates NATS errors to tool error results", async () => {
    if (process.env.SKIP_NATS) return;

    const toolDefs: ToolDef[] = [
      {
        name: "mock_tool",
        description: "A mock tool",
        input_schema: { type: "object" },
        subject: "tool.exec.mock_tool",
      },
    ];

    const bridge = new ToolBridge(nc);
    const tools = bridge.build(toolDefs);

    // Call the tool handler with fail=true
    const handler = tools[0].handler;
    const result = await handler({ fail: true }, {});

    // Error should be translated to tool result format
    expect(result).toBeDefined();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run ts/test/bridge.test.ts`
Expected: FAIL — `ToolBridge` not found

- [ ] **Step 3: Implement ToolBridge**

Create `ts/src/bridge.ts`:

```typescript
// ts/src/bridge.ts
// ToolBridge converts tool definitions into Agent SDK tool() handlers.
// Each handler forwards the tool call to a Go service via NATS
// request/reply and translates the response back to SDK format.
//
// Deep module: small interface (build(defs) → tools[]), hides NATS
// connection, request/reply, timeout, error-to-tool-result translation.
import { tool, z } from "@anthropic-ai/claude-agent-sdk";
import type { NatsConnection } from "nats";
import type { ToolDef, ToolResponse } from "./types.js";

const DEFAULT_TIMEOUT_MS = 30_000;

type SdkTool = ReturnType<typeof tool>;

export class ToolBridge {
  private nc: NatsConnection;

  constructor(nc: NatsConnection) {
    this.nc = nc;
  }

  /** Build Agent SDK tool() handlers from registry tool definitions. */
  build(toolDefs: ToolDef[]): SdkTool[] {
    return toolDefs.map((td) => this.buildOne(td));
  }

  private buildOne(td: ToolDef): SdkTool {
    const nc = this.nc;
    const timeoutMs = td.timeout_ms ?? DEFAULT_TIMEOUT_MS;

    // Use z.any() for the schema since we forward raw JSON —
    // the Go service does the real validation.
    return tool(
      td.name,
      td.description,
      { input: z.any().describe("Tool input (forwarded to service)") },
      async (args) => {
        try {
          const payload = new TextEncoder().encode(
            JSON.stringify(args.input),
          );
          const resp = await nc.request(td.subject, payload, {
            timeout: timeoutMs,
          });
          const body: ToolResponse = JSON.parse(
            new TextDecoder().decode(resp.data),
          );

          if (body.error) {
            return {
              content: [
                { type: "text" as const, text: `Error: ${body.error}` },
              ],
              isError: true,
            };
          }

          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify(body.result),
              },
            ],
          };
        } catch (err) {
          const msg =
            err instanceof Error ? err.message : String(err);
          return {
            content: [
              { type: "text" as const, text: `Tool error: ${msg}` },
            ],
            isError: true,
          };
        }
      },
    );
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run ts/test/bridge.test.ts`
Expected: PASS (or SKIP if SKIP_NATS is set)

- [ ] **Step 5: Run all tests**

Run: `go test ./... -v && npx vitest run`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add ts/src/bridge.ts ts/test/bridge.test.ts
git commit -m "feat(ts): ToolBridge forwards Agent SDK tool calls to Go services via NATS"
```

---

### Task 9: Create tool service binary

**Files:**
- Create: `tools/cmd/dagnats-tools/main.go`

- [ ] **Step 1: Create the binary entry point**

Create `tools/cmd/dagnats-tools/main.go`:

```go
// tools/cmd/dagnats-tools/main.go
// Binary that starts all Go tool services and registers their
// definitions in the tool registry KV.
package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Craft-Design-Group/dagnats-agents/tools/filetools"
	"github.com/Craft-Design-Group/dagnats-agents/tools/registry"
	"github.com/Craft-Design-Group/dagnats-agents/tools/shelltools"
	"github.com/nats-io/nats.go"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	// Initialize registry and register tool definitions
	reg, err := registry.NewRegistry(js)
	if err != nil {
		log.Fatalf("init registry: %v", err)
	}
	registerToolDefs(reg)

	// Start tool services
	fileSvc, err := filetools.Register(nc)
	if err != nil {
		log.Fatalf("start file tools: %v", err)
	}
	defer fileSvc.Stop()

	shellSvc, err := shelltools.Register(nc)
	if err != nil {
		log.Fatalf("start shell tools: %v", err)
	}
	defer shellSvc.Stop()

	log.Println("dagnats-tools running, press Ctrl+C to stop")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}

func registerToolDefs(reg *registry.Registry) {
	tools := []registry.ToolDef{
		{
			Name:        "file_read",
			Description: "Read file contents",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"path": {"type": "string", "description": "File path"},
					"offset": {"type": "number", "description": "Byte offset"},
					"limit": {"type": "number", "description": "Max bytes"}
				},
				"required": ["path"]
			}`),
			Subject: "tool.exec.file_read",
		},
		{
			Name:        "shell_exec",
			Description: "Execute a shell command with bounded timeout",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"command": {"type": "string", "description": "Shell command"},
					"timeout_ms": {"type": "number", "description": "Timeout in ms"},
					"cwd": {"type": "string", "description": "Working directory"}
				},
				"required": ["command"]
			}`),
			Subject: "tool.exec.shell_exec",
		},
	}

	for _, td := range tools {
		if err := reg.RegisterTool(td); err != nil {
			log.Fatalf("register tool %q: %v", td.Name, err)
		}
	}

	// Register bundles
	reg.RegisterBundle(registry.BundleDef{
		Name:  "file-ops",
		Tools: []string{"file_read"},
	})
	reg.RegisterBundle(registry.BundleDef{
		Name:  "shell-ops",
		Tools: []string{"shell_exec"},
	})
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./tools/cmd/dagnats-tools/`
Expected: Builds without errors

- [ ] **Step 3: Commit**

```bash
git add tools/cmd/
git commit -m "feat: dagnats-tools binary registers and starts all Go tool services"
```

---

### Task 10: Final verification

- [ ] **Step 1: Run all Go tests**

Run: `go test ./... -v -count=1`
Expected: ALL PASS

- [ ] **Step 2: Run all TypeScript tests**

Run: `npx vitest run`
Expected: ALL PASS (or SKIP for NATS-dependent tests)

- [ ] **Step 3: Verify Go binary builds**

Run: `go build ./tools/cmd/dagnats-tools/`
Expected: Clean build

- [ ] **Step 4: Commit final state**

```bash
git add -A
git commit -m "chore: final verification — all tests passing"
```
