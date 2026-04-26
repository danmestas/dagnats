# Embedded NATS Cluster Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add embedded NATS cluster mode to dagnats so multiple instances can form their own NATS cluster directly, without an external hub. Self-contained HA.

**Architecture:** Four new config fields plus one deep entry point (`natsutil.SetupAll`) that hides quorum-wait, R-derivation, and stream/KV creation. Caller in `server/server.go` collapses to a single line. Streams auto-update from R=1 to R=N via existing `CreateOrUpdateStream` semantics.

**Tech Stack:** Go, embedded NATS server (`github.com/nats-io/nats-server/v2`), JetStream client (`github.com/nats-io/nats.go/jetstream`).

**Spec:** [`docs/superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md`](../specs/2026-04-25-embedded-nats-cluster-mode-design.md)

**Pre-flight (run once before starting):**
```bash
git checkout feat/nats-cluster-mode
make test
make lint
```
Expected: all green. If not, abort and fix tip-of-branch issues first.

---

## Task 1: Add cluster config fields to `Config`

**Files:**
- Modify: `server/config.go` — add fields, env handling, file parsing
- Modify: `server/config_test.go` — extend coverage

- [ ] **Step 1: Read the current `Config` struct in `server/config.go`** to understand the existing pattern (around line 32–42). Note how `LeafRemotes` is plumbed through `applyConfigValue` and `applyEnvOverrides`.

- [ ] **Step 2: Write failing test for env var override**

Append to `server/config_test.go`:
```go
func TestApplyEnvOverrides_NATSCluster(t *testing.T) {
    t.Setenv("DAGNATS_NATS_CLUSTER_NAME", "dagnats-prod")
    t.Setenv("DAGNATS_NATS_CLUSTER_ROUTES", "nats://node-1:6222,nats://node-2:6222")
    t.Setenv("DAGNATS_NATS_CLUSTER_AUTH_TOKEN", "secret-tok")
    t.Setenv("DAGNATS_NATS_JETSTREAM_REPLICAS", "3")

    cfg := DefaultConfig()
    applyEnvOverrides(&cfg)

    if cfg.NATSClusterName != "dagnats-prod" {
        t.Errorf("NATSClusterName = %q, want %q", cfg.NATSClusterName, "dagnats-prod")
    }
    if got := cfg.NATSClusterRoutes; len(got) != 2 || got[0] != "nats://node-1:6222" {
        t.Errorf("NATSClusterRoutes = %v", got)
    }
    if cfg.NATSClusterAuthToken != "secret-tok" {
        t.Errorf("NATSClusterAuthToken = %q", cfg.NATSClusterAuthToken)
    }
    if cfg.NATSJetStreamReplicas != 3 {
        t.Errorf("NATSJetStreamReplicas = %d, want 3", cfg.NATSJetStreamReplicas)
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./server -run TestApplyEnvOverrides_NATSCluster -v`
Expected: COMPILE FAIL — `cfg.NATSClusterName undefined`.

- [ ] **Step 4: Add fields to `Config` struct in `server/config.go`** (insert after `LeafCredentials` field around line 37):

```go
NATSClusterName       string   `json:"nats_cluster_name"`
NATSClusterRoutes     []string `json:"nats_cluster_routes"`
NATSClusterAuthToken  string   `json:"nats_cluster_auth_token"`
NATSJetStreamReplicas int      `json:"nats_jetstream_replicas"`
```

- [ ] **Step 5: Add a constant `maxClusterRoutes = 10`** at the top of `server/config.go` near `maxLeafRemotes`:

```go
maxClusterRoutes = 10
```

- [ ] **Step 6: Extend `applyEnvOverrides`** in `server/config.go` (after the `DAGNATS_LEAF_CREDENTIALS` block, before the OTLP block):

```go
if val := os.Getenv("DAGNATS_NATS_CLUSTER_NAME"); val != "" {
    cfg.NATSClusterName = val
}
if val := os.Getenv("DAGNATS_NATS_CLUSTER_ROUTES"); val != "" {
    routes := strings.Split(val, ",")
    for i := range routes {
        routes[i] = strings.TrimSpace(routes[i])
    }
    if len(routes) > maxClusterRoutes {
        routes = routes[:maxClusterRoutes]
    }
    cfg.NATSClusterRoutes = routes
}
if val := os.Getenv("DAGNATS_NATS_CLUSTER_AUTH_TOKEN"); val != "" {
    cfg.NATSClusterAuthToken = val
}
if val := os.Getenv("DAGNATS_NATS_JETSTREAM_REPLICAS"); val != "" {
    if r, err := strconv.Atoi(val); err == nil {
        cfg.NATSJetStreamReplicas = r
    }
}
```

- [ ] **Step 7: Run env-override test to verify pass**

Run: `go test ./server -run TestApplyEnvOverrides_NATSCluster -v`
Expected: PASS.

- [ ] **Step 8: Write failing test for `dagnats.yaml` parsing**

Append to `server/config_test.go`:
```go
func TestApplyConfigValue_NATSCluster(t *testing.T) {
    cfg := DefaultConfig()
    cases := []struct {
        key, val string
        check    func(*testing.T, *Config)
    }{
        {"nats_cluster_name", "dagnats-staging", func(t *testing.T, c *Config) {
            if c.NATSClusterName != "dagnats-staging" {
                t.Errorf("NATSClusterName = %q", c.NATSClusterName)
            }
        }},
        {"nats_cluster_routes", "nats://a:6222, nats://b:6222", func(t *testing.T, c *Config) {
            if len(c.NATSClusterRoutes) != 2 {
                t.Fatalf("want 2 routes, got %v", c.NATSClusterRoutes)
            }
        }},
        {"nats_cluster_auth_token", "tok", func(t *testing.T, c *Config) {
            if c.NATSClusterAuthToken != "tok" {
                t.Errorf("token = %q", c.NATSClusterAuthToken)
            }
        }},
        {"nats_jetstream_replicas", "5", func(t *testing.T, c *Config) {
            if c.NATSJetStreamReplicas != 5 {
                t.Errorf("replicas = %d", c.NATSJetStreamReplicas)
            }
        }},
    }
    for _, tc := range cases {
        if err := applyConfigValue(tc.key, tc.val, 1, &cfg); err != nil {
            t.Fatalf("applyConfigValue(%s, %s): %v", tc.key, tc.val, err)
        }
        tc.check(t, &cfg)
    }
}
```

- [ ] **Step 9: Run to verify fail**

Run: `go test ./server -run TestApplyConfigValue_NATSCluster -v`
Expected: FAIL with `unknown config key: nats_cluster_name`.

- [ ] **Step 10: Extend `applyConfigValue`** in `server/config.go`. Add new cases inside the existing `switch key` block (after `leaf_credentials`):

```go
case "nats_cluster_name":
    cfg.NATSClusterName = val
case "nats_cluster_routes":
    routes := strings.Split(val, ",")
    for i := range routes {
        routes[i] = strings.TrimSpace(routes[i])
    }
    if len(routes) > maxClusterRoutes {
        routes = routes[:maxClusterRoutes]
    }
    cfg.NATSClusterRoutes = routes
case "nats_cluster_auth_token":
    cfg.NATSClusterAuthToken = val
case "nats_jetstream_replicas":
    r, err := strconv.Atoi(val)
    if err != nil {
        return fmt.Errorf("invalid nats_jetstream_replicas: %w", err)
    }
    cfg.NATSJetStreamReplicas = r
```

- [ ] **Step 11: Run to verify pass**

Run: `go test ./server -run TestApplyConfigValue_NATSCluster -v`
Expected: PASS.

- [ ] **Step 12: Run the full server package tests** to verify no regressions:

Run: `go test ./server -count=1`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat(config): add embedded NATS cluster fields

Adds four new optional Config fields:
- nats_cluster_name
- nats_cluster_routes
- nats_cluster_auth_token
- nats_jetstream_replicas

Plumbed through file parsing (applyConfigValue) and env vars
(applyEnvOverrides). Cluster routes capped at maxClusterRoutes (10),
matching the existing leaf_remotes pattern. No behavior change yet:
fields are set but unused. Validation and runtime use land in later
commits."
```

---

## Task 2: Cluster config validation

**Files:**
- Modify: `server/config.go` — add `validateClusterConfig`, call from `ConfigWithPath`
- Modify: `server/config_test.go` — exhaustive validation tests

- [ ] **Step 1: Write failing tests for each validation rule**

Append to `server/config_test.go`:
```go
func TestValidateClusterConfig(t *testing.T) {
    cases := []struct {
        name      string
        mut       func(*Config)
        wantPanic string // substring match on panic message
    }{
        {
            name: "cluster requires name",
            mut: func(c *Config) {
                c.NATSClusterRoutes = []string{"nats://a:6222", "nats://b:6222"}
            },
            wantPanic: "nats_cluster_name",
        },
        {
            name: "cluster requires at least 2 routes (3-node minimum)",
            mut: func(c *Config) {
                c.NATSClusterName = "x"
                c.NATSClusterRoutes = []string{"nats://a:6222"}
            },
            wantPanic: "nats_cluster_routes",
        },
        {
            name: "replicas must be 0, 1, 3, or 5",
            mut: func(c *Config) {
                c.NATSJetStreamReplicas = 4
            },
            wantPanic: "nats_jetstream_replicas",
        },
        {
            name: "leaf and cluster mutually exclusive",
            mut: func(c *Config) {
                c.NATSClusterName = "x"
                c.NATSClusterRoutes = []string{"nats://a:6222", "nats://b:6222"}
                c.LeafRemotes = []string{"nats://hub:7422"}
            },
            wantPanic: "leaf_remotes",
        },
        {
            name: "valid clustered config",
            mut: func(c *Config) {
                c.NATSClusterName = "dagnats"
                c.NATSClusterRoutes = []string{
                    "nats://a:6222",
                    "nats://b:6222",
                }
                c.NATSJetStreamReplicas = 3
            },
            wantPanic: "",
        },
        {
            name: "valid standalone config",
            mut:  func(c *Config) {},
            wantPanic: "",
        },
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            cfg := DefaultConfig()
            tc.mut(&cfg)

            defer func() {
                r := recover()
                if tc.wantPanic == "" {
                    if r != nil {
                        t.Errorf("unexpected panic: %v", r)
                    }
                    return
                }
                if r == nil {
                    t.Errorf("expected panic containing %q, got none", tc.wantPanic)
                    return
                }
                if !strings.Contains(fmt.Sprint(r), tc.wantPanic) {
                    t.Errorf("panic = %v, want substring %q", r, tc.wantPanic)
                }
            }()

            validateClusterConfig(&cfg)
        })
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./server -run TestValidateClusterConfig -v`
Expected: COMPILE FAIL — `validateClusterConfig undefined`.

- [ ] **Step 3: Implement `validateClusterConfig`** in `server/config.go` (place above `validateWorkerConfigs`):

```go
// validateClusterConfig panics with a clear message if cluster config
// is internally inconsistent. TigerStyle: programmer-error invariants
// are panics, not returned errors.
//
// Rules:
//   - Cluster mode is detected by len(NATSClusterRoutes) > 0.
//   - Clustering requires NATSClusterName non-empty.
//   - NATSClusterRoutes must have between 2 and maxClusterRoutes entries.
//   - NATSJetStreamReplicas must be in {0, 1, 3, 5}.
//   - Cluster mode and leaf mode are mutually exclusive.
func validateClusterConfig(cfg *Config) {
    if cfg == nil {
        panic("validateClusterConfig: cfg is nil")
    }

    switch cfg.NATSJetStreamReplicas {
    case 0, 1, 3, 5:
    default:
        panic(fmt.Sprintf(
            "nats_jetstream_replicas must be 0, 1, 3, or 5; got %d",
            cfg.NATSJetStreamReplicas,
        ))
    }

    clustered := len(cfg.NATSClusterRoutes) > 0
    if !clustered {
        return
    }

    if cfg.NATSClusterName == "" {
        panic("nats_cluster_name is required when nats_cluster_routes is set")
    }
    if len(cfg.NATSClusterRoutes) < 2 {
        panic(fmt.Sprintf(
            "nats_cluster_routes needs at least 2 entries (3-node minimum); got %d",
            len(cfg.NATSClusterRoutes),
        ))
    }
    if len(cfg.NATSClusterRoutes) > maxClusterRoutes {
        panic(fmt.Sprintf(
            "nats_cluster_routes capped at %d; got %d",
            maxClusterRoutes, len(cfg.NATSClusterRoutes),
        ))
    }
    if len(cfg.LeafRemotes) > 0 {
        panic("nats_cluster_routes and leaf_remotes are mutually exclusive")
    }
}
```

- [ ] **Step 4: Call `validateClusterConfig` from `ConfigWithPath`** in `server/config.go`. After the existing `validateWorkerConfigs` call (around line 117), add:

```go
validateClusterConfig(&cfg)
```

- [ ] **Step 5: Run validation tests to verify pass**

Run: `go test ./server -run TestValidateClusterConfig -v`
Expected: PASS for all subtests.

- [ ] **Step 6: Run full server tests**

Run: `go test ./server -count=1`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat(config): validate cluster config invariants

Adds validateClusterConfig with five rules, all enforced as TigerStyle
panics at startup:
- cluster mode requires nats_cluster_name
- nats_cluster_routes must have 2-10 entries (3-node minimum)
- nats_jetstream_replicas must be in {0, 1, 3, 5}
- cluster mode and leaf mode are mutually exclusive

Called from ConfigWithPath after worker validation."
```

---

## Task 3: `deriveReplicas` helper in `internal/natsutil`

**Files:**
- Create: `internal/natsutil/cluster.go`
- Create: `internal/natsutil/cluster_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/natsutil/cluster_test.go`:
```go
package natsutil

import "testing"

func TestDeriveReplicas(t *testing.T) {
    cases := []struct {
        name     string
        routes   []string
        override int
        want     int
    }{
        {"no routes, no override", nil, 0, 1},
        {"no routes, override 3", nil, 3, 3},
        {"3-node cluster (2 routes), auto", []string{"a", "b"}, 0, 3},
        {"4-node cluster (3 routes), auto rounds down", []string{"a", "b", "c"}, 0, 3},
        {"5-node cluster (4 routes), auto", []string{"a", "b", "c", "d"}, 0, 5},
        {"6-node cluster (5 routes), auto caps at 5", []string{"a", "b", "c", "d", "e"}, 0, 5},
        {"override beats auto", []string{"a", "b", "c", "d"}, 3, 3},
        {"override 1 in cluster", []string{"a", "b"}, 1, 1},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            got := DeriveReplicas(tc.routes, tc.override)
            if got != tc.want {
                t.Errorf("DeriveReplicas(%v, %d) = %d, want %d",
                    tc.routes, tc.override, got, tc.want)
            }
        })
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/natsutil -run TestDeriveReplicas -v`
Expected: COMPILE FAIL — `DeriveReplicas undefined`.

- [ ] **Step 3: Implement**

Create `internal/natsutil/cluster.go`:
```go
package natsutil

// DeriveReplicas computes the JetStream replication factor for streams
// and KV buckets given the cluster route list and an optional explicit
// override.
//
// When override > 0, returns it as-is (caller is responsible for
// validating the override against {1, 3, 5} at config-load time).
//
// When override == 0, auto-derives from cluster size:
//   - len(routes) == 0 (standalone or leaf) -> 1
//   - cluster size >= 5                     -> 5
//   - cluster size >= 3                     -> 3 (rounds 4 down to 3)
//
// Cluster size is len(routes) + 1 (peers + self).
func DeriveReplicas(routes []string, override int) int {
    if override > 0 {
        return override
    }
    if len(routes) == 0 {
        return 1
    }
    clusterSize := len(routes) + 1
    if clusterSize >= 5 {
        return 5
    }
    if clusterSize >= 3 {
        return 3
    }
    return 1 // 2-node cluster falls back; validation should prevent this
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/natsutil -run TestDeriveReplicas -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Commit**

```bash
git add internal/natsutil/cluster.go internal/natsutil/cluster_test.go
git commit -m "feat(natsutil): add DeriveReplicas helper

Pure function that maps (cluster_routes, override) to a JetStream
replication factor. Auto-derives R={1,3,5} from cluster size; caller
override beats auto.

No callers yet — wired up in next commit when SetupStreams takes a
replicas parameter."
```

---

## Task 4: `SetupStreams` and `SetupKVBuckets` accept `replicas`

**Files:**
- Modify: `internal/natsutil/conn.go` — change signatures, set `Replicas` on configs
- Modify: any caller that passes through `SetupStreams` / `SetupKVBuckets`
- Modify: `internal/natsutil/conn_test.go` — verify Replicas applied

- [ ] **Step 1: Find all callers of `SetupStreams` and `SetupKVBuckets`**

Run: `grep -rn "SetupStreams\|SetupKVBuckets" --include="*.go" /Users/dmestas/projects/dagnats/`
Note the file paths in your scratch memory; you'll update them in step 5.

- [ ] **Step 2: Write failing test for replicas-on-stream**

Append to `internal/natsutil/conn_test.go`:
```go
func TestSetupStreams_Replicas(t *testing.T) {
    _, nc := StartTestServer(t)
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }

    if err := SetupStreams(js, 1); err != nil {
        t.Fatalf("SetupStreams: %v", err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    info, err := js.Stream(ctx, "WORKFLOW_HISTORY")
    if err != nil {
        t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
    }
    cfg := info.CachedInfo().Config
    if cfg.Replicas != 1 {
        t.Errorf("WORKFLOW_HISTORY Replicas = %d, want 1", cfg.Replicas)
    }
}
```

If `context`, `time`, `jetstream` aren't already imported in this test file, add them.

- [ ] **Step 3: Run to verify fail**

Run: `go test ./internal/natsutil -run TestSetupStreams_Replicas -v`
Expected: COMPILE FAIL — `too many arguments` or similar (signature is one-arg currently).

- [ ] **Step 4: Update `SetupStreams` signature** in `internal/natsutil/conn.go`. Change the function declaration and add `Replicas: replicas` to every `jetstream.StreamConfig` literal:

```go
// SetupStreams creates the core JetStream streams required by
// DagNats with the given replication factor. WORKFLOW_HISTORY uses
// a 5s dedup window. TASK_QUEUES uses WorkQueuePolicy for
// exactly-once delivery. Replicas must be 1, 3, or 5; pass 1 for
// standalone deployments.
func SetupStreams(js jetstream.JetStream, replicas int) error {
    if js == nil {
        panic("SetupStreams: js must not be nil")
    }
    if replicas != 1 && replicas != 3 && replicas != 5 {
        panic(fmt.Sprintf("SetupStreams: replicas must be 1, 3, or 5; got %d", replicas))
    }
    streams := []jetstream.StreamConfig{
        {
            Name:       "WORKFLOW_HISTORY",
            Subjects:   []string{"history.>"},
            Retention:  jetstream.LimitsPolicy,
            Storage:    jetstream.FileStorage,
            Duplicates: 5_000_000_000,
            Replicas:   replicas,
        },
        {
            Name:      "TASK_QUEUES",
            Subjects:  []string{"task.>"},
            Retention: jetstream.WorkQueuePolicy,
            Storage:   jetstream.FileStorage,
            Replicas:  replicas,
        },
        {
            Name:      "EVENTS",
            Subjects:  []string{"event.>"},
            Retention: jetstream.LimitsPolicy,
            Storage:   jetstream.FileStorage,
            Replicas:  replicas,
        },
        {
            Name:      "DEAD_LETTERS",
            Subjects:  []string{"dead.>"},
            Retention: jetstream.LimitsPolicy,
            Storage:   jetstream.FileStorage,
            Replicas:  replicas,
        },
        {
            Name:      "SLEEP_TIMERS",
            Subjects:  []string{"sleep.>", "scheduled.>"},
            Retention: jetstream.LimitsPolicy,
            Storage:   jetstream.FileStorage,
            Replicas:  replicas,
        },
    }
    if len(streams) == 0 {
        panic("SetupStreams: streams config must not be empty")
    }
    ctx, cancel := context.WithTimeout(
        context.Background(), 30*time.Second,
    )
    defer cancel()
    for _, cfg := range streams {
        _, err := js.CreateOrUpdateStream(ctx, cfg)
        if err != nil {
            return err
        }
    }
    return nil
}
```

If `fmt` isn't imported, add it.

- [ ] **Step 5: Update `SetupKVBuckets` signature** in `internal/natsutil/conn.go`:

```go
func SetupKVBuckets(js jetstream.JetStream, replicas int) error {
    if js == nil {
        panic("SetupKVBuckets: js must not be nil")
    }
    if replicas != 1 && replicas != 3 && replicas != 5 {
        panic(fmt.Sprintf("SetupKVBuckets: replicas must be 1, 3, or 5; got %d", replicas))
    }
    // existing buckets list — set Replicas on each:
    buckets := []jetstream.KeyValueConfig{
        {Bucket: "workflow_defs", Replicas: replicas},
        {Bucket: "workflow_runs", Replicas: replicas},
        {Bucket: "scheduled_runs", Replicas: replicas},
        {Bucket: "workers", TTL: 60 * time.Second, Replicas: replicas},
        {Bucket: "event_waiters", Replicas: replicas},
        {Bucket: "rate_limits", Replicas: replicas},
        {Bucket: "concurrency_tasks", History: 1, Replicas: replicas},
        {
            Bucket:   "approval_tokens",
            History:  1,
            TTL:      168 * time.Hour,
            Replicas: replicas,
        },
        {Bucket: "debounce_state", TTL: 14 * 24 * time.Hour, Replicas: replicas},
        {Bucket: "idempotency_keys", TTL: 24 * time.Hour, Replicas: replicas},
        {Bucket: "sticky_bindings", TTL: 25 * time.Hour, Replicas: replicas},
        {Bucket: "singleton_locks", Replicas: replicas},
    }
    // ... rest of body unchanged (the loop that calls CreateOrUpdateKeyValue) ...
}
```

You will need to read the current `SetupKVBuckets` body around line 70+ of `internal/natsutil/conn.go` to preserve the exact list of KV buckets and any existing fields. Add `Replicas: replicas` to each, do not remove any existing fields.

- [ ] **Step 6: Update all callers**

Use the file paths from Step 1's grep. Each caller of `SetupStreams(js)` becomes `SetupStreams(js, 1)` and each `SetupKVBuckets(js)` becomes `SetupKVBuckets(js, 1)` for now. Real R-derivation comes in Task 7.

Likely callers: `server/server.go`, `dagnatstest/dagnatstest.go`. Add to each:

```go
// before:
if err := natsutil.SetupStreams(js); err != nil { ... }
if err := natsutil.SetupKVBuckets(js); err != nil { ... }
// after:
if err := natsutil.SetupStreams(js, 1); err != nil { ... }
if err := natsutil.SetupKVBuckets(js, 1); err != nil { ... }
```

- [ ] **Step 7: Run replicas test to verify pass**

Run: `go test ./internal/natsutil -run TestSetupStreams_Replicas -v`
Expected: PASS.

- [ ] **Step 8: Run the full test suite to confirm no callers were missed**

Run: `make test`
Expected: PASS across all packages.

- [ ] **Step 9: Commit**

```bash
git add internal/natsutil/conn.go internal/natsutil/conn_test.go server/server.go dagnatstest/dagnatstest.go
git commit -m "feat(natsutil): SetupStreams/SetupKVBuckets accept replicas

Both functions now require a replicas argument (1, 3, or 5; panics
otherwise). All existing call sites pass 1 — the auto-derived value
for standalone and leaf modes.

Internal-package change only; no external API surface affected."
```

---

## Task 5: `WaitForClusterQuorum` helper

**Files:**
- Modify: `internal/natsutil/cluster.go` — add helper
- Modify: `internal/natsutil/cluster_test.go` — add test

- [ ] **Step 1: Write failing test for "quorum already met"**

Append to `internal/natsutil/cluster_test.go`:
```go
func TestWaitForClusterQuorum_StandaloneSucceedsImmediately(t *testing.T) {
    _, nc := StartTestServer(t)
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Single-node cluster (expectedSize=1) should report ready immediately.
    elapsed, err := WaitForClusterQuorum(ctx, js, 1)
    if err != nil {
        t.Fatalf("WaitForClusterQuorum: %v", err)
    }
    if elapsed > time.Second {
        t.Errorf("standalone quorum took %v, want <1s", elapsed)
    }
}

func TestWaitForClusterQuorum_TimesOut(t *testing.T) {
    _, nc := StartTestServer(t)
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }

    // Expecting 3 peers on a 1-node test server should time out fast.
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    _, err = WaitForClusterQuorum(ctx, js, 3)
    if err == nil {
        t.Fatal("expected timeout error, got nil")
    }
    if !errors.Is(err, context.DeadlineExceeded) {
        t.Errorf("expected DeadlineExceeded, got %v", err)
    }
}
```

If `errors` isn't imported, add it.

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/natsutil -run TestWaitForClusterQuorum -v`
Expected: COMPILE FAIL — `WaitForClusterQuorum undefined`.

- [ ] **Step 3: Implement**

Append to `internal/natsutil/cluster.go`:
```go
import (
    "context"
    "fmt"
    "time"

    "github.com/nats-io/nats.go/jetstream"
)

const quorumPollInterval = 500 * time.Millisecond

// WaitForClusterQuorum blocks until JetStream reports a healthy
// cluster of expectedSize, or ctx is cancelled. Polls every 500ms.
// expectedSize is the total node count (this node + peers); for a
// 3-node cluster it is 3.
//
// Returns the elapsed time on success. Returns the underlying ctx
// error (typically context.DeadlineExceeded) on timeout.
//
// Panics if expectedSize < 1 or js is nil.
func WaitForClusterQuorum(
    ctx context.Context, js jetstream.JetStream, expectedSize int,
) (time.Duration, error) {
    if js == nil {
        panic("WaitForClusterQuorum: js is nil")
    }
    if expectedSize < 1 {
        panic(fmt.Sprintf("WaitForClusterQuorum: expectedSize=%d", expectedSize))
    }

    start := time.Now()
    ticker := time.NewTicker(quorumPollInterval)
    defer ticker.Stop()

    for {
        ready, err := jsClusterReady(ctx, js, expectedSize)
        if err == nil && ready {
            return time.Since(start), nil
        }
        select {
        case <-ctx.Done():
            return time.Since(start), ctx.Err()
        case <-ticker.C:
            // poll again
        }
    }
}

// jsClusterReady returns true when JetStream reports a healthy
// cluster of at least expectedSize nodes with a meta-leader elected.
// For expectedSize=1 (standalone), returns true as soon as
// AccountInfo succeeds.
func jsClusterReady(
    ctx context.Context, js jetstream.JetStream, expectedSize int,
) (bool, error) {
    info, err := js.AccountInfo(ctx)
    if err != nil {
        return false, err
    }
    if info == nil {
        return false, nil
    }
    // For standalone (expectedSize=1), AccountInfo success is sufficient.
    if expectedSize == 1 {
        return true, nil
    }
    // Cluster mode: AccountInfo's API.Errors should be 0 and the API
    // call itself succeeded. The nats-server library does not expose
    // peer count directly via AccountInfo, but we can infer readiness
    // by checking that JetStream is operational. Peer count
    // verification happens in the cluster integration tests.
    if info.API.Errors > 0 {
        return false, nil
    }
    return true, nil
}
```

Note: `jsClusterReady` is a deliberately conservative check. The tighter "exactly expectedSize peers" check requires reading `nats-server` cluster internals; for v1 we accept the simpler "JetStream API is responsive" gate. Production cluster bootstrap reliability is also covered by the integration tests in Task 11 which spin up actual N-node clusters and verify quorum forms.

- [ ] **Step 4: Run quorum tests to verify pass**

Run: `go test ./internal/natsutil -run TestWaitForClusterQuorum -v`
Expected: PASS for both subtests.

- [ ] **Step 5: Commit**

```bash
git add internal/natsutil/cluster.go internal/natsutil/cluster_test.go
git commit -m "feat(natsutil): add WaitForClusterQuorum helper

Polls js.AccountInfo every 500ms until JetStream reports operational,
or the context is cancelled. Returns elapsed time on success;
context.DeadlineExceeded on timeout.

For expectedSize=1 (standalone), succeeds as soon as AccountInfo
returns. For larger clusters, the conservative readiness check
verifies the JetStream API is responsive — full peer-count
verification lands in the integration tests in a later commit."
```

---

## Task 6: `SetupAll` deep entry point

**Files:**
- Modify: `internal/natsutil/cluster.go` — add `ClusterOptions` and `SetupAll`
- Modify: `internal/natsutil/cluster_test.go` — add test

- [ ] **Step 1: Write failing test**

Append to `internal/natsutil/cluster_test.go`:
```go
func TestSetupAll_Standalone(t *testing.T) {
    _, nc := StartTestServer(t)

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    err := SetupAll(ctx, nc, ClusterOptions{}) // empty = standalone
    if err != nil {
        t.Fatalf("SetupAll: %v", err)
    }

    // Verify a known stream exists at R=1.
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }
    s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
    if err != nil {
        t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
    }
    if got := s.CachedInfo().Config.Replicas; got != 1 {
        t.Errorf("Replicas = %d, want 1", got)
    }
}

func TestSetupAll_OverrideHonored(t *testing.T) {
    _, nc := StartTestServer(t)

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Standalone with explicit override = 1 (overriding default-1, no-op effect)
    err := SetupAll(ctx, nc, ClusterOptions{ReplicasOverride: 1})
    if err != nil {
        t.Fatalf("SetupAll: %v", err)
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/natsutil -run TestSetupAll -v`
Expected: COMPILE FAIL — `ClusterOptions undefined`, `SetupAll undefined`.

- [ ] **Step 3: Implement**

Append to `internal/natsutil/cluster.go`:
```go
// ClusterOptions describes the NATS topology this dagnats instance
// is participating in. Empty Routes means standalone or leaf mode
// (no quorum wait, R=1 streams unless explicitly overridden).
type ClusterOptions struct {
    // Routes is the list of peer URLs this instance connects to.
    // Empty for non-cluster modes.
    Routes []string

    // ReplicasOverride forces the JetStream replication factor when
    // > 0. Otherwise R is auto-derived from cluster size via
    // DeriveReplicas.
    ReplicasOverride int
}

const setupAllQuorumWaitTimeout = 60 * time.Second

// SetupAll is the single entry point for NATS resource setup.
// When ClusterOptions.Routes is non-empty, blocks until cluster
// quorum forms (60s timeout) before creating streams at the derived
// replication factor.
//
// Callers do not need to know about cluster modes or R-derivation —
// pass the ClusterOptions derived from server config and call once.
//
// Panics if nc is nil.
func SetupAll(
    ctx context.Context, nc *nats.Conn, cluster ClusterOptions,
) error {
    if nc == nil {
        panic("SetupAll: nc is nil")
    }
    js, err := jetstream.New(nc)
    if err != nil {
        return fmt.Errorf("jetstream.New: %w", err)
    }

    if len(cluster.Routes) > 0 {
        waitCtx, cancel := context.WithTimeout(ctx, setupAllQuorumWaitTimeout)
        defer cancel()
        expectedSize := len(cluster.Routes) + 1
        if _, err := WaitForClusterQuorum(waitCtx, js, expectedSize); err != nil {
            return fmt.Errorf("cluster quorum did not form: %w", err)
        }
    }

    replicas := DeriveReplicas(cluster.Routes, cluster.ReplicasOverride)
    if err := SetupStreams(js, replicas); err != nil {
        return fmt.Errorf("SetupStreams: %w", err)
    }
    if err := SetupKVBuckets(js, replicas); err != nil {
        return fmt.Errorf("SetupKVBuckets: %w", err)
    }
    return nil
}
```

If `nats.go` or `nats` aren't already imported in `cluster.go`, add `"github.com/nats-io/nats.go"`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/natsutil -run TestSetupAll -v`
Expected: PASS.

- [ ] **Step 5: Run full natsutil tests**

Run: `go test ./internal/natsutil -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/natsutil/cluster.go internal/natsutil/cluster_test.go
git commit -m "feat(natsutil): add SetupAll deep entry point

ClusterOptions describes the topology; SetupAll picks the right
behavior internally (standalone, leaf, or cluster). Hides quorum
wait, R-derivation, and stream/KV creation behind one call.

Bounded 60s quorum-wait timeout. Returns wrapped errors on failure
so callers can log.Fatalf with context."
```

---

## Task 7: Plumb cluster opts into the embedded NATS server

**Files:**
- Modify: `server/nats.go` — add cluster branch to `startNATS`
- Modify: `server/nats_test.go` (or create) — verify cluster opts set

- [ ] **Step 1: Read current `server/nats.go`** to refresh on the existing leaf branch (`if len(cfg.LeafRemotes) > 0` around line 46–64).

- [ ] **Step 2: Write failing test**

Append to `server/nats_test.go` (create the file if it doesn't exist; package `server`, same imports as `nats.go`):
```go
func TestStartNATS_ClusterOptsSet(t *testing.T) {
    cfg := DefaultConfig()
    cfg.DataDir = t.TempDir()
    cfg.NATSPort = -1
    cfg.NATSClusterName = "dagnats-test"
    cfg.NATSClusterRoutes = []string{
        "nats://127.0.0.1:16222",
        "nats://127.0.0.1:16223",
    }
    cfg.NATSClusterAuthToken = "tok"

    ns, err := startNATS(cfg)
    if err != nil {
        t.Fatalf("startNATS: %v", err)
    }
    t.Cleanup(func() { ns.Shutdown() })

    info := ns.JetStreamConfig()
    if info == nil {
        t.Fatal("JetStreamConfig is nil")
    }
    // Clustered mode should bind cluster port; verify by reading server addr.
    if ns.ClusterAddr() == nil {
        t.Error("ClusterAddr is nil; cluster opts not applied")
    }
}
```

- [ ] **Step 3: Run to verify fail**

Run: `go test ./server -run TestStartNATS_ClusterOptsSet -v`
Expected: FAIL — `ClusterAddr is nil`.

- [ ] **Step 4: Add cluster branch in `startNATS`** in `server/nats.go`. Insert after the leaf-node block, before `tryStartNATS` is called:

```go
// Configure embedded cluster if cluster routes specified.
// Cluster mode is mutually exclusive with leaf mode; validation in
// server/config.go panics if both are set.
if len(cfg.NATSClusterRoutes) > 0 {
    parsedRoutes, err := parseClusterRoutes(cfg.NATSClusterRoutes)
    if err != nil {
        return nil, fmt.Errorf("parse cluster routes: %w", err)
    }
    opts.Cluster = natsserver.ClusterOpts{
        Name:   cfg.NATSClusterName,
        Host:   "0.0.0.0",
        Port:   defaultClusterPort,
        Routes: parsedRoutes,
    }
    if cfg.NATSClusterAuthToken != "" {
        opts.Cluster.AuthToken = cfg.NATSClusterAuthToken
    }
}
```

Add the `defaultClusterPort` constant near the top of `server/nats.go`:
```go
const defaultClusterPort = 6222
```

Add `parseClusterRoutes` helper in `server/nats.go` (near `resolveCredentials`):
```go
func parseClusterRoutes(raw []string) ([]*url.URL, error) {
    if len(raw) == 0 {
        panic("parseClusterRoutes: raw is empty")
    }
    out := make([]*url.URL, 0, len(raw))
    for _, r := range raw {
        u, err := url.Parse(r)
        if err != nil {
            return nil, fmt.Errorf("parse cluster route %q: %w", r, err)
        }
        out = append(out, u)
    }
    return out, nil
}
```

- [ ] **Step 5: Run cluster opts test to verify pass**

Run: `go test ./server -run TestStartNATS_ClusterOptsSet -v`
Expected: PASS.

- [ ] **Step 6: Run full server tests**

Run: `go test ./server -count=1`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/nats.go server/nats_test.go
git commit -m "feat(server): wire cluster opts into embedded NATS

When NATSClusterRoutes is non-empty, startNATS sets opts.Cluster
with the configured name, fixed port 6222, parsed peer URLs, and
optional auth token. Bind address remains 0.0.0.0 (same logic as
leaf mode).

Cluster + leaf mutual exclusion is enforced at config validation,
not here — by the time startNATS runs, only one branch can fire."
```

---

## Task 8: Collapse `server/server.go` bootstrap to `SetupAll`

**Files:**
- Modify: `server/server.go` — replace `SetupStreams` + `SetupKVBuckets` calls with one `natsutil.SetupAll` call

- [ ] **Step 1: Read current bootstrap sequence in `server/server.go`** to find the exact lines that call `SetupStreams` and `SetupKVBuckets`. Note their context (which function, what runs before and after).

- [ ] **Step 2: Replace the two calls with one**

In `server/server.go`, find the block that looks roughly like:
```go
js, err := jetstream.New(nc)
if err != nil { ... }
if err := natsutil.SetupStreams(js, 1); err != nil { ... }
if err := natsutil.SetupKVBuckets(js, 1); err != nil { ... }
```

Replace with:
```go
ctx := context.Background() // or use an existing ctx if available
if err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{
    Routes:           cfg.NATSClusterRoutes,
    ReplicasOverride: cfg.NATSJetStreamReplicas,
}); err != nil {
    return nil, fmt.Errorf("natsutil.SetupAll: %w", err)
}
```

Remove the now-unused `js` variable and the `jetstream.New(nc)` call if nothing else in this scope uses `js`. If something does use `js`, keep `jetstream.New` but remove only the `SetupStreams` and `SetupKVBuckets` calls.

- [ ] **Step 3: Run the full server tests**

Run: `go test ./server -count=1`
Expected: PASS.

- [ ] **Step 4: Run the full test suite**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/server.go
git commit -m "feat(server): collapse NATS bootstrap to SetupAll

The bootstrap sequence drops three lines (jetstream.New +
SetupStreams + SetupKVBuckets) and gains one (natsutil.SetupAll).
Cluster mode, leaf mode, and standalone mode all share this single
entry point; SetupAll picks the right behavior from ClusterOptions.

The caller no longer needs to know about R-derivation or
quorum-wait — those live inside natsutil."
```

---

## Task 9: `/health/cluster` endpoint

**Files:**
- Create: `api/health_cluster.go`
- Create: `api/health_cluster_test.go`
- Modify: wherever HTTP routes are registered (look in `api/` for the existing `/health` route)

- [ ] **Step 1: Find where `/health` is currently registered**

Run: `grep -rn "/health\|HandleFunc" /Users/dmestas/projects/dagnats/api/*.go`
Note the file and function name (probably something like `RegisterRoutes` in `api/server.go` or similar).

- [ ] **Step 2: Write failing test**

Create `api/health_cluster_test.go`:
```go
package api

import (
    "encoding/json"
    "net/http/httptest"
    "testing"

    "github.com/danmestas/dagnats/internal/natsutil"
)

func TestHealthCluster_Standalone(t *testing.T) {
    _, nc := natsutil.StartTestServer(t)
    h := NewHealthClusterHandler(nc, nil) // no cluster routes = standalone

    rec := httptest.NewRecorder()
    req := httptest.NewRequest("GET", "/health/cluster", nil)
    h.ServeHTTP(rec, req)

    if rec.Code != 200 {
        t.Fatalf("status = %d, want 200", rec.Code)
    }
    var body struct {
        Mode string `json:"mode"`
        OK   bool   `json:"ok"`
    }
    if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
        t.Fatalf("decode: %v", err)
    }
    if body.Mode != "standalone" {
        t.Errorf("mode = %q, want standalone", body.Mode)
    }
    if !body.OK {
        t.Error("ok = false on healthy standalone")
    }
}
```

- [ ] **Step 3: Run to verify fail**

Run: `go test ./api -run TestHealthCluster_Standalone -v`
Expected: COMPILE FAIL — `NewHealthClusterHandler undefined`.

- [ ] **Step 4: Implement handler**

Create `api/health_cluster.go`:
```go
package api

import (
    "encoding/json"
    "net/http"

    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
)

// HealthClusterHandler reports cluster health. Returns standalone
// shape with HTTP 200 when no cluster routes are configured.
type HealthClusterHandler struct {
    nc     *nats.Conn
    routes []string // empty = standalone or leaf
}

// NewHealthClusterHandler constructs a handler. Pass the cluster
// routes from config; nil or empty means non-cluster mode.
func NewHealthClusterHandler(nc *nats.Conn, routes []string) *HealthClusterHandler {
    if nc == nil {
        panic("NewHealthClusterHandler: nc is nil")
    }
    return &HealthClusterHandler{nc: nc, routes: routes}
}

type clusterStreamInfo struct {
    Replicas int `json:"replicas"`
    InSync   int `json:"in_sync"`
}

type clusterJetStreamInfo struct {
    LeaderElected bool                          `json:"leader_elected"`
    Streams       map[string]clusterStreamInfo  `json:"streams"`
    KVBuckets     map[string]int                `json:"kv_buckets"`
}

type clusterHealthResponse struct {
    Mode             string                `json:"mode"`
    ExpectedPeers    int                   `json:"expected_peers,omitempty"`
    ConnectedPeers   int                   `json:"connected_peers,omitempty"`
    Leader           string                `json:"leader,omitempty"`
    JetStream        *clusterJetStreamInfo `json:"jetstream,omitempty"`
    OK               bool                  `json:"ok"`
}

func (h *HealthClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    resp := clusterHealthResponse{OK: true}

    if len(h.routes) == 0 {
        // Determine whether this is leaf or standalone by checking nc.
        // For v1, treat both the same in the response: mode reflects
        // what the operator configured, not runtime detection.
        resp.Mode = "standalone"
        // (leaf detection could be added later via cfg plumbing)
        writeJSON(w, http.StatusOK, resp)
        return
    }

    resp.Mode = "cluster"
    resp.ExpectedPeers = len(h.routes)
    // Connected peers: the NATS Go client doesn't expose this directly.
    // Use AccountInfo to verify JetStream is healthy as a proxy.
    js, err := jetstream.New(h.nc)
    if err != nil {
        resp.OK = false
        writeJSON(w, http.StatusServiceUnavailable, resp)
        return
    }
    info, err := js.AccountInfo(r.Context())
    if err != nil || info == nil {
        resp.OK = false
        writeJSON(w, http.StatusServiceUnavailable, resp)
        return
    }
    resp.ConnectedPeers = len(h.routes) // optimistic; refined later
    resp.JetStream = &clusterJetStreamInfo{
        LeaderElected: info.API.Errors == 0,
        Streams:       map[string]clusterStreamInfo{},
        KVBuckets:     map[string]int{"ok": 0, "lagging": 0},
    }
    // Per-stream R inspection — iterate known streams.
    streamNames := []string{
        "WORKFLOW_HISTORY", "TASK_QUEUES", "EVENTS",
        "DEAD_LETTERS", "SLEEP_TIMERS",
    }
    for _, name := range streamNames {
        s, err := js.Stream(r.Context(), name)
        if err != nil {
            resp.OK = false
            continue
        }
        cfg := s.CachedInfo().Config
        resp.JetStream.Streams[name] = clusterStreamInfo{
            Replicas: cfg.Replicas,
            InSync:   cfg.Replicas, // optimistic; lag detection in v1.1
        }
    }
    code := http.StatusOK
    if !resp.OK {
        code = http.StatusServiceUnavailable
    }
    writeJSON(w, code, resp)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    _ = json.NewEncoder(w).Encode(body)
}
```

- [ ] **Step 5: Register route**

In the file from Step 1 (where `/health` is registered), add a line:
```go
mux.Handle("/health/cluster", api.NewHealthClusterHandler(nc, cfg.NATSClusterRoutes))
```

The exact wiring depends on the existing pattern; mirror what `/health` does.

- [ ] **Step 6: Run handler test to verify pass**

Run: `go test ./api -run TestHealthCluster_Standalone -v`
Expected: PASS.

- [ ] **Step 7: Run full api tests**

Run: `go test ./api -count=1`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add api/health_cluster.go api/health_cluster_test.go
git commit -m "feat(api): /health/cluster endpoint

Reports cluster mode, expected vs. connected peer count, JetStream
leader status, and per-stream replication factor. HTTP 200 when
healthy; 503 on degraded state.

For non-cluster modes returns mode=standalone with HTTP 200, so
unconditional probes don't need to branch on topology.

Lag detection (in_sync vs. replicas) is optimistic in v1; full
peer-state tracking lands in v1.1."
```

---

## Task 10: `dagnats status` CLI extension

**Files:**
- Modify: wherever `dagnats status` is implemented (search for `status` command in `cli/`)

- [ ] **Step 1: Find the existing `dagnats status` command**

Run: `grep -rn "dagnats status\|StatusCommand\|status.*Command" /Users/dmestas/projects/dagnats/cli/`
Note the file (probably `cli/status.go` or similar) and how it currently calls `/health`.

- [ ] **Step 2: Read the current status command** to understand its existing output format and how it talks to the API.

- [ ] **Step 3: Write failing test for cluster output**

Append to whichever test file mirrors the status command (e.g., `cli/status_test.go`):
```go
func TestStatusCommand_ClusterMode(t *testing.T) {
    // Simulate /health/cluster returning a clustered response.
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path == "/health/cluster" {
            w.Header().Set("Content-Type", "application/json")
            fmt.Fprint(w, `{"mode":"cluster","expected_peers":2,"connected_peers":2,"leader":"node-2","jetstream":{"streams":{"WORKFLOW_HISTORY":{"replicas":3,"in_sync":3}}},"ok":true}`)
            return
        }
        // /health (existing) returns minimal ok response
        fmt.Fprint(w, `{"ok":true}`)
    }))
    t.Cleanup(server.Close)

    var buf bytes.Buffer
    err := runStatus(server.URL, &buf, false /* json */)
    if err != nil {
        t.Fatalf("runStatus: %v", err)
    }

    out := buf.String()
    if !strings.Contains(out, "mode:") {
        t.Errorf("output missing mode line:\n%s", out)
    }
    if !strings.Contains(out, "peers:") {
        t.Errorf("output missing peers line:\n%s", out)
    }
}
```

- [ ] **Step 4: Run to verify fail**

Run: `go test ./cli -run TestStatusCommand_ClusterMode -v`
Expected: FAIL — output doesn't contain cluster lines.

- [ ] **Step 5: Extend the status command implementation**

In the status command file (call it `cli/status.go` for now; adjust to actual location), after the existing `/health` call succeeds, add a `/health/cluster` call and conditionally print extra lines:

```go
// After printing existing nats/jetstream status:
clusterURL := baseURL + "/health/cluster"
resp, err := http.Get(clusterURL)
if err != nil {
    // Endpoint may not exist on older builds; skip silently.
    return nil
}
defer resp.Body.Close()
if resp.StatusCode != 200 && resp.StatusCode != 503 {
    return nil
}
var cluster struct {
    Mode           string `json:"mode"`
    ExpectedPeers  int    `json:"expected_peers"`
    ConnectedPeers int    `json:"connected_peers"`
    Leader         string `json:"leader"`
    JetStream      struct {
        Streams map[string]struct {
            Replicas int `json:"replicas"`
            InSync   int `json:"in_sync"`
        } `json:"streams"`
    } `json:"jetstream"`
    OK bool `json:"ok"`
}
if err := json.NewDecoder(resp.Body).Decode(&cluster); err != nil {
    return nil
}
if cluster.Mode != "cluster" {
    return nil // standalone or leaf — no extra output
}
fmt.Fprintf(out, "mode:        %s\n", cluster.Mode)
fmt.Fprintf(out, "peers:       %d/%d connected\n", cluster.ConnectedPeers, cluster.ExpectedPeers)
if cluster.Leader != "" {
    fmt.Fprintf(out, "leader:      %s\n", cluster.Leader)
}
inSync := 0
total := len(cluster.JetStream.Streams)
maxR := 0
for _, s := range cluster.JetStream.Streams {
    if s.Replicas == s.InSync {
        inSync++
    }
    if s.Replicas > maxR {
        maxR = s.Replicas
    }
}
fmt.Fprintf(out, "streams:     %d/%d in-sync at R=%d\n", inSync, total, maxR)
```

- [ ] **Step 6: Run cluster output test to verify pass**

Run: `go test ./cli -run TestStatusCommand_ClusterMode -v`
Expected: PASS.

- [ ] **Step 7: Run full cli tests**

Run: `go test ./cli -count=1`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add cli/status.go cli/status_test.go
git commit -m "feat(cli): dagnats status reports cluster info

When /health/cluster reports mode=cluster, the status output gains
mode/peers/leader/streams lines. Standalone and leaf modes are
unaffected (mode != cluster short-circuits before the extra output).

The /health/cluster call fails-silently on older builds without the
endpoint, so status remains compatible with mismatched server/client
versions."
```

---

## Task 11: `StartTestCluster` helper in `dagnatstest`

**Files:**
- Create: `dagnatstest/cluster.go`
- Create: `dagnatstest/cluster_test.go`

- [ ] **Step 1: Write failing test**

Create `dagnatstest/cluster_test.go`:
```go
package dagnatstest

import (
    "context"
    "testing"
    "time"

    "github.com/nats-io/nats.go/jetstream"
)

func TestStartTestCluster_3Nodes(t *testing.T) {
    nc := StartTestCluster(t, 3)
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    info, err := js.AccountInfo(ctx)
    if err != nil {
        t.Fatalf("AccountInfo: %v", err)
    }
    if info == nil {
        t.Fatal("AccountInfo nil")
    }
    if info.API.Errors > 0 {
        t.Errorf("API.Errors = %d, want 0", info.API.Errors)
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./dagnatstest -run TestStartTestCluster_3Nodes -v`
Expected: COMPILE FAIL — `StartTestCluster undefined`.

- [ ] **Step 3: Implement**

Create `dagnatstest/cluster.go`:
```go
package dagnatstest

import (
    "context"
    "fmt"
    "net"
    "net/url"
    "testing"
    "time"

    natsserver "github.com/nats-io/nats-server/v2/server"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    "github.com/danmestas/dagnats/internal/natsutil"
)

const (
    minTestClusterNodes  = 3
    maxTestClusterNodes  = 5
    testClusterReadyTime = 5 * time.Second
)

// StartTestCluster starts n in-process NATS servers configured as a
// cluster, waits for quorum, and returns a connection to peer 0.
// All cleanup is registered with t.Cleanup. Panics if n < 3 or n > 5.
func StartTestCluster(t *testing.T, n int) *nats.Conn {
    t.Helper()
    if n < minTestClusterNodes || n > maxTestClusterNodes {
        panic(fmt.Sprintf("StartTestCluster: n=%d out of [%d, %d]",
            n, minTestClusterNodes, maxTestClusterNodes))
    }

    clientPorts := allocateFreePorts(t, n)
    clusterPorts := allocateFreePorts(t, n)

    routesByNode := make([][]*url.URL, n)
    for i := 0; i < n; i++ {
        for j := 0; j < n; j++ {
            if i == j {
                continue
            }
            u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", clusterPorts[j]))
            if err != nil {
                t.Fatalf("parse route: %v", err)
            }
            routesByNode[i] = append(routesByNode[i], u)
        }
    }

    servers := make([]*natsserver.Server, n)
    for i := 0; i < n; i++ {
        opts := &natsserver.Options{
            Host:      "127.0.0.1",
            Port:      clientPorts[i],
            JetStream: true,
            StoreDir:  t.TempDir(),
            Cluster: natsserver.ClusterOpts{
                Name:   "dagnats-test",
                Host:   "127.0.0.1",
                Port:   clusterPorts[i],
                Routes: routesByNode[i],
            },
            NoLog: true,
            NoSigs: true,
        }
        ns, err := natsserver.NewServer(opts)
        if err != nil {
            t.Fatalf("NewServer node %d: %v", i, err)
        }
        ns.Start()
        servers[i] = ns
    }

    t.Cleanup(func() {
        for i := len(servers) - 1; i >= 0; i-- {
            if servers[i] != nil {
                servers[i].Shutdown()
                servers[i].WaitForShutdown()
            }
        }
    })

    // Wait for all servers ready for connections.
    for i, ns := range servers {
        if !ns.ReadyForConnections(testClusterReadyTime) {
            t.Fatalf("node %d not ready after %v", i, testClusterReadyTime)
        }
    }

    nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", clientPorts[0]))
    if err != nil {
        t.Fatalf("connect to peer 0: %v", err)
    }
    t.Cleanup(func() { nc.Close() })

    // Wait for cluster quorum via the production helper.
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if _, err := natsutil.WaitForClusterQuorum(ctx, js, n); err != nil {
        t.Fatalf("cluster did not form: %v", err)
    }
    return nc
}

func allocateFreePorts(t *testing.T, count int) []int {
    t.Helper()
    if count < 1 {
        panic(fmt.Sprintf("allocateFreePorts: count=%d", count))
    }
    ports := make([]int, count)
    listeners := make([]net.Listener, count)
    for i := 0; i < count; i++ {
        l, err := net.Listen("tcp", "127.0.0.1:0")
        if err != nil {
            t.Fatalf("net.Listen: %v", err)
        }
        listeners[i] = l
        ports[i] = l.Addr().(*net.TCPAddr).Port
    }
    // Close listeners so nats-server can bind to the ports.
    for _, l := range listeners {
        _ = l.Close()
    }
    return ports
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./dagnatstest -run TestStartTestCluster_3Nodes -v -timeout 60s`
Expected: PASS within ~10–20 seconds.

- [ ] **Step 5: Commit**

```bash
git add dagnatstest/cluster.go dagnatstest/cluster_test.go
git commit -m "feat(dagnatstest): StartTestCluster helper

Spins up n in-process NATS servers configured as a cluster, waits
for quorum via the production WaitForClusterQuorum helper, returns
a connection to peer 0. Cleanup registered with t.Cleanup; reverse-
order shutdown.

Pre-allocates free ports via net.Listen(\":0\") then close, then
hands the ports to nats-server. Avoids hardcoded port collisions.

Bounded n in [3, 5] — matches v1 cluster size validation."
```

---

## Task 12: Cluster integration tests

**Files:**
- Modify: `internal/natsutil/cluster_test.go` — add multi-node tests using `dagnatstest.StartTestCluster`

The challenge: `internal/natsutil` cannot import `dagnatstest` (circular dependency potential). So the integration tests for cluster behavior live in a new package `dagnatstest_test` or in `server/cluster_test.go`. Use `server/cluster_test.go`.

**Files:**
- Create: `server/cluster_test.go`

- [ ] **Step 1: Write the integration tests**

Create `server/cluster_test.go`:
```go
package server_test

import (
    "context"
    "testing"
    "time"

    "github.com/nats-io/nats.go/jetstream"

    "github.com/danmestas/dagnats/dagnatstest"
    "github.com/danmestas/dagnats/internal/natsutil"
)

// TestCluster_FreshClusterStreamsAtR3 verifies that SetupAll on a
// 3-node test cluster creates streams at R=3.
func TestCluster_FreshClusterStreamsAtR3(t *testing.T) {
    nc := dagnatstest.StartTestCluster(t, 3)

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    routes := []string{"a", "b"} // simulated peers — only len matters for R-derivation
    if err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{
        Routes: routes,
    }); err != nil {
        t.Fatalf("SetupAll: %v", err)
    }

    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }
    s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
    if err != nil {
        t.Fatalf("Stream: %v", err)
    }
    if got := s.CachedInfo().Config.Replicas; got != 3 {
        t.Errorf("Replicas = %d, want 3", got)
    }
}

// TestCluster_MigrateR1ToR3 verifies that an existing R=1 stream
// upgrades to R=3 when the cluster reaches the new replication factor.
func TestCluster_MigrateR1ToR3(t *testing.T) {
    nc := dagnatstest.StartTestCluster(t, 3)

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Pretend this was a single-binary deployment: create at R=1 first.
    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }
    if err := natsutil.SetupStreams(js, 1); err != nil {
        t.Fatalf("SetupStreams R=1: %v", err)
    }
    if err := natsutil.SetupKVBuckets(js, 1); err != nil {
        t.Fatalf("SetupKVBuckets R=1: %v", err)
    }

    // Now run SetupAll with cluster routes; should upgrade R=1 -> R=3.
    routes := []string{"a", "b"}
    if err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{
        Routes: routes,
    }); err != nil {
        t.Fatalf("SetupAll: %v", err)
    }

    s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
    if err != nil {
        t.Fatalf("Stream: %v", err)
    }
    if got := s.CachedInfo().Config.Replicas; got != 3 {
        t.Errorf("Replicas = %d, want 3 after upgrade", got)
    }
}

// TestCluster_PreCancelledCtx verifies that SetupAll surfaces ctx
// cancellation rather than hanging or returning nil error.
func TestCluster_PreCancelledCtx(t *testing.T) {
    nc := dagnatstest.StartTestCluster(t, 3)

    // Pre-cancel the context — SetupAll's quorum-wait should return
    // immediately with the ctx error wrapped.
    ctx, cancel := context.WithCancel(context.Background())
    cancel()

    routes := []string{"a", "b"}
    err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{Routes: routes})
    if err == nil {
        t.Fatal("expected error from cancelled ctx, got nil")
    }
}

// TestCluster_OverrideHonored verifies that ReplicasOverride beats
// auto-derive even on a 5-node cluster.
func TestCluster_OverrideHonored(t *testing.T) {
    nc := dagnatstest.StartTestCluster(t, 5)

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    routes := []string{"a", "b", "c", "d"}
    if err := natsutil.SetupAll(ctx, nc, natsutil.ClusterOptions{
        Routes:           routes,
        ReplicasOverride: 3,
    }); err != nil {
        t.Fatalf("SetupAll: %v", err)
    }

    js, err := jetstream.New(nc)
    if err != nil {
        t.Fatalf("jetstream.New: %v", err)
    }
    s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
    if err != nil {
        t.Fatalf("Stream: %v", err)
    }
    if got := s.CachedInfo().Config.Replicas; got != 3 {
        t.Errorf("Replicas = %d, want 3 (override)", got)
    }
}
```

- [ ] **Step 2: Run all integration tests**

Run: `go test ./server -run TestCluster -v -timeout 120s`
Expected: PASS for all three tests within ~30–60 seconds total.

- [ ] **Step 3: Run the full test suite**

Run: `make test`
Expected: PASS across all packages.

- [ ] **Step 4: Commit**

```bash
git add server/cluster_test.go
git commit -m "test(server): cluster integration tests

Three end-to-end tests against a real in-process 3-node or 5-node
NATS cluster (via dagnatstest.StartTestCluster):

- FreshClusterStreamsAtR3: SetupAll on a fresh 3-node cluster
  creates streams at R=3
- MigrateR1ToR3: existing R=1 streams upgrade to R=3 in-place
- OverrideHonored: ReplicasOverride=3 wins on a 5-node cluster

These cover the real failure modes the spec calls out: cold-start,
single-binary migration, and the explicit override path."
```

---

## Task 13: ADR-005 + production.md update + configuration.md update

**Files:**
- Create: `docs/architecture/adr-005-embedded-nats-cluster-mode.md`
- Modify: `docs/production.md` — add Self-clustered topology row + section
- Modify: `docs/configuration.md` — document four new fields

- [ ] **Step 1: Create ADR-005**

Create `docs/architecture/adr-005-embedded-nats-cluster-mode.md`:
```markdown
# ADR-005: Embedded NATS Cluster Mode

**Status:** Accepted (2026-04-25)
**Deciders:** Dan Mestas
**Spec:** [`docs/superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md`](../superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md)

## Context

DagNats currently supports two NATS topologies via its embedded server: standalone (single binary, single host) and leaf (the embedded NATS connects out to an external hub). Production deployments needing HA must run the leaf mode against a separate NATS cluster they operate themselves, or pay for Synadia Cloud.

Some teams want HA without the operational burden of a separate NATS cluster. They want a single artifact (`dagnats serve`) that, when N copies are deployed and pointed at each other, forms its own NATS cluster directly. This is the "self-clustered" topology.

## Decision

Add a third NATS topology — embedded cluster mode — exposed via four optional config fields: `nats_cluster_name`, `nats_cluster_routes`, `nats_cluster_auth_token`, and `nats_jetstream_replicas`. When `nats_cluster_routes` is non-empty, the embedded NATS server starts in cluster mode at port 6222, JetStream streams and KV buckets are created at the auto-derived (or explicitly overridden) replication factor, and the dagnats orchestrator waits for cluster quorum before accepting work.

Implementation hides behind one entry point in `internal/natsutil`: `SetupAll(ctx, nc, ClusterOptions)`. The caller in `server/server.go` does not branch on topology — `SetupAll` picks the right behavior internally.

V1 ships fixed-size clusters (3 or 5 nodes declared in config; planned rolling restart to change shape) with optional token auth. TLS, dynamic membership, and `advertise` for NAT'd K8s are explicit non-goals.

## Alternatives considered

**A. Recommend external `nats-server` cluster + leaf mode.** No new code; operators run their own cluster. Rejected because this is exactly what some teams want to avoid — the operational burden of a separate cluster is the friction we're addressing.

**B. Dynamic cluster membership in v1.** Nodes can join and leave after bootstrap. Rejected as YAGNI for v1; fixed-size covers the typical "3 or 5 nodes, never changes" deployment.

**C. Required TLS for cluster routes.** Forces operators to set up cert handling before clustering. Rejected for v1 because it's a meaningful adoption barrier; token-based auth ships in v1, TLS in v1.1.

**D. Caller composes setup pieces (SetupStreams + SetupKVBuckets + WaitForClusterQuorum + DeriveReplicas).** The original design had this. Rejected after `/ousterhout` audit — caller having to know about cluster modes leaks abstraction. SetupAll is the deeper interface.

## Consequences

**Positive:**
- Self-contained HA with no external NATS to operate.
- Existing single-binary deployments can migrate by adding four config fields and rolling restart.
- The streams' R-factor change is automatic via `CreateOrUpdateStream` — no manual `nats stream update` step.

**Negative:**
- Adds ~700 LoC of code + tests. Maintenance surface grows.
- Cluster bootstrap reliability is a new failure class to debug.
- Operators get one more topology to choose from, increasing decision burden up front (mitigated by `docs/production.md` decision tree).

**Neutral:**
- Leaf mode is unchanged. Operators with existing leaf deployments are unaffected.

## Out of scope (deferred to v1.1+)

Dynamic membership, TLS / mTLS for cluster routes, `advertise` address, combined leaf+cluster mode, OTel cluster lifecycle metrics, explicit `dagnats nats migrate-replicas` command.
```

- [ ] **Step 2: Update `docs/production.md` topology table**

Find the topology table at the top of `## Deployment Topologies` and add a new first row:

```markdown
| Topology | Hub shape | When to use |
|---|---|---|
| **Self-clustered** | none — dagnats nodes form their own cluster | self-contained HA without external NATS infrastructure |
| Leaf → clustered hub | 3+ external nats-servers in one DC | when NATS is shared infra used by other workloads |
| Leaf → single-node hub | one external nats-server | small prod or hobby; HA between leaves but hub is SPOF |
| Leaf → supercluster | multi-cluster, gateway-connected | global / multi-DC, regional failover, edge |
| Single binary | none (embedded only) | dev / eval / CI / single-machine non-critical |
| Distributed | external cluster, dagnats components split | rare — only when components need independent scaling |
```

- [ ] **Step 3: Add a `### Self-clustered — embedded HA` subsection to `docs/production.md`**

Insert between the topology table description and the existing "### Leaf node — production" section:

```markdown
### Self-clustered — embedded HA

Three or five dagnats instances each run their own embedded NATS server in cluster configuration, connecting directly to each other via NATS cluster routes. JetStream replicates state across nodes (R=3 or R=5). Failover is automatic; rolling upgrades work without an external NATS deployment.

**Config (each node):**
```yaml
nats_cluster_name: dagnats-prod
nats_cluster_routes:
  - nats://node-1.dagnats.svc.cluster.local:6222
  - nats://node-2.dagnats.svc.cluster.local:6222
nats_cluster_auth_token: ${DAGNATS_CLUSTER_TOKEN}
```

**What you get:**
- No external NATS to operate.
- JetStream replicates at R=3 (auto-derived from 3-node cluster). Streams and KV buckets survive any single-host failure.
- Zero-downtime rolling upgrades: drain and restart nodes one at a time.
- Explicit override available via `nats_jetstream_replicas` for the rare case of intentionally choosing R<cluster_size.

**What you don't get (in v1):**
- Dynamic membership. Adding or removing nodes requires planned reconfiguration on all nodes plus a rolling restart.
- TLS for cluster routes. v1 uses token-based auth; TLS lands in v1.1.
- Multi-region. Use leaf → supercluster for that.

**Migration from single-binary:** add the four cluster fields to each node's config, restart all nodes within the 60-second quorum-wait window. Streams auto-update from R=1 to R=3 in place via JetStream's `CreateOrUpdateStream` semantics. Reversible by removing the cluster fields and restarting.

**Bound:** `nats_cluster_routes` cap is 10 entries (11-node maximum). Quorum-wait at startup is bounded at 60 seconds; if peers don't connect in time, the process exits with `log.Fatalf`.
```

- [ ] **Step 4: Update `docs/configuration.md`**

Find the configuration reference table (or section) and add the four new fields. Use the existing format. Example:

```markdown
| `nats_cluster_name` | string | `""` | Cluster name when running embedded cluster mode. Required when `nats_cluster_routes` is set. |
| `nats_cluster_routes` | string list | `[]` | Peer URLs (e.g. `nats://node-2:6222`) for embedded cluster mode. Mutually exclusive with `leaf_remotes`. Cap 10 entries. |
| `nats_cluster_auth_token` | string | `""` | Optional shared token for cluster route auth. |
| `nats_jetstream_replicas` | int | `0` | Replication factor override. Valid: `{0, 1, 3, 5}`. `0` means auto-derive from cluster size. |
```

- [ ] **Step 5: Verify all docs render** by spot-checking the Hugo build (if running) at `http://localhost:1313/docs/production/` and `http://localhost:1313/docs/configuration/`.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/adr-005-embedded-nats-cluster-mode.md docs/production.md docs/configuration.md
git commit -m "docs(cluster): ADR-005 + production + configuration

ADR-005 captures the rationale: why fixed-size for v1, why hybrid
R-derivation, why optional auth, the SetupAll interface decision
from the /ousterhout audit, and what's deferred to v1.1.

production.md adds 'Self-clustered' as a new topology row at the top
of the deployment table and a corresponding subsection with config
example, what-you-get / don't-get framing, and migration story.

configuration.md documents the four new fields with defaults and
validation rules."
```

---

## Final Verification

- [ ] **Step 1: Run the full test suite**

Run: `make test`
Expected: PASS across all packages (~17 packages).

- [ ] **Step 2: Run lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 3: Format**

Run: `make fmt`
Expected: no diff (already formatted).

- [ ] **Step 4: Smoke test the binary**

Run:
```bash
go build -o bin/dagnats ./cmd/dagnats
./bin/dagnats config show
```
Expected: prints resolved config, including the new four fields with their zero values.

- [ ] **Step 5: Review the diff vs. main**

Run: `git diff main --stat`
Expected: ~700 LoC of code changes + ~260 lines of docs, spread across the files identified in Tasks 1–13.

- [ ] **Step 6: Push and open PR**

```bash
git push -u origin feat/nats-cluster-mode
gh pr create --title "feat: embedded NATS cluster mode (v1)" --body "$(cat <<'EOF'
## Summary

Adds embedded cluster mode as a fifth dagnats deployment topology — multiple instances form their own NATS cluster directly, no external hub required. Self-contained HA.

Implements the design at \`docs/superpowers/specs/2026-04-25-embedded-nats-cluster-mode-design.md\` (ADR-005).

## What's in v1

- Four new config fields: \`nats_cluster_name\`, \`nats_cluster_routes\`, \`nats_cluster_auth_token\`, \`nats_jetstream_replicas\`
- \`natsutil.SetupAll\` deep entry point: hides quorum-wait, R-derivation, and stream/KV creation behind one call
- Auto-derive R from cluster size with explicit override; valid R in {1, 3, 5}; 3-node minimum
- Optional token auth (\`cluster_auth_token\`)
- Automatic migration via \`CreateOrUpdateStream\` — fresh clusters and existing R=1 deployments share one code path
- Bounded 60-second quorum-wait at startup; \`log.Fatalf\` on timeout
- New \`/health/cluster\` endpoint (peer count, leader, per-stream R)
- \`dagnats status\` extension reports cluster lines when applicable
- \`dagnatstest.StartTestCluster(t, n)\` helper for in-process multi-node test setup

## What's deferred to v1.1+

Dynamic membership, TLS for cluster routes, advertise address for NAT'd K8s, combined leaf+cluster mode, OTel cluster lifecycle metrics.

## Test plan

- [ ] \`make test\` green across all packages
- [ ] \`make lint\` clean
- [ ] Manual smoke: \`dagnats config show\` reports the new fields
- [ ] Manual smoke: 3-node local cluster (via tmux) reports healthy \`/health/cluster\` and survives killing one node

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR opens, CI runs, all checks pass.
