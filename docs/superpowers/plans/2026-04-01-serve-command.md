# `dagnats serve` Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Single-binary server that starts embedded NATS + engine + API + triggers with zero config.

**Architecture:** New `server/` package owns full lifecycle. `Config` resolves from env vars → config file → platform defaults. Embedded NATS runs in standalone or leaf node mode. All components connect to localhost. 15s hard shutdown deadline.

**Tech Stack:** Go, nats-server/v2 (already in go.mod), nats.go

---

## File Structure

```
server/
  config.go    — Config struct, ConfigFromEnv(), file loading, platform defaults
  config_test.go
  nats.go      — startNATS() embedded server setup
  nats_test.go
  server.go    — Server struct, New(), Run(), Stop()
  server_test.go
cli/
  serve.go     — runServeCmd() thin wrapper (modify)
  root.go      — add "serve" to dispatcher (modify)
```

---

## Chunk 1: Config

### Task 1: Config Struct and Platform Defaults

**Files:**
- Create: `server/config.go`
- Create: `server/config_test.go`

- [ ] **Step 1: Write failing test for platform defaults**

```go
// server/config_test.go
// Methodology: unit tests for config resolution. No NATS, no I/O beyond
// temp dirs. Positive and negative space for each behavior.
package server

import (
    "os"
    "path/filepath"
    "runtime"
    "testing"
)

func TestDefaultConfig_HasPlatformDataDir(t *testing.T) {
    cfg := DefaultConfig()

    if cfg.DataDir == "" {
        t.Fatal("DataDir must not be empty")
    }
    if runtime.GOOS == "darwin" {
        home, _ := os.UserHomeDir()
        expected := filepath.Join(home, "Library",
            "Application Support", "dagnats")
        if cfg.DataDir != expected {
            t.Fatalf("want %s, got %s", expected, cfg.DataDir)
        }
    }
}

func TestDefaultConfig_PortsAndLimits(t *testing.T) {
    cfg := DefaultConfig()

    if cfg.HTTPAddr != ":8080" {
        t.Fatalf("want :8080, got %s", cfg.HTTPAddr)
    }
    if cfg.NATSPort != 4222 {
        t.Fatalf("want 4222, got %d", cfg.NATSPort)
    }
    if cfg.MaxStoreBytes != 10<<30 {
        t.Fatalf("want 10GB, got %d", cfg.MaxStoreBytes)
    }
    if len(cfg.LeafRemotes) != 0 {
        t.Fatal("LeafRemotes must be empty by default")
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestDefaultConfig -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Write minimal implementation**

```go
// server/config.go
package server

import (
    "os"
    "path/filepath"
    "runtime"
)

const (
    defaultHTTPAddr      = ":8080"
    defaultNATSPort      = 4222
    defaultMaxStoreBytes = 10 << 30 // 10GB
    maxLeafRemotes       = 10
)

// Config controls the embedded server.
type Config struct {
    DataDir       string
    HTTPAddr      string
    NATSPort      int
    LeafRemotes   []string
    MaxStoreBytes int64
}

// DefaultConfig returns platform-appropriate defaults.
func DefaultConfig() Config {
    dataDir := defaultDataDir()
    if dataDir == "" {
        panic("DefaultConfig: could not resolve data directory")
    }
    return Config{
        DataDir:       dataDir,
        HTTPAddr:      defaultHTTPAddr,
        NATSPort:      defaultNATSPort,
        MaxStoreBytes: defaultMaxStoreBytes,
    }
}

func defaultDataDir() string {
    home, err := os.UserHomeDir()
    if err != nil {
        return filepath.Join(os.TempDir(), "dagnats")
    }
    if runtime.GOOS == "darwin" {
        return filepath.Join(home, "Library",
            "Application Support", "dagnats")
    }
    // XDG_DATA_HOME or ~/.local/share
    if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
        return filepath.Join(xdg, "dagnats")
    }
    return filepath.Join(home, ".local", "share", "dagnats")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestDefaultConfig -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat(server): add Config struct with platform defaults"
```

### Task 2: Env Var Override

**Files:**
- Modify: `server/config.go`
- Modify: `server/config_test.go`

- [ ] **Step 1: Write failing test for env var resolution**

```go
func TestConfigFromEnv_OverridesDefaults(t *testing.T) {
    t.Setenv("DAGNATS_DATA_DIR", "/tmp/test-dagnats")
    t.Setenv("DAGNATS_HTTP_ADDR", ":9090")
    t.Setenv("DAGNATS_NATS_PORT", "4333")
    t.Setenv("DAGNATS_LEAF_REMOTES", "nats://a:7422,nats://b:7422")
    t.Setenv("DAGNATS_MAX_STORE_BYTES", "5368709120")

    cfg := ConfigFromEnv()

    if cfg.DataDir != "/tmp/test-dagnats" {
        t.Fatalf("want /tmp/test-dagnats, got %s", cfg.DataDir)
    }
    if cfg.HTTPAddr != ":9090" {
        t.Fatalf("want :9090, got %s", cfg.HTTPAddr)
    }
    if cfg.NATSPort != 4333 {
        t.Fatalf("want 4333, got %d", cfg.NATSPort)
    }
    if len(cfg.LeafRemotes) != 2 {
        t.Fatalf("want 2 remotes, got %d", len(cfg.LeafRemotes))
    }
    if cfg.MaxStoreBytes != 5368709120 {
        t.Fatalf("want 5GB, got %d", cfg.MaxStoreBytes)
    }
}

func TestConfigFromEnv_NoEnvUsesDefaults(t *testing.T) {
    // Clear all DAGNATS_ env vars
    for _, key := range []string{
        "DAGNATS_DATA_DIR", "DAGNATS_HTTP_ADDR",
        "DAGNATS_NATS_PORT", "DAGNATS_LEAF_REMOTES",
        "DAGNATS_MAX_STORE_BYTES",
    } {
        t.Setenv(key, "")
        os.Unsetenv(key)
    }

    cfg := ConfigFromEnv()
    def := DefaultConfig()

    if cfg.HTTPAddr != def.HTTPAddr {
        t.Fatalf("want default %s, got %s", def.HTTPAddr, cfg.HTTPAddr)
    }
    if cfg.NATSPort != def.NATSPort {
        t.Fatalf("want default %d, got %d", def.NATSPort, cfg.NATSPort)
    }
}

func TestConfigFromEnv_LeafRemotesCapped(t *testing.T) {
    remotes := "a,b,c,d,e,f,g,h,i,j,k,l"
    t.Setenv("DAGNATS_LEAF_REMOTES", remotes)

    cfg := ConfigFromEnv()

    if len(cfg.LeafRemotes) > maxLeafRemotes {
        t.Fatalf("want max %d remotes, got %d",
            maxLeafRemotes, len(cfg.LeafRemotes))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestConfigFromEnv -v`
Expected: FAIL — `ConfigFromEnv` undefined

- [ ] **Step 3: Write minimal implementation**

Add to `server/config.go`:

```go
import (
    "os"
    "path/filepath"
    "runtime"
    "strconv"
    "strings"
)

// ConfigFromEnv builds a Config by layering file → env vars over defaults.
func ConfigFromEnv() Config {
    cfg := DefaultConfig()

    // File layer (lowest priority after defaults)
    loadConfigFile("dagnats.yaml", &cfg)

    // Env var layer (highest priority)
    applyEnvOverrides(&cfg)

    if cfg.DataDir == "" {
        panic("ConfigFromEnv: DataDir resolved to empty")
    }
    if cfg.MaxStoreBytes <= 0 {
        panic("ConfigFromEnv: MaxStoreBytes must be positive")
    }
    return cfg
}

func applyEnvOverrides(cfg *Config) {
    if v := os.Getenv("DAGNATS_DATA_DIR"); v != "" {
        cfg.DataDir = v
    }
    if v := os.Getenv("DAGNATS_HTTP_ADDR"); v != "" {
        cfg.HTTPAddr = v
    }
    if v := os.Getenv("DAGNATS_NATS_PORT"); v != "" {
        if port, err := strconv.Atoi(v); err == nil {
            cfg.NATSPort = port
        }
    }
    if v := os.Getenv("DAGNATS_LEAF_REMOTES"); v != "" {
        remotes := strings.Split(v, ",")
        if len(remotes) > maxLeafRemotes {
            remotes = remotes[:maxLeafRemotes]
        }
        cfg.LeafRemotes = remotes
    }
    if v := os.Getenv("DAGNATS_MAX_STORE_BYTES"); v != "" {
        if n, err := strconv.ParseInt(v, 10, 64); err == nil {
            cfg.MaxStoreBytes = n
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestConfigFromEnv -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat(server): add ConfigFromEnv with env var overrides"
```

### Task 3: Config File Loading

**Files:**
- Modify: `server/config.go`
- Modify: `server/config_test.go`

Config file uses simple `key: value` format (one per line). No YAML
dependency — the config has 5 scalar fields and one list. We parse it
in ~40 lines.

- [ ] **Step 1: Write failing test for file loading**

```go
func TestLoadConfigFile_ParsesAllFields(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "dagnats.yaml")
    content := `data_dir: /var/lib/dagnats
http_addr: :9090
nats_port: 4333
leaf_remotes: nats://a:7422, nats://b:7422
max_store_bytes: 5368709120
`
    os.WriteFile(path, []byte(content), 0644)

    cfg := DefaultConfig()
    err := loadConfigFile(path, &cfg)

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if cfg.DataDir != "/var/lib/dagnats" {
        t.Fatalf("want /var/lib/dagnats, got %s", cfg.DataDir)
    }
    if cfg.HTTPAddr != ":9090" {
        t.Fatalf("want :9090, got %s", cfg.HTTPAddr)
    }
    if cfg.NATSPort != 4333 {
        t.Fatalf("want 4333, got %d", cfg.NATSPort)
    }
    if len(cfg.LeafRemotes) != 2 {
        t.Fatalf("want 2 remotes, got %d", len(cfg.LeafRemotes))
    }
    if cfg.MaxStoreBytes != 5368709120 {
        t.Fatalf("want 5GB, got %d", cfg.MaxStoreBytes)
    }
}

func TestLoadConfigFile_MissingFileIsNotError(t *testing.T) {
    cfg := DefaultConfig()
    err := loadConfigFile("/nonexistent/dagnats.yaml", &cfg)

    if err != nil {
        t.Fatalf("missing file should not error: %v", err)
    }
    // Config unchanged
    if cfg.HTTPAddr != defaultHTTPAddr {
        t.Fatal("config should be unchanged")
    }
}

func TestLoadConfigFile_UnknownKeysIgnored(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "dagnats.yaml")
    content := `http_addr: :9090
future_field: some_value
`
    os.WriteFile(path, []byte(content), 0644)

    cfg := DefaultConfig()
    err := loadConfigFile(path, &cfg)

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if cfg.HTTPAddr != ":9090" {
        t.Fatalf("want :9090, got %s", cfg.HTTPAddr)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestLoadConfigFile -v`
Expected: FAIL — `loadConfigFile` undefined

- [ ] **Step 3: Write minimal implementation**

Add to `server/config.go`:

```go
import (
    "bufio"
    "errors"
    "io/fs"
    "log"
    // ... existing imports
)

// loadConfigFile reads a simple key:value config file into cfg.
// Missing file is not an error. Unknown keys are logged and skipped.
func loadConfigFile(path string, cfg *Config) error {
    if cfg == nil {
        panic("loadConfigFile: cfg must not be nil")
    }
    if path == "" {
        panic("loadConfigFile: path must not be empty")
    }

    f, err := os.Open(path)
    if errors.Is(err, fs.ErrNotExist) {
        return nil
    }
    if err != nil {
        return err
    }
    defer f.Close()

    scanner := bufio.NewScanner(f)
    lineNum := 0
    const maxLines = 100
    for scanner.Scan() && lineNum < maxLines {
        lineNum++
        line := strings.TrimSpace(scanner.Text())
        if line == "" || strings.HasPrefix(line, "#") {
            continue
        }
        key, val, ok := strings.Cut(line, ":")
        if !ok {
            return fmt.Errorf("line %d: expected key: value",
                lineNum)
        }
        err := applyConfigValue(
            strings.TrimSpace(key),
            strings.TrimSpace(val),
            lineNum, cfg,
        )
        if err != nil {
            return err
        }
    }
    return scanner.Err()
}

func applyConfigValue(
    key, val string, lineNum int, cfg *Config,
) error {
    switch key {
    case "data_dir":
        cfg.DataDir = val
    case "http_addr":
        cfg.HTTPAddr = val
    case "nats_port":
        n, err := strconv.Atoi(val)
        if err != nil {
            return fmt.Errorf("line %d: bad nats_port: %w",
                lineNum, err)
        }
        cfg.NATSPort = n
    case "leaf_remotes":
        parts := strings.Split(val, ",")
        remotes := make([]string, 0, len(parts))
        for _, p := range parts {
            if p = strings.TrimSpace(p); p != "" {
                remotes = append(remotes, p)
            }
        }
        if len(remotes) > maxLeafRemotes {
            remotes = remotes[:maxLeafRemotes]
        }
        cfg.LeafRemotes = remotes
    case "max_store_bytes":
        n, err := strconv.ParseInt(val, 10, 64)
        if err != nil {
            return fmt.Errorf("line %d: bad max_store_bytes: %w",
                lineNum, err)
        }
        cfg.MaxStoreBytes = n
    default:
        log.Printf("config:%d: unknown key %q (ignored)",
            lineNum, key)
    }
    return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestLoadConfigFile -v`
Expected: PASS

- [ ] **Step 5: Run all config tests**

Run: `go test ./server/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add server/config.go server/config_test.go
git commit -m "feat(server): add config file loading (key:value format)"
```

---

## Chunk 2: Embedded NATS

### Task 4: Embedded NATS Server Startup

**Files:**
- Create: `server/nats.go`
- Create: `server/nats_test.go`

- [ ] **Step 1: Write failing test for standalone NATS**

```go
// server/nats_test.go
// Methodology: integration tests with real embedded NATS. Each test
// gets its own server on a random port. Verify JetStream availability.
package server

import (
    "strings"
    "testing"

    "github.com/nats-io/nats.go"
)

func TestStartNATS_Standalone(t *testing.T) {
    cfg := DefaultConfig()
    cfg.NATSPort = -1 // random port
    cfg.DataDir = t.TempDir()

    ns, err := startNATS(cfg)
    if err != nil {
        t.Fatalf("startNATS failed: %v", err)
    }
    defer ns.Shutdown()

    if !ns.ReadyForConnections(5_000_000_000) {
        t.Fatal("server not ready")
    }

    // Verify JetStream is available
    nc, err := nats.Connect(ns.ClientURL())
    if err != nil {
        t.Fatalf("connect failed: %v", err)
    }
    defer nc.Close()

    js, err := nc.JetStream()
    if err != nil {
        t.Fatalf("JetStream failed: %v", err)
    }
    if js == nil {
        t.Fatal("JetStream must not be nil")
    }
}

func TestStartNATS_StandaloneBindsLocalhost(t *testing.T) {
    cfg := DefaultConfig()
    cfg.NATSPort = -1
    cfg.DataDir = t.TempDir()

    ns, err := startNATS(cfg)
    if err != nil {
        t.Fatalf("startNATS failed: %v", err)
    }
    defer ns.Shutdown()

    addr := ns.Addr().String()
    if addr == "" {
        t.Fatal("server addr must not be empty")
    }
    // Standalone should bind 127.0.0.1
    if !strings.Contains(addr, "127.0.0.1") {
        t.Fatalf("standalone should bind 127.0.0.1, got %s", addr)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestStartNATS -v`
Expected: FAIL — `startNATS` undefined

- [ ] **Step 3: Write minimal implementation**

```go
// server/nats.go
package server

import (
    "fmt"
    "net/url"
    "path/filepath"
    "time"

    natsserver "github.com/nats-io/nats-server/v2/server"
)

const natsReadyTimeout = 5 * time.Second

// startNATS boots an embedded NATS server with JetStream.
// Standalone mode binds 127.0.0.1. Leaf mode binds 0.0.0.0.
func startNATS(cfg Config) (*natsserver.Server, error) {
    if cfg.DataDir == "" {
        panic("startNATS: DataDir must not be empty")
    }
    if cfg.MaxStoreBytes <= 0 {
        panic("startNATS: MaxStoreBytes must be positive")
    }

    host := "127.0.0.1"
    if len(cfg.LeafRemotes) > 0 {
        host = "0.0.0.0"
    }

    opts := &natsserver.Options{
        Host:              host,
        Port:              cfg.NATSPort,
        JetStream:         true,
        StoreDir:          filepath.Join(cfg.DataDir, "jetstream"),
        JetStreamMaxStore: cfg.MaxStoreBytes,
        NoLog:             true,
        NoSigs:            true,
    }

    if len(cfg.LeafRemotes) > 0 {
        remotes := make([]*natsserver.RemoteLeafOpts, 0,
            len(cfg.LeafRemotes))
        for _, raw := range cfg.LeafRemotes {
            u, err := url.Parse(raw)
            if err != nil {
                return nil, fmt.Errorf("bad leaf remote %q: %w",
                    raw, err)
            }
            remotes = append(remotes,
                &natsserver.RemoteLeafOpts{
                    URLs: []*url.URL{u},
                })
        }
        opts.LeafNode = natsserver.LeafNodeOpts{
            Remotes: remotes,
        }
    }

    ns, err := natsserver.NewServer(opts)
    if err != nil {
        return nil, fmt.Errorf("create NATS server: %w", err)
    }

    ns.Start()

    if !ns.ReadyForConnections(natsReadyTimeout) {
        ns.Shutdown()
        return nil, fmt.Errorf(
            "NATS server not ready after %s", natsReadyTimeout)
    }

    return ns, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestStartNATS -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/nats.go server/nats_test.go
git commit -m "feat(server): add embedded NATS server startup"
```

---

## Chunk 3: Server Lifecycle

### Task 5: Server New, Run, Stop

**Files:**
- Create: `server/server.go`
- Create: `server/server_test.go`

- [ ] **Step 1: Write failing test for full lifecycle**

```go
// server/server_test.go
// Methodology: integration tests with real embedded NATS. Each test
// gets isolated server on random port. Verify startup, health, shutdown.
package server

import (
    "context"
    "fmt"
    "net"
    "net/http"
    "testing"
    "time"
)

func testConfig(t *testing.T) Config {
    t.Helper()
    cfg := DefaultConfig()
    cfg.NATSPort = -1
    cfg.DataDir = t.TempDir()
    // Find a free port for HTTP
    l, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatal(err)
    }
    cfg.HTTPAddr = l.Addr().String()
    l.Close()
    return cfg
}

func TestServer_StartsAndStops(t *testing.T) {
    cfg := testConfig(t)
    srv := New(cfg)

    errCh := make(chan error, 1)
    go func() { errCh <- srv.Run() }()

    // Wait for ready
    deadline := time.Now().Add(10 * time.Second)
    ready := false
    for time.Now().Before(deadline) {
        resp, err := http.Get(
            fmt.Sprintf("http://%s/ready", cfg.HTTPAddr))
        if err == nil && resp.StatusCode == 200 {
            resp.Body.Close()
            ready = true
            break
        }
        if resp != nil {
            resp.Body.Close()
        }
        time.Sleep(50 * time.Millisecond)
    }
    if !ready {
        t.Fatal("/ready never returned 200")
    }

    // Health should also work
    resp, err := http.Get(
        fmt.Sprintf("http://%s/health", cfg.HTTPAddr))
    if err != nil {
        t.Fatalf("health check failed: %v", err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != 200 {
        t.Fatalf("health want 200, got %d", resp.StatusCode)
    }

    // Stop
    srv.Stop()

    select {
    case err := <-errCh:
        if err != nil {
            t.Fatalf("Run returned error: %v", err)
        }
    case <-time.After(20 * time.Second):
        t.Fatal("Run did not return after Stop")
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/ -run TestServer_StartsAndStops -v -timeout 30s`
Expected: FAIL — `New` undefined

- [ ] **Step 3: Write implementation**

```go
// server/server.go
package server

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "os/signal"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/danmestas/dagnats/api"
    "github.com/danmestas/dagnats/engine"
    "github.com/danmestas/dagnats/natsutil"
    "github.com/danmestas/dagnats/observe"
    "github.com/danmestas/dagnats/observe/simple"
    "github.com/danmestas/dagnats/trigger"
    natsserver "github.com/nats-io/nats-server/v2/server"
    "github.com/nats-io/nats.go"
)

const shutdownDeadline = 15 * time.Second

// Server runs the full DagNats stack in a single process.
type Server struct {
    cfg     Config
    ns      *natsserver.Server
    nc      *nats.Conn
    orch    *engine.ActorOrchestrator
    svc     *api.Service
    trig    *trigger.TriggerService
    httpSrv *http.Server
    tel     *observe.Telemetry
    telStop func()
    ready   atomic.Bool
    stopCh  chan struct{}
}

// New creates a server with the given config.
func New(cfg Config) *Server {
    if cfg.DataDir == "" {
        panic("server.New: DataDir must not be empty")
    }
    return &Server{
        cfg:    cfg,
        stopCh: make(chan struct{}),
    }
}

// Run starts all components and blocks until Stop or SIGINT/SIGTERM.
// Returns nil on clean shutdown.
func (s *Server) Run() error {
    if err := os.MkdirAll(s.cfg.DataDir, 0750); err != nil {
        return fmt.Errorf("create data dir: %w", err)
    }
    if err := s.startComponents(); err != nil {
        return err
    }

    httpErrCh := s.startHTTP()

    s.ready.Store(true)
    fmt.Printf("dagnats serving on :%d (HTTP %s)\n",
        s.cfg.NATSPort, s.cfg.HTTPAddr)

    return s.waitAndShutdown(httpErrCh)
}

// startComponents boots NATS, resources, and all services.
func (s *Server) startComponents() error {
    ns, err := startNATS(s.cfg)
    if err != nil {
        return fmt.Errorf("start NATS: %w", err)
    }
    s.ns = ns

    nc, err := nats.Connect(ns.ClientURL())
    if err != nil {
        ns.Shutdown()
        return fmt.Errorf("connect to NATS: %w", err)
    }
    s.nc = nc

    err = natsutil.SetupAll(nc,
        natsutil.WithKVBuckets(
            natsutil.KVConfig{Bucket: "triggers"},
            natsutil.KVConfig{Bucket: "trigger_state"},
            natsutil.KVConfig{Bucket: "signals"},
            natsutil.KVConfig{Bucket: "checkpoints"},
            natsutil.KVConfig{Bucket: "concurrency_runs"},
        ),
    )
    if err != nil {
        nc.Close()
        ns.Shutdown()
        return fmt.Errorf("setup NATS resources: %w", err)
    }

    tel, telStop := simple.SetupTelemetry(nc)
    s.tel = tel
    s.telStop = telStop

    s.svc = api.NewService(nc, tel)
    s.orch = engine.NewActorOrchestrator(nc, tel)

    trig, err := trigger.NewTriggerService(nc)
    if err != nil {
        return fmt.Errorf("trigger service: %w", err)
    }
    s.trig = trig

    s.orch.Start()
    if err := s.trig.Start(); err != nil {
        return fmt.Errorf("trigger start: %w", err)
    }
    return nil
}

// startHTTP launches the HTTP server in a goroutine.
func (s *Server) startHTTP() <-chan error {
    mux := http.NewServeMux()
    mux.Handle("/", api.NewRESTHandler(s.svc))
    mux.HandleFunc("/health", s.handleHealth)
    mux.HandleFunc("/ready", s.handleReady)
    s.httpSrv = &http.Server{
        Addr:    s.cfg.HTTPAddr,
        Handler: mux,
    }
    errCh := make(chan error, 1)
    go func() {
        if err := s.httpSrv.ListenAndServe(); err != nil &&
            err != http.ErrServerClosed {
            errCh <- err
        }
        close(errCh)
    }()
    return errCh
}

// waitAndShutdown blocks until stop signal, then shuts down.
func (s *Server) waitAndShutdown(
    httpErrCh <-chan error,
) error {
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

    select {
    case <-sig:
    case <-s.stopCh:
    case err := <-httpErrCh:
        if err != nil {
            return fmt.Errorf("HTTP server: %w", err)
        }
    }
    return s.shutdown()
}

// Stop signals the server to shut down.
func (s *Server) Stop() {
    select {
    case <-s.stopCh:
    default:
        close(s.stopCh)
    }
}

func (s *Server) shutdown() error {
    if s.ns == nil {
        panic("shutdown: server was never started")
    }
    if shutdownDeadline <= 0 {
        panic("shutdown: deadline must be positive")
    }
    s.ready.Store(false)
    fmt.Println("shutting down...")

    ctx, cancel := context.WithTimeout(
        context.Background(), shutdownDeadline)
    defer cancel()

    done := make(chan struct{})
    go func() {
        // 1. HTTP
        if s.httpSrv != nil {
            httpCtx, httpCancel := context.WithTimeout(
                ctx, 5*time.Second)
            s.httpSrv.Shutdown(httpCtx)
            httpCancel()
        }

        // 2. Triggers
        if s.trig != nil {
            s.trig.Stop()
        }

        // 3. Orchestrator
        if s.orch != nil {
            s.orch.Stop()
        }

        // 4. Telemetry
        if s.telStop != nil {
            s.telStop()
        }

        // 5. NATS client
        if s.nc != nil {
            s.nc.Drain()
        }

        // 6. NATS server
        if s.ns != nil {
            s.ns.Shutdown()
            s.ns.WaitForShutdown()
        }

        close(done)
    }()

    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return fmt.Errorf("shutdown exceeded %s deadline",
            shutdownDeadline)
    }
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
    if w == nil {
        panic("handleHealth: ResponseWriter must not be nil")
    }
    if r == nil {
        panic("handleHealth: Request must not be nil")
    }
    if s.nc == nil || !s.nc.IsConnected() {
        w.WriteHeader(http.StatusServiceUnavailable)
        fmt.Fprint(w, "NATS disconnected")
        return
    }
    js, err := s.nc.JetStream()
    if err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        fmt.Fprintf(w, "JetStream unavailable: %v", err)
        return
    }
    // Quick JetStream health check
    _, err = js.AccountInfo()
    if err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        fmt.Fprintf(w, "JetStream unhealthy: %v", err)
        return
    }
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "ok")
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
    if w == nil {
        panic("handleReady: ResponseWriter must not be nil")
    }
    if r == nil {
        panic("handleReady: Request must not be nil")
    }
    if !s.ready.Load() {
        w.WriteHeader(http.StatusServiceUnavailable)
        fmt.Fprint(w, "not ready")
        return
    }
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "ready")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./server/ -run TestServer_StartsAndStops -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/server.go server/server_test.go
git commit -m "feat(server): add Server with full lifecycle management"
```

---

## Chunk 4: CLI Integration

### Task 6: Wire `serve` into CLI

**Files:**
- Create: `cli/serve.go`
- Modify: `cli/root.go:16-25` (add serve case to switch)

- [ ] **Step 1: Create serve command**

```go
// cli/serve.go
package cli

import (
    "fmt"
    "os"

    "github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
    cfg := server.ConfigFromEnv()
    srv := server.New(cfg)
    if err := srv.Run(); err != nil {
        fmt.Fprintf(os.Stderr, "error: %v\n", err)
        os.Exit(1)
    }
}
```

- [ ] **Step 2: Add serve to root dispatcher**

In `cli/root.go`, add the `serve` case to the switch and update usage:

```go
// In Run(), add to switch:
case "serve":
    runServeCmd(args[2:])

// In printUsage(), add:
fmt.Fprintln(os.Stderr, "  serve     start embedded server")
```

- [ ] **Step 3: Verify build compiles**

Run: `go build ./cmd/dagnats/`
Expected: SUCCESS (no errors)

- [ ] **Step 4: Commit**

```bash
git add cli/serve.go cli/root.go
git commit -m "feat(cli): add serve command to start embedded server"
```

### Task 7: Smoke Test

- [ ] **Step 1: Run all tests**

Run: `go test ./server/ -v -timeout 60s`
Expected: all PASS

- [ ] **Step 2: Run full test suite**

Run: `go test ./... -timeout 120s`
Expected: all PASS

- [ ] **Step 3: Final commit if any fixes needed**
