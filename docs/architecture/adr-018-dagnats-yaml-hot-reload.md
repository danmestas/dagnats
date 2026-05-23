# ADR-018: `dagnats.yaml` declarative workflows + triggers with hot-reload

Status: accepted (2026-05-22, #358).

## Context

Phases 1–3 of the operator-ergonomics arc closed the gap between "running
DagNats" and "registering a workflow via the CLI / REST API". The
remaining friction is that every workflow + trigger registration today
requires a CLI invocation or a REST call. There is no single source of
truth on disk an operator can read, diff, version-control, or hot-edit.

The Phase-4 promise (issue #358) was:

> One file. One edit. One reload — workflows + triggers live alongside
> the existing server config, KV records stay file-managed, and an edit
> takes effect within ~1 s of save without restarting the server.

That promise has three load-bearing pieces:

1. A YAML file shape that an operator can author by hand.
2. A reliable file-edit signal across macOS + Linux editors, including
   editors that atomic-save (`vim :w`, VSCode, JetBrains).
3. A safe path from "file changed" → "KV updated" that does not
   delete records the file did not author.

Each piece had at least one trap that bit prior prototypes; this ADR
records the decisions taken.

## Decision

### File shape — separate YAML structs in `internal/configfile/`

Reusing `dag.WorkflowDef` and `trigger.TriggerDef` directly would tie
the file surface to internal JSON tags. `gopkg.in/yaml.v3` keys off
`yaml:` tags or lowercased field names by default — `json:` tags do
not drive the YAML decoder. We define `ConfigFile{ Workflows[],
Triggers[] }` with `yaml:` tags in `internal/configfile/types.go` and
convert at the package boundary via `ToWorkflowDef` / `ToTriggerDef`.

Same dagnats.yaml carries the legacy `data_dir` / `http_addr` /
worker keys at the top level. `yaml.v3` with `KnownFields(false)`
tolerates them so the operator keeps one file rather than two.

### Pure Load / Validate / Diff, applied via direct KV writes

`internal/configfile/` is pure: `Load(io.Reader) → ConfigFile`,
`Validate(ConfigFile) error`, `Diff(current, desired) Plan`. No NATS.
`Apply(ctx, KVHandles, Plan)` is the only filesystem / NATS toucher,
and it writes directly to `workflow_defs` + `triggers` via
`jetstream.KeyValue` — the same path `internal/api.Service` uses for
its REST surface. Going through `api.Service` would have been the
abstraction-correct choice but created a cycle (server → api needs
dag, configfile would need server). Direct KV writes are equivalent
on the wire and the least entangled.

### `fsnotify` on the parent directory, not the file

On macOS `fsnotify` v1.10.1 uses kqueue. kqueue ties watches to the
file's inode. `vim :w` (and most editors) atomic-save by writing a
sibling temp file and `rename(2)`-ing it onto the target — which
allocates a new inode. A file-level watch loses its target. The
fsnotify README explicitly recommends "Watch the parent directory and
use `Event.Name` to filter" for this case; the same recommendation
applies to Linux inotify users running atomic-save editors.

`Watcher.Start` adds the parent directory to the fsnotify watcher
and filters incoming events by `Event.Name == cfgPath`. kqueue also
reports parent-dir `Write` with empty `Name` when *anything* in the
directory changes — we treat empty `Name` as "directory changed,
check the file" and rely on the content-hash dedup to skip noise.

### 500ms debounce + content-hash dedup

Editors fire many fsnotify events per save (a single `vim :w` can
produce up to four). A 500ms debounce coalesces the burst into a
single reload. The debounce alone is not sufficient: macOS Spotlight
metadata bumps (and editor "touch" semantics) produce
content-identical writes. The watcher hashes the file's contents
(sha256) on every fired reload and skips when the hash matches the
last applied state. That keeps "open in Finder", "Spotlight
re-index", and `touch` from re-applying a no-op plan.

### Source annotations on KV records

Adding `Source string json:"source,omitempty"` to `trigger.TriggerDef`
is additive and backward-compatible: existing KV entries written by
`internal/api.Service.createTriggerInner` etc. simply leave the field
zero, and `internal/trigger/service.go:322,:495` unmarshal sites
keep working without change.

The watcher writes records with `Source = "file:<basename>"` (e.g.
`file:dagnats.yaml`). The CLI's `trigger delete` reads the field
before deletion: a file-managed trigger refuses without `--force` so
the operator doesn't delete a record the next reload will resurrect.

`ReadCurrent` filters the triggers KV to file-managed records only —
the diff therefore proposes "remove" only for records the file itself
authored. KV-managed entries (no Source, or a different prefix) stay
untouched even if the file removes their identical-ID counterpart.

### Initial apply on startup

The server's `startConfigWatcher` runs one synthesized reload after
arming the watcher so a freshly-started server picks up the file's
declarations without waiting for an edit. The initial apply uses the
same `Load → Validate → Diff → Apply` pipeline as a hot reload.

## Alternatives considered

### Reuse `dag.WorkflowDef` JSON tags via lowercased-key YAML

`yaml.v3` falls back to `strings.ToLower(field.Name)` when no tag is
present. We could have left `dag.WorkflowDef` untouched and relied on
the fallback, but it ties the YAML key spelling to Go field names —
a refactor of `WorkflowDef` would silently break dagnats.yaml files.
Separate YAML structs let the wire shape evolve independently of the
runtime type.

### File-level fsnotify watch with re-arm on rename

We could detect the rename event and `fsw.Add(cfgPath)` again on each
save. That works in theory but races the next save's `Write`; the
parent-dir watch is one fewer moving part and the cost is filtering
empty-Name events (the hash dedup absorbs them).

### Watcher reports diff to a `TriggerService.Upsert(def)` method

A `TriggerRegistrar`-level API would have given us audit logging for
free, but it required exposing public `Upsert` / `Remove` methods on
`TriggerService` purely for one caller. The KV is already the
source of truth — writing through it directly preserves the existing
KV-watcher path (`internal/trigger/service.go:handleKVUpdate`) so a
file-authored change still flows through the same registrar Activate
/ Deactivate cycle as a CLI / REST write.

## Consequences

* Operators can author and version-control dagnats.yaml; edits land
  within ~1 s of save with no server restart.
* The CLI's `trigger delete` now distinguishes file-managed records
  and refuses deletion without `--force`.
* `internal/configfile/` adds ~600 lines of pure code + a watcher
  with no exotic dependencies (fsnotify and yaml.v3 are already in
  go.mod).
* The triggers KV gains a `Source` column. Future code that lists
  triggers can colour-code by origin without schema changes.
* Workflows do not yet carry a Source field — workflow_defs ownership
  remains a single-administrator-per-name model. If file-managed
  workflows ever need the same "don't delete from CLI" guard, the
  field can be added to `dag.WorkflowDef` later.
* A pathological dagnats.yaml ≥ 1 MiB or > 500 entries is rejected at
  Load. That bound matches the existing `maxActiveTriggers = 500`.
