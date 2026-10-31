# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **The lake can supersede a committed part, for lode's compaction and retention.** `lake.Supersede(ctx, store, commit, by, at)` writes a create-only `part-<stem>.json.superseded` record beside the part's commit. The record holds the commit with the new `SupersededBy` field, the stem of the part that now holds its rows (empty when retention dropped them), and `WrittenAt` set to when the part stopped being live. `lake.ListCommitted` lists superseded parts in the new `Listing.Superseded` instead of `Commits` and leaves them out of `Overlaps`. A supersede record whose part is gone is not rejected. Superseding a part again to the same successor returns the stored record; naming another successor is an error. It writes through `lake.RecordStore`, which every `writer.Store` satisfies. `lake/writer` is unchanged.
- **`dagnats-mcp-duckdb --lake-dir` adds a view per lake table, reading only live parts.** Point it at a lake root, a local path or `s3://bucket/prefix`, and each `<program>/<table>` becomes a view named `<program>_<table>`. A view reads the parts that have a commit record (`part-<stem>.json`) and no supersede record (`part-<stem>.json.superseded`), so a writer's orphan part is never read, and a part lode has compacted or dropped stays out even while its file exists. Views are refreshed before every query, so a compaction shows in the next one. A table with no live part, a name outside the lake's naming rules, and a name a telemetry view holds get no view. The read lock covers the lake dir too, and the row cap applies. The five telemetry views are unchanged, and without the flag there are no lake views.
- **A bridge worker's claims are released as soon as its connection drops (#873).** A crashed worker's task used to wait the full AckWait (5 minutes by default) before NATS redelivered it. `poll` now accepts `worker_id`: the worker must hold an open `connect` stream under that id on the bridge, owned by the polling token by the same rule as connect (else `409` with code `worker_id_owned`); an id with no open connection on that bridge is accepted unbound, with a warning and a `bridge.poll.unbound_worker_id` count, and redelivers after AckWait as before. The bridge records the connection on each claim, and when the stream closes it Naks every claim of that connection still unresolved, so the task is redelivered within seconds. Resolved and fenced claims are skipped; a reconnect under the same id keeps its new claims, because the release is keyed on the connection instance; a close from a token that no longer owns the id releases nothing. A poll without `worker_id` is unchanged. The release and a resolve that ends a claim exclude each other per claim, so a task is never both completed and redelivered. The fast release needs the connect and the poll to reach the same bridge. `sdk/ts` `poll` sends the Connection's worker id automatically while its stream is open.

### Fixed

- **`make build-release` names artifacts after the `v*` release tag.** With the lake module's own `lake/v*` tag on the same commit, `git describe --tags` returned `lake/v0.1.0`, so `VERSION` broke the tarball paths. The Makefile's version lookups now match `v*` only.
- **`TestWatchTriggers_BoundedCatchUp` no longer times out on a loaded machine.** It wrote 10,001 KV entries and read 10,000 back one round trip at a time, so its wall time grew with CPU starvation until the 30s insert deadline ran out. The catch-up cap is now a per-Worker field defaulting to 10,000, and the test proves the cap with 11 entries and a cap of 10.

## [0.0.38] - 2026-10-31

### Upgrade notes

- **`Continue` with a wait is refused on a step that is not an `agent_loop` step.** The Go SDK's `Continue` returns `protocol.ErrWaitRequiresAgentLoop` and the bridge answers `400 wait_requires_agent_loop`. Such a wait never woke the step: it sat until its AckWait and was then refused as committed. Code that relied on it should complete the step or move it to an `agent_loop`.
- **`lake` is tagged on its own** as `lake/v0.1.0`, the module programs such as lode depend on for `lake/writer`.
- **`@dagnats/bridge` ships as `dagnats-bridge-0.0.38.tgz`** on this release.

### Added

- **The bridge's `inputs.place` action places a step's queued inputs (#848).** `{"action":"inputs.place","mode":"steer"|"follow_up"}` answers `200 {"inputs":[{"request_id","mode","content"}]}` (`{"inputs":[]}` when none, never 204; any other mode is `400 {"code":"invalid_mode"}`). It claims the journal on first use and runs the same code as the Go SDK's `PlaceInputs`, now `journal.Placer` (compare-and-swap claim, `input.placed` append, unanswered fallback). A delivery gets each placement once, a redelivery of the same iteration gets the earlier placements back once, and the placed inputs settle to `done` when the iteration commits. `sdk/ts` gains `ctx.inputs.place(mode)`.
- **`lake/writer` is exported, with a storage seam, so other programs write the lake (#868).** lode writes Parquet parts and commit records under its own program prefix with the writer dagnats uses, so `lake.ListCommitted`'s `Rejected` and `Overlaps` checks and dagnats-mcp-duckdb hold over every program's data. `writer.New(store, program, cfg)` returns a writer scoped to one program and table: it writes only under `<program>/<table>/`, and any other key panics. `writer.Store` has two methods. `Put` creates a key and fails with an error matching `fs.ErrExist` when the key exists; a store without conditional writes may check first and must document the race. `Get` fails with `fs.ErrNotExist` when the key is absent. A commit is a Put of the part, then a Put of its record, with no rename, so it works on S3. The writer stays correct when two Puts race, because a part's and a record's keys come from the batch's content: two writers only meet on a key when they hold the same batch. `writer.LocalStore` stores to a local directory and links each file into place, so its Put is atomic and create-only. A date whose rows encode past `writer.PartBytesMax` (64 MiB) is split into several parts, each with its own commit, at stream sequence boundaries; `Append` refuses a row that alone would encode past it. `lake.PartitionKey`, `lake.DecodeCommit` and `Commit.Key` are new. `Writer.Flush` takes a context, and the root moved from `Config` into the store.

### Fixed

- **A bridge `load_checkpoint` called first now reads the journal's state.** On a journal step whose earlier iteration committed state, `load_checkpoint` before any other journal action answered `{"source":"payload","state":null}` (the stale dispatch state), so a client that loaded first started fresh; after `journal.view` it answered the journal's. It now opens the claim's journal first when the step already has journal records, so the source is `journal` whichever call comes first. A step with no journal records is not claimed.
- **A `continue` with a `wait` on a step that is not an `agent_loop` step is refused.** The bridge answered 200 and the step looked parked, but the engine has no wake for it, so it waited out its AckWait and was then refused as already committed. The bridge now answers `400 {"code":"wait_requires_agent_loop"}` and leaves the task claimed; the Go SDK's `Continue` returns `protocol.ErrWaitRequiresAgentLoop`. The engine marks agent-loop dispatches with a new `agent_loop` field on the task payload; a payload without a state cap (an older engine) is not refused. `sdk/ts` maps it to the non-terminal `WaitRequiresAgentLoop`.
- **A cluster node no longer panics at startup when one bucket lookup goes unanswered.** In a cluster only a stream's leader answers `STREAM.INFO`, so a lookup sent while that stream elects a leader gets no reply. The startup binds of `workflow_runs`, `workflow_defs` and the other buckets the API service, orchestrator and config watcher open were single 5 s requests, and `NewSnapshotStore` panicked on the timeout (`workflow_runs bucket not found: context deadline exceeded`). The optional buckets silently stayed unbound on the same timeout. `natsutil.BindKeyValue` now re-sends an unanswered lookup with the provisioning retry bounds, and a missing bucket still returns at once. The 3-process cluster test now waits for every node's own health endpoint before it proceeds. It used to check only the first node, whose count of nodes up includes a peer that has opened its NATS routes but then died in startup. That is how the flaky `TestMultiProcessCluster_JournalFollowResumesAcrossNodeLoss` ran on two nodes, killed one of them, and lost quorum.

## [0.0.37] - 2026-10-31

### Upgrade notes

- **The bridge now caps request sizes.** A resolve body over 8 MiB answers `413 body_too_large`, a journal record over the server's `max_payload` answers `413 record_too_large` (it used to be an untyped `500`), and a `continue` on a journal step is now checked against `loop_state_max_bytes` like a plain step. A bridge worker that sent larger state or bodies than these limits needs to shrink them or raise `loop_state_max_bytes`.
- **`@dagnats/bridge` ships as a release asset,** `dagnats-bridge-0.0.37.tgz`, not on npm.

### Added

- **The bridge's `continue` accepts a `wait` (#847).** `{"action":"continue","output":...,"wait":{"delay_ms":N,"until":"<RFC 3339>","input":true}}` parks the step durably, as the Go SDK's `Continue(..., Delay, Until, UntilInput)` does: the wait rides the `step.continue` payload (and a journal step's commit record) and the engine schedules it, so a non-Go agent worker no longer holds a task slot while idle. A negative `delay_ms` or zero `until` answers `400 {"code":"invalid_wait"}`. With `wait.input` set while a submission is already queued, nothing is parked or acked and the answer is `409 {"code":"inputs_queued"}`; the task stays claimed so the worker can place the inputs and continue without the wait. The check is now `inputs.Store.RefuseIfQueued`, shared with the Go SDK (`worker.ErrInputsQueued` is `inputs.ErrQueued`). `sdk/ts` gains `ctx.continue({output?, state?, wait?})` and the non-terminal `InputsQueued` and `InvalidWait` errors.
- **The bridge refuses oversize loop state and request bodies with a typed `413`.** A `checkpoint` over the task's `loop_state_max_bytes` (256 KiB by default) was accepted and only failed at `continue`, with an untyped error. Both now answer `413 {"code":"state_too_large","max_bytes":N,"bytes":M}`, and the `continue` check applies to journal steps too. A journal append (or other resolve) whose record exceeds the server's `max_payload` answers `413 {"code":"record_too_large","max_bytes":N}` instead of an untyped 500. A resolve body over 8 MiB, the largest `max_payload`, answers `413 {"code":"body_too_large"}` instead of being buffered whole. The HTTP bridge reference now says a checkpoint is durable only once a later `continue` commits it.
- **A poll item names its `task_type` and carries its `dispatch_nonce`.** One poll can ask for several task types, so each item now says which it came from. `dispatch_nonce` is read by the same code as the journal claim's, so a client has the claim's dispatch identity before its first journal call; it is omitted when the dispatch had none.
- **`sdk/ts`: a TypeScript client for the HTTP bridge (#851).** `@dagnats/bridge` (Node 22.19 or later, ESM, no runtime dependencies, `.d.ts` included) wraps `connect` (the heartbeat SSE stream, reconnecting with bounded backoff and stopping on any 4xx), `poll`, `resolve` and `logs`, each with an `AbortSignal` and a bounded default timeout, and a `TaskContext` per polled task (`PollItem` carries `task_type` and `dispatch_nonce`): `complete`, `fail` with `failure_type` and `retry_after_ms`, `checkpoint`, `loadCheckpoint`, `heartbeat`, `stream`, `journal.view` and `journal.append`. Errors are told apart by status and body, so `Fenced` (`409 {"code":"fenced"}`) is not `RunCancelled` (`409`, plain text), and `Stale`, `Committed`, `RestartLimit` and `NotProvisioned` follow the `410` and `503` codes; `isTerminal(err)` says whether to stop. `@dagnats/bridge/journal` implements the ADR-026 replay rules over a `Log` (`Journal<M>`, `BridgeLog`, `MemoryLog`) and is tested against `journal/testdata/replay`. It ships as a release asset, `dagnats-bridge-<version>.tgz`, which `make build-release` builds into `dist/` and lists in `SHA256SUMS`; it is not published to npmjs or GitHub Packages. A CI job (`sdk-ts.yml`) typechecks, builds, packs and tests it on Node 22.19 and 24 against an in-process dev server.
- **The `program` package: the contract for programs on a shared dagnats.** A program declares a `Manifest` (name, task types, workflows, streams, buckets, protocols); its name is the prefix root of everything it owns. `Validate` checks a manifest without NATS. `PermissionsFor` derives the NATS permission set for the program's NKey user: every subject the worker SDK and a micro service use, the program's own subjects, reads and consumers on its own streams and buckets (which the host creates with `Provision`), and subscribe on `_INBOX.<name>.>` only, so no program can read another's task bodies or replies. `LoadOrCreateKey` generates the program's NKey; the seed is written once, mode 0600, and only the public key leaves the process. `Attach` takes a connection dialed with `nats.CustomInboxPrefix(program.InboxPrefix(name))` (it panics otherwise), calls the program's `Start`, and serves its handlers, each under a recover so a panicking handler fails its task instead of the process. Self-registration (`program.Run`) and server-side users follow in later releases. `examples/program-min` serves one task over a permission-restricted user.
- **A leafnode listener with a user/password login, and user/password leaf remotes.** `leaf_port` (`DAGNATS_LEAF_PORT`, `0` and off by default) opens a listener other servers dial as leaf nodes, bound to `leaf_host` (`DAGNATS_LEAF_HOST`, `127.0.0.1` when unset, never all interfaces by default). It always requires `leaf_listen_user` and `leaf_listen_password_file`, and dagnats refuses to start without them, because a linked leaf shares every subject and stream. The listener works in embedded cluster mode; `leaf_remotes` stays excluded there. On the dialing side, `leaf_user` and `leaf_password_file` log in to leaf remotes with a plain user and password, so a dagnats leaf can link to a dagnats hub without operator-mode creds; they exclude `leaf_credentials`. dagnats warns at start when a leaf's client port is open to the network with no `nats_user`, since a client there shares every subject of the hub, and when `leaf_host` is `0.0.0.0` or `::`, since the leaf login travels without TLS. Nothing changes while the new keys are unset.

### Fixed

- **Test clusters retry when a reserved port is taken.** `dagnatstest.StartTestCluster` and the natsutil provisioning race fixture reserved ports by binding and closing them, so another process could take one before its node bound it; nats-server then served clients without a route listener and logged nothing, and readiness waited its full timeout. Each node now captures its fatal log, a failed bind is detected at once, and the cluster restarts on fresh ports, up to three attempts, as the journal conformance fixture already does (#865).
- **A bridge worker's own token passes the API token gate.** With `DAGNATS_API_TOKEN_FILE` set, every HTTP request had to carry the API token, so a bridge worker presenting its `DAGNATS_BRIDGE_TOKEN` or a minted worker token got a 401 before the bridge saw it. On the bridge's worker routes the gate now also admits a token the bridge accepts, and the bridge checks it as before. A bridge in dev mode (no `DAGNATS_BRIDGE_TOKEN`) accepts nothing through the gate, so the API token still guards it.

## [0.0.36] - 2026-10-31

### Upgrade notes

- **Drain agent journal steps before upgrading workers** (let in-flight agent steps finish). An in-flight agent step whose attempt was recorded by an older SDK, with a fresh tool call at a position a prior attempt's unsafe call held, is replayed under the new positions on a redelivery after the upgrade: that call runs again, under a new key.
- **`worker.(*Worker).Start` now returns an error.** Code that calls `w.Start()` as a statement still compiles, but drops the error; check it, since a consumer that cannot be set up no longer panics.

### Added

- **`jetstream_domain` names the embedded server's JetStream domain** (env `DAGNATS_JETSTREAM_DOMAIN`, unset by default). A dagnats running as a leaf node with its own JetStream, such as a laptop that works offline, sets a domain so the hub it links to can reach its streams as `$JS.<domain>.API` (`jetstream.NewWithDomain(nc, "e-mac")`) while the hub's own `$JS.API` stays the hub's. Local clients and dagnats itself keep using plain `$JS.API`. The value must be one NATS token (no dots, spaces or wildcards); anything else fails config load.
- **HTTP bridge clients can apply the agent journal's replay rules themselves (#849).** A poll item now carries its delivery identity, `delivered` and `stream_seq`: the values in the journal claim the bridge writes for it. A new resolve action, `journal.view`, answers `{"claim", "records"}`: the task's claim and the records of its iteration written before it, the input the Go SDK replays from; a task another claim has replaced gets `409 fenced` instead, from `journal.view` and now from `journal.read` too. `journal.Journal.Resync` reports that fence to a Go caller without writing. The wire-protocol reference gains "Replay rules for clients", and `journal/testdata/replay/` holds JSON fixtures that the Go SDK's `Tool` and `Model` are tested against, for clients in other languages to run.
- **`lake` module: the file layout and Parquet writer of the shared analytics store.** `github.com/danmestas/dagnats/lake` is its own module (pure Go). The writer (internal to the module) buffers events and writes them as `<root>/<program>/<table>/dt=<event date>/part-<sha>.parquet`, then a `part-<sha>.json` commit record after the part, so `lake.ListCommitted` trusts only whole parts; a crash between the two leaves an ignored orphan. Part names are the hash of their bytes. `event_id`, `event_time`, `stream_seq`, `schema_version` and `payload` are reserved columns on every row, and a declared column that is missing or mistyped in an event is NULL. The writer is not exported: only the archive program writes, so it will live in this module. Local roots only.
- **The bridge enforces the restart limit for bridge workers (#850).** A journal step claimed 5 times with no journal record between the claims now fails with `restart_limit` instead of being redelivered forever, as it already did for Go workers (ADR-028). The worker sees `410 {"code":"restart_limit"}` and the step's retry policy takes over. Set the limit with `Bridge.SetRestartLimit(n)` or `DAGNATS_BRIDGE_RESTART_LIMIT` (default 5). `worker.RestartLimitMax` is now exported.
- **A Go worker heartbeats while a handler runs.** It sends `InProgress` on the task message every half AckWait of the consumer that delivered it (including a `WithAckWait` override), and stops before the final ack or NAK and before `Pause`'s delayed NAK. A handler that outlived AckWait (5 minutes by default; 5 s on `WithPartitions` consumers) used to be redelivered and run a second time unless it called `ctx.Heartbeat()`, which still works. The heartbeat stops when the run is cancelled, when `Stop` abandons the handler after its drain timeout, and at the task's max run time, past which the worker logs a warning and lets AckWait redeliver the task. The default is one hour (`worker.WithHeartbeatMax` on `NewWorker`); `worker.WithMaxRunTime(d)` on `Handle` sets it per task type, up to 30 days. Long waits should prefer `ctx.Pause(name, d)`, which holds no worker.
- **A worker does not run one task message twice.** A redelivery that arrives while the message still runs in the same worker (a `WithConcurrency` lane can receive one) is skipped with no ack or NAK, so the running handler's outcome settles it. A redelivery that arrives within two AckWaits after the worker acked the message (queued behind the run in a lane of one, or resent after a lost ack) is acked without running. A NAK'd message runs again as usual. The check is per worker.

### Changed

- **`worker.(*Worker).Start` returns an error** instead of panicking when a consumer cannot be set up (stream missing, permission denied, orphan cleanup failing). It stops what it already started before returning, so the Worker is stopped and a retry needs a new one. Programmer errors (no handlers, colliding task types) still panic. A bare `w.Start()` statement still compiles; check the error to catch the failure. The embedded server's `Run` and `dagnatstest` now report it.

### Fixed

- **Ordered-consumer reads no longer leave a consumer behind.** nats.go creates an ordered consumer's server-side consumer when it opens, replaces it on every `Fetch`, `FetchBytes`, `FetchNoWait` or `Next`, and deletes the replaced one from a goroutine nobody waits on. So every journal, log, trace and metrics read briefly held two consumers, and a process that exited right after a read kept the old one until the one-minute inactivity threshold. `natsutil.OrderedConsumer` now deletes each replaced consumer before the fetch returns, and each delete waits until the stream no longer holds the consumer. That wait matters because nats-server answers a delete that races another delete of the same consumer with success before the consumer is removed. This also fixes the flaky `TestConsumerLeak_OpenAndReadReleaseTheirConsumer`.
- **The journal conformance suite's three-node cluster no longer fails with "server journal-N not ready".** The fixture reserved each route port by binding and closing it, so another process, such as an outgoing connection drawing it as its ephemeral source port, could take the port before the node bound it. With logging off, nats-server reports that bind failure nowhere and serves clients without a route listener, so the readiness check waited its full 10 s and failed. The fixture now captures the node's fatal log, detects the failed bind immediately and restarts the cluster on fresh ports, up to three attempts.
- **The console opens workflows whose names contain a slash** (`acme/support-ticket`), including a name that itself ends in `/run`: only a POST to a trailing `/run` starts a run, and a GET always shows the detail page. Contributed by @calionauta (#836).
- **A fresh tool call in a new attempt no longer reuses a prior unsafe call's idempotency key** (affects v0.0.32 through v0.0.35, since agent journal tool replay shipped). A new attempt numbered its fresh calls from 0 without skipping the positions the prior attempt's unsafe intents held, so a fresh call could run under the key `{run}.{step}.{iteration}.{call}` of a call whose effect may already have happened, and a service deduplicating on the key dropped the real effect: a second `send` after an interrupted one, or a different call at a prior unsafe call's position. A redelivery of such an attempt could also answer the interrupted call with the fresh call's result. Fresh calls in a new attempt now skip every position a prior attempt's unsafe intent holds in the iteration. `dagnats journal check` accepts an unanswered unsafe intent once a higher attempt wrote to the iteration, and reports it when that attempt ran the same call (name and canonical input) afresh instead of honouring it. The idempotency key is the same on every delivery of an attempt; a fresh call's key can differ between attempts.

### Security

- **`dagnats-mcp-duckdb` takes `klauspost/compress` v1.20.1** (was v1.18.3), past the fix for GHSA high-severity integer overflow and out-of-bounds write in its s2 decoder (fixed in v1.18.7).

## [0.0.35] - 2026-10-30

### Upgrade notes

- **`dagnats-mcp-duckdb` release tarballs for macOS now need macOS 13 or later** (was 11). Linux tarballs and the main `dagnats` binaries are unchanged.

### Added

- **`dagnats-mcp-duckdb` reads an S3 data dir.** `--data-dir s3://bucket/prefix` uses the AWS credential chain. `--s3-endpoint` points it at an S3-compatible store (R2, Garage, Tigris), with credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION`. The first S3 use downloads DuckDB's signed `httpfs` extension.
- **`worker.WithConcurrency(n)` runs up to n tasks of one type at once on each of its consumers (#842).** A Worker used to run one task of a given type at a time, so one long `digest.wait` window or slow agent turn held up every other task of that type. Pass it to `Handle`: `w.Handle("digest.wait", h, worker.WithConcurrency(16))`. The default stays 1, one at a time. n applies per consumer lane: the pull consumer, the sticky consumer, each `WithGroups` group and each `WithPartitions` member each run up to n, just as each runs one today. n is capped at `worker.ConcurrencyMax` (256), and `Stop` waits for all running tasks as before. With n above 1, tasks of that type no longer finish in delivery order. `HandleSingleton` types refuse it.
- **`server.Server.NATSURL()` reports the embedded NATS client URL** once the server is ready, so an embedder using a random port (`NATSPort = -1`) no longer has to probe for a free one.

### Changed

- **`dagnats-mcp-duckdb` moves to the official DuckDB driver,** `github.com/duckdb/duckdb-go/v2`. The release tarballs for macOS now need **macOS 13 or later** (was 11), the minimum the driver's prebuilt DuckDB library is built for. Linux tarballs are unchanged.
- **Its queries can read only the data dir.** Once the views exist, file access is locked to that path or prefix: reading other files, writing, attaching another database or lifting the lock is refused.

### Fixed

- **An HTTP bridge long poll ends soon after its client leaves or the server stops.** It used to hold the fetch for the whole `timeout_ms` (up to 60 s), which also delayed server shutdown. The poll now waits in one-second slices and checks its request between them. It also returns as soon as any task is available instead of waiting to fill `max_tasks`.
- **`dagnats-mcp-duckdb` enforces its row cap whatever the query says.** It used to skip the cap whenever the query text contained `LIMIT` anywhere: in a comment, a column alias, or a larger `LIMIT`. Queries are now wrapped in a capped subquery, and a second stacked statement is refused. CI now runs this module's tests.

## [0.0.34] - 2026-10-30

### Added

- **Optional logins for a dagnats other local users can reach.** By default dagnats' NATS client port and HTTP API accept any local process, under any user account.
  - `nats_user` with `nats_password_file` (`DAGNATS_NATS_USER`, `DAGNATS_NATS_PASSWORD_FILE`) requires that login on the client port and the WebSocket listener. dagnats' own engine and embedded workers log in with it automatically. CLI commands and outside workers put it in their URL: `nats://user:password@host:4222`.
  - `api_token_file` (`DAGNATS_API_TOKEN_FILE`) requires `Authorization: Bearer <token>` on every HTTP request except `/health`, `/health/cluster` and `/ready`. That includes the console, the HTTP bridge and trigger routes, so a browser or bridge worker needs a proxy that adds the header.
  - Both read their secret from a file, trimmed. A file readable by other users logs a warning.
  - With neither set, nothing changes.
  - **Not covered:**
    - The NATS monitoring port (`monitor_port`), which shows connection names, subjects and streams to anyone who can reach it. Leave it unset, or keep it on a port only you can reach. dagnats warns when it's set alongside `nats_user`.
    - Cluster routes, which need `nats_cluster_auth_token`. dagnats warns when routes are set without it.
    - The separate `dagnats-api` binary.
    - Logins passed in `DAGNATS_NATS_URL`: visible in the process environment.

## [0.0.33] - 2026-10-29

### Added

- `nats_host` (`DAGNATS_NATS_HOST`) sets the client listener's bind address. In leaf mode it was always `0.0.0.0`, which exposed the unauthenticated client port to the network; set `127.0.0.1` to keep it on the machine. The default is unchanged.

### Changed

- **A tool that returns plain text now fails with a clear error.** `Tool` names the tool and says its output is not JSON, instead of an opaque marshal error. `ToolFunc` and the docs say to encode text with `json.Marshal`.

### Fixed

- **A follow-up sent while the model was thinking is no longer lost on a redelivery.** When a delivery placed an input and died before committing, `PlaceInputs` on the redelivery returned nothing, so the handler never saw the input. It now returns the iteration's recorded placements first, then any new ones, the way `Tool` and `Model` return recorded results.
- **The durable-agent handler example places follow-ups at the start of the turn after a final answer.** It used to place them after its checkpoint, so they never reached the history.

### Docs

- A guide to durable agents.

## [0.0.32] - 2026-10-22

### Upgrade notes

- **Upgrade engines before workers**, as for 0.0.31. A worker on an older engine keeps working, but on an agent loop's first iteration it falls back to numbering attempts by delivery count, so the iteration-0 durability fix below needs the engine upgraded first.
- **Crash redeliveries no longer use up retry budget.** Every new dispatch now carries its attempt number, starting at 1, instead of the worker counting NATS deliveries. A worker that crashes repeatedly is stopped by `restart_limit` for journal steps, or by the step timeout otherwise. A step with neither was unbounded before as well.
- **Journal handles are single-goroutine.** A handler that runs a model's parallel tool calls must issue them through the journal one at a time.

### Added

- **Tool and model replay in the agent journal.** `j.Tool(ctx, call, Safe|Unsafe, fn)` records the call's intent and result, and `j.Model(ctx, fn)` records each model response. After a crash or takeover a recorded result is reused. An unfinished unsafe call is not rerun: the model gets an `interrupted` result carrying the call's idempotency key. A retry after a failure reruns safe calls and model calls, and keeps a prior unsafe call's outcome.
- **Crash-loop guard.** `worker.WithRestartLimit(n)` (default 5) fails a journal step with `restart_limit` after that many consecutive claims of one iteration with no progress. A run retry resumes from the journal.
- **Inputs, steering and durable waits.** `POST /runs/{run}/steps/{step}/inputs` (and `api.inputs.*`) sends a running agent a `steer` or `follow_up` input, idempotent by `request_id`, with status, long-poll and withdraw. `Continue(out, worker.Delay(d) | worker.Until(t) | worker.UntilInput())` waits without holding a worker. Inputs are capped at 256 KiB. A new `submissions` bucket is added.
- **Journal stream.** `GET /runs/{run}/steps/{step}/journal` pages a step's journal, and `follow=1` streams it as SSE, resuming from any sequence with `Last-Event-ID`.
- **The journal over the HTTP bridge.** New `journal.append`, `journal.read` and `load_checkpoint` resolve actions. Once a bridge worker has claimed, `complete`, `fail`, `continue` and `pause` commit to the journal before publishing.
- **Durability verification.** A crash-point matrix (`e2e/durability`) runs in the test suite, and a seeded chaos soak (`DAGNATS_CHAOS=1`, `make test-chaos`) runs nightly and on the `chaos` PR label.

### Changed

- `agent.loop.iteration` is removed; it was declared but never emitted. A journal commit with `action: continue` marks each iteration.
- The `checkpoints` bucket keeps entries for 30 days, applied in place on restart.

### Fixed

- **A redelivery on an agent loop's first iteration no longer wedges the run.** The worker numbered the redelivery as a new attempt, so the engine dropped the winning worker's continue.
- **A continue accepted while its step was still queued is no longer lost** if the engine dies before publishing the next task.
- **A stalled worker and its replacement can always be told apart**, including after a dispatch resend: a claim now carries the task message's stream sequence and delivery count, and a newer delivery of a message fences an older one.
- **Journal reads, the journal and logs streams, span reads and several CLI commands no longer leak NATS consumers.** Each left one behind, and after about 300 journal runs claims failed at the server's limit of 1,000 consumers per stream.
- **Journal reads retry** when the stream briefly has no leader or JetStream is temporarily unavailable, instead of failing.
- A retried `Complete`, `Continue` or `Fail` after a commit whose publish failed now republishes the stored commit, from both the Go SDK and the bridge.
- A handler that hit its own fenced journal append no longer hands the task back to the stalled worker.

## [0.0.31] - 2026-10-21

### Upgrade notes

- **Upgrade engines before workers.** A worker from this release on an older engine still works: it detects the old engine and also writes agent-loop state to the `checkpoints` bucket. The reverse is not covered.
- **Workers and bridge clients built before this release keep working.** Their lifecycle events carry no dispatch identity, and the new engine setting `legacy_step_events` (env `DAGNATS_LEGACY_STEP_EVENTS`) accepts them by default, logging a warning at startup. Turn it off once every worker is upgraded. While it is on, a duplicate `step.continue` from an old worker can still advance an agent loop twice.
- Workers using the HTTP bridge need no change: the bridge stamps the new fields itself.

### Added

- **Agent journal.** A handler that calls `tc.Journal()` gets a fenced, append-only journal for its step on the new `AGENT_JOURNAL` stream (30-day retention, 4% of the store budget). Every append is checked against the journal tail its writer last saw, so a worker whose claim lapsed (a stalled worker whose task was redelivered) can no longer write, commit or complete. An iteration commits as one record, and a worker that crashed after committing has its lifecycle event republished instead of the turn re-run. `dagnats journal check <run>` audits a run's journal against its history. Opting in must be fleet-wide per task type. Known limit: on an agent loop's first iteration, a crash between commit and publish repeats that turn (ADR-026).
- **Idempotent child runs.** `api.runs.spawn` and the Go SDK's `ControlPlane.StartRun` take an optional key (`worker.WithSpawnKey`); a repeated spawn with the same key returns the existing child run instead of starting another.
- **Cancellation reaches running handlers.** Cancelling a run ends `tc.Context()` for every handler still working on it, and `Heartbeat()` returns `worker.ErrRunCancelled`. The engine signals on `dagnats.cancel.{runID}`; because that signal is at-most-once, `Heartbeat()` also checks run state. Handlers should pass `tc.Context()` to their model and tool calls.
- ADRs 025–029 describe the durable-agent design.

### Changed

- **Agent-loop state is committed with `Continue`.** `Checkpoint` now buffers state and `Continue` carries it in the `step.continue` event; the engine stores it with the run in the same save that advances the iteration and hands it to the next iteration and to any retry. State is capped per step by `loop_state_max_bytes` (256 KiB by default); `Continue` fails over the cap. `Checkpoint(nil)` clears it. Over the HTTP bridge, a `checkpoint` resolve is buffered for the next `continue`, and the next poll's task carries the state.
- **`loop_delay` survives an engine restart.** It is scheduled on `SLEEP_TIMERS` instead of an in-memory timer.
- The `checkpoints` bucket keeps entries for 30 days, applied in place on restart.

### Fixed

- **A crash between `Checkpoint` and `Continue` no longer repeats an agent turn.**
- **A duplicate `step.continue` no longer advances an agent loop twice** and starts two workers.
- **A stalled worker can no longer complete or fail a step after another worker took it over.** `step.continue`, `step.completed` and `step.failed` carry the iteration, attempt and dispatch nonce of the task they answer, and the engine drops events for any other dispatch (counted in `engine.step_events.rejected`). Message IDs now include the iteration, so a later iteration's completion is no longer deduplicated as a repeat of an earlier one.
- A spawn event handled twice (a redelivery after an engine crash) no longer resets a child run that had already started; an existing child is left alone, or has its first step queued if that never happened.
- A step-timeout watchdog still fires for an agent loop after its first iteration.
- In a cluster, an event for a step that one engine still sees as queued (another engine is applying its `step.started`) is accepted when its dispatch nonce matches, and a late `step.started` for a dispatch the step has left no longer cancels a pending retry.

## [0.0.30] - 2026-10-19

### Changed

- **A workflow's `OutputSchema` is now enforced.** When a run would complete, the output of each terminal step (one no other step depends on) that finished with output is validated against it; a mismatch fails the run with the violation (path and reason) as the failing step's error and a `workflow.failed` event. A schema that cannot be parsed is rejected at register time. Workflows without an `OutputSchema` behave as before.

### Fixed

- **A step is no longer stranded when the engine dies while dispatching it.** The engine saves a step as queued before it publishes the step's task (or schedules a sleep timer or registers a wait-for-event waiter). If it died in between, the run stayed `running` with a step no worker would ever receive, and redelivering the event did not help because the step already looked dispatched. The dispatch save now marks the step as not yet sent and a second save clears the mark once the publish lands, so each dispatch costs one extra snapshot write. Any later event for the run, including the redelivered one, and the reconciler's periodic pass send every step still marked again. A task that is still queued from the first publish, or that a worker has already started, is not sent a second time.

## [0.0.29] - 2026-10-19

### Fixed

- **`dagnats-engine` and `dagnats-api` no longer scale a replicated cluster down to one replica.** They read `DAGNATS_NATS_JETSTREAM_REPLICAS` like `dagnats serve`, and with `0` or unset they derive the factor from the cluster they connect to (5 or 3, by placing a throwaway stream, or 1 outside a cluster). A cluster that still refuses three replicas after about 40 seconds, as a booting one can, fails startup and names `DAGNATS_NATS_JETSTREAM_REPLICAS` instead of falling back to one replica. Separately, provisioning never lowers the replica count of an existing stream or bucket: a lower factor, from any binary or from `nats_jetstream_replicas`, keeps the existing count and logs a warning. Reduce replicas with the `nats` CLI if you mean to.
- **`workflow delete` no longer misses a running run whose active-run marker is gone.** The guard now also scans the run index for the workflow's unfinished runs and counts a run that either source reports. The scan reads at most the newest 10,000 runs; past that the delete is refused with the count given as "at least N" (`total_is_lower_bound: true` in the REST 409 body), and `--force` / `force=true` is needed.
- **A run whose pinned definition was deleted can be cancelled.** `workflow.cancelled` used to fail with "pinned to workflow def version ... which no longer exists", go to the dead-letter stream, and leave the run `running`. The run and its unfinished steps are now marked cancelled from the run snapshot, its event waiters, task and run concurrency slots, singleton lock, approval tokens and active-run marker are released as on a normal cancel, and its child runs are cancelled. Later events for that run (a sleep timer firing, a late step result) are dropped instead of dead-lettered.

## [0.0.28] - 2026-10-19

### Changed

- **`cron.backfill` now replays missed fires.** Each cron fire records `<trigger-id>.last_run_at` in `trigger_state`, and when the trigger service starts, a `backfill: true` trigger fires once for every matching minute since then, oldest first, up to 100 per trigger. The flag used to be stored and ignored.
- **Subject-trigger `debounce` now takes effect.** A burst of messages inside `period` starts one run, with the last message's payload, once `period` passes quietly; `timeout` caps how long a window can keep extending. The block used to be validated and ignored, so every message started a run. The orchestrator fires the debounce timers, so `dagnats serve` and `dagnats-engine` both handle them.
- **`triggers`, `trigger_state`, `signals`, `checkpoints` and `concurrency_runs` are provisioned by `SetupAll`.** `dagnats-engine` and `dagnats-api` now create them too, instead of depending on `dagnats serve` having run first.

### Fixed

- **Buckets created outside `SetupAll` were single-replica in a cluster.** The five buckets above and `console_audit` now use the cluster's replication factor and appear in `/health/cluster` (`console_audit` on nodes that have created it). Existing deployments are upgraded in place on restart. Buckets passed to `SetupAll` through `WithKVBuckets` also get the replication factor, and one that names a bucket `SetupAll` already provisions is left as provisioned.
- `dagnats dlq replay <seq>` looks the entry up directly, so sequences beyond the oldest 100 replay instead of being reported as not found.
- `dagnats trigger test` accepts flags before or after the cron expression.
- Top-level `dagnats --help` lists every command and its real subcommands.
- `POST /v1/ci/validate` always includes `valid`, including `"valid": false`.
- The bridge poll error no longer says grouped task types can't be polled; it names the overlapping work-queue consumer and points to `worker_group`.
- Nil functional options no longer panic in `worker.NewWorker`, `Worker.Handle`, `worker.Typed` or `httpclient.New`; they are skipped.
- An HTTP trigger whose `workflow.started` publish runs past `timeout_ms` now answers `504 workflow_timeout` instead of `500`.
- `workflow-schema.json` no longer lists `version` as required. `Validate` never required it, and runs are pinned by definition hash, not by version.

- **A malformed workflow definition is an error, not a crash.** `workflow validate`, `workflow register` and the API panicked on a def with no name, a step with no ID, a task-type step with no `task`, or a `map` step with no `config`. They now report what is missing.
- **An incomplete `cancel_on` entry is rejected when the def is validated.** An entry with no `match`, or a match missing `left`, `op` or `right`, used to pass validation and then panic the engine every time a run of that workflow started. `match.op` must be `eq` and `match.right` must be an `input.` path, since cancel waiters are resolved against the run input; anything else could never match. The JSON schema now describes `cancel_on` entries.

### Documentation

- The docs site, README and engineering docs were audited against the code and corrected throughout. New pages: Clustering and Coordination Leases.
- `llms.txt` is now generated from the docs site (an index of every page plus a curated header), `llms-full.txt` no longer drops text or leaks template markup, and CI validates both on every PR.
- New docs site landing page. The site now shows the real release version.

## [0.0.27] - 2026-10-19

### Added

- **Coordination leases over NATS request/reply** (ADR-024). Downstream
  services can now elect a leader or split shard ownership through four
  new subjects: `api.leases.acquire`, `api.leases.renew`,
  `api.leases.release` and `api.leases.owner`. Leases live in a new
  `leases` KV bucket. The NATS server enforces each lease's TTL (1s to
  10m), and the key's KV revision is the fencing token. renew and release
  return `kind: "lost"` when the caller's revision is no longer current.
  The server republishes every change on `dagnats.lease.changed.<key>`
  (acquired, renewed, released, expired), once per change even in cluster
  mode. Leases need nats-server 2.11 or newer. On an older server dagnats
  still starts, and the lease subjects reply `kind: "unsupported"`. See
  "Coordination leases" in `docs/wire-protocol.md`.
- **Forward a request to a lease's owner** (ADR-024). `api.leases.forward`
  takes `{key, payload, timeout_ms}`, looks up the key's holder, and
  requests `dagnats.lease.inbox.<holder>.<key>` with the lease revision in
  the `Dagnats-Lease-Fence` header. Owners refuse a stale fence by
  replying with `Dagnats-Lease-Reject: stale`; forward then re-reads the
  lease and tries the new owner if it moved. Failures are explicit:
  `no_owner`, `owner_unavailable` and `busy` (the request was not
  processed), `owner_timeout` (it may have been), or `reply_too_large`
  (it was, but the reply does not fit). `timeout_ms` is 100 to 30000.
- **`/health/cluster` reports real cluster state.** It now lists member
  nodes (route connected, JetStream meta leader, caught up, lag), counts
  live nodes (`nodes_up` of `nodes_expected`), and reports per-stream
  `in_sync` from the replicas themselves. The stream and bucket lists
  come from `natsutil`, so the nine provisioned streams and every bucket
  are covered, and an unreachable one is named under
  `jetstream.unavailable` instead of failing the whole report. Each
  report names the answering node (`node`); caught-up and lag are
  reported only by the meta leader.
- **`api.cluster.health` on the NATS control API** returns the same
  report, for callers that only talk to dagnats over NATS.
- **`nats_cluster_port` / `DAGNATS_NATS_CLUSTER_PORT`** sets the route
  listener port (default 6222), so several nodes can share a host.
- A three-process cluster test (`make test-cluster-e2e`, CI job
  `Cluster E2E`) kills a node and checks a second workflow completes.

- `nats_cluster_host` (`DAGNATS_NATS_CLUSTER_HOST`) sets the route listener's bind address, which was always `0.0.0.0`. The default is unchanged.

### Changed

- **`/health/cluster` status semantics.** It used to report optimistic
  numbers and could not go degraded from node loss. It now returns 503
  when replicated streams or buckets lose quorum, and stays 200 (with the
  names under `jetstream.degraded`) when only single-replica assets are
  unreachable, so a per-node load-balancer check keeps the survivors in
  rotation.
- **`TELEMETRY`, `STICKY_TASKS` and `TRIGGER_HISTORY` use the cluster's
  replication factor.** They were single-replica in a cluster, so losing
  the node that held one made it unavailable. Existing deployments are
  upgraded in place on restart.
- **Startup provisioning shares one time budget** (30s standalone, 3
  minutes in a cluster) instead of a fresh one per resource, and
  `--die-with-parent` is armed before startup so a killed spawner cannot
  leave a server stuck in startup.
- **Config load rejects a bad `nats_cluster_port`** (out of range, or
  equal to another listener's port).

### Fixed

- Cluster nodes starting together no longer stall for up to minutes provisioning streams and KV buckets. On nats-server 2.15, an update of an R3 stream whose create is still electing a leader leaves both requests unanswered; replicated resources are now created first and updated only if they already exist.

- KV key listings could hang forever if the nats.go watcher stalled. A listing now abandons a watcher that delivers nothing for 10 seconds and retries with a fresh one, up to three attempts.

- A clustered node could exit at startup with "no suitable peers for placement" when the meta leader had not seen enough peers for an R3 stream yet. That error (10005) is now retried like a missing meta leader, and the pause between provisioning attempts backs off from 500ms to 4s.

- `GET /runs/{id}/logs` with no `attempt` now resolves a step that hasn't started yet to attempt 1, the subject its first attempt writes to. A follow attached early used to wait on attempt 0, which nothing writes.

- Multi-step runs could wedge at `running` in cluster mode even though their history recorded every step, and sometimes `workflow.completed`. Each node's engine consumes the same history stream, so consecutive events of one run are handled on different nodes. Run snapshots were read with a KV direct get, which any replica may answer, possibly from before the last write, and saved with an unconditional put. An engine holding an older snapshot overwrote a newer one, erasing a finished sleep step (the run never completed) or a completed run. Snapshot reads now go to the stream leader, and every save of a loaded run is a compare-and-set on the revision it was read at. On a conflict the event is handled again from a fresh read, after a short jittered backoff. To make that safe, dispatching ready steps now saves their state once, with every publish, timer and waiter after that save, so a lost save never leaves a step queued but never dispatched, and one failed publish or timer no longer stops the steps dispatched with it. A running run with ready steps still pending (a start, or a detached sub-workflow step, whose dispatch save was lost) is now dispatched when `workflow.started` is handled again, instead of being skipped. A lost ReleasePending save is retried from a fresh read.

- A task slot (`MaxTaskConcurrency`) could be freed twice when a step's completion was handled twice, or taken twice when a retried step was published again, so the limit drifted. `concurrency_tasks` now records which steps hold a slot (`<run_id>/<step_id>`), and taking or freeing a slot the step already holds or has already freed does nothing. Values still in the old counter form read as no holders, so a limit can over-admit once for tasks in flight across the upgrade.

- A cluster with `nats_cluster_auth_token` set never formed. Nodes required the token on routes they accepted but did not send it on routes they dialed, so every route was rejected and startup failed with "cluster quorum did not form". The token is now added to every route URL that has no credentials of its own.

- A revoked worker token could briefly authorize again. If the token store's KV watch delivered the mint event after `Revoke` had already cached the revoked record, the older event overwrote it until the revoke event arrived. Cache updates now apply only when their KV revision is newer.

- Clustered nodes now keep the same NATS server name across restarts. It was derived from the process ID, so every restart joined the cluster as a new JetStream peer and left the old name behind as a replica that never came back. The default is now `<cluster>-<hostname>-<cluster port>`, and `nats_server_name` (`DAGNATS_NATS_SERVER_NAME`) overrides it. Nodes upgrading from an earlier version change name once.

- An HTTP trigger request whose client hangs up while the run is still being started now answers 499 `client_closed`, the same as a hang-up while awaiting the response, instead of 500 "failed to publish trigger".
- **Cluster start no longer stalls on a dropped JetStream request.**
  The quorum wait gave its first `AccountInfo` probe the whole 60s
  budget, so a probe sent before the meta leader was elected was never
  retried; probes now time out after 2s and are retried. Stream and
  bucket creation also retries on timeouts and "JetStream system
  temporarily unavailable" (JetStream error 10008) instead of aborting
  startup; other errors still fail at once.
- **The `worker_tokens` bucket keeps its replication factor.** Starting
  a node reset the bucket to a single replica; it now binds to the
  bucket `SetupAll` provisioned.
- Stream and bucket creation errors name the stream or bucket.

### Documentation

- Route TLS is not supported yet; `docs/production.md` says so and
  recommends a private network or tailnet.

## [0.0.26] - 2026-10-17

### Changed

- **Time spent paused counts toward a step's timeout** (#743). `Pause`
  keeps the step running, and a resumed task now continues the
  attempt's timeout rather than starting a new one. Retried attempts
  already behaved this way; a first attempt got a fresh timeout on each
  resume only through the redelivery numbering fixed below. A step that
  pauses repeatedly (a polling loop, say) needs a `Timeout` covering
  its whole run, pauses included.

### Fixed

- **`dagnats init` scaffolds a workflow that validates.** The
  `$schema` editor hint it writes was rejected as an unknown field by
  `workflow validate` and `workflow register`, as were the
  `agent-loop` and `log-offload` examples. `$schema` is now accepted
  and ignored.
- **Workflow JSON accepts duration strings** such as `"timeout": "30s"`
  everywhere a duration appears: step and workflow timeouts, retry
  delays, agent-loop, sleep, approval and wait-for-event settings,
  `cancel_on`, rate limits and trigger debounce. Integer nanoseconds
  still work and are what dagnats writes back. A Go type that embeds
  `dag.WorkflowDef` (or another of these types) now inherits its
  `UnmarshalJSON`, so it needs its own to decode its extra fields.
- **An unknown step `type` is an error, not a crash.**
  `workflow validate` and `workflow register` panicked on a typo such
  as `"type": "agentloop"`; they now report `unknown step type`.
- **A step's timeout fires even when its task keeps being redelivered**
  (#743). Each redelivery of a task (its worker died or stopped
  extending its claim) reported a new attempt and restarted the step's
  timeout, so such a step never timed out and its run read "running"
  indefinitely. The timeout now runs from when the current attempt
  first started: a redelivery, or a paused task resuming, re-arms it
  only for the time left. A retry the engine schedules still gets the
  full timeout.
- **Docs match the code again.** The CLI reference gains the nine
  commands it was missing (`trace`, `metrics`, `singleton`, `service`,
  `trigger-type`, `demo`, `observe`, `sidecar`, `completion`) plus
  `workflow delete`, `run count` and `trigger fire`. The workflow
  schema reference and `workflow-schema.json` now put agent-loop
  settings under `config` (there is no `loop` key) and list the step
  types and fields they were missing. The config pages show the real
  `http_addr` default (`127.0.0.1:8080`) and document `max_memory_bytes`,
  `nats_ws_port`, `nats_ws_no_tls` and the console, bridge and
  build-log variables. Pages that described step timeouts as NATS
  `AckWait`, or retries as a NAK of the task message, now describe the
  engine's watchdog and retry timers. The README's builder sample and
  several handler samples compile again.

## [0.0.25] - 2026-10-17

### Added

- **Cron sleeps can name their timezone** (#750). `cron_timezone` sets
  a fixed IANA zone, validated when the def is registered.
  `cron_timezone_input_path` reads the zone from the run input at
  dispatch, so one def can wake each run at 09:00 in its recipient's
  zone; a missing or unknown zone there falls back to UTC with a
  logged warning. Zone data is now built into the binary, so named
  zones also resolve in minimal images (the Docker image ships no
  zoneinfo), for cron triggers' `timezone` as well.

### Changed

- **A cron sleep with no zone is evaluated in UTC, not the server's
  local zone** (#750). The same def used to fire at different instants
  depending on the host's `TZ`. On a host not running in UTC, existing
  cron sleeps now wake at a different wall-clock time: `0 9 * * 1` on
  a server in US Central time used to mean 09:00 Central and now means
  09:00 UTC. Set `cron_timezone` to keep the old wall-clock time.

### Fixed

- **A malformed cron expression is an error, not a crash** (#740). An
  empty comma item (`0,,30 3 * * *`) or an expression over 256 bytes
  panicked the cron parser, taking the server down when a trigger def
  or cron sleep carrying it was validated, and again on every restart
  that re-read it. Both now return an error.

## [0.0.24] - 2026-10-02

### Fixed

- **A heartbeating HTTP worker keeps its task past 5m30s** (#741). The
  bridge dropped a claimed task from its in-flight map 5m30s after the
  claim, even while the worker heartbeated, so the next claim's sweep
  made every later `heartbeat`, log upload or `complete` answer
  `404 task not found`, and NATS redelivered the task to run again.
  Every action that extends the claim (`heartbeat`, `stream`,
  `checkpoint`, `send_signal`, `wait_signal`) now restarts that window,
  so only a worker that stops extending its claim loses the task.

## [0.0.23] - 2026-10-01

### Fixed

- **Cancelling a run withdraws its tasks from HTTP workers** (#737).
  - **Queued tasks:** a task still queued when its run was cancelled used
    to be handed out by `POST /v1/tasks/poll`, possibly minutes later, run
    to the end, and show its step as completed under the cancelled run.
    The bridge now drops such a task when a poll draws it, as native NATS
    workers already did.
  - **Claimed tasks:** every resolve action on a claimed task of a
    cancelled run (`heartbeat` included) answers `409 Conflict` with
    `run cancelled` and withdraws the task, so a worker knows to stop and
    nothing it reports is recorded. Go workers on `sdk/httpclient` get
    `httpclient.ErrRunCancelled`.
  - **Engine:** a `step.queued` or `step.started` that arrives after its
    run has finished no longer moves the step. Before, such an event
    could flip a cancelled step back to queued or running.

## [0.0.22] - 2026-09-30

### Fixed

- **Step metadata reaches workers again** (#732). Since 0.0.14 the engine
  published tasks without `StepDef.Metadata`: the dispatch path that copied
  it (#450) was replaced in #652 and the copy was lost, so
  `TaskContext.Metadata()` was empty for every worker, and a `jobs:`
  workflow's `ci.job` (#728) never arrived. Every dispatch now carries it,
  including timer-driven retries and the dead-letter body. The HTTP bridge
  also returns it: `POST /v1/tasks/poll` tasks gain `metadata`, omitted when
  the step has none.

## [0.0.21] - 2026-09-30

### Added

- **A GitHub-Actions-shaped `ci.yml`** (#728): `on:`, `jobs:` with
  `needs:`, and `steps:` of shell `run:` commands, alongside the existing
  `checks:` shape, which compiles exactly as before. Each job compiles to
  one step with task `<task_namespace>.job` in worker group
  `<task_namespace>`, where the namespace comes from the caller; the job
  itself travels as JSON in `Metadata["ci.job"]`. `on:` is returned,
  normalised but uninterpreted, so the caller decides when a workflow
  runs. Secrets are referenced by name only (`${{ secrets.NAME }}` as an
  entire env value, bound to an env var of the same name) and scoped as in
  Actions: workflow and job secrets reach every step, a step's secret only
  that step. No other expressions are accepted. A `jobs:` spec rejects
  YAML aliases, duplicate keys and malformed env var names. A spec that
  has both `jobs:` and `checks:` now gets that one diagnostic in place of
  its other `checks:` diagnostics. Unsupported Actions keys such as `runs-on`,
  `uses`, `if` and `strategy` are reported as unsupported rather than as
  typos, and every diagnostic carries a line and column. `POST
  /v1/ci/compile` and `/v1/ci/validate` accept `task_namespace` and return
  `on`. In the Go SDK, `ci.CompileYAMLWith` takes the namespace;
  `ci.CompileYAML` is unchanged.

### Changed

- **Dependencies:** `github.com/nats-io/nats.go` 1.54.0 (#727) and
  `github.com/mark3labs/mcp-go` 1.1.1 (#726).

## [0.0.20] - 2026-09-23

### Breaking / behavior changes

- **Go 1.27.1 is now the minimum toolchain** (was 1.26.2, and 1.26.1 for
  the `dagnats-mcp-duckdb` module). This raises the required Go for
  anyone importing the SDK packages. It also collapses four different
  versions that had drifted apart: CI tested on 1.26.2 while the
  published container image built its binaries on 1.27, the
  mcp-duckdb release builder used 1.26-bookworm, and a local `go build`
  used whatever was installed. Tests, the container, and the release
  builders now all use the same toolchain, and the 1.26 series was six
  patch releases behind upstream.

### Fixed

- **A grouped step that failed once was never retried, and its run never
  ended** (#721). Retries are re-published by a timer, and the timer
  rebuilt the task subject without the worker group, so every retry of a
  grouped step landed on the ungrouped subject, where no grouped worker
  listens. The step timeout then scheduled another retry to the same
  wrong subject, leaving the run `running` indefinitely. This affected
  rate-limit and concurrency retries, `retry_after`, exponential/fixed
  backoff retries, and the sticky soft-fallback. Retries now reuse the
  exact subject the first dispatch was published on. Runs already stuck
  this way recover on their own after upgrading: the next step-timeout
  retry is published to the grouped subject and claimed normally.

### Added

- **Worker registrations record the worker groups they drain** (#719),
  as `worker_groups` in `GET /v1/workers`, so a consumer can answer "who
  serves group G" and show capacity per group. Previously every
  registration carried no group, so a grouped worker that was actively
  draining its group appeared attached to nothing. Go SDK workers report
  the groups they were started with. Bridge workers can name a
  `worker_group` on `POST /v1/workers/connect`, validated exactly as a
  poll's group is; if they omit it, the registration records the
  token's group scope rather than claiming the ungrouped queue.

## [0.0.19] - 2026-09-22

### Fixed

- **`dagnats clean`'s scheduled `--older-than` run could purge nothing while
  the run index grew unbounded.** On 2026-09-19 a production outage's retry
  storm wrote 353,614 `workflow_runs` KV entries and 513,001
  `WORKFLOW_HISTORY` messages in under an hour. The scheduled cleanup job
  (age-only, 7-day floor) ran on time and reported success — but every one
  of those records was hours old, so nothing qualified and nothing was
  purged. dagnats then walked that oversized run index on every scheduler
  tick until it stopped firing triggers entirely for 80 minutes, at 176% CPU
  and 550MB RSS, with no code change and no stream backlog to explain it —
  purging the streams did nothing, only draining the KV bucket did. Age and
  count are orthogonal failure modes; `--older-than` only ever guarded the
  first. `clean` now also enforces a count ceiling (`--max-keep`, default
  50,000) on the "runs" category after every age/full clean, whether or not
  the age pass found anything — the exact case a retry storm produces. It
  purges server-side via the same `WithPurgeKeep` mechanism `--keep` already
  offered by hand, skips work-queue streams so live un-acked tasks are never
  evicted, and is on by default so existing deployments get the fix from a
  binary upgrade alone, with no change needed to the scheduled job's command
  line. Pass `--max-keep=0` to disable it.

### Changed

- Dependency bumps: `nats-server` and `mcp-go`.

## [0.0.18] - 2026-09-18

### Changed

- **A worker token's 403 now explains itself** (#711). Refusals used to
  say only `task type "X" not permitted for this token`, which reads as
  "that work isn't for me" when the usual cause is a scope that matches
  nothing. The message now names the requested task type and the
  token's scopes, says plainly when a token has no task types at all,
  and flags the common near miss: `task_type_prefixes` entries match a
  whole task type or a namespace the type continues after a `.`, not a
  text prefix, so `dantest-` never matches `dantest-puzzles.<id>`. For
  a near miss it suggests the scope that would work, taken from the
  requested type. The worker-group 403 names the token's group scope
  too. The field's real semantics are now documented where it's
  defined and in the wire-protocol docs; the field is not renamed.

## [0.0.17] - 2026-09-16

### Fixed

- **`dagnats status` rendered a grouped consumer's task type with the
  group sentinel attached** (e.g. `greet.=fast` instead of `greet`), for
  consumers whose durable name is not self-describing so the filter
  subject is parsed instead. Display only; introduced with #704's
  encoding.
- **The Audit page showed stale events on a busy deployment** (#698).
  Its key listing truncated to the *oldest* entries while the renderer
  walked the result backwards for newest-first, so once the bucket
  passed the 2,000-key cap the page displayed old activity while
  presenting it as recent.
- **Worker-token, service, trigger-type and audit listings could omit a
  live entry** (#698) when a concurrent write replaced that key's only
  revision, the same `kv.ListKeys` watcher-snapshot race fixed for the
  worker directory in #699. These listings now enumerate from the
  stream's subject state, which the server resolves under the store
  lock. The run-active and run-index enumerations are unaffected and
  deliberately unchanged: their keys are created once and never
  replaced, and the run index additionally depends on creation-order
  replay that the new enumeration cannot provide.
- **A failed key fetch during those listings no longer silently shortens
  the result** (#698). Four of them treated any error as "skip this
  key", making a transient failure indistinguishable from a deleted
  one; only a genuinely missing key is skipped now.
- **`dagnats services list` and `dagnats trigger-type list` emit a
  stable order** (#698), including their JSON output, so results can be
  diffed across invocations. `dagnats trigger-type list` also no longer
  aborts when the bucket holds more than 10,000 keys; it warns and
  lists the first 10,000.

## [0.0.16] - 2026-09-14

### Breaking / behavior changes

- **The worker-group subject token is now sentinel-prefixed** (#704):
  `task.{task}.={group}.{runID}`, with the matching durable name
  `workers-{task}-={group}`. Previously `task.{task}.{group}.{runID}`
  could not be distinguished from a dotted task type, which is why a
  dotted task type could not carry a worker group at all and why a token
  granted the dotted prefix `a.b` could reach type `a` / group `b`
  (#695). Both mappings are now injective, so a dotted task type carries
  a worker group normally and `dag.ValidTaskGroupCombo` — an exported
  symbol — is **removed** along with the poll-time aliasing check it
  backed. Ungrouped subjects and durable names are unchanged.
  **Migration, required for any deployment using worker groups: upgrade
  the engine FIRST, then the workers.** Work-queue retention plus
  `DeliverAllPolicy` means grouped tasks published before an upgraded
  worker exists are held in the stream and delivered in full once it
  starts, so engine-first loses nothing. **Workers-first is unsupported**
  — the engine would still publish the legacy subject, and a grouped task
  published to a subject no consumer matches strands *indefinitely*
  rather than failing: `TASK_QUEUES` has no MaxAge, AckWait and
  MaxDeliver are inert with no consumer, and the per-step watchdog never
  arms because `step.started` never fires. The orchestrator logs any such
  stranded subject at startup, naming the message count and remediation.

## [0.0.15] - 2026-09-13

### Breaking / behavior changes

- **The default `max_store_bytes`** is no longer an unconditional 10 GiB.
  An unconfigured operator now gets `min(10 GiB, available disk at
  data_dir / 2)`, derived at config-resolution time (#687) — a small host
  (e.g. a 2 GB VM) no longer has its JetStream store budget silently set
  larger than the disk backing it. A budget over 80% of available disk
  (derived or explicit) is logged as a warning. If the disk backing
  `data_dir` has no space at all, startup now fails with a config-load
  error naming the data dir, instead of starting with an unusable budget.
- **A malformed (non-numeric) `DAGNATS_MAX_STORE_BYTES` env var** (previously
  silently ignored, leaving the 10 GiB default in place) **now aborts
  startup with a config-load error** naming the bad value. A well-formed
  but non-positive value (`0` or negative), from either the env var or the
  `max_store_bytes` `dagnats.yaml` key, already crashed startup before this
  change; it now fails the same way but with a clear, source-specific
  error message instead of a generic panic (#687).
- **Unrecognized `ci.yml` keys are now diagnosed instead of silently
  dropped** (#681) — in checks, the nested `retry:` block, `deploy:`,
  `defaults:`, and at the top level. A spec with a typo'd or unsupported
  key that previously validated clean now fails `/v1/ci/validate` with a
  positioned diagnostic. YAML merge keys (`<<:`) are respected, and
  top-level `x-`-prefixed keys are reserved extension/anchor space and
  left unscanned.
- **An exponential retry policy with no `multiplier` now backs off ×2 per
  attempt** (#683) — previously the unset multiplier computed
  `InitialDelay * 0^(n-1)`, so every retry after the first fired with zero
  delay. An explicit multiplier is now validated to `[1, 100]` at
  definition time.
- **A worker token granted a dotted task-type prefix no longer reaches the
  equivalent grouped queue** (#695). `task.{type}.{group}.{runID}` cannot
  distinguish a dotted task type from a task-plus-group, so a token holding
  `["a.b"]` could drain type `a` / group `b` — work it was refused when it
  asked by name. A poll is now authorized against every reading of the
  subject it derives, not the spelling the caller chose. **Migration:** a
  token scoped to an exact dotted prefix without also holding the ancestor
  (`["a"]`) can no longer poll that type ungrouped; scope to the ancestor.

### Added

- **`ci.yml` checks can declare retries** (#681): `retries: N` as
  shorthand (fixed strategy, 5s delay, mirroring the native format's
  legacy `Retries` defaults) or a full `retry:` block (`max_attempts`,
  `strategy`, `initial_delay`, `max_delay`, `multiplier`) mapped onto the
  step's retry policy. Previously both were accepted, validated, and
  silently discarded at compile.
- **Registered workflows can be deleted** (#682): `DELETE
  /workflows/{name}` and `dagnats workflow delete <name>`. Refuses with
  `409` while non-terminal runs exist (listing them) or while triggers
  still reference the workflow; `--force`/`?force=true` overrides. Both
  guards live in the service, so the CLI and HTTP surfaces share one
  contract. Deleting a definition removes its version keys but leaves run
  history readable.
- **Worker-group scoping, so a repository owner can supply their own
  runners** (#695). `POST /v1/tasks/poll` takes an optional `worker_group`
  and polls that group's consumer — previously a bridge-connected worker
  could only ever drain the ungrouped one, making `WorkerGroup` unreachable
  over HTTP. `POST /v1/tokens` takes an optional `worker_groups`, reported
  back on mint and in the listing; a group-scoped token cannot reach
  another group or the ungrouped queue. Absent means unscoped by group
  (existing tokens are unaffected); a present-but-empty list is refused at
  mint so it can never silently widen to "every group". Isolation is by
  subject: two groups derive non-overlapping consumer filters, so neither
  ever receives the other's messages.
- **`ci.yml` checks and `deploy` accept `worker_group:`** (#695), with a
  compile diagnostic when it is paired with a dotted compiled task type —
  including the `call:` path, which compiles to `dagger.call`. Without the
  diagnostic the failure was silent: a worker that idles forever against a
  subject nobody publishes to. Grouped steps need a single-token task type.

### Fixed

- **A live worker could intermittently vanish from `dagnats workers list`,
  the console, and trigger-owner liveness** (#699), roughly one listing in
  2000. `Directory.List()` enumerated via `kv.ListKeys()`, whose answer is
  built from a watcher's initial delivery and is not atomic with respect to
  concurrent writers; on the `history=1` workers bucket a heartbeat's `Put`
  landing inside the watcher's setup window dropped the key from the
  listing entirely, while `Get` still returned it. Enumeration now reads
  the stream's subject state, which the server resolves under the store
  lock, and costs one round trip instead of an ephemeral consumer per call.

### Changed

- **Lowering `max_store_bytes` below what an established store already
  reserves** no longer requires a manual per-stream trim before it takes
  effect (#688, following #685/#686). The embedded server now auto-trims
  stream ceilings over one restart: it temporarily admits the existing
  reservation, shrinks every stream's `MaxBytes` down to the configured
  budget's ceilings — discarding each stream's oldest messages as needed to
  comply, same as any other `MaxBytes` reduction — and the configured value
  takes full effect on the restart after that. If a stream outside dagnats's
  own set (operator-created, a mirror, an orphan) still holds the aggregate
  over budget once the managed streams have shrunk, startup fails with the
  same actionable message #686 introduced, naming the offenders, instead of
  silently re-entering recovery at an inflated limit on every future boot.

## [0.0.14] - 2026-08-29

### Breaking / behavior changes

- **`sidecar.InstallAll`** now takes the dagnats build version:
  `InstallAll(w io.Writer, dagnatsVersion string)`. Breaking for direct
  importers of the `sidecar` package (see Changed).
- **`worker.TaskContext`** gained `LogOut() io.Writer` / `LogErr() io.Writer`
  (#624/#652). Any out-of-repo type implementing `TaskContext` directly
  (not via `dagnatstest.MockTaskContext`, and not embedding a value this
  module constructs) stops compiling until it adds both methods.
- **`api.MountV1`** gained a third parameter, `tokenStore *workertoken.Store`
  (#627/#647).
- **`POST /workflows`** now fails with `400` on a def with an unsafe task
  type or `WorkerGroup`, or a dotted task type paired with a `WorkerGroup`
  (#674/#677) — previously it registered a run that could never dispatch.
- **An ungrouped worker no longer drains a grouped task type's work, and
  vice versa** (#674/#677). **Migration:** a deployment relying on an
  ungrouped worker to serve a task type that grouped workers also poll
  must add a matching grouped worker for that task type — see
  docs/wire-protocol.md "Task Subjects". A not-yet-upgraded durable for
  the same task type/group auto-upgrades in place the first time upgraded
  code claims it (logged once at `warn`, no maintenance window needed); a
  message already published under the old scheme to a subject with an
  extra token the new filter no longer matches is not redelivered to that
  durable and instead ages out via `MaxAge`.
- **`DAGNATS_BRIDGE_TOKEN`, when set, is the admin/root credential** for
  worker-token minting — the only credential `/v1/tokens` mint/list/revoke
  accept — and minting fails closed with `503` when it's unset (#627/#647).
  Bridge auth's existing dev-mode/env-token gate is otherwise unchanged.
- **`Orchestrator.Start()` now returns an error and fails startup** when the
  active-run index cannot converge, instead of silently starting with an
  incomplete reconciler view (#664/#668).
- **`GET /runs` and `POST /runs/cancel` are exact only over a newest-first
  scanned window**, not the full run population; both responses carry a
  `truncated` flag when the scan hit its bound before exhausting the index
  (#659/#663).

### Added

- **`GET /v1/workers` exposes the live worker directory (#628/#635).**
  Returns `{"workers": [...], "count": N}` (`workers` never `null`); wins
  Go 1.22+ mux precedence over the worker-runtime bridge's `/v1/` catch-all
  for this exact path.
- **`def_hash` on workflows + an annotation contract (#630/#636, #639).**
  `dag.DefHash` is a hex SHA-256 over a workflow def's canonical JSON,
  surfaced as `def_hash` on register/list; `protocol.Annotations` is the
  blessed (optional) shape a worker may put in `TaskResolution.Data` so a
  forge can pin failure/warning markers to a diff view — the engine never
  parses it. Docs correct an earlier claim that in-flight runs keep the def
  they started with; that guarantee ships separately, below.
- **Per-step `StartedAt`/`CompletedAt` timestamps (#626/#638).** Added to
  `dag.StepState`, stamped on the same snapshot writes the engine already
  makes (no extra KV writes); a UI can now render per-step durations or a
  waterfall from a run snapshot.
- **Run labels, label filters, bulk cancel by label (#629/#645).**
  `dag.WorkflowRun.Labels` (bounded: 16 labels max, key/value charset and
  length limits), settable on `POST /runs` and `POST /runs/bulk`, queryable
  via repeatable `?label=key=value` on `GET /runs`, and filterable on
  `POST /runs/cancel` — all composing with the existing `workflow`/`status`
  filters.
- **Terminal run events + a documented consumer contract (#625/#646).**
  Every terminal transition (including cancelled/compensated paths that
  previously published nothing) now publishes
  `event.run.{workflow}.{runID}.{status}` on `EVENTS` after the terminal
  snapshot durably persists, replacing the old `workflow.completed`/
  `workflow.failed` events that only 4 of 12 terminal paths triggered.
- **Mintable, revocable, scoped worker tokens (#627/#647).** New
  `internal/workertoken` KV-backed store: operators mint per-machine
  bearer tokens scoped to task-type prefixes (segment-aware matching)
  instead of handing out the admin secret; poll/resolve are enforced
  per-token, and revoked tokens are pruned once past a bounded count.
- **Queue-depth API + periodic snapshot events (#632/#649).** `GET
  /v1/queue` returns a per-task-type pending/oldest-wait snapshot sourced
  directly from `TASK_QUEUES`' unacked messages (no separate counter to
  drift); `event.queue.snapshot` on `EVENTS` publishes the same shape on
  change (plus a 60s heartbeat) so a consumer can watch depth live.
- **`ci/` package + `/v1/ci/{compile,validate}` (#633/#651).** Promotes the
  CI-spec compiler out of the nested `dagnats-ci` module's `internal/`
  tree into a public root package, and mounts two control-plane endpoints
  on top of it. Diagnostics now accumulate (bounded at 100) instead of
  failing fast on the first problem; `POST /workflows` itself stays
  spec-agnostic — the two `/v1/ci/*` routes are the only place `ci.yml`
  awareness enters the control plane.
- **Build-log hot lane, worker log writers, tail/follow API (#624/#652).**
  New `BUILD_LOGS` stream (`logs.{runID}.{stepID}.{attempt}`, TTL via
  `DAGNATS_BUILD_LOGS_TTL`); `TaskContext.LogOut()`/`LogErr()` for Go
  workers, `POST /v1/tasks/{id}/logs` for the bridge; `GET
  /runs/{id}/logs` for a paged read or SSE follow. Scope is deliberately
  the hot lane only — no S3 offload, no cold cache; retention past the
  TTL is a consumer's job, same posture as `history.{runID}`.
- **`run_terminal` trigger with loop guards (#634/#653).** Starts a target
  workflow when another workflow's run reaches a terminal state, sourced
  from the `event.run.*` contract above. Register-time self-trigger
  rejection, a `TriggerDepth`-bounded (8) refusal for cross-workflow
  cycles, and restart-safe dedup (the started run's ID is a deterministic
  hash of `{triggerID}|{sourceRunID}`, not a duplicate-window trick).
- **`ci.yml` checks may name a plain task type (#671/#673).** `Check.Call`/
  `DeployStep.Call` no longer force every check through `dagger.call`; a
  new `task:` field (mutually exclusive with `call:`) lets a repo whose
  workers already speak the plain worker protocol use `ci.yml` too.

### Fixed

- **`workflow validate` and `workflow register` silently skipped
  respond-reachability warnings (#613/#619).** `dag.ValidateRespondReachability`
  was already wired into the REST registration handler and the console's
  registration page, but neither CLI command an author actually runs
  offline called it — both reported clean success while missing/duplicate
  `respond` steps and missing schemas went unflagged. `workflow validate`
  now detects HTTP triggers offline (no NATS round-trip) from the parsed
  file's own `triggers` array; `workflow register` shares the same
  helper. `workflowValidateResult` gains `Warnings []dag.Warning`;
  `workflowRegisterResult` gains a distinct `RespondWarnings
  []dag.Warning` (kept separate from the existing missing-worker
  `Warnings []string` field rather than conflating the two shapes).
  Warnings print to stderr and never change exit code.
- **`dag.Validate` accepted task types that could never dispatch, and a
  dotted task type's consumer filter wildcard-matched unrelated sibling
  types (#674).** A step's `Task` was only checked for non-emptiness;
  `internal/engine`'s `StepSubject` builds `task.{Task}.{runID}` from it
  verbatim (no sanitization), so a `Task` with whitespace, a NATS
  wildcard, or a stray leading/trailing/empty dotted token registered
  cleanly and then silently never got dispatched. Separately,
  `internal/consumername.FilterFor` anchored on a trailing `>` wildcard,
  so a worker polling `build` also received `task.build.linux.<runID>` —
  a differently-typed task that only shared a dotted prefix. `dag.Validate`
  now enforces one charset/shape rule (`dag.ValidTaskType`, shared with
  `ci/compile.go`'s diagnostic and, for `WorkerGroup`, `dag.Validate`
  itself — `StepSubject` appends `WorkerGroup` as its own subject token,
  so an unsafe value is exactly as dangerous as an unsafe `Task`), and
  `FilterFor` anchors on exactly one trailing token instead of `>`. A
  workflow that pairs a dotted `Task` with a `WorkerGroup` is now also
  rejected: `FilterFor("render.gpu", "")` and `FilterFor("render",
  "gpu")` derive the byte-identical filter subject and durable name, so
  the combination could silently collide with an unrelated step. Dots
  remain legal in task types on their own (`dagger.call` still works) —
  isolation comes from the filter anchor, not from banning dots.
  A worker's `WithPartitions` (pcgroups elastic consumer) path built its
  filters inline and bypassed both the anchor fix and consumer-name
  sanitization; it now routes through the same `FilterFor`/`NameFor`
  helpers as the non-partitioned path.
  **Behavior changes:**
  - a workflow def with an unsafe task type or `WorkerGroup` (or a
    dotted task type paired with a `WorkerGroup`) now fails
    `POST /workflows` with 400 instead of registering a run that could
    never dispatch correctly;
  - an ungrouped worker no longer drains a grouped task type's work (or
    vice versa) — on `main` this was accidental overlap from the same
    `>` wildcard bug, not an intentional feature. **Migration:** any
    deployment that has been relying on an ungrouped worker to serve a
    task type other, grouped workers also poll must add a matching
    grouped worker for that task type — see docs/wire-protocol.md
    "Task Subjects".
  A durable a not-yet-upgraded worker or bridge process left behind
  (the old `>`-anchored filter, for the SAME task type/group) is
  auto-upgraded in place the first time the new code claims it — deleted
  and recreated with the new filter, logged once at `warn`. This is
  live, not a maintenance-window migration step. A durable whose filter
  differs for a genuinely different task type still collides loudly,
  unchanged. **Caveat:** any message already published under the old
  scheme to a subject with an extra token beyond what `*` now matches
  (e.g. `task.{type}.{extra}.{runID}` from a pre-upgrade dispatch the
  old `>` filter would have drained but the new one-token `*` does not)
  is no longer delivered to that durable — it sits on TASK_QUEUES until
  the stream's `MaxAge` retention expires it, rather than being picked
  up. This only affects messages published in the narrow window before
  a deployment's first upgraded process claims the durable; steady
  state is unaffected.
- **`dagnats sidecar install` could never fetch `dagnats-mcp-duckdb` (#621/#622).**
  The release workflow built the four tarballs but only uploaded them as
  workflow artifacts, expecting an operator to attach them by hand — which
  never happened, so no release from v0.0.1 through v0.0.13 shipped the
  asset and every prebuilt download 404'd. Compounding it, the download
  version was pinned to a hardcoded `0.0.1` that drifted 12 releases behind.
  The workflow now attaches the tarballs to the release on tag push, and the
  version is derived from the running dagnats build instead of a hand-bumped
  constant. Hosts with Go were unaffected (the install falls back to a local
  build); clean prebuilt hosts silently lost MCP DuckDB queries.
- **`worker_id` could be re-registered by a token that didn't own it
  (#650/#660, #667).** `POST /v1/workers/connect` wrote the registration
  under whatever `worker_id` the request body named, with no check tying
  the id to the caller's token — a second worker presenting a different
  token could overwrite the first's directory entry (hostname, task
  types, `token_id`). Not exploitable for task execution (poll/resolve
  were already token-scoped since #647), but it corrupted the `GET
  /v1/workers` directory. A connect from a different token now gets
  `409`; the admin bearer can still always take over. A follow-up
  (#667) found the ownership check mapped *any* KV revision conflict to
  that same `409`, including a same-owner race between a reconnect and
  the prior connection's own disconnect-time deregister, or its own
  heartbeat loop — those now retry against fresh state (bounded, 5
  attempts) before giving up, and persistent contention returns `503`
  with `Retry-After` instead of a wrongly-rejected `409`.
- **Admission-release recovery after finalize failures (#648/#661).** If
  the singleton-lock/concurrency-slot release step after a run's terminal
  snapshot save failed, the lock or slot leaked forever — three terminal
  paths (Failed, CompensateFailed, Compensated) had no debt recording at
  all, so the reconciler never even knew to look. `finalizeRun` now
  records the debt (`WorkflowRun.ReleasePending`) durably before
  publishing terminal events, and every terminal path routes through one
  shared `releaseAdmission` so the reconciler can recover it.
  `concurrency_runs` moved from a bare counter to a bounded member set
  keyed by run ID, so a replayed release can't double-decrement.
- **Runs could silently pick up a different workflow definition mid-flight
  (#637/#662).** `Orchestrator.loadRunAndDef` re-read the workflow def by
  name on every advance, so re-registering an existing name (e.g. a forge
  re-registering `ci/*.json` on every check-in) changed an in-flight run's
  steps, retries, concurrency, or timeouts on its very next advance. Runs
  now pin to the `def_hash` (#630) they started with via an immutable
  `name.v.<hash>` snapshot; a legacy run with no pinned hash keeps today's
  mutable-pointer behavior, and a missing version key for a pinned run
  self-heals when the mutable pointer's content hash still matches.
- **Filtered/paginated run scans could silently miss recent runs
  (#659/#663).** `GET /runs` and bulk cancel capped a lexicographically
  (NUID) ordered KV key scan *before* applying the filter, so once the
  run population exceeded the cap, a labeled or filtered query sampled an
  arbitrary subset — the exact runs a label filter exists to find were
  the likely casualties. A creation-ordered `runidx.<runID>` index now
  backs a single newest-first scan primitive shared by `GET /runs`, bulk
  cancel, and `ListRecent`; both responses report `truncated` when the
  scan hit its bound. A bounded repair sweep backfills the index for
  pre-existing runs.
- **The reconciler and quota scans could miss active runs beyond a capped
  window (#664/#668).** Recovery of runs owed an admission-release debt
  (see #648/#661 above) and the wedged-run sweep both depended on a
  bounded scan of the run population. A new `runactive.<runID>` marker —
  present iff the reconciler owns the run (non-terminal, or terminal with
  `ReleasePending`) — backs one `ListActive` listing per reconciler tick,
  independent of total run count; startup now fails loudly
  (`Orchestrator.Start()` returns an error) if the index can't converge
  rather than silently reconciling an incomplete view.
- **Every ordered JetStream consumer is now bounded via one helper; lint
  enforces it repo-wide (#675/#676).** nats.go v1.53.1's unbounded default
  `MaxResetAttempts` made an ordered consumer's `Next()` retry forever
  once its stream was deleted (first caught in `internal/api`'s build-log
  read/follow paths, #672). The bound now lives in one place,
  `internal/natsutil/ordered_consumer.go`, and seven more TELEMETRY/
  BUILD_LOGS ordered-consumer call sites (`internal/observe/spanread`,
  `cli/logs.go`, `cli/logs_search.go`, `cli/trace.go`, `cli/metrics.go`,
  `cli/clean.go`) route through it; CI now greps the whole tree to forbid
  a raw `jetstream.OrderedConsumerConfig{`/`OrderedConsumer(` call outside
  that file.

### Changed

- **`sidecar.InstallAll` now takes the dagnats build version**
  (`InstallAll(w io.Writer, dagnatsVersion string)`). It selects the matching
  `dagnats-mcp-duckdb` release asset; unversioned dev builds fall through to
  the existing build-from-source path. Breaking for direct importers of the
  `sidecar` package, which is not part of the published SDK reference.
- **OTel Collector bumped 0.102.0 → 0.159.0 (#620).** The pin was the
  file's birth value and had never been revisited, leaving it ~2 years
  behind upstream. Generated configs now use the `otlp_http` exporter name;
  0.159.0 still accepts the old `otlphttp` alias but warns per signal on
  every start.
- **CI hardening.** A `Docker` workflow builds the image and smoke-runs it
  on PRs touching the Dockerfile/Go sources (#657); a `race` job runs `go
  test -race` over the concurrency-heavy packages (`bridge`, `worker`,
  `internal/engine`, `internal/api`, `internal/workertoken`,
  `internal/natsutil`) that the fast `test` job doesn't cover (#669/#670);
  `.github/dependabot.yml` now watches all four Go modules, Actions, and
  the Dockerfile, grouped so the OTel/NATS release trains don't fragment
  into a dozen PRs a week, with a `binary-pins.yml` job covering the
  external `otelcol`/`dagnats-mcp-duckdb` pins Dependabot structurally
  can't see (#623), later corrected so OTel/NATS don't fall into the
  catch-all group (#655); and a round of flaky-test fixes (KV-purge
  cutoff determinism, a port-reservation TOCTOU, a log-follow test's
  wrong failure premise, a deleted-stream ordered-consumer hang) (#654,
  #665, #666, #672).
- **Dependency bumps.** OTel 1.44.0→1.46.0 / `otel/log`+`sdk/log`
  0.20.0→0.22.0 (with the resulting `attribute.KeyValue` migration in the
  NATS log exporter), `nats-server` 2.14.3→2.14.6, `nats.go`
  1.52.0→1.53.1, `orbit.go/pcgroups` 0.2.1→0.2.2 (#658);
  `mark3labs/mcp-go` 0.47.0→0.58.0 in `cmd/dagnats-mcp-duckdb` (#642); the
  Docker base images `golang:1.26-alpine`→`1.27-alpine` and
  `alpine:3.19`→`3.24` (#640, #641); `golang.org/x/net` and
  `google.golang.org/protobuf` (#656); GitHub Actions, grouped (#643).

## [0.0.13] - 2026-08-14

### Changed

- **OpenAPI 3.1 document types promoted to a public `openapi` package (#614).**
  `internal/openapi/types.go` moves to `openapi/` at module root (stdlib-only,
  zero dagnats-internal dependencies), matching the shape of `dag`,
  `dagnatsext`, and `dagnatstest`. Lets other modules import the document
  model without depending on this repo's `internal/` tree, which Go
  disallows. Synthesis logic (mapping workflows/triggers into the document
  model) stays in `internal/openapi`, now importing the public types. No
  behavior change — output is byte-for-byte identical.

### Documentation

- **Durable Endpoints made discoverable.** The HTTP-trigger + respond-step
  pairing (ADR-013 names it "Durable Endpoints") had solid dedicated docs
  but the name itself never appeared anywhere public-facing, the capability
  was missing from both the README's and docs site's top-level pitch, and
  onboarding pages had zero cross-links to it. Added the name to the
  existing README bullet, a docs-site Key Differentiators entry, a
  quickstart callout, and the page title.
- **Fixed duplicate page-title headings** on two docs-site pages (Service
  Discovery, Runtime-Generated Workflows) where a redundant body H1
  duplicated the theme-rendered frontmatter title.

## [0.0.12] - 2026-08-06

Bug-fix and cleanup release. Two real correctness bugs land: a silently-dropped
workflow trigger declaration (#607) and a shared JetStream drain loop that
could truncate DLQ listings and run event-history under load (#609). Rounds
out with the architecture-review nit sweep (#599) and removal of the unused
`actor/` package (#598).

### Fixed

- **`workflow register` no longer silently drops an unrecognized trigger
  declaration (#607).** A workflow-definition file that declared its trigger
  as a singular `trigger` object instead of the expected `triggers` array (or
  carried any other unrecognized top-level field) registered successfully
  with the trigger data thrown away — no error, no warning, no failed run.
  `workflow register` and `workflow validate` now reject unrecognized
  top-level fields with a clear error before any write. Also adds
  `workflow delete <name>` (`--force`/`--json`), which refuses to delete a
  workflow that still has triggers referencing it unless `--force` is passed.
- **`dlq list` and run event-history reads could silently truncate under load
  (#609).** The shared `fetchMessages` drain loop broke on the first
  inter-message delivery gap over 5ms, even with more messages still in the
  stream and the overall 10s deadline nowhere near — so `dlq list --all`
  could under-report entries while printing a misleading "truncated, use
  --all" footer. The drain now checks the consumer's pending count before
  giving up, so a brief delivery gap under load no longer ends the read
  early. Also fixes the DLQ confirm-modal's Retry/Discard buttons going
  silently dead on rows inserted by the live SSE feed after initial page
  load, by delegating the click listener instead of binding it once at load
  time.
- **Step-timeout retry test sized to the real worker dispatch floor (#591).**
  The worker dispatches task handlers serially per consumer, so a wedged
  attempt blocks the next redelivery for the full self-timeout window; the
  test's wait budget now reflects the real ~5.3s floor instead of an
  optimistic ~1s guess.

### Changed

- **Architecture-review nit sweep (#599).** `internal/engine/orchestrator.go`
  split into five per-event-kind handler files; the CLI's connect path
  replaced a panic/recover control-flow pattern with an error-returning
  `api.ResourcesReady` probe; the three root-level e2e test files migrated
  into `e2e/features/` under the shared topology-matrix harness; the
  one-consumer `testutil` package folded into its sole consumer; a stale
  precondition doc in `internal/natsutil/publish.go` corrected and backed by
  an assertion.
- **`dagnatstest` → `internal/api` coupling documented, not split (#601).**
  Investigated during the `testutil` fold above; no live consumer is
  currently blocked by the coupling, so ADR-023 records the decision to
  leave it as-is rather than take on speculative package-splitting work.

### Removed

- **Unused `actor/` package deleted (#598).** Zero non-test importers since
  its only consumer was removed in ADR-009; its own package doc had gone
  false (claimed NATS integration lived in `engine/`, which never imported
  it). Stale references swept from `docs/architecture/` and the docs site,
  including a full public page describing the runtime as still in use.
  ADR-009 gets a dated addendum recording the reversal.

## [0.0.11] - 2026-07-27

Refactor and reliability release. Splits three console god-files and the
`api.Service` control-plane facade into small, domain-focused units (#564–#569,
#576), and root-causes eight intermittent CI failures instead of papering over
them with longer timeouts — several turned out to be real bugs, not just
timing luck: a NATS KV `Keys()` scan racing an async multi-write snapshot
pipeline (#570), and a run marked `Failed` before its dead-letter entry
finishes publishing (#577).

### Changed

- **Console `DataSource` mega-interface split into ten domain ports (#564,
  #576).** The 41-method interface backing every console page test is now ten
  small ports (`RunStore`, `TriggerStore`, `DLQStore`, ...); the page-test fake
  is decomposed to match, so a test that only cares about one page no longer
  drags in the whole surface.
- **Console `pages.go` (2,150 LOC) split by domain (#567).** Workflow, run,
  DLQ-fragment, and search page builders move to their own files behind a
  single shared `pageWindow` pagination helper — the one piece of chrome
  genuinely common across list pages; everything else stayed with its domain
  builder rather than forcing a one-size-fits-all abstraction.
- **Console route registration replaced with a typed registry (#568).** ~50
  near-identical `mux.HandleFunc` closures become an ordered `[]consoleRoute`
  table, validated whole (empty pattern / nil handler / duplicate) before a
  single route installs — an invalid table can no longer partially mutate the
  mux.
- **`api.Service` (2,370 LOC) split by domain; run-list API collapsed (#566).**
  Four overlapping run-list entrypoints become two purpose-built ones
  (`ScanRuns` for bounded reads, `ListRuns` for an honest total/truncated
  envelope) instead of one API forcing every caller through the more
  expensive path.
- **`RunMutator` port extracted from the orchestrator (#565).** Approval and
  sleep step-kind handlers no longer take four separate load/save/complete/
  enqueue callbacks per call — they hold one cohesive port, shrinking
  `orchestrator.go` and the coupling surface for the next step kind.
- **`internal/engine` test suite (4,508 LOC in one file) reorganized by event
  kind (#569)**, alongside the handlers #565 extracted — lifecycle, spawn,
  map, sleep/wait, retry/recovery, and step-execution tests now live in
  focused files with zero scenario loss (verified by an exhaustive
  before/after function-body diff).
- **No-recursion rule now enforced repo-wide (#554, #560)** — `parseField`'s
  last recursive call site is gone, and a lint step keeps it that way.

### Added

- **Dispatch-time-resolved sleep flavors (#551)** — cron- and
  deadline-from-run-input-based sleeps are now resolved when the step
  dispatches, not when the workflow is defined.

### Fixed

- **Eight intermittent CI failures root-caused and fixed, not just
  retried-to-green (#562, #570, #573, #574, #577, #580, #584, #588).** Each
  was reproduced under induced contention with a real before/after failure
  count rather than assumed from a plausible-sounding timing story. Two
  turned out to be genuine bugs: `TestListRunsTruncation`'s undercount was a
  real race in the vendored `nats.go` KV `Keys()` ordered-consumer scan
  against an in-flight multi-write snapshot pipeline (#570); the bridge
  DLQ-population flake was a real ordering gap where a run's status flips to
  `Failed` and saves before its dead-letter entry finishes publishing (#577).
  The rest were timing budgets too tight for realistic `-p 4` parallel-test
  contention — a headless-Chrome print-smoke subprocess retry (#573), a
  topology-aware HTTP-respond timeout for the slower supercluster topology
  (#574), a widened worker-staleness test window (#584), and a relaxed
  cluster-setup sanity ceiling (#588) — each with a measured, justified
  margin rather than a round-number bump.
- **Discarded enrichment errors now logged, not silenced (#580)** — the
  workflows-list and runs-list page builders swallowed errors from
  secondary/enrichment DataSource calls; a real backend outage there is now
  observable instead of indistinguishable from "no data."
- **Trace context no longer silently dropped at four propagation gaps
  (#527, #530, #534, #538)** — inbound W3C trace context is now extracted in
  NATS micro handlers, trigger-service entrypoints, and carried through the
  bridge poll response into `protocol.TaskPayload`, so traces stay connected
  across those hops instead of restarting.
- **Bridge dispatch reliability** — `AckMap` entries now have a bounded
  lifetime so an abandoned dispatch cannot leak forever (#545, with its size
  gauge actually emitting per #549); bridge poll reuses durable task
  consumers instead of creating one per poll (#532), and span volume is now
  proportional to work done, not requests made (#531).
- **POSIX cron day-of-month/day-of-week semantics (#552)** — a schedule with
  both fields set now ORs them per POSIX cron, instead of requiring both to
  match.
- **Five previously-untested API handlers now covered, three made observable
  (#529).**
- **E2E test hardening**: worker-readiness waits are adaptive instead of
  fixed-short (#555); several fixed-duration sleeps replaced with real
  precondition polling (#558); a wall-clock race removed from the scheduler
  backfill test; JetStream store budgets derived from the server instead of
  guessed; trace-provenance assertions strengthened from absence-checking to
  presence-checking.

### Security

- **`google.golang.org/grpc`** v1.81.1 → v1.82.1, clearing a high-severity xDS
  RBAC / HTTP/2 advisory (transitive, via OpenTelemetry's OTLP exporters —
  dagnats does not use gRPC directly).

### Docs

- Documented that grouped task types are not pollable over the bridge (#546).

## [0.0.10] - 2026-07-17

Reliability release addressing **#523** — a bloated `workflow_runs` KV bucket
could stall *all* workflow execution, and there was no bounded way to recover
it. The orchestrator scans pending run snapshots on every completion, and the
reconciler scans the whole run set periodically; both used an all-or-nothing
batch read, so one slow key past the deadline turned into an engine-wide stall.

### Fixed

- **Reconciler survives a bloated run bucket (#523).** Pending-run scans are now
  bounded (cap 1000 keys) and *best-effort*: a slow or missing key is skipped,
  not fatal. A skipped or capped scan emits a `WARN` plus a
  `workflow.runs.scan_degraded` metric so the degradation is observable, and the
  engine keeps making progress while the bucket is oversized.

### Added

- **`clean --keep=N` / `--before-seq=N` bulk prune (#523).** An O(1) server-side
  stream-sequence purge to recover a bucket that has already cliffed past what
  the per-key `--older-than` loop can drain within the deadline. Both are
  force-gated (unless `--dry-run`), mutually exclusive with each other and with
  `--older-than`, and **skip work-queue streams** (`TASK_QUEUES`) so queued
  un-acked tasks are never silently discarded. `--dry-run` never prompts or
  mutates and reads backing-stream sizes so the estimate reflects tombstones the
  KV key count hides. These purge by *sequence, not age* — a blunt recovery
  tool; `--older-than` remains the safe default.

## [0.0.9] - 2026-07-17

Bug-fix release addressing **#521** — unbounded run storage and two
`dagnats clean` failures.

### Fixed

- **Bounded run retention by default (#521).** Terminal run snapshots in the
  `workflow_runs` KV now age out after 30 days by default (matching
  `WORKFLOW_HISTORY`), so run storage stays bounded without operator
  intervention. The pruner is terminal-only — in-flight runs are never touched,
  preserving snapshot-first recovery. Configure with `DAGNATS_RUNS_MAX_AGE`: a
  window (`720h`, `30d`, `2w`) or `0`/`off` to disable.
- **`clean` no longer errors on work-queue streams (#521).** `clean
  --older-than` now skips `TASK_QUEUES` (a work queue holds live pending work,
  not history) instead of failing with a pull-consumer ack-policy error
  (`10084`). A full `clean` still drains it.
- **`clean` no longer times out deleting run keys (#521).** Full bucket clears
  use an O(1) backing-stream purge; the `--older-than` path bounds each delete
  with its own timeout and stays cancellable.

### Changed

- **Run pruner is now on by default (30d).** An upgraded server begins deleting
  terminal runs older than 30 days, so they disappear from the console
  (consistent with the 30d history window). Set `DAGNATS_RUNS_MAX_AGE=off` to
  keep the previous keep-forever behavior.

## [0.0.8] - 2026-07-16

Hardens run-dispatch reliability, eliminates the CI test-harness flakes, and
brings the full dependency stack current — clearing all outstanding security
advisories.

### Fixed

- **WORKFLOW_HISTORY poison-loop backstop (#508):** the orchestrator history
  consumer now has a bounded `MaxDeliver` with a redelivery schedule and
  dead-letters exhausted handler-error events to `DEAD_LETTERS` instead of
  NAK-looping forever; a dead-letter-publish failure NAKs rather than silently
  dropping the event.
- **Fan-out span differentiation (#513):** map-instance (fan-out) enqueue spans
  now carry the real `workflow_name` without regranting control-plane
  capabilities — the capability-grant key and telemetry label are decoupled (the
  #380 deny-by-default invariant is preserved and regression-tested).
- **Cleaner shutdown:** `Orchestrator` / `Correlator` / `SleepTimer` `Stop()`
  now wait (bounded) for their JetStream consume loop to fully quiesce instead
  of only signaling it, closing a lifecycle race where a goroutine could still
  be mid-fetch after `Stop()` returned.
- **CI flakes fixed at the root** — both OS ephemeral-port / temp-dir races in
  shared test helpers: `StartTestCluster` cross-call port collision (#516) and
  `StartTestServer` `StoreDir` cleanup race (#520). No more rerun-to-green.

### Security

- **All 14 Dependabot alerts cleared** (7 critical / 2 high / 5 moderate):
  `golang.org/x/crypto` v0.49.0 → v0.54.0 (critical SSH auth-bypass /
  key-constraint / FIDO-U2F) and `golang.org/x/net` v0.52.0 → v0.57.0.

### Changed

- **OpenTelemetry** → v1.44.0 / v0.20.0 (coordinated across the tree).
- **NATS** server → v2.14.3, client (`nats.go`) → v1.52.0,
  `orbit.go/jetstreamext` → v0.3.1 — changelog-reviewed to confirm the JetStream
  consumer/advisory semantics this release depends on are unchanged.

## [0.0.7] - 2026-07-15

Closes the observability gaps blocking downstream freshness/trace tooling and
hardens run-dispatch durability. All six changes since v0.0.6.

### Added

- **Per-trigger `last_fired` / `next_fire` metrics (#501)** — the scheduler
  exposes `trigger_last_fired_seconds` and `trigger_next_fire_seconds` gauges so
  a consumer can detect a *missed* cron fire without hardcoding each schedule.
- **Differentiated task/enqueue span names + `workflow_name` (#503)** —
  enqueue/execute spans are named per task/type and carry the workflow name, so
  traces are no longer collapsed into one undifferentiated wall.
- **Trigger-fire spans + trace propagation (#504)** — cron fires start a
  `trigger.fire` span and propagate context to the engine, giving trigger
  identity and a proper trigger→engine parent hop.
- **Guaranteed W3C propagator in workers (#505)** — `NewWorker` installs a W3C
  TraceContext+Baggage propagator when none is configured, so incoming
  `traceparent` headers are no longer silently dropped.

### Fixed

- **Durable visibility for singleton-skipped runs (#502)** — a run skipped by
  singleton admission now persists a terminal record instead of silently
  vanishing (acked but never dispatched/findable); a snapshot-write failure NAKs
  for redelivery rather than dropping the run.
- **Deterministic supercluster test ports (#509)** — fixed a port-collision
  flake that surfaced as ~500s CI readiness timeouts.

## [0.0.6] - 2026-07-10

### Fixed

- **`worker.Stop()` now drains in-flight handlers before returning (#498 /
  #499)** — fixes the shutdown `sql: database is closed` race.

## [0.0.5] - 2026-06-25

A feature release — 14 PRs since `v0.0.4`. Two headlines: **runtime-generated
workflows** (agent runtimes, ADR-021 Phase A) — gated task handlers can author
and launch brand-new DAGs at runtime, bounded on every axis — and **nats-micro
service discovery** for the internal control plane. Also lands honest run
listing, opt-in run retention, and the `dagnats-ci` add-on scaffold.

### Added

Agent runtimes (ADR-021 Phase A) — runtime-generated workflows:

- **`ControlPlane` handle** on gated task handlers: `RegisterWorkflow` authors an ephemeral workflow def at runtime and `StartRun` launches a child run of it, so an LLM planner can compose known tools into a *novel* DAG and execute it durably — crash-recoverable like any other run. Opt-in and deny-by-default (#459, #376).
- **Generation-tree lineage**: every spawned run is namespaced under its root run (`agent.<root>.*`); a bounded, idempotent reaper GCs ephemeral defs after the root run is terminal, plus promotion wiring for durable (`promoted.*`) defs (#460, #377).
- **Per-runtime safety bounds**: per-tree quotas (max active runs, max ephemeral defs), a generation-depth cap (≤ the engine nesting ceiling), a register rate limit, and a `Budget()` method so a handler can self-throttle *before* hitting a quota (#461, #378).
- **Capability-grant security model**: deny-by-default `policy.control_plane.grant` / `promote` lists, a per-dispatch **nonce** binding each request to the run the worker is actually executing, promotion authorization, and an audit record for every grant decision and control-plane mutation (#462, #380).
- **Console → Agent runtimes** view: per-tree generation lineage, per-runtime budget consumption, and run **provenance** (a "runtime" tag on agent-spawned runs), live over SSE (#463, #379).

Service discovery (nats-micro):

- The internal control plane now runs as discoverable **`micro.Service`s** — `dagnats-api` (#456) and `dagnats-trigger` (#458) — answering the reserved `$SRV.PING` / `$SRV.INFO` / `$SRV.STATS` protocol, with a live **Console → Services** page sourced from `$SRV` discovery rather than a static registry (#457). All under the `#449` umbrella.

Other:

- **Run retention**: opt-in, drop-only retention sweeper (`runs_max_age` / `DAGNATS_RUNS_MAX_AGE`, disabled by default) (#455, #453).
- **`dagnats-ci` add-on module**: scaffold with a `ci.yml` compiler + webhook core, as its own nested Go module (#451).
- **Public `dagnatsext` worker seam** + per-step task metadata (#450).
- **Docs**: runtime-generated-workflows + service-discovery guides, README + configuration refresh, and ADR-021 Phase A implementation status (#464); auto-generated SDK reference docs refreshed with a CI drift guard that pins gomarkdoc's source-link ref so output is reproducible on CI's detached-HEAD checkout (#465).

### Changed

- The control-plane request/reply **subjects are unchanged** by the nats-micro adoption — every existing caller keeps working. The `micro.Service` wrapper only adds the discovery + per-endpoint statistics surface, and fan-out is preserved (no queue group, matching the prior plain-subscribe behavior) (#456, #457, #458).

### Fixed

- **CLI `run list` is now honest**: a globally time-ordered listing across all workflows, plus a run count and a `--since` filter, instead of the previous per-workflow truncated view (#454, #452).

## [0.0.4] - 2026-06-18

A bug-fix + console-completion release — 9 PRs since `v0.0.3`. Resolves two
firestorm production bugs (unbounded JetStream/heap growth; cron fire-record
"zombie" duration) and completes the engine-gated console telemetry/trace
surfaces (p50, Trace-ID/duration, waterfall).

### Added

Console / web UI:

- **Trace detail**: a span **waterfall** (per-span offset/width geometry from the existing span data) + a clickable **span-detail KV panel** (span/parent/workflow/step/task/run id + status), with a disclosure caret (#444).
- **Traces list**: real **Trace ID** + **Duration** columns (#443).
- **Triggers list**: inline **enable/disable toggle** (reusing the existing toggle route, read-only-gated) (#439).
- **Audit page**: **outcome filter** (success/denied/failed), a **denied-attempts callout**, and an **identity/auth-mode banner** (#439).

### Changed

- **Run completion is now honest across every terminal path.** `dag.WorkflowRun` gains `TraceParent` + `CompletedAt`; all 8 terminal transitions (complete/fail/cancel/loop-fail/map-fail/schema-fail/failed-start/compensated) funnel through a single `markTerminal` helper that stamps `CompletedAt` — no path can forget it (#443).
- **JetStream streams are now bounded.** History streams get `max_age` (WORKFLOW_HISTORY 30d, EVENTS 14d, DEAD_LETTERS 30d) + proportional `max_bytes` ceilings (a fraction of `JetStreamMaxStore`, so they scale from a 2 GiB host to 10 GiB+); `TASK_QUEUES`/`SLEEP_TIMERS` keep **no `max_age`** (they hold live/pending work) (#441, #446, #447).
- Embedded NATS server now sets `JetStreamMaxMemory` + a soft `GOMEMLIMIT` (`debug.SetMemoryLimit`) via a new `MaxMemoryBytes` config / `DAGNATS_MAX_MEMORY_BYTES` env, so the Go heap returns to the OS (#446).
- The snapshot-p50 tile is **neutral-colored** (snapshot-save isn't a run-latency SLO) (#442).
- The full test suite runs at bounded package parallelism (`-p 4`) so it's deterministic on high-core machines (#437).

### Fixed

- **JetStream store + heap grew unbounded under workqueue churn** (firestorm #441): the real cause was *unbounded history streams* (not the workqueue, which deletes on ack). Now bounded by `max_age` + proportional `max_bytes` + a memory limit. Verified recovery-safe (the orchestrator is snapshot-first; the `workflow_runs` KV is authoritative).
- **Cron fire-record duration ticked up forever** after a run completed / engine restart (firestorm #440): `enrichFireStatus` computed `time.Since(CreatedAt)`; now frozen at `CompletedAt − CreatedAt`.
- **Snapshot p50 never reached the console**: the metrics aggregator typed OTel `Temporality` as `int`, but the SDK serializes it as a string — so every histogram/sum record failed to decode and was silently dropped. Fixed the decoder (#442).

### Documentation

- Preserved the console design mockup (the deleted MagicPath source) in-repo under `docs/design/mockup/` so it's version-controlled and backed up (#438).

## [0.0.3] - 2026-06-16

A console (web UI) release — ~60 commits since `v0.0.2`, bringing the operator
console into fidelity with the design mockup and fixing two observability
correctness/resilience bugs.

### Added

Console / web UI:

- New pages: **Server** (NATS identity + JetStream account, live Varz/Jsz), **Connections** (`/connz`), **Consumers** (work-queue health), **Concurrency** (admission-control: slot pools, singleton locks, rate-limit + debounce gates), **Services** roster, **Traces** (cross-run + deep-linkable) with a per-run Trace tab, read-only **Worker detail** and **Function detail**, **KV** catalog + value inspector, **Config** self-portrait (access posture + engine invariants).
- **Dashboard** reshaped to the mockup: two-row layout (status tiles + telemetry sparkcards for throughput / p50 / error rate), recent-failures table, datatype sparklines, nav badges.
- **Workflow detail**: numbered step-DAG (type pills + `depends_on` edges) and a **Run workflow** action.
- **Logs**: dedicated Trace-ID column linking to traces.
- **Triggers** Add / Edit / Delete and per-run **Signal / Cancel** actions (existing API, read-only gated).
- Design foundation: teal / IoskeleyMono / borderless cards, Lucide SVG nav icons + collapse-to-icon rail, `dagnats://` wordmark, muted status palette matched to the mockup, IA grouped into Inventory / Activity / System.
- `dagnats demo seed --keep-alive`: rich demo mode for populating a console for review (#425).

### Changed

- **Metric export is now cumulative temporality** (was delta) with a 10s NATS reader interval, so the console's rate/sparkline/chart math (which assumes monotonic counters) renders correctly (#434).
- Real ldflags build-stamp surfaced in the console footer (#420).
- Nav IA reorganised; the Leases page and the Ops hub were removed/consolidated.

### Fixed

- **Metrics pump now uses an ephemeral consumer** — a durable consumer with an immutable start-time previously failed (`nats 10012`) on every engine restart and silently disabled all console metrics; restarts now keep the observability surface alive, and the legacy durable is cleaned up on upgrade (#435).
- Active-runs tile no longer shows a negative count (sourced from real run state) (#427); dashboard throughput no longer renders `-0.0`; the broken/garbled metrics throughput chart now draws correctly (#426, #429).
- Readable detail-page values (no longer near-black on dark) (#428); transparent table headers + consistent hover/focus/active states (#405); run-detail underline tabs (#423, #424).
- `observe`: `buildResource` honors `OTEL_RESOURCE_ATTRIBUTES` / `OTEL_SERVICE_NAME` via `resource.New` + `WithFromEnv()` (`cfg.Resource` still wins); `LogExporter` derives `service.name` from the record resource (#367, #368).
- `dag`: `sub_workflow` treated as a no-task step type (#371); `serve` fail-fast flag + loopback-preserving port fallback (#372); retired `/ui` stub redirects to `/console/` (301) (#366); HTTP-bridge workers propagate `AttemptNumber` so retry backoff + dead-lettering work (#384).

### Honesty discipline

- Mockup features lacking backing data were **omitted, not fabricated** (e.g. per-entity stat tiles, Services instance/version columns, snapshot-p50). Traces-list trace-id/service/duration and trace-detail waterfall geometry remain engine-gated and intentionally unbuilt.

## [0.0.2] - 2026-06-03

A large console (web UI) and engine release — ~77 commits since `v0.0.1`.

### Added

Console / web UI:

- Logs page with trace-ID search; Task Types registry page; Configuration self-portrait page.
- Workers / KV / Streams promoted to top-level navigation; collapse-to-icons rail with footer strip.
- Fire-now trigger button backed by a `FireTrigger` HTTP endpoint; inline Run button on workflow rows.
- Page-header partials with tile counters, empty-state partials, drill chevrons, build/identity footers.
- IBM Plex Sans/Mono typography (bundled OFL-1.1).
- NATS WebSocket listener for browser clients (live UI updates).

Engine / triggers / workers:

- `dagnats.yaml` configuration file with hot-reload.
- Trigger-type system: external trigger variant + schema validation, trigger-type versioning, `trigger_types` KV bucket + `TriggerTypeDef`, `RegisterTriggerType` / `WatchTriggers` SDK, `trigger-type list/describe` CLI, and `ExternalRegistrar`.
- Services registry: `services` KV + `RegisterService` SDK.
- `WorkerRegistration` enriched with identity + heartbeat fields.
- filewatcher external-trigger example.

### Changed

- Observability: raw publishes routed through `TracingPublisher`; handler-extractor wrapper.
- `TriggerRegistrar` interface + table-driven trigger dispatch.

### Fixed

- Numerous console fixes: dashboard tile rendering on empty metrics, run-detail SSE patches, connection-pill state, CSP fixture gating, print CSS, and empty-bucket workflow listing.

## [0.0.1] - 2026-05-03

Initial public release of `dagnats`, a workflow orchestration engine combining
DAG-based task graphs with NATS-backed coordination. Single-binary deployable
with embedded NATS server and webhook/cron triggers. Supersedes internal
pre-release tags `v0.1.0` and `v0.1.1`, which were never published.

### Added

- DAG-based workflow definition and validation engine.
- Embedded NATS JetStream server (no external broker required).
- Worker, server, CLI, sidecar, bridge, and SDK packages.
- Webhook and cron trigger sources with `backfill` semantics.
- Lazy orchestrator subsystem initialization (ADR-004).
- Apache-2.0 LICENSE.
- Auto-sync of landing-page version from the latest git tag.
- Step lifecycle events (`step.queued`, `step.started`, `step.completed`, `step.failed`) with deterministic ordering and an `AttemptNumber` semantic ([#137](https://github.com/danmestas/dagnats/issues/137)).
- Engine-side retry-backoff scheduler honouring per-policy delays ([#147](https://github.com/danmestas/dagnats/issues/147)).
- Step-level timeout watchdog with staleness checks ([#140](https://github.com/danmestas/dagnats/issues/140)).
- Per-task `WithAckWait` handler-registration option ([#144](https://github.com/danmestas/dagnats/issues/144)).
- Cross-process consumer name collision detection ([#145](https://github.com/danmestas/dagnats/issues/145), ADR-010).
- Worker durable consumers on `TASK_QUEUES` with orphan ephemeral migration ([#136](https://github.com/danmestas/dagnats/issues/136), ADR-006).
- Multi-stage `Dockerfile` and cross-platform release binaries via `make release`.

### Changed

- Engine is now the sole retry authority. Workers report failures via `step.failed`; the engine schedules backoff and dispatches the next attempt (ADR-011).
- Generic worker handler errors now publish `step.failed` (retriable) and Ack the message instead of NAKing with a hardcoded 5s delay ([#141](https://github.com/danmestas/dagnats/issues/141)).
- Removed the experimental `ActorOrchestrator` / `WorkflowActor` (ADR-009) — single orchestrator path going forward.

### Fixed

- Step state correctly transitions to `Running` when a worker pulls the task ([#137](https://github.com/danmestas/dagnats/issues/137)).
- Retriable `step.failed` now schedules a retry instead of leaving the run wedged at `attempts: 1/N` ([#147](https://github.com/danmestas/dagnats/issues/147)).
- Step `Timeout` now fires a watchdog instead of being a silent no-op ([#140](https://github.com/danmestas/dagnats/issues/140)).
- Fast-failing worker handlers no longer leave runs in `running, attempts: 0/N` ([#141](https://github.com/danmestas/dagnats/issues/141)).
- `Worker.Stop()` logs directory deregistration failures instead of swallowing them.
- Cron triggers with `backfill: false` no longer fire on registration ([#139](https://github.com/danmestas/dagnats/issues/139)).
- Workflow run input correctly forwards to root steps.

### Documentation

- ADR-006: durable task-queue consumers.
- ADR-009: remove experimental actor orchestrator.
- ADR-010: cross-process consumer name collision detection.
- ADR-011: engine as sole retry authority.

### Tests

- Regression guards for multi-task-type concurrency ([#138](https://github.com/danmestas/dagnats/issues/138)), `dagnats run start --json` ([#143](https://github.com/danmestas/dagnats/issues/143)), and `ListRunEvents` step-event inclusion ([#142](https://github.com/danmestas/dagnats/issues/142)).
- End-to-end test suites for retry-backoff, fail-fast, step-timeout, and `publishStarted` NAK-recovery paths.
