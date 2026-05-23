# ADR-017: Services Namespace

**Status:** Accepted
**Date:** 2026-05-22
**Context:** Parent #273 Phase 3.1 (issue #321); foundation for #274 R11 (Task Types registry page)

## Context

Task types are dispatched by exact-match name on the `task.{type}.>` subject hierarchy. As the workflow corpus grows, operators need a way to *group* related task types under a logical service — e.g. `billing::charge`, `billing::refund`, `billing::receipt` — for visibility, ownership, and (eventually) UI navigation in the console.

Two paths were considered for delivering this:

1. **Enforce grouping in the engine.** Make `service::task` a first-class addressing scheme, gate task dispatch on a registered service, reject unknown prefixes.
2. **Treat grouping as pure metadata.** Workers publish a service description to a KV bucket; the engine never reads it. `service::task` remains a naming convention, not a routing rule.

The audit settled on path (2) for this PR. Grouping is a presentation concern (the planned Task Types registry page in #274 R11) and an observability surface (operators want to know "which services exist on this cluster?"). Pulling the engine into a grouping contract would (a) couple dispatch to a metadata bucket the engine doesn't otherwise need, (b) force migrations whenever the metadata schema evolved, and (c) gain nothing the engine actually executes against — task-type names are already the unique routing key.

This is the first piece of #273 Phase 3 (the services / task-types observability arc). It exists to unblock #274 R11 without making any change the engine has to compensate for later.

## Decision

### 1. Separate `services` KV bucket

A new KV bucket is provisioned by `natsutil.SetupKVBuckets`:

```go
{
    Bucket:   "services",
    History:  1,
    Replicas: replicas,
}
```

No TTL, no MaxAge, no MaxBytes. Service definitions are stable descriptions, not liveness signals — they must survive worker restarts, NATS reboots, and operational downtime without re-registration pressure.

`History: 1` because readers only ever need the latest payload. Bumping history would inflate storage with no consumer.

### 2. `ServiceDef` minimal struct

```go
type ServiceDef struct {
    Name         string    `json:"name"`
    Description  string    `json:"description"`
    RegisteredAt time.Time `json:"registered_at"`
}
```

The audit explicitly deferred a `ParentService` grouping field. Zero consumers exist in this PR's scope; #274 R11 may add it later (additive change, last-write-wins handles the migration). Shipping unused fields would invite premature schema commitments.

### 3. `Worker.RegisterService` SDK method — last-write-wins

```go
func (w *Worker) RegisterService(def ServiceDef) error
```

Internally calls `kv.Put` (not `kv.Create`). Re-registering the same `Name` with different metadata silently replaces the prior entry without returning an error. This is the documented contract — callers must be able to call `RegisterService` on every worker boot without conflict-handling boilerplate.

Concrete cases covered by last-write-wins:

- Worker restarts after a deploy → same `Name`, possibly updated `Description` → second call wins, no operator intervention.
- Two workers on the same logical service register concurrently → race resolves to whichever Put lands last; the metadata is descriptive, not authoritative, so the race is benign.
- A service is renamed → the old `Name` lingers until manually purged (see Consequences §3 below) but does not produce conflict errors at the new `Name`.

### 4. Deliberate separation from `worker/directory.go`

The `workers` bucket (`worker/directory.go`, #289) is the worker liveness directory: 60 s TTL, heartbeat loop, `MaxWorkerStaleness` read-time filter. The `services` bucket is descriptive: no TTL, no heartbeat, no staleness filter.

These two surfaces are *not* unified, even though both store JSON KV entries describing "things running on the cluster". The lifecycle difference is the whole point:

- A worker that crashes should drop out of the workers directory within seconds. Operators need that signal.
- A service whose worker is temporarily down must *not* disappear from the services list. Operators still want to see that the service exists and re-deploy.

Sharing machinery would conflate the two — see Alternatives §1 below.

## Consequences

1. **Pure metadata, no invocation gating.** Task types prefixed `service::task` dispatch exactly like any other task type. The engine never reads the `services` bucket. A typo in a service prefix produces no enforcement error; the dispatch path is unchanged.

2. **CLI surface is read-only this PR.** `dagnats service list` reads the bucket and prints a table. No `dagnats service register` — registration is an SDK call from worker code, deliberately, because the metadata only matters when paired with the worker that owns the task types. A CLI-side register would invite divergence between the bucket and reality.

3. **Stale `Name` entries are an operator problem.** Without TTL, a service renamed `billing` → `payments` will leave a `billing` row in the bucket forever. This PR ships no purge mechanism — once R11 adds the Task Types page, a delete affordance follows. For v1 the explicit answer is: operators delete the key directly via `nats kv del services <name>` when they retire a service.

4. **Future `ParentService` is additive.** When #274 R11 needs grouping, adding a `ParentService string` field is backwards-compatible: old entries deserialize with the zero value; readers tolerate both shapes; the last-write-wins idempotency means re-registration migrates entries naturally.

## Alternatives Considered

### 1. Re-use the `workers` bucket / `Directory` machinery

Tempting because the struct shapes look similar (Name, Description-ish field, a timestamp). Rejected.

The lifecycles are incompatible. `Directory.Register` stamps `LastSeen` on every call and the bucket's 60 s TTL purges entries that miss a heartbeat. A service definition that obeyed that contract would vanish whenever its worker was down — exactly the operator-visibility regression the services surface is meant to prevent.

Threading a "no-TTL mode" through `Directory` would push complexity sideways: every `Directory` method would need a "this is a service, not a worker" branch, and the read-time `MaxWorkerStaleness` filter would have to be conditional. Two thin slices with disjoint contracts cost less than one fat slice with a `kind` discriminator.

This is also #289's lesson, in reverse: that ADR consolidated worker identity and heartbeat into a single bucket because they shared a lifecycle. Services do not.

### 2. Gate task invocation on service registration

The engine could reject `task.billing::charge.>` publishes when `billing` is not in the `services` bucket. Rejected.

- Adds engine read-load on a metadata bucket it otherwise doesn't touch (one KV lookup per dispatch, or a watch with cache-coherence headaches).
- Couples dispatch correctness to a description bucket, so a forgotten `RegisterService` call surfaces as "tasks mysteriously fail" instead of "service is missing from the list".
- Buys nothing the engine actually executes against — the task type is already the unique routing key, and the convention of `service::task` is a UI affordance, not a routing rule.

The audit was explicit: services are pure metadata. If a future requirement genuinely needs invocation gating, file a follow-up with the concrete user story — but the default should remain non-gating.

### 3. Engine-side service registry instead of worker SDK

`api.Service.RegisterService(...)` instead of `worker.Worker.RegisterService(...)`. Rejected for two reasons:

- The natural owner of "what task types belong to which service" is the worker process that registers those handlers. Pulling registration into the control-plane API would let the bucket and the worker drift.
- Forces every operator who registers a service to also have an active control-plane connection. Workers already have a `*nats.Conn`; using it directly is the shortest path.

A future control-plane *read* surface (e.g. an HTTP endpoint that lists services for the console) is fine — that's a presentation concern. Registration stays where the metadata is generated.

## Test Plan

Acceptance criteria from #321:

- `TestServicesBucketExistsAtBoot` — bucket exists with TTL=0, History=1.
- `TestServiceDef_Roundtrip` — Put then Get preserves Name, Description; RegisteredAt is stamped in [before, after].
- `TestRegisterService_LastWriteWins` — register, re-register with different Description, assert one entry remains with the second Description.
- `TestRegisterService_EmptyNamePanics` — guard the programmer-error invariant.
- `TestListServices_EmptyBucket` — empty bucket returns a non-nil empty slice, not an error.
- `TestRunServiceListCmd_EndToEnd` — `dagnats service list` outputs the registered service after a worker SDK registration.
- `TestRunServiceListCmd_EmptyBucket` — empty-bucket CLI prints "No services registered."
- `TestRunServiceListCmd_JSON` — `--json` output uses snake_case field names.

Local CI gate: `gofmt -l .` empty, `go vet ./...` clean, `staticcheck ./...` clean, full `go test ./...` green.
