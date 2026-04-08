# ADR-004: Lazy Orchestrator Subsystems

**Status:** Implemented  
**Date:** 2026-04-08  
**PR:** #118  

## Context

Hipp review flagged the orchestrator creating 15 subsystems at boot,
including two background JetStream consumers (SleepTimer, Correlator)
that run forever even if no workflow uses sleep or wait-for-event.
The constructor also had 11 individual metric instrument creation calls
obscuring the initialization logic.

## Decisions

### 1. Bundle Metrics into Structs

`orchMetrics` (6 instruments) and `pubMetrics` (3 instruments) replace
individual fields on Orchestrator and TaskPublisher. Constructor drops
from ~95 lines to ~60.

### 2. Self-Starting SleepTimer and Correlator

Both gain internal `sync.Once`. Background consumers start lazily on
first `Schedule()` / `AddWaiter()` call instead of at
`Orchestrator.Start()`. `Start()` becomes idempotent — existing callers
that call it explicitly still work. `Stop()` already nil-checks.

### 3. Nil-Safe ApprovalGate

All public methods get nil-receiver guards, matching the existing
`StickyRouter` pattern. `Enqueue` on nil returns an error; `Handle*`
methods return nil (silent no-op for events without a gate);
`CleanupTokens` returns early.

### 4. Simplified Orchestrator.Start()

`Start()` now only creates the WORKFLOW_HISTORY consumer — its sole
responsibility. Removed 12 lines of eager subsystem startup.

## Consequences

- No background goroutines for unused features.
- Consistent nil-safe pattern across optional subsystems
  (StickyRouter, ApprovalGate).
- Zero API changes — all existing callers and tests work unchanged.
- `sync.Once` means `Start()` errors are captured on first call and
  returned on all subsequent calls.
