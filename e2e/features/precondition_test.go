// e2e/features/precondition_test.go
// Shared readiness ceilings for e2e tests that must ACT once some
// asynchronous setup is live. Methodology: such tests poll the real
// precondition via harness.WaitForPrecondition instead of sleeping a
// fixed duration, so a loaded box costs wall-clock rather than
// producing a downstream symptom that reads as a product bug (#558).
package features

import "time"

// triggerReadyCeiling bounds the wait for the trigger service's KV
// watcher to activate a freshly created trigger. Generous by design:
// the watcher competes for CPU with every other e2e NATS server on
// the box. A miss costs wall-clock and never masks a real bug —
// WaitForPrecondition names the unregistered trigger when it gives up.
const triggerReadyCeiling = 30 * time.Second

// stepRunningCeiling bounds the wait for a worker to claim a step and
// begin executing it. Same rationale as triggerReadyCeiling.
const stepRunningCeiling = 30 * time.Second
