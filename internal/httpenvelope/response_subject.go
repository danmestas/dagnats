package httpenvelope

// httpResponseSubjectPrefix is the engine-private NATS subject root
// for HTTP-trigger correlation per ADR-013. Both the API handler
// (subscriber) and the engine respond-step (publisher) build the
// per-run subject through ResponseSubject — hardcoding the prefix in
// two packages would be change amplification.
//
// Lives in httpenvelope because internal/trigger imports
// internal/engine (debounce timers) and engine must import the
// subject helper from a package outside both. httpenvelope already
// hosts the body and envelope helpers shared across the HTTP trigger
// surface, so the dependency direction is engine → httpenvelope ←
// trigger with no cycle.
const httpResponseSubjectPrefix = "dagnats.http.response."

// ResponseSubject is the single producer of the engine-private NATS
// subject for an HTTP-triggered run's response payload. The shape
// dagnats.http.response.<runID> is part of the contract; renaming it
// later is a single-site edit.
//
// Panics on empty runID (programmer error). The empty case is bound
// by callers — API handler generates a fresh runID before calling;
// engine respond-step pulls runID off the WorkflowRun — so an empty
// argument here signals a callsite bug, not a runtime input.
func ResponseSubject(runID string) string {
	if runID == "" {
		panic("ResponseSubject: runID must not be empty")
	}
	if httpResponseSubjectPrefix == "" {
		panic("ResponseSubject: prefix must not be empty")
	}
	return httpResponseSubjectPrefix + runID
}
