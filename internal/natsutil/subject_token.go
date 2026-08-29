package natsutil

import "strings"

// subjectTokenMaxLen bounds a sanitized subject token so a
// pathological input string (a workflow name, a step ID) cannot grow
// a NATS subject beyond practical limits or blow out subject-based
// dashboards/ACLs.
const subjectTokenMaxLen = 128

// SubjectToken makes s safe for use as a NATS subject token: any byte
// outside [A-Za-z0-9_-] becomes '_', and the result is capped to
// subjectTokenMaxLen. Originally introduced (#625) for workflow-name
// sanitization feeding event.run.{workflow}.{runID}.{status}; moved
// here (#624) so the BUILD_LOGS subject builder (logs.{runID}.{stepID})
// can reuse it without internal/natsutil importing internal/engine —
// natsutil sits below engine in the import graph, so the sanitizer
// belongs at this layer.
//
// Pure and total: every input (including "") is valid and produces a
// well-formed (if degenerate) output, so there is no invariant to
// assert here.
func SubjectToken(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > subjectTokenMaxLen {
		out = out[:subjectTokenMaxLen]
	}
	return out
}
