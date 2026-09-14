// Package consumername owns the naming convention for dagnats-managed
// JetStream consumers on TASK_QUEUES. All durable names live under the
// "workers-" prefix; sanitization maps task-type/group strings to
// NATS-legal name fragments.
//
// Internal, not exported from worker: TASK_QUEUES is a work-queue stream,
// so the bridge's poll path must land on the byte-identical durable a
// native worker would create or JetStream rejects the second consumer for
// overlapping filters (issue #532). Both packages therefore share this
// one definition rather than the bridge reaching into worker's public
// SDK surface — the scheme is an internal invariant, not an API promise.
package consumername

import (
	"strings"
	"time"
)

// DefaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (planned: ADR-008 heartbeats, tracked as follow-up to
// issue #136) or override at handler registration via WithAckWait.
// See ADR-006 §"Out of scope (deferred)" for the full deferred-list.
const DefaultAckWait = 5 * time.Minute

// GroupSentinel (#704) is the byte NameFor/FilterFor prefix onto a
// non-empty group to disambiguate it from a taskType's own dotted
// segments. Its value is the UNIQUE character satisfying all four
// constraints its uses cross simultaneously — this is not a style
// choice, so do not "simplify" it to another punctuation character
// without re-checking every constraint below:
//
//  1. Legal in a NATS subject token: excludes the subject metacharacters
//     `.` (token separator), `*` and `>` (wildcards), and whitespace.
//  2. Legal in a JetStream consumer (durable) name: nats.go additionally
//     forbids `/` and `\` there.
//  3. Legal as a NATS KV key: nats.go's key validator is
//     `^[-/_=\.a-zA-Z0-9]+$` — nothing outside that set. This constraint
//     is invisible from this package's own call sites: it arrives
//     because worker/worker.go's partitioned/elastic consumer-group path
//     (WithPartitions + WithGroups) hands NameFor's output straight to
//     github.com/synadia-io/orbit.go/pcgroups, which keys its own group
//     metadata in a KV bucket by that exact string
//     (pcgroups.CreateElastic -> composeKey(streamName, consumerGroupName)
//     -> kv.Put). A sentinel illegal there panics at consumer-group
//     creation time ("nats: invalid key"), not at validation time.
//  4. OUTSIDE ValidTaskType's allowed charset (`A-Za-z0-9_-.`) — the
//     whole point is that it stays unreachable from a caller-supplied
//     Task or WorkerGroup value, so Sanitize must never add it to its
//     own allowed set either.
//
// Intersecting all four: constraint 3's KV-legal set is
// `[-/_=.a-zA-Z0-9]`; constraint 1 removes `.`; constraint 2 removes
// `/`; constraint 4 removes every character remaining EXCEPT `=`. `=`
// is the only character that survives all four.
const GroupSentinel = "="

// Sanitize maps a task-type or group string to a NATS-legal consumer-name
// fragment. Dots collapse to hyphens for the common dotted-namespace case;
// other disallowed characters fall back to underscore. Empty input or
// empty output is a programmer error.
//
// The mapping is lossy: "send.email" and "send-email" both yield
// "send-email". Callers that adopt a consumer by name must verify the
// adopted filter subject rather than trusting the name alone — see
// NameFor's contract.
func Sanitize(s string) string {
	if s == "" {
		panic("consumername.Sanitize: input must not be empty")
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-',
			c == '_':
			out = append(out, c)
		case c == '.':
			out = append(out, '-')
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		panic("consumername.Sanitize: result must not be empty")
	}
	return string(out)
}

// NameFor produces the durable consumer name for a (taskType, group)
// pair. group=="" means the default branch, unchanged from before #704.
// Both inputs are sanitized via Sanitize before being concatenated under
// the "workers-" prefix. The "workers-" prefix is reserved for
// dagnats-managed consumers.
//
// The grouped branch prefixes the sanitized group with "-" + GroupSentinel
// — the same sentinel FilterFor uses (#704) — so a dotted taskType
// combined with a group can never sanitize to the same name as an
// equivalent dotted taskType alone: NameFor("a.b", "") = "workers-a-b"
// while NameFor("a", "b") = "workers-a-=b". GroupSentinel is never added
// to Sanitize's allowed set, so it stays unreachable from user input and
// this distinction cannot be forged by a caller-supplied taskType or
// group.
//
// Still not injective within EACH branch: distinct (taskType, group)
// pairs can still collide on one name because Sanitize itself is lossy
// (e.g. NameFor("a.b", "c") == NameFor("a-b", "c")). A name alone
// therefore does not identify the subjects a consumer serves; pair it
// with FilterFor when adopting.
func NameFor(taskType, group string) string {
	if taskType == "" {
		panic("consumername.NameFor: taskType must not be empty")
	}
	if group == "" {
		out := "workers-" + Sanitize(taskType)
		if out == "" {
			panic("consumername.NameFor: result must not be empty")
		}
		return out
	}
	out := "workers-" + Sanitize(taskType) + "-" + GroupSentinel + Sanitize(group)
	if out == "" {
		panic("consumername.NameFor: result must not be empty")
	}
	return out
}

// FilterFor produces the filter subject for a (taskType, group) pair.
// Inputs are NOT sanitized — they appear in the message-subject hierarchy
// and must round-trip exactly. Subject validity is the publisher's
// contract; sanitization is a consumer-naming concern.
//
// The trailing token is a single `*`, not `>`, anchoring the filter to
// exactly the token count internal/engine/task_publisher.go's StepSubject
// appends after (taskType[, group]): one token, the run ID (a 32-char hex
// string with no dots — see internal/runid.New). A trailing `>` would
// wildcard-match ANY number of following tokens, so a dotted taskType like
// "build" would also receive "task.build.linux.<runID>" — a completely
// different, unrelated task type that merely shares a dotted prefix. `*`
// closes that leak (issue #674) while still allowing taskType itself to
// contain dots ("dagger.call" is a production task type). Any existing
// durable consumer's FilterSubject is brought in line the next time its
// owner calls jetstream.CreateOrUpdateConsumer (worker.subscribePullConsumer,
// bridge.taskConsumer) — see docs/wire-protocol.md "Task Subjects".
//
// The grouped branch (#704) prefixes group with GroupSentinel:
// "task.{taskType}.={group}.*". Without it, a dotted taskType and an
// undotted taskType-plus-group could derive the byte-identical filter —
// FilterFor("a.b", "") and FilterFor("a", "b") both used to be
// "task.a.b.*". ValidTaskType forbids GroupSentinel in a taskType and
// ValidWorkerGroup forbids it in a group (via the same rule), so the
// sentinel is unreachable from either input and the two forms are now
// provably distinct: FilterFor("a.b", "") == "task.a.b.*" while
// FilterFor("a", "b") == "task.a.=b.*".
func FilterFor(taskType, group string) string {
	if taskType == "" {
		panic("consumername.FilterFor: taskType must not be empty")
	}
	if group == "" {
		out := "task." + taskType + ".*"
		if out == "" {
			panic("consumername.FilterFor: result must not be empty")
		}
		return out
	}
	out := "task." + taskType + "." + GroupSentinel + group + ".*"
	if out == "" {
		panic("consumername.FilterFor: result must not be empty")
	}
	return out
}

// LegacyFilterFor returns every filter-subject shape a dagnats process
// predating the CURRENT FilterFor could have used for (taskType, group).
//
// Ungrouped is unchanged by #704: there is exactly one legacy shape, the
// pre-#674 trailing-">"-wildcard form.
//
// Grouped has TWO legacy shapes, because #704 itself changed FilterFor's
// grouped output (adding GroupSentinel): the pre-#674 ">"-wildcard
// form, AND the "*"-anchored, sentinel-free form FilterFor produced for
// every dagnats release between #674 and #704
// ("task.{taskType}.{group}.*" — no sentinel). A grouped durable from
// EITHER era is a differently-NAMED consumer from today's (see LegacyNameFor),
// not an in-place-upgradeable one, so unlike the ungrouped case these
// shapes feed ONLY the stranded-subject check (engine startup's
// checkStrandedGroupSubjects and its worker/bridge counterparts) — no
// consumer filters on either any more.
func LegacyFilterFor(taskType, group string) []string {
	if taskType == "" {
		panic("consumername.LegacyFilterFor: taskType must not be empty")
	}
	if group == "" {
		return []string{"task." + taskType + ".>"}
	}
	return []string{
		"task." + taskType + "." + group + ".>",
		"task." + taskType + "." + group + ".*",
	}
}

// LegacyNameFor returns the durable name a process predating #704's
// sentinel would have used for a grouped (taskType, group) pair — today's
// NameFor computation minus the sentinel. Grouped only: NameFor's
// ungrouped branch is untouched by #704, so an ungrouped legacy name is
// just NameFor(taskType, ""); callers needing it should call NameFor
// directly rather than this function.
//
// Feeds only the stranded-subject check (see LegacyFilterFor) — never a
// consumer's actual Durable field, and never adopted or deleted the way
// the #674 in-place upgrade adopts a stale FilterSubject: a legacy
// grouped durable at this name (if one still exists) is a DIFFERENT
// consumer from today's NameFor(taskType, group), not an old version of
// it, so the existing cross-process, name-keyed collision scan
// (assertNoCrossProcessCollision) will never even see it.
func LegacyNameFor(taskType, group string) string {
	if taskType == "" {
		panic("consumername.LegacyNameFor: taskType must not be empty")
	}
	if group == "" {
		panic(
			"consumername.LegacyNameFor: group must not be empty; " +
				"ungrouped names are unaffected by #704, call NameFor",
		)
	}
	return "workers-" + Sanitize(taskType) + "-" + Sanitize(group)
}

// FilterIsLegacyUpgrade reports whether old is exactly one of new's
// legacy twins — the same (taskType, group) pair's filter as it would
// have been produced by a process predating the CURRENT FilterFor. It
// compares strings only — new and old never need to be decomposed back
// into taskType and group.
//
// For an ungrouped new (no sentinel), there is exactly one legacy
// shape: the pre-#674 ">"-wildcard twin, unchanged from before #704.
//
// For a grouped new ("task.{t}.={g}.*"), #704 itself changed what
// FilterFor produces, so old is checked against BOTH legacy shapes
// LegacyFilterFor now returns for a grouped pair: the sentinel-free
// "*"-anchored form (what FilterFor produced between #674 and #704) and
// the sentinel-free ">"-wildcard form (pre-#674). Either shape accepted.
//
// This is what lets a worker or bridge process upgrading past #674
// treat a durable left by a not-yet-upgraded sibling process as an
// in-place upgrade (delete and recreate with the new anchor) instead of
// a genuine cross-type collision (which still panics/500s loudly). In
// practice a grouped legacy durable is never reached through this path
// post-#704 — its durable NAME lacks the sentinel too (see
// LegacyNameFor), so the name-keyed collision scan that calls this
// never finds it in the first place; the stranded-subject check is what
// catches that case. This function stays correct for grouped filters
// regardless, rather than silently regressing to false for every
// grouped pair. Returns false — never a collision misdetected as an
// upgrade — for any old that isn't byte-for-byte one of new's legacy
// twins.
func FilterIsLegacyUpgrade(newFilter, oldFilter string) bool {
	if newFilter == "" {
		panic("consumername.FilterIsLegacyUpgrade: newFilter must not be empty")
	}
	if oldFilter == "" {
		panic("consumername.FilterIsLegacyUpgrade: oldFilter must not be empty")
	}
	if !strings.HasSuffix(newFilter, ".*") {
		// Not a filter FilterFor could have produced; cannot have a
		// legacy twin under this rule.
		return false
	}
	trimmed := strings.TrimSuffix(newFilter, "*")
	if oldFilter == trimmed+">" {
		return true
	}
	sentinelIdx := strings.Index(trimmed, "."+GroupSentinel)
	if sentinelIdx < 0 {
		// Ungrouped: no second legacy shape to check.
		return false
	}
	desentineled := trimmed[:sentinelIdx+1] + trimmed[sentinelIdx+1+len(GroupSentinel):]
	return oldFilter == desentineled+"*" || oldFilter == desentineled+">"
}
