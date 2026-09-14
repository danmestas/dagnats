// internal/consumername/stranded_ambiguity_test.go
// Pins the known false positive in the stranded-subject check, so the
// limitation is recorded as tested behavior rather than discovered in
// production by an operator who acted on the report.
//
// Methodology: real embedded NATS (subject-filter counts are server
// behavior). Seed messages on the CURRENT, HEALTHY subject for an
// ungrouped dotted task type, then assert the check reports them for the
// colliding grouped pair — because it cannot do otherwise.
//
// WHY THIS IS EXPECTED, NOT A BUG TO FIX: a legacy grouped subject for
// (Task "render", Group "gpu") is "task.render.gpu.{runID}", which is
// byte-identical to the live subject for ungrouped task type
// "render.gpu" — a combination #704 newly legalized. That
// indistinguishability is the precise defect #704 removes going forward
// and cannot remove retroactively, so no query can separate the two
// readings. The mitigation is in the REPORT, not the query: the
// engine/worker/bridge messages state the ambiguity and make the
// remediation conditional, because republishing the healthy reading
// would duplicate work that is already executing.
package consumername

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCheckStrandedSubjects_AmbiguousWithUngroupedDottedType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Healthy, current-encoding work for the UNGROUPED task type
	// "render.gpu". Post-#704 this is ordinary traffic, and under the
	// new encoding a genuinely grouped task would be published to
	// "task.render.=gpu.{runID}" instead.
	healthy := "task.render.gpu.deadbeefdeadbeefdeadbeefdeadbeefaa"
	if _, err := js.Publish(ctx, healthy, []byte("live")); err != nil {
		t.Fatalf("Publish(%q): %v", healthy, err)
	}

	stranded, err := CheckStrandedSubjects(ctx, stream, []GroupPair{
		{Task: "render", Group: "gpu"},
	})
	if err != nil {
		t.Fatalf("CheckStrandedSubjects: %v", err)
	}
	// Positive space for the limitation: it IS reported, and the test
	// exists to say so out loud. If this ever stops reporting, the check
	// has gained a disambiguation that is not possible from the subject
	// alone — verify what changed rather than deleting this test.
	if len(stranded) == 0 {
		t.Fatal(
			"expected the ungrouped dotted subject to be reported: it " +
				"is byte-identical to the legacy grouped shape, so the " +
				"check cannot distinguish them. If this now passes " +
				"silently, confirm why before relaxing the report.",
		)
	}

	// Negative space: a pair that collides with NOTHING on the stream is
	// still silent, so the report above is specific to the collision and
	// not the check indiscriminately flagging every pair it is given.
	quiet, err := CheckStrandedSubjects(ctx, stream, []GroupPair{
		{Task: "render", Group: "cpu"},
	})
	if err != nil {
		t.Fatalf("CheckStrandedSubjects(quiet pair): %v", err)
	}
	if len(quiet) != 0 {
		t.Fatalf("quiet pair reported %+v, want none", quiet)
	}
}
