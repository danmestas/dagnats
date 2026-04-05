// api/task_check_test.go
// Integration tests for task consumer validation using embedded NATS.
// Methodology: real NATS server with JetStream. Each test provisions its
// own server to avoid shared state. Validates both positive (consumers
// present) and negative (consumers absent) paths.
package api

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCheckTaskConsumersNoWorkers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	wb := dag.NewWorkflow("check-wf")
	stepGreet := wb.Task("step-greet", "greet")
	wb.Task("step-upper", "uppercase").After(stepGreet)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Positive: both task types should be missing with no consumers.
	missing := CheckTaskConsumers(js, def)
	if len(missing) != 2 {
		t.Fatalf("len(missing) = %d, want 2", len(missing))
	}

	// Negative: result must contain both task types.
	found := make(map[string]bool)
	for _, m := range missing {
		found[m] = true
	}
	if !found["greet"] || !found["uppercase"] {
		t.Fatalf(
			"missing = %v, want greet and uppercase",
			missing,
		)
	}
}

func TestCheckTaskConsumersWithWorker(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	// Simulate a worker subscribing to "task.greet.>"
	stream, streamErr := js.Stream(
		context.Background(), "TASK_QUEUES",
	)
	if streamErr != nil {
		t.Fatalf("Stream failed: %v", streamErr)
	}
	_, subErr := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			Durable:       "worker-greet",
			FilterSubject: "task.greet.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
		},
	)
	if subErr != nil {
		t.Fatalf("CreateConsumer failed: %v", subErr)
	}

	wb := dag.NewWorkflow("check-wf")
	stepGreet := wb.Task("step-greet", "greet")
	wb.Task("step-upper", "uppercase").After(stepGreet)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Positive: only "uppercase" should be missing.
	missing := CheckTaskConsumers(js, def)
	if len(missing) != 1 {
		t.Fatalf("len(missing) = %d, want 1", len(missing))
	}

	// Negative: "greet" should NOT be in the missing list.
	if missing[0] != "uppercase" {
		t.Fatalf(
			"missing[0] = %q, want %q",
			missing[0], "uppercase",
		)
	}
}

func TestCheckTaskConsumersAllPresent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	ctx := context.Background()
	stream, streamErr := js.Stream(ctx, "TASK_QUEUES")
	if streamErr != nil {
		t.Fatalf("Stream failed: %v", streamErr)
	}
	// Register consumers for both task types.
	_, err = stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			Durable:       "worker-greet",
			FilterSubject: "task.greet.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
		},
	)
	if err != nil {
		t.Fatalf("CreateConsumer greet failed: %v", err)
	}
	_, err = stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			Durable:       "worker-upper",
			FilterSubject: "task.uppercase.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
		},
	)
	if err != nil {
		t.Fatalf("CreateConsumer uppercase failed: %v", err)
	}

	wb := dag.NewWorkflow("check-wf")
	stepGreet := wb.Task("step-greet", "greet")
	wb.Task("step-upper", "uppercase").After(stepGreet)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Positive: no tasks should be missing.
	missing := CheckTaskConsumers(js, def)
	if len(missing) != 0 {
		t.Fatalf("len(missing) = %d, want 0", len(missing))
	}
}

func TestExtractTaskType(t *testing.T) {
	// Positive: standard patterns extract correctly.
	if got := extractTaskType("task.greet.>"); got != "greet" {
		t.Fatalf("got = %q, want %q", got, "greet")
	}
	if got := extractTaskType("task.upper.*"); got != "upper" {
		t.Fatalf("got = %q, want %q", got, "upper")
	}

	// Negative: non-task subjects return empty.
	if got := extractTaskType("history.run1"); got != "" {
		t.Fatalf("got = %q, want empty", got)
	}
	if got := extractTaskType("task"); got != "" {
		t.Fatalf("got = %q for short subject, want empty", got)
	}
}

func TestCollectTaskTypes(t *testing.T) {
	wb := dag.NewWorkflow("test-wf")
	stepA := wb.Task("a", "greet")
	stepB := wb.Task("b", "uppercase").After(stepA)
	wb.Task("c", "greet").After(stepB)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Positive: deduplicates task types.
	types := collectTaskTypes(def)
	if len(types) != 2 {
		t.Fatalf("len(types) = %d, want 2", len(types))
	}

	// Negative: result is sorted.
	if types[0] != "greet" || types[1] != "uppercase" {
		t.Fatalf("types = %v, want [greet uppercase]", types)
	}
}

func TestFindMissingTypes(t *testing.T) {
	active := map[string]struct{}{
		"greet": {},
	}

	// Positive: missing type is returned.
	missing := findMissingTypes(
		[]string{"greet", "uppercase"}, active,
	)
	if len(missing) != 1 {
		t.Fatalf("len(missing) = %d, want 1", len(missing))
	}
	if missing[0] != "uppercase" {
		t.Fatalf(
			"missing[0] = %q, want %q",
			missing[0], "uppercase",
		)
	}

	// Negative: all-present yields empty result.
	active["uppercase"] = struct{}{}
	missing = findMissingTypes(
		[]string{"greet", "uppercase"}, active,
	)
	if len(missing) != 0 {
		t.Fatalf("len(missing) = %d, want 0", len(missing))
	}
}
