// cli/status_health.go
// Detailed health collectors and printers for the --detail flag on
// status command. Provides queue health, DLQ summary, and engine lag.
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// queueHealth holds per-task consumer health metrics.
type queueHealth struct {
	Task        string `json:"task"`
	Pending     uint64 `json:"pending"`
	InFlight    uint64 `json:"in_flight"`
	Redelivered uint64 `json:"redelivered"`
	AckWaitMS   int64  `json:"ack_wait_ms"`
}

// dlqSummary holds dead-letter queue statistics.
type dlqSummary struct {
	Total  uint64            `json:"total"`
	Oldest *time.Time        `json:"oldest,omitempty"`
	Newest *time.Time        `json:"newest,omitempty"`
	ByTask map[string]uint64 `json:"by_task"`
}

// engineLag holds orchestrator processing lag metrics.
type engineLag struct {
	HistoryLagMessages uint64  `json:"history_lag_messages"`
	HistoryLagSeconds  float64 `json:"history_lag_seconds"`
	ScheduledTimers    uint64  `json:"scheduled_timers"`
}

// collectQueueHealth iterates consumers on the TASK_QUEUES stream
// and extracts per-task health metrics from consumer info.
func collectQueueHealth(
	ctx context.Context, js jetstream.JetStream,
) []queueHealth {
	if ctx == nil {
		panic("collectQueueHealth: ctx must not be nil")
	}
	if js == nil {
		panic("collectQueueHealth: js must not be nil")
	}

	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return nil
	}

	return iterateConsumers(ctx, stream)
}

// iterateConsumers lists consumers and builds queueHealth entries.
func iterateConsumers(
	ctx context.Context, stream jetstream.Stream,
) []queueHealth {
	if ctx == nil {
		panic("iterateConsumers: ctx must not be nil")
	}
	if stream == nil {
		panic("iterateConsumers: stream must not be nil")
	}

	const maxConsumers = 10000
	result := make([]queueHealth, 0, 16)
	lister := stream.ListConsumers(ctx)
	count := 0

	for info := range lister.Info() {
		if count >= maxConsumers {
			break
		}
		count++
		taskName := extractTaskFromConsumer(info)
		if taskName == "" {
			continue
		}
		result = append(result, queueHealth{
			Task:        taskName,
			Pending:     info.NumPending,
			InFlight:    uint64(info.NumAckPending),
			Redelivered: uint64(info.NumRedelivered),
			AckWaitMS:   info.Config.AckWait.Milliseconds(),
		})
	}
	return result
}

// extractTaskFromConsumer gets the task name from a consumer's
// filter subject. Strips "task-" prefix from durable name, or
// parses from FilterSubject.
func extractTaskFromConsumer(
	info *jetstream.ConsumerInfo,
) string {
	if info == nil {
		panic(
			"extractTaskFromConsumer: info must not be nil",
		)
	}
	// Try consumer name with task- prefix first.
	if strings.HasPrefix(info.Name, "task-") {
		return strings.TrimPrefix(info.Name, "task-")
	}

	// Fall back to parsing FilterSubject: "task.greet.>"
	subject := info.Config.FilterSubject
	return parseTaskFromSubject(subject)
}

// parseTaskFromSubject extracts the task name from a filter subject
// like "task.greet.>" or "task.greet.*". Returns "" if not matched.
func parseTaskFromSubject(subject string) string {
	if len(subject) < 6 {
		return ""
	}
	if !strings.HasPrefix(subject, "task.") {
		return ""
	}

	rest := subject[5:]
	if idx := strings.LastIndex(rest, "."); idx > 0 {
		return rest[:idx]
	}
	return rest
}

// collectDLQSummary gathers dead-letter queue statistics from the
// DEAD_LETTERS stream including per-task breakdown.
func collectDLQSummary(
	ctx context.Context, js jetstream.JetStream,
) dlqSummary {
	if ctx == nil {
		panic("collectDLQSummary: ctx must not be nil")
	}
	if js == nil {
		panic("collectDLQSummary: js must not be nil")
	}

	result := dlqSummary{ByTask: make(map[string]uint64)}

	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		return result
	}

	info, err := stream.Info(
		ctx, jetstream.WithSubjectFilter("dead.>"),
	)
	if err != nil {
		return result
	}

	result.Total = info.State.Msgs
	if result.Total > 0 {
		oldest := info.State.FirstTime
		newest := info.State.LastTime
		result.Oldest = &oldest
		result.Newest = &newest
	}

	result.ByTask = buildDLQTaskMap(info.State.Subjects)
	return result
}

// buildDLQTaskMap extracts task names from dead-letter subjects.
// Subject format: "dead.<task>.<run-id>".
func buildDLQTaskMap(
	subjects map[string]uint64,
) map[string]uint64 {
	if subjects == nil {
		return make(map[string]uint64)
	}

	const maxSubjects = 10000
	byTask := make(map[string]uint64)
	count := 0

	for subject, msgs := range subjects {
		if count >= maxSubjects {
			break
		}
		count++
		task := extractDLQTask(subject)
		if task != "" {
			byTask[task] += msgs
		}
	}
	return byTask
}

// extractDLQTask parses a task name from a dead-letter subject.
// Format: "dead.<task>.<run-id>" -> returns <task>.
func extractDLQTask(subject string) string {
	if len(subject) < 6 {
		return ""
	}
	if !strings.HasPrefix(subject, "dead.") {
		return ""
	}

	rest := subject[5:]
	idx := strings.Index(rest, ".")
	if idx > 0 {
		return rest[:idx]
	}
	return rest
}

// collectEngineLag computes orchestrator lag by comparing
// WORKFLOW_HISTORY stream state with consumer progress, and
// counts pending timer messages.
func collectEngineLag(
	ctx context.Context, js jetstream.JetStream,
) engineLag {
	if ctx == nil {
		panic("collectEngineLag: ctx must not be nil")
	}
	if js == nil {
		panic("collectEngineLag: js must not be nil")
	}

	result := engineLag{}
	result.HistoryLagMessages = computeHistoryLag(ctx, js)
	result.HistoryLagSeconds = computeHistoryLagSeconds(
		ctx, js,
	)
	result.ScheduledTimers = countScheduledTimers(ctx, js)
	return result
}

// computeHistoryLag returns the message count gap between the
// WORKFLOW_HISTORY stream's last sequence and the furthest
// consumer's delivered sequence.
func computeHistoryLag(
	ctx context.Context, js jetstream.JetStream,
) uint64 {
	if ctx == nil {
		panic("computeHistoryLag: ctx must not be nil")
	}
	if js == nil {
		panic("computeHistoryLag: js must not be nil")
	}

	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		return 0
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0
	}

	lastSeq := info.State.LastSeq
	maxDelivered := findMaxDelivered(ctx, stream)
	if maxDelivered >= lastSeq {
		return 0
	}
	return lastSeq - maxDelivered
}

// findMaxDelivered iterates consumers on a stream and returns
// the highest delivered stream sequence.
func findMaxDelivered(
	ctx context.Context, stream jetstream.Stream,
) uint64 {
	if ctx == nil {
		panic("findMaxDelivered: ctx must not be nil")
	}
	if stream == nil {
		panic("findMaxDelivered: stream must not be nil")
	}

	const maxConsumers = 1000
	var maxSeq uint64
	lister := stream.ListConsumers(ctx)
	count := 0

	for info := range lister.Info() {
		if count >= maxConsumers {
			break
		}
		count++
		if info.Delivered.Stream > maxSeq {
			maxSeq = info.Delivered.Stream
		}
	}
	return maxSeq
}

// computeHistoryLagSeconds estimates lag time from stream
// timestamps. Returns 0 if no lag or no messages.
func computeHistoryLagSeconds(
	ctx context.Context, js jetstream.JetStream,
) float64 {
	if ctx == nil {
		panic("computeHistoryLagSeconds: ctx must not be nil")
	}
	if js == nil {
		panic("computeHistoryLagSeconds: js must not be nil")
	}

	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		return 0
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0
	}

	if info.State.Msgs == 0 {
		return 0
	}

	// If there are unprocessed messages, the lag is the age
	// of the oldest unprocessed message. Approximate using
	// stream first/last time delta.
	elapsed := time.Since(info.State.FirstTime)
	if elapsed < 0 {
		return 0
	}
	return elapsed.Seconds()
}

// countScheduledTimers returns the message count in the
// SLEEP_TIMERS stream.
func countScheduledTimers(
	ctx context.Context, js jetstream.JetStream,
) uint64 {
	if ctx == nil {
		panic("countScheduledTimers: ctx must not be nil")
	}
	if js == nil {
		panic("countScheduledTimers: js must not be nil")
	}

	stream, err := js.Stream(ctx, "SLEEP_TIMERS")
	if err != nil {
		return 0
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0
	}
	return info.State.Msgs
}

// appendDetailToStatus populates detail fields on systemStatus
// for JSON output when --detail is set.
func appendDetailToStatus(
	status *systemStatus, nc *nats.Conn,
) {
	if status == nil {
		panic("appendDetailToStatus: status must not be nil")
	}
	if nc == nil {
		panic("appendDetailToStatus: nc must not be nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return
	}

	ctx := context.Background()
	queues := collectQueueHealth(ctx, js)
	status.Queues = queues

	dlq := collectDLQSummary(ctx, js)
	status.DLQ = &dlq

	lag := collectEngineLag(ctx, js)
	status.Engine = &lag
}

// printDetailSections prints all --detail sections to stdout.
func printDetailSections(nc *nats.Conn) {
	if nc == nil {
		panic("printDetailSections: nc must not be nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"detail: JetStream unavailable: %v\n", err)
		return
	}

	ctx := context.Background()
	printQueueHealth(collectQueueHealth(ctx, js))
	printDLQSummary(collectDLQSummary(ctx, js))
	printEngineLag(collectEngineLag(ctx, js))
}

// printQueueHealth prints a table of per-task consumer health.
func printQueueHealth(queues []queueHealth) {
	if queues == nil {
		panic("printQueueHealth: queues must not be nil")
	}
	if len(queues) > 10000 {
		panic("printQueueHealth: queues exceeds max bound")
	}

	if len(queues) == 0 {
		fmt.Println("\nTask Queues: none")
		return
	}

	fmt.Println("\nTask Queues:")
	w := tabwriter.NewWriter(os.Stdout, 2, 0, 2, ' ', 0)
	fmt.Fprintf(w,
		"  TASK\tPENDING\tIN-FLIGHT\t"+
			"REDELIVERED\tACK WAIT\n",
	)
	for _, q := range queues {
		fmt.Fprintf(w, "  %s\t%d\t%d\t%d\t%dms\n",
			q.Task, q.Pending, q.InFlight,
			q.Redelivered, q.AckWaitMS,
		)
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
	}
}

// printDLQSummary prints a human-readable dead-letter summary.
func printDLQSummary(summary dlqSummary) {
	if summary.ByTask == nil {
		panic("printDLQSummary: ByTask must not be nil")
	}
	if summary.Total > 1<<50 {
		panic("printDLQSummary: total exceeds max bound")
	}

	if summary.Total == 0 {
		fmt.Println("\nDead Letters: none")
		return
	}

	fmt.Printf("\nDead Letters: %d total\n", summary.Total)
	if summary.Oldest != nil {
		fmt.Printf("  Oldest: %s\n",
			summary.Oldest.Format("2006-01-02 15:04:05"),
		)
	}
	if summary.Newest != nil {
		fmt.Printf("  Newest: %s\n",
			summary.Newest.Format("2006-01-02 15:04:05"),
		)
	}

	printDLQByTask(summary.ByTask)
}

// printDLQByTask prints per-task dead letter counts.
func printDLQByTask(byTask map[string]uint64) {
	if byTask == nil {
		panic("printDLQByTask: byTask must not be nil")
	}
	if len(byTask) > 10000 {
		panic("printDLQByTask: byTask exceeds max bound")
	}

	if len(byTask) == 0 {
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 2, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  TASK\tCOUNT\n")
	for task, count := range byTask {
		fmt.Fprintf(w, "  %s\t%d\n", task, count)
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
	}
}

// printEngineLag prints orchestrator lag and timer information.
func printEngineLag(lag engineLag) {
	if lag.HistoryLagSeconds < 0 {
		panic("printEngineLag: negative lag seconds")
	}
	if lag.HistoryLagMessages > 1<<50 {
		panic("printEngineLag: lag messages exceeds max bound")
	}

	fmt.Println("\nEngine:")
	fmt.Printf("  History lag:     %d messages (%.1fs)\n",
		lag.HistoryLagMessages, lag.HistoryLagSeconds,
	)
	fmt.Printf("  Scheduled timers: %d\n", lag.ScheduledTimers)
}
