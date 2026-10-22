package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Replay is a tool's declaration of what a rerun costs: Safe when
// running the call twice is harmless (a read, an idempotent write
// keyed on ToolCall.Key), Unsafe when it is not (send a message, open
// a pull request, charge a card).
type Replay string

const (
	Safe   Replay = "safe"
	Unsafe Replay = "unsafe"
)

// ErrInterrupted is the error text of a tool result recorded for a
// call a previous holder of the step started but never finished.
const ErrInterrupted = "interrupted"

// ErrReplayMismatch: the call at this position was recorded with
// another name or input. The handler is not replaying the sequence
// of calls it recorded; nothing is reused or written.
var ErrReplayMismatch = errors.New("journal: tool call does not match the recorded call")

// callsMax bounds the tool and model calls of one iteration; a step
// past it has outgrown a per-step journal.
const callsMax = recordsMax

// ToolCall is one tool invocation. The handler sets Name and Input;
// Tool fills Call (the call's position in the iteration, counting
// from 0) and Key, the idempotency key {run}.{step}.{iteration}.{call},
// which is the same on every delivery and attempt of the iteration so
// the tool can hand it to an external system that deduplicates.
type ToolCall struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input,omitempty"`
	Call  int             `json:"call"`
	Key   string          `json:"key"`
}

// ToolFunc runs the tool. The error it returns is the tool's own
// failure, recorded in the result for the model to see; it is not a
// reason to stop the step.
type ToolFunc func(ctx context.Context, call ToolCall) ([]byte, error)

// ToolIntent is the body of a tool.intent record: the call and its
// replay declaration, written before the tool runs.
type ToolIntent struct {
	ToolCall
	Replay Replay `json:"replay"`
}

// ToolResult is the body of a tool.result record. Key is the call's
// idempotency key. Interrupted marks a call a previous holder started
// and never finished, which was not rerun; Partial then holds, as a
// JSON array, the entry records that holder appended while the tool
// ran.
type ToolResult struct {
	Call        int             `json:"call"`
	Key         string          `json:"key"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	Interrupted bool            `json:"interrupted,omitempty"`
	Partial     json.RawMessage `json:"partial,omitempty"`
}

// ModelResponse is the body of a model.response record.
type ModelResponse struct {
	Call   int             `json:"call"`
	Output json.RawMessage `json:"output,omitempty"`
}

// ModelFunc requests one model response. An error means no response:
// nothing is recorded and the next holder requests again.
type ModelFunc func(ctx context.Context) ([]byte, error)

// Tool runs one tool call durably: it appends the call's intent with
// its replay declaration, runs fn, and appends the result. Calls are
// matched by position within the iteration, and a recorded call at
// the position must be the same call (name and input), else
// ErrReplayMismatch. On a claim that recovers an attempt a previous
// holder started: a recorded result is returned without running fn;
// an intent without a result is rerun only when the recorded and the
// current declaration both say Safe, and otherwise answered with an
// interrupted result carrying the partial output and the call's key,
// which the model then sees. On a new attempt of the iteration (after
// a commit, or a higher attempt from iteration 1) only unsafe intents
// are honoured, matched by content rather than position: the first
// prior unsafe intent with the same name and input not yet honoured
// in this attempt answers the call under its own key, with its
// recorded result or as interrupted; safe calls run afresh.
//
// The guarantee is that the SDK starts an unsafe call at most once
// per position. A holder stalled between its intent and the tool's
// effect may still complete that effect after a successor recorded
// the call as interrupted; the key is what an external system
// deduplicates on. The returned error is the journal's (ErrFenced,
// ErrCommitted, transport), never the tool's own, which is in the
// result. Tool must be called from the goroutine that owns the
// handle; a model's parallel tool calls are issued one at a time.
func (j *Journal) Tool(
	ctx context.Context, call ToolCall, replay Replay, fn ToolFunc,
) (ToolResult, error) {
	if replay != Safe && replay != Unsafe {
		panic("journal.Tool: replay must be Safe or Unsafe")
	}
	if fn == nil || call.Name == "" {
		panic("journal.Tool: fn and call.Name must not be empty")
	}
	defer j.enter("Tool")()
	if j.toolCalls >= callsMax {
		return ToolResult{}, fmt.Errorf("journal: more than %d tool calls in one iteration", callsMax)
	}
	call.Call = j.toolCalls
	call.Key = j.task.RunID + "." + j.task.StepID + "." +
		strconv.Itoa(j.task.Iteration) + "." + strconv.Itoa(call.Call)
	// Prior unsafe intents are matched by content before this
	// attempt's positions, so a redelivery replays the honoured calls
	// in the same order it first made them.
	if prior, record, ok := j.priorUnsafeIntent(call); ok {
		return j.honourPrior(ctx, prior, record)
	}
	intent, recorded, found := j.recordedToolIntent(call.Call)
	if !found {
		return j.freshTool(ctx, call, replay, fn)
	}
	if !sameCall(intent.ToolCall, call) {
		return ToolResult{}, fmt.Errorf("%w: call %d recorded as %s %s, now %s %s",
			ErrReplayMismatch, call.Call, intent.Name, intent.Input, call.Name, call.Input)
	}
	j.toolCalls++
	if result, done := j.recordedToolResult(call.Call, recorded.Seq); done {
		return result, nil
	}
	if intent.Replay == Safe && replay == Safe {
		return j.runTool(ctx, call, fn)
	}
	return j.appendToolResult(ctx, interruptedResult(intent.ToolCall, j.partialOutput(recorded)))
}

// honourPrior answers a call from a prior attempt's unsafe intent: its
// recorded result when the call finished, else an interrupted result.
// The call keeps the prior intent's position and key, and does not
// take a position in this attempt.
func (j *Journal) honourPrior(
	ctx context.Context, intent ToolIntent, record Record,
) (ToolResult, error) {
	if intent.Replay != Unsafe {
		panic("honourPrior: only unsafe intents are honoured")
	}
	if record.Seq == 0 {
		panic("honourPrior: record must have a sequence")
	}
	j.honoured[record.Seq] = true
	if result, done := j.recordedToolResult(intent.Call, record.Seq); done {
		return result, nil
	}
	return j.appendToolResult(ctx, interruptedResult(intent.ToolCall, j.partialOutput(record)))
}

// interruptedResult is the result recorded for a call that was
// started and never finished.
func interruptedResult(call ToolCall, partial json.RawMessage) ToolResult {
	if call.Key == "" {
		panic("interruptedResult: call must carry its key")
	}
	if call.Call < 0 {
		panic("interruptedResult: call must not be negative")
	}
	return ToolResult{
		Call: call.Call, Key: call.Key, Error: ErrInterrupted, Interrupted: true, Partial: partial,
	}
}

// priorUnsafeIntent finds the first unsafe intent of a prior attempt
// with the same name and input as call that this attempt has not yet
// honoured.
func (j *Journal) priorUnsafeIntent(call ToolCall) (ToolIntent, Record, bool) {
	if call.Name == "" {
		panic("priorUnsafeIntent: call must be named")
	}
	if len(j.records) > recordsMax {
		panic("priorUnsafeIntent: records exceed the read bound")
	}
	for _, r := range j.records {
		if r.Type != TypeToolIntent || !j.priorAttempt(r) || j.honoured[r.Seq] {
			continue
		}
		var intent ToolIntent
		if err := json.Unmarshal(r.Data, &intent); err != nil || intent.Replay != Unsafe {
			continue
		}
		if sameCall(intent.ToolCall, call) {
			return intent, r, true
		}
	}
	return ToolIntent{}, Record{}, false
}

// freshTool appends a new intent for call and runs it.
func (j *Journal) freshTool(
	ctx context.Context, call ToolCall, replay Replay, fn ToolFunc,
) (ToolResult, error) {
	if call.Key == "" {
		panic("freshTool: call must carry its key")
	}
	if fn == nil {
		panic("freshTool: fn must not be nil")
	}
	data, err := json.Marshal(ToolIntent{ToolCall: call, Replay: replay})
	if err != nil {
		return ToolResult{}, fmt.Errorf("marshal tool intent: %w", err)
	}
	if _, err := j.append(ctx, TypeToolIntent, data); err != nil {
		return ToolResult{}, err
	}
	j.toolCalls++
	return j.runTool(ctx, call, fn)
}

// sameCall reports whether two calls name the same tool with the same
// input, compared as canonical JSON so formatting does not matter.
func sameCall(recorded, current ToolCall) bool {
	if recorded.Name == "" || current.Name == "" {
		panic("sameCall: calls must be named")
	}
	if recorded.Name != current.Name {
		return false
	}
	return canonicalJSON(recorded.Input) == canonicalJSON(current.Input)
}

// canonicalJSON renders input with keys sorted and no whitespace; an
// empty input and invalid JSON are rendered as themselves.
func canonicalJSON(input json.RawMessage) string {
	if len(input) > recordsMax*1024 {
		panic("canonicalJSON: input too large")
	}
	if len(bytes.TrimSpace(input)) == 0 {
		return ""
	}
	var value any
	if err := json.Unmarshal(input, &value); err != nil {
		return string(input)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return string(input)
	}
	return string(canonical)
}

// priorAttempt reports whether r was written by an attempt the engine
// has since retried: it sits before the iteration's last commit, or
// carries a lower attempt than the task's. On iteration 0 the attempt
// follows the delivery count, so there only the commit marks the
// boundary (ADR-026).
func (j *Journal) priorAttempt(r Record) bool {
	if r.Seq == 0 {
		panic("priorAttempt: record must have a sequence")
	}
	if j.task.Attempt < 1 {
		panic("priorAttempt: attempt must be >= 1")
	}
	if r.Seq <= j.lastCommitSeq {
		return true
	}
	return j.task.Iteration > 0 && r.Attempt < j.task.Attempt
}

// runTool runs fn and records what it returned.
func (j *Journal) runTool(ctx context.Context, call ToolCall, fn ToolFunc) (ToolResult, error) {
	if fn == nil {
		panic("runTool: fn must not be nil")
	}
	if call.Key == "" {
		panic("runTool: call must carry its key")
	}
	output, err := fn(ctx, call)
	result := ToolResult{Call: call.Call, Key: call.Key, Output: output}
	if err != nil {
		result.Error = err.Error()
	}
	return j.appendToolResult(ctx, result)
}

// appendToolResult records result and returns it.
func (j *Journal) appendToolResult(ctx context.Context, result ToolResult) (ToolResult, error) {
	if result.Call < 0 || result.Key == "" {
		panic("appendToolResult: result must carry its call and key")
	}
	if result.Interrupted && result.Error == "" {
		panic("appendToolResult: an interrupted result must carry an error")
	}
	data, err := json.Marshal(result)
	if err != nil {
		return ToolResult{}, fmt.Errorf("marshal tool result: %w", err)
	}
	if _, err := j.append(ctx, TypeToolResult, data); err != nil {
		return ToolResult{}, err
	}
	return result, nil
}

// recordedToolResult finds the newest result recorded for the call at
// position call after the intent at intentSeq.
func (j *Journal) recordedToolResult(call int, intentSeq uint64) (ToolResult, bool) {
	if call < 0 || intentSeq == 0 {
		panic("recordedToolResult: call must not be negative and intentSeq must be positive")
	}
	if len(j.records) > recordsMax {
		panic("recordedToolResult: records exceed the read bound")
	}
	var found ToolResult
	ok := false
	for _, r := range j.records {
		if r.Type != TypeToolResult || r.Seq <= intentSeq {
			continue
		}
		var result ToolResult
		if err := json.Unmarshal(r.Data, &result); err == nil && result.Call == call {
			found, ok = result, true
		}
	}
	return found, ok
}

// recordedToolIntent finds the newest intent this attempt recorded
// for the call at position call, with its record.
func (j *Journal) recordedToolIntent(call int) (ToolIntent, Record, bool) {
	if call < 0 {
		panic("recordedToolIntent: call must not be negative")
	}
	if len(j.records) > recordsMax {
		panic("recordedToolIntent: records exceed the read bound")
	}
	var found ToolIntent
	var record Record
	ok := false
	for _, r := range j.records {
		if r.Type != TypeToolIntent || j.priorAttempt(r) {
			continue
		}
		var intent ToolIntent
		if err := json.Unmarshal(r.Data, &intent); err == nil && intent.Call == call {
			found, record, ok = intent, r, true
		}
	}
	return found, record, ok
}

// partialOutput gathers, as a JSON array, the entry records the
// holder of the unfinished intent appended after it: what the tool
// managed to report before its worker died.
func (j *Journal) partialOutput(intent Record) json.RawMessage {
	if intent.Type != TypeToolIntent {
		panic("partialOutput: record must be a tool intent")
	}
	if len(j.records) > recordsMax {
		panic("partialOutput: records exceed the read bound")
	}
	holder := taskOf(intent)
	entries := []json.RawMessage{}
	for _, r := range j.records {
		if r.Type == TypeEntry && r.Seq > intent.Seq && r.sameClaim(holder) {
			entries = append(entries, r.Data)
		}
	}
	if len(entries) == 0 {
		return nil
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return nil
	}
	return data
}

// Model requests one model response durably: the response a previous
// holder of this attempt recorded for the call at this position is
// returned without calling fn; otherwise fn runs and its response is
// appended as model.response. A request that fails records nothing
// and leaves the position free, and a new attempt of the iteration
// requests afresh. Streaming tokens are not the journal's business:
// PutStream is a non-durable preview, and a crash mid-stream
// re-sends the request on the next delivery. Model must be called
// from the goroutine that owns the handle.
func (j *Journal) Model(ctx context.Context, fn ModelFunc) ([]byte, error) {
	if fn == nil {
		panic("journal.Model: fn must not be nil")
	}
	if ctx == nil {
		panic("journal.Model: ctx must not be nil")
	}
	defer j.enter("Model")()
	if j.modelCalls >= callsMax {
		return nil, fmt.Errorf("journal: more than %d model calls in one iteration", callsMax)
	}
	call := j.modelCalls
	if output, found := j.recordedModelResponse(call); found {
		j.modelCalls++
		return output, nil
	}
	// No append precedes the request, so check the tail first: a
	// holder already fenced must not spend a model call.
	if err := j.resync(ctx); err != nil {
		return nil, err
	}
	output, err := fn(ctx)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(ModelResponse{Call: call, Output: output})
	if err != nil {
		return nil, fmt.Errorf("marshal model response: %w", err)
	}
	if _, err := j.append(ctx, TypeModelResponse, data); err != nil {
		return nil, err
	}
	j.modelCalls++
	return output, nil
}

// recordedModelResponse finds the response a previous holder recorded
// for the model call at position call.
func (j *Journal) recordedModelResponse(call int) ([]byte, bool) {
	if call < 0 {
		panic("recordedModelResponse: call must not be negative")
	}
	if len(j.records) > recordsMax {
		panic("recordedModelResponse: records exceed the read bound")
	}
	for _, r := range j.records {
		if r.Type != TypeModelResponse || j.priorAttempt(r) {
			continue
		}
		var response ModelResponse
		if err := json.Unmarshal(r.Data, &response); err == nil && response.Call == call {
			return response.Output, true
		}
	}
	return nil, false
}

// resync brings the writer's view of the tail up to date without
// writing: an unmoved tail or a newer record of its own claim (an
// append whose acknowledgement was lost) is fine, a record from
// another claim for this or a later iteration fences it.
func (j *Journal) resync(ctx context.Context) error {
	if ctx == nil {
		panic("journal.resync: ctx must not be nil")
	}
	if j.stream == nil {
		panic("journal.resync: stream must be bound")
	}
	if j.fenced {
		return ErrFenced
	}
	tail, err := j.readTail(ctx)
	if err != nil {
		return err
	}
	switch {
	case tail.Seq == j.seq, tail.Iteration < j.task.Iteration:
		return nil
	case tail.sameClaim(j.task):
		j.seq = tail.Seq
		return nil
	}
	j.fenced = true
	return ErrFenced
}
