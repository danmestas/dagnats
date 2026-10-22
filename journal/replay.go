package journal

import (
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

// ToolResult is the body of a tool.result record. Interrupted marks a
// call a previous holder started and never finished, which was not
// rerun; Partial then holds, as a JSON array, the entry records that
// holder appended while the tool ran.
type ToolResult struct {
	Call        int             `json:"call"`
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
// its replay declaration, runs fn, and appends the result. On a claim
// that recovers an iteration a previous holder started, the call is
// matched by position to that holder's records: a recorded result is
// returned without running fn; an intent without a result is rerun
// only when the recorded and the current declaration both say Safe,
// and otherwise answered with an interrupted result carrying the
// partial output, which the model then sees. The returned error is
// the journal's (ErrFenced, ErrCommitted, transport), never the
// tool's own, which is in the result.
func (j *Journal) Tool(
	ctx context.Context, call ToolCall, replay Replay, fn ToolFunc,
) (ToolResult, error) {
	if replay != Safe && replay != Unsafe {
		panic("journal.Tool: replay must be Safe or Unsafe")
	}
	if fn == nil || call.Name == "" {
		panic("journal.Tool: fn and call.Name must not be empty")
	}
	if j.toolCalls >= callsMax {
		return ToolResult{}, fmt.Errorf("journal: more than %d tool calls in one iteration", callsMax)
	}
	call.Call = j.toolCalls
	call.Key = j.task.RunID + "." + j.task.StepID + "." +
		strconv.Itoa(j.task.Iteration) + "." + strconv.Itoa(call.Call)
	j.toolCalls++
	if result, found := j.recordedToolResult(call.Call); found {
		return result, nil
	}
	intent, found := j.recordedToolIntent(call.Call)
	if !found {
		data, err := json.Marshal(ToolIntent{ToolCall: call, Replay: replay})
		if err != nil {
			return ToolResult{}, fmt.Errorf("marshal tool intent: %w", err)
		}
		if _, err := j.append(ctx, TypeToolIntent, data); err != nil {
			return ToolResult{}, err
		}
		return j.runTool(ctx, call, fn)
	}
	if intent.Replay == Safe && replay == Safe {
		return j.runTool(ctx, call, fn)
	}
	return j.appendToolResult(ctx, ToolResult{
		Call: call.Call, Error: ErrInterrupted, Interrupted: true,
		Partial: j.partialOutput(call.Call),
	})
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
	result := ToolResult{Call: call.Call, Output: output}
	if err != nil {
		result.Error = err.Error()
	}
	return j.appendToolResult(ctx, result)
}

// appendToolResult records result and returns it.
func (j *Journal) appendToolResult(ctx context.Context, result ToolResult) (ToolResult, error) {
	if result.Call < 0 {
		panic("appendToolResult: call must not be negative")
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

// recordedToolResult finds the result a previous holder recorded for
// the call at position call.
func (j *Journal) recordedToolResult(call int) (ToolResult, bool) {
	if call < 0 {
		panic("recordedToolResult: call must not be negative")
	}
	if len(j.records) > recordsMax {
		panic("recordedToolResult: records exceed the read bound")
	}
	for _, r := range j.records {
		if r.Type != TypeToolResult {
			continue
		}
		var result ToolResult
		if err := json.Unmarshal(r.Data, &result); err == nil && result.Call == call {
			return result, true
		}
	}
	return ToolResult{}, false
}

// recordedToolIntent finds the intent a previous holder recorded for
// the call at position call, and its journal sequence.
func (j *Journal) recordedToolIntent(call int) (ToolIntent, bool) {
	if call < 0 {
		panic("recordedToolIntent: call must not be negative")
	}
	if len(j.records) > recordsMax {
		panic("recordedToolIntent: records exceed the read bound")
	}
	for _, r := range j.records {
		if r.Type != TypeToolIntent {
			continue
		}
		var intent ToolIntent
		if err := json.Unmarshal(r.Data, &intent); err == nil && intent.Call == call {
			return intent, true
		}
	}
	return ToolIntent{}, false
}

// partialOutput gathers, as a JSON array, the entry records the
// holder of the unfinished intent for call appended after it: what
// the tool managed to report before its worker died.
func (j *Journal) partialOutput(call int) json.RawMessage {
	if call < 0 {
		panic("partialOutput: call must not be negative")
	}
	if len(j.records) > recordsMax {
		panic("partialOutput: records exceed the read bound")
	}
	var holder Task
	entries := []json.RawMessage{}
	for _, r := range j.records {
		switch {
		case r.Type == TypeToolIntent:
			var intent ToolIntent
			if err := json.Unmarshal(r.Data, &intent); err == nil && intent.Call == call {
				holder = taskOf(r)
			}
		case r.Type == TypeEntry && holder.Delivered > 0 && r.sameClaim(holder):
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
// holder recorded for the call at this position is returned without
// calling fn; otherwise fn runs and its response is appended as
// model.response. Streaming tokens are not the journal's business:
// PutStream is a non-durable preview, and a crash mid-stream
// re-sends the request on the next delivery.
func (j *Journal) Model(ctx context.Context, fn ModelFunc) ([]byte, error) {
	if fn == nil {
		panic("journal.Model: fn must not be nil")
	}
	if ctx == nil {
		panic("journal.Model: ctx must not be nil")
	}
	if j.modelCalls >= callsMax {
		return nil, fmt.Errorf("journal: more than %d model calls in one iteration", callsMax)
	}
	call := j.modelCalls
	j.modelCalls++
	if output, found := j.recordedModelResponse(call); found {
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
		if r.Type != TypeModelResponse {
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

// errNoResponse is what ScriptedModel returns past its script.
var errNoResponse = errors.New("scripted model: no response left")
