package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// TypedHandlerFunc is a task handler with typed input and output.
// The worker.Typed wrapper handles JSON marshal/unmarshal so handlers
// work with concrete Go types instead of raw []byte.
type TypedHandlerFunc[I, O any] func(ctx TaskContext, input I) (O, error)

// TypedOption configures the Typed/HandleTyped wrapper. Variadic on
// Typed and HandleTyped keeps existing call sites source-compatible.
// Distinct from HandlerOption (which mutates the Worker at registration
// time): TypedOption mutates only the in-memory wrapper config.
type TypedOption func(*typedConfig)

// typedConfig holds per-wrapper knobs. Defaults are zero-valued —
// callers without options get the historical pass-through behavior.
type typedConfig struct {
	unwrapTrigger bool
}

// UnwrapTrigger asks the Typed wrapper to auto-detect trigger
// envelopes in the task input and unmarshal the typed parameter from
// the envelope's `data` field instead of the raw input. Auto-detect
// is structural: if the input is a JSON object with both a top-level
// `trigger` string AND a top-level `data` field, the input is treated
// as an envelope and `data` is extracted as the unmarshal source.
// Otherwise the input passes through unchanged — plain non-envelope
// inputs still work, e.g. during local unit tests or when the
// workflow is invoked directly without a trigger.
//
// Metadata access (trigger kind, source, timestamp) is out of scope
// for v1. Workers that need those fields should drop to ctx.Input()
// and unmarshal the full envelope manually. See issue #229 for the
// path to first-class metadata access.
func UnwrapTrigger() TypedOption {
	return func(c *typedConfig) {
		if c == nil {
			panic("UnwrapTrigger: config must not be nil")
		}
		c.unwrapTrigger = true
	}
}

// HandleTyped registers a typed task handler that automatically
// marshals/unmarshals JSON. Combines Typed() and Handle() into a
// single call so workers don't need to know about the wrapping.
// Optional TypedOption values (e.g. UnwrapTrigger) tune the wrapper.
func HandleTyped[I, O any](
	w *Worker, taskType string, fn TypedHandlerFunc[I, O],
	opts ...TypedOption,
) {
	if w == nil {
		panic("HandleTyped: worker must not be nil")
	}
	if taskType == "" {
		panic("HandleTyped: taskType must not be empty")
	}
	w.Handle(taskType, Typed(fn, opts...))
}

// Typed wraps a TypedHandlerFunc into a HandlerFunc by handling JSON
// serialization. Marshal/unmarshal failures are wrapped in
// NonRetryableError because bad serialization will not fix itself on
// retry. Optional TypedOption values tune the wrapper.
func Typed[I, O any](
	fn TypedHandlerFunc[I, O], opts ...TypedOption,
) HandlerFunc {
	if fn == nil {
		panic("Typed: fn must not be nil")
	}
	cfg := typedConfig{}
	for _, opt := range opts {
		if opt == nil {
			panic("Typed: opt must not be nil")
		}
		opt(&cfg)
	}
	handler := func(ctx TaskContext) error {
		raw := ctx.Input()
		if cfg.unwrapTrigger && len(raw) > 0 {
			data, ok, err := extractEnvelopeData(raw)
			if err != nil {
				return NewNonRetryableError(
					fmt.Errorf("detect envelope: %w", err),
				)
			}
			if ok {
				raw = data
			}
		}
		return invokeTyped(ctx, raw, fn)
	}
	return handler
}

// invokeTyped unmarshals raw into I, calls fn, marshals the result,
// and calls Complete. Extracted to keep Typed under the 70-line limit
// and to make the unmarshal-then-call-then-marshal flow read linearly.
func invokeTyped[I, O any](
	ctx TaskContext, raw []byte, fn TypedHandlerFunc[I, O],
) error {
	if ctx == nil {
		panic("invokeTyped: ctx must not be nil")
	}
	if fn == nil {
		panic("invokeTyped: fn must not be nil")
	}
	var input I
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &input); err != nil {
			return NewNonRetryableError(
				fmt.Errorf("unmarshal input: %w", err),
			)
		}
	}
	output, err := fn(ctx, input)
	if err != nil {
		return err
	}
	data, err := json.Marshal(output)
	if err != nil {
		return NewNonRetryableError(
			fmt.Errorf("marshal output: %w", err),
		)
	}
	return ctx.Complete(data)
}

// extractEnvelopeData inspects raw to see if it looks like a
// TriggerEnvelope. Returns (dataBytes, true, nil) only when raw is a
// JSON object with both a top-level "trigger" string AND a top-level
// "data" field. The returned bytes are the verbatim JSON value of
// "data" (no remarshal), so json.RawMessage round-trips cleanly.
// Returns (nil, false, nil) on any structural mismatch — non-object,
// missing key, wrong type. Returns an error only on malformed JSON
// (which the caller surfaces as NonRetryableError, since redelivery
// will not fix bad bytes).
func extractEnvelopeData(raw []byte) ([]byte, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	if len(raw) > envelopeScanLimitBytes {
		return nil, false, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, false, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, false, nil
	}
	return scanEnvelopeFields(dec)
}

// envelopeScanLimitBytes bounds how large an input the auto-detect
// will scan. Above this size the wrapper treats the input as a
// passthrough — better to err on the side of "not an envelope" than
// to walk a megabyte of JSON looking for two keys. Real envelopes
// produced by the trigger service are well under this cap.
const envelopeScanLimitBytes = 4 * 1024 * 1024

// scanEnvelopeFields reads keys at the top of the already-opened
// object and gathers the "trigger" string presence and the "data"
// field's verbatim bytes. Bounded by a key cap so a pathological
// input still terminates in linear time.
func scanEnvelopeFields(
	dec *json.Decoder,
) ([]byte, bool, error) {
	const keyCapMax = 256
	if dec == nil {
		panic("scanEnvelopeFields: dec must not be nil")
	}
	hasTrigger := false
	var dataBytes []byte
	for i := 0; i < keyCapMax && dec.More(); i++ {
		key, err := readKey(dec)
		if err != nil {
			return nil, false, err
		}
		switch key {
		case "trigger":
			ok, err := readTriggerString(dec)
			if err != nil {
				return nil, false, err
			}
			hasTrigger = ok
		case "data":
			var rm json.RawMessage
			if err := dec.Decode(&rm); err != nil {
				return nil, false, err
			}
			dataBytes = []byte(rm)
		default:
			if err := skipValue(dec); err != nil {
				return nil, false, err
			}
		}
	}
	if hasTrigger && dataBytes != nil {
		return dataBytes, true, nil
	}
	return nil, false, nil
}

// readKey reads the next JSON field name from the decoder.
func readKey(dec *json.Decoder) (string, error) {
	if dec == nil {
		panic("readKey: dec must not be nil")
	}
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	s, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("readKey: token %T not a string", tok)
	}
	return s, nil
}

// readTriggerString reads the value of the top-level "trigger" key.
// Returns true only when the value is a non-empty JSON string —
// matches the envelope contract from internal/trigger.TriggerEnvelope.
// Other types (number, bool, object, array, null) → false, with the
// decoder positioned past the value either way.
func readTriggerString(dec *json.Decoder) (bool, error) {
	if dec == nil {
		panic("readTriggerString: dec must not be nil")
	}
	var rm json.RawMessage
	if err := dec.Decode(&rm); err != nil {
		return false, err
	}
	if len(rm) == 0 {
		return false, nil
	}
	if rm[0] != '"' {
		return false, nil
	}
	var s string
	if err := json.Unmarshal(rm, &s); err != nil {
		return false, nil
	}
	return s != "", nil
}

// skipValue discards the next value from the decoder. Used for keys
// other than "trigger" and "data" so the decoder advances past them.
func skipValue(dec *json.Decoder) error {
	if dec == nil {
		panic("skipValue: dec must not be nil")
	}
	var rm json.RawMessage
	if err := dec.Decode(&rm); err != nil {
		return err
	}
	if len(rm) == 0 {
		return fmt.Errorf("skipValue: empty value")
	}
	return nil
}
