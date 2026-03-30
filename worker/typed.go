package worker

import (
	"encoding/json"
	"fmt"
)

// TypedHandlerFunc is a task handler with typed input and output.
// The worker.Typed wrapper handles JSON marshal/unmarshal so handlers
// work with concrete Go types instead of raw []byte.
type TypedHandlerFunc[I, O any] func(ctx TaskContext, input I) (O, error)

// Typed wraps a TypedHandlerFunc into a HandlerFunc by handling JSON
// serialization. Marshal/unmarshal failures are wrapped in NonRetryableError
// because bad serialization will not fix itself on retry.
func Typed[I, O any](fn TypedHandlerFunc[I, O]) HandlerFunc {
	if fn == nil {
		panic("Typed: fn must not be nil")
	}
	return func(ctx TaskContext) error {
		var input I
		if len(ctx.Input()) > 0 {
			if err := json.Unmarshal(ctx.Input(), &input); err != nil {
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
}
