package worker

// NonRetryableError wraps an error to signal that retrying will not help.
// The worker framework detects this via errors.As and calls ctx.Fail()
// instead of NakWithDelay, causing immediate permanent failure.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NewNonRetryableError wraps err so the worker framework skips retries.
// Panics if err is nil — a nil non-retryable error is a programmer mistake.
func NewNonRetryableError(err error) *NonRetryableError {
	if err == nil {
		panic("NewNonRetryableError: err must not be nil")
	}
	return &NonRetryableError{Err: err}
}
