package worker

import "time"

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
	if err.Error() == "" {
		panic("NewNonRetryableError: err message must not be empty")
	}
	return &NonRetryableError{Err: err}
}

// RateLimitError wraps an error to signal a rate limit was hit.
// The worker framework detects this via errors.As and calls
// ctx.FailRetryAfter with the specified delay instead of using
// the default NAK backoff.
type RateLimitError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string { return e.Err.Error() }
func (e *RateLimitError) Unwrap() error { return e.Err }

// NewRateLimitError wraps err with a suggested retry delay.
// Panics if err is nil or retryAfter is not positive.
func NewRateLimitError(err error, retryAfter time.Duration) *RateLimitError {
	if err == nil {
		panic("NewRateLimitError: err must not be nil")
	}
	if retryAfter <= 0 {
		panic("NewRateLimitError: retryAfter must be positive")
	}
	return &RateLimitError{Err: err, RetryAfter: retryAfter}
}
