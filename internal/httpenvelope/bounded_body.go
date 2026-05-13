// Package httpenvelope is the single home for HTTP-trigger body bounding
// and envelope construction. Webhook and HTTP triggers both reach into
// this package so the body limit + envelope shape never drift under
// independent maintenance.
package httpenvelope

import (
	"errors"
	"fmt"
	"io"
)

// ErrBodyTooLarge is returned by BoundedBody when the reader yields more
// than max bytes. Callers should map this to HTTP 413.
var ErrBodyTooLarge = errors.New("request body too large")

// BoundedBody reads at most max bytes from r. Returns the full body on
// success, ErrBodyTooLarge if the reader has more than max bytes, or an
// I/O error otherwise. Empty bodies return a zero-length slice and no
// error. Panics on programmer errors (nil reader, non-positive max);
// these are bound by the caller's HTTPConfig validation and would
// indicate a configuration drift rather than a runtime input.
func BoundedBody(r io.Reader, max int64) ([]byte, error) {
	if r == nil {
		panic("BoundedBody: reader must not be nil")
	}
	if max <= 0 {
		panic(fmt.Sprintf("BoundedBody: max must be > 0, got %d", max))
	}

	// Read max+1 so we can detect overflow without exhausting an
	// arbitrarily large body. ReadAll on a bounded reader stops at the
	// limit; we only need one extra byte to know we'd have overflowed.
	limited := io.LimitReader(r, max+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(data)) > max {
		return nil, ErrBodyTooLarge
	}
	return data, nil
}
