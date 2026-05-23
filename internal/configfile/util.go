// internal/configfile/util.go
// Tiny helpers kept separate from the apply / load logic so the
// reader doesn't have to scroll past plumbing to see policy.
package configfile

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvOpTimeout caps each KV op so a wedged JetStream doesn't hang
// the watcher's reload pipeline. 5s is the same order of magnitude
// the rest of the codebase uses for KV operations.
const kvOpTimeout = 5 * time.Second

// contextWithTimeout wraps ctx with a kvOpTimeout deadline.
// Returns the new context and its cancel so callers can defer the
// cancel cleanly.
func contextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		panic("contextWithTimeout: ctx must not be nil")
	}
	return context.WithTimeout(ctx, kvOpTimeout)
}

// isNoKeysFound recognises the jetstream "no keys yet" sentinel as
// a benign empty-bucket signal rather than a real error.
func isNoKeysFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, jetstream.ErrNoKeysFound)
}

// appendErr appends only non-nil entries from add. Keeps the apply
// path readable when both workflow and trigger sides may contribute
// errors.
func appendErr(dst []error, add []error) []error {
	if len(add) == 0 {
		return dst
	}
	for _, e := range add {
		if e == nil {
			continue
		}
		dst = append(dst, e)
	}
	return dst
}

// joinErrs returns a single error whose String() concatenates each
// child message. errors.Join would give the same semantic shape, but
// we get tighter control over presentation for the operator log.
func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, e.Error())
	}
	return errors.New(strings.Join(parts, "; "))
}
