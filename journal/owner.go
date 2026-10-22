package journal

import (
	"bytes"
	"runtime"
	"strconv"
)

// A Journal is single-goroutine: replay matches calls by position, so
// two goroutines issuing calls would take positions in one order and
// replay them in another. The owner guard turns a concurrent call into
// a panic at the call site instead of a corrupted journal. It is
// re-entrant: a tool function calling back into the journal from the
// goroutine that owns it (partial-output entries) is allowed.

// enter marks the calling goroutine as the handle's owner for the
// duration of a public method and returns the matching leave. It
// panics when another goroutine already owns the handle.
func (j *Journal) enter(method string) func() {
	if method == "" {
		panic("journal.enter: method must not be empty")
	}
	if j.depth < 0 {
		panic("journal.enter: depth must not be negative")
	}
	id := goroutineID()
	if !j.owner.CompareAndSwap(0, id) && j.owner.Load() != id {
		panic("journal." + method + ": a Journal must not be used from more than one goroutine")
	}
	j.depth++
	return func() {
		j.depth--
		if j.depth == 0 {
			j.owner.Store(0)
		}
	}
}

// goroutineStackMax bounds the stack header read for the goroutine id.
const goroutineStackMax = 64

// goroutineID parses the calling goroutine's id from its stack
// header, "goroutine N [". Go exposes no cheaper identity, and the
// header read costs far less than the fenced append it guards.
func goroutineID() uint64 {
	var buf [goroutineStackMax]byte
	n := runtime.Stack(buf[:], false)
	if n == 0 {
		panic("goroutineID: empty stack header")
	}
	header := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	end := bytes.IndexByte(header, ' ')
	if end <= 0 {
		panic("goroutineID: unexpected stack header")
	}
	id, err := strconv.ParseUint(string(header[:end]), 10, 64)
	if err != nil || id == 0 {
		panic("goroutineID: unexpected goroutine id: " + string(header[:end]))
	}
	return id
}
