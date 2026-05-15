package console

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
)

// sseWriter is a minimal in-house Datastar-compatible SSE writer.
// It emits `event: datastar-patch-elements` events whose `data:` lines
// carry the HTML fragment to patch into the DOM by matching id. This
// is the same wire shape the Datastar client expects — see ADR-014.
//
// We write our own instead of importing a Go SDK because the surface
// is tiny (about 30 lines), and dagnats's CLAUDE.md rule is "if you
// can write the 50 lines yourself, do it."
type sseWriter struct {
	w  http.ResponseWriter
	bw *bufio.Writer
	fl http.Flusher
}

func newSSEWriter(w http.ResponseWriter) (*sseWriter, error) {
	if w == nil {
		panic("newSSEWriter: w is nil")
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	return &sseWriter{w: w, bw: bufio.NewWriter(w), fl: fl}, nil
}

// PatchElements writes a Datastar `datastar-patch-elements` SSE event
// whose data lines contain the rendered HTML. The fragment must
// include an `id="..."` attribute on the root element it patches.
// Multi-line HTML is folded into one `data:` line per source line per
// the SSE spec.
func (s *sseWriter) PatchElements(html string) error {
	if s == nil {
		panic("PatchElements: s is nil")
	}
	if s.bw == nil {
		panic("PatchElements: writer not initialised")
	}
	if _, err := s.bw.WriteString("event: datastar-patch-elements\n"); err != nil {
		return fmt.Errorf("sse write event: %w", err)
	}
	if err := writeSSEData(s.bw, "elements "+html); err != nil {
		return err
	}
	if _, err := s.bw.WriteString("\n"); err != nil {
		return fmt.Errorf("sse write terminator: %w", err)
	}
	if err := s.bw.Flush(); err != nil {
		return fmt.Errorf("sse flush: %w", err)
	}
	s.fl.Flush()
	return nil
}

// writeSSEData splits payload into newline-terminated `data:` records.
// Bounded by the input length; no recursion. Returns the first write
// error encountered.
func writeSSEData(bw *bufio.Writer, payload string) error {
	if bw == nil {
		panic("writeSSEData: bw is nil")
	}
	if payload == "" {
		_, err := bw.WriteString("data:\n")
		if err != nil {
			return fmt.Errorf("sse write empty data: %w", err)
		}
		return nil
	}
	// Bound: payload length is the upper bound; SplitN avoids
	// pathological reallocations on huge fragments.
	lines := strings.Split(payload, "\n")
	const maxLines = 4096
	if len(lines) > maxLines {
		return fmt.Errorf("sse payload exceeds %d lines", maxLines)
	}
	for _, ln := range lines {
		if _, err := bw.WriteString("data: "); err != nil {
			return fmt.Errorf("sse write data prefix: %w", err)
		}
		if _, err := bw.WriteString(ln); err != nil {
			return fmt.Errorf("sse write data line: %w", err)
		}
		if _, err := bw.WriteString("\n"); err != nil {
			return fmt.Errorf("sse write data newline: %w", err)
		}
	}
	return nil
}
