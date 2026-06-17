// Methodology: red-green TDD against the trace-detail span-disclosure
// affordance. The waterfall span tree renders each span name as a native
// <details>/<summary>; expanding it reveals the span's backed KV
// attributes. The summary suppresses the native disclosure marker
// (list-style:none + ::-webkit-details-marker{display:none}) but the
// Norman review caught that no replacement caret was painted — a user saw
// a plain span name with no triangle signalling it is expandable.
//
// This test reads app.css and asserts the summary now carries a visible
// disclosure caret that reuses the existing .config-yaml-toggle idiom: a
// ▸ ::before glyph that rotates 90deg when the <details> is [open].
// Positive space: the caret rule + the open-state rotation exist.
// Negative space: the caret is NOT left bare (it must transition + flip).
package console

import (
	"io/fs"
	"strings"
	"testing"
)

func TestAppCSS_traceSpanSummaryHasDisclosureCaret(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	caret := cssBlock(t, css, ".trace-span-detail > summary::before")
	if !strings.Contains(caret, `content: "▸"`) {
		t.Errorf("trace span summary must paint a ▸ disclosure caret; got %q", caret)
	}
	if !strings.Contains(caret, "transition: transform") {
		t.Errorf("trace span caret must animate its rotation; got %q", caret)
	}

	open := cssBlock(t, css, ".trace-span-detail[open] > summary::before")
	if !strings.Contains(open, "rotate(90deg)") {
		t.Errorf("open trace span must rotate the caret to ▾ (90deg); got %q", open)
	}
}
