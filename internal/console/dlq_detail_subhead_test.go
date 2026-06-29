// dlq_detail_subhead_test.go pins an explicit readable color on the
// DLQ-detail "Error message" / "Original input" H3 sub-headers.
//
// Methodology:
//   - The H3s previously carried only inline margin/size styles, so the
//     text color fell through to basecoat's light-mode foreground and
//     computed near-black on the dark console surface — the same
//     inherit-basecoat-light-foreground bug fixed for .console-step-name.
//   - Render the DLQ-detail page through the handler and assert the H3s
//     carry the dlq-detail-subhead hook (positive markup).
//   - Fetch the served app.css and assert the .dlq-detail-subhead rule
//     pins an explicit color token rather than inheriting (positive CSS).
//   - Own Mount per assertion; nothing shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/api"
)

// TestDLQDetailSubhead_hasExplicitColorClass asserts the DLQ-detail
// sub-headers carry the dlq-detail-subhead class and the served CSS
// pins it to an explicit text token. RED before the fix (H3s had no
// color hook), GREEN after.
func TestDLQDetailSubhead_hasExplicitColorClass(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(7, "deadline exceeded waiting on task"),
	}
	body := getPage(t, fake, "/console/dlq/7")
	if !strings.Contains(body, `class="dlq-detail-subhead"`) {
		t.Errorf("DLQ-detail sub-headers must carry dlq-detail-subhead hook; body had no such class")
	}

	css := servedDLQAppCSS(t, fake)
	idx := strings.Index(css, ".dlq-detail-subhead {")
	if idx < 0 {
		t.Fatalf("app.css missing .dlq-detail-subhead rule")
	}
	end := strings.Index(css[idx:], "}")
	if end < 0 {
		t.Fatalf(".dlq-detail-subhead rule has no closing brace")
	}
	block := css[idx : idx+end]
	if !strings.Contains(block, "color: var(--text-") {
		t.Errorf(".dlq-detail-subhead must pin an explicit --text-* token; got\n%s", block)
	}
}

// servedDLQAppCSS fetches /console/assets/app.css through the given
// fake's Mount.
func servedDLQAppCSS(t *testing.T, fake *fakeDataSource) string {
	t.Helper()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	return rr.Body.String()
}
