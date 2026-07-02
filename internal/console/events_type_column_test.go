// events_type_column_test.go guards the run-detail Events table against
// the Type-column overflow: the shared .console-table is table-layout:
// fixed, so a too-narrow Type column let a long event-type badge
// (workflow.child.completed, white-space:nowrap) spill into the Step
// column. Type must be wide enough to hold it, and the badge must be
// allowed to wrap as a belt-and-suspenders guarantee against overlap.
// Reuses cssBlock from borderless_cards_test.go.
package console

import (
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestAppCSS_eventsTypeColumnFitsBadge(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// The Type column must be wide enough for the longest event-type
	// badge. 150px (the regression) overflowed; require a comfortably
	// wider fixed width.
	typeCol := cssBlock(t, css, "#run-detail-events thead th:nth-child(2)")
	m := regexp.MustCompile(`width:\s*(\d+)px`).FindStringSubmatch(typeCol)
	if m == nil {
		t.Fatalf("Type column has no px width: %q", typeCol)
	}
	w, _ := strconv.Atoi(m[1])
	if w < 200 {
		t.Errorf("Type column width %dpx too narrow for the longest event "+
			"badge (regression: was 150px, overflowed into Step)", w)
	}

	// Belt-and-suspenders: the Type badge must wrap rather than overflow,
	// so an even longer event type can never overlap the Step column.
	badge := cssBlock(t, css, "#run-detail-events td:nth-child(2) .badge")
	if !strings.Contains(badge, "white-space: normal") {
		t.Errorf("Type badge must wrap (white-space: normal); got %q", badge)
	}
}
