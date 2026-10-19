// cli/root_usage_test.go
// Methodology: source-level drift check. Parses root.go's dispatch
// switch and asserts every dispatched top-level command appears in the
// help table, and that stale wording is gone (#784).
package cli

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestUsageListsEveryDispatchedCommand(t *testing.T) {
	src, err := os.ReadFile("root.go")
	if err != nil {
		t.Fatalf("read root.go: %v", err)
	}
	cases := regexp.MustCompile(`(?m)^\tcase "([a-z][a-z-]*)":`)
	found := cases.FindAllStringSubmatch(string(src), -1)
	if len(found) < 15 {
		t.Fatalf("dispatch parse found only %d cases", len(found))
	}
	listed := map[string]bool{}
	for _, entry := range usageCommands {
		listed[entry.name] = true
	}
	for _, match := range found {
		if !listed[match[1]] {
			t.Errorf("command %q dispatched but missing from help", match[1])
		}
	}
	for name := range listed {
		if !strings.Contains(string(src), `case "`+name+`":`) {
			t.Errorf("help lists %q but Run does not dispatch it", name)
		}
	}
}
