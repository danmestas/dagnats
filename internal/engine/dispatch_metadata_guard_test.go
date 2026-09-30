// engine/dispatch_metadata_guard_test.go
// Structural guard for #732. The engine builds task payloads in several
// places (first dispatch, iteration, sticky, the timer re-dispatches, the
// DLQ body), and #652 lost StepDef.Metadata simply by rebuilding one of
// them without the field. The functional tests in
// step_metadata_dispatch_test.go cover the first dispatch and the retry
// path; this test covers every construction site at once.
//
// Methodology: parse the package's non-test sources and inspect every
// composite literal of protocol.TaskPayload, and of TimerMessage that
// sets Subject (the re-dispatch timers: a timer that sends a task sets
// its subject, #721). Each must set Metadata. Negative space: the scan
// must find literals of both kinds, so a rename cannot make it pass
// vacuously.
package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryDispatchLiteralCarriesMetadata(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	payloads, timers := 0, 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			switch literalTypeName(lit) {
			case "protocol.TaskPayload":
				payloads++
				requireKey(t, fset, lit, "TaskPayload", "Metadata")
			case "TimerMessage":
				if hasKey(lit, "Subject") {
					timers++
					requireKey(t, fset, lit, "re-dispatch TimerMessage", "Metadata")
				}
			}
			return true
		})
	}
	if payloads == 0 || timers == 0 {
		t.Fatalf("found %d TaskPayload and %d re-dispatch TimerMessage literals; "+
			"the guard no longer sees what it guards", payloads, timers)
	}
}

// literalTypeName renders a composite literal's type as written, e.g.
// "protocol.TaskPayload" or "TimerMessage"; "" for anything else.
func literalTypeName(lit *ast.CompositeLit) string {
	switch typ := lit.Type.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.SelectorExpr:
		if pkg, ok := typ.X.(*ast.Ident); ok {
			return pkg.Name + "." + typ.Sel.Name
		}
	}
	return ""
}

func hasKey(lit *ast.CompositeLit, key string) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if ident, ok := kv.Key.(*ast.Ident); ok && ident.Name == key {
			return true
		}
	}
	return false
}

func requireKey(t *testing.T, fset *token.FileSet, lit *ast.CompositeLit, what, key string) {
	t.Helper()
	if !hasKey(lit, key) {
		t.Errorf("%s: %s literal does not set %s; a worker would receive "+
			"the step without its metadata (#732)", fset.Position(lit.Pos()), what, key)
	}
}
