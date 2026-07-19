// Methodology: CLAUDE.md prohibits recursion outright, but a self-call is
// invisible to gofmt/vet/staticcheck -- nothing in the toolchain enforces
// the rule. This test enforces it structurally: it parses every non-test
// source file in the package and fails if any top-level function or method
// body contains a call to itself. Direct self-calls only; that is the shape
// the rule has actually been violated in (issue #554), and a full call-graph
// cycle check would need type resolution this leaf package does not warrant.
package cronexpr

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestPackageHasNoDirectRecursion(t *testing.T) {
	// Files are read and parsed individually rather than via
	// parser.ParseDir, which is deprecated as of Go 1.25. The documented
	// replacement pulls in golang.org/x/tools -- a dependency this leaf
	// package does not warrant when the directory holds one package.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fileSet := token.NewFileSet()
	sources := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		if parsed.Name.Name != "cronexpr" {
			t.Fatalf("%s: package %q, want cronexpr", name, parsed.Name.Name)
		}
		sources[name] = parsed
	}

	if len(sources) == 0 {
		t.Fatal("found no non-test source files; test is vacuous")
	}

	inspected := 0
	for path, file := range sources {
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			inspected++
			if callee := findSelfCall(fn); callee != "" {
				t.Errorf(
					"%s: %s calls itself at %s; CLAUDE.md forbids recursion",
					path, callee, fileSet.Position(fn.Body.Pos()))
			}
		}
	}

	if inspected == 0 {
		t.Fatal("inspected no function declarations; test is vacuous")
	}
}

// findSelfCall returns the function's own name if its body calls it directly,
// otherwise "".
func findSelfCall(fn *ast.FuncDecl) string {
	if fn == nil {
		panic("findSelfCall: fn must not be nil")
	}
	if fn.Body == nil {
		panic("findSelfCall: fn body must not be nil")
	}

	found := ""
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		switch target := call.Fun.(type) {
		case *ast.Ident:
			if target.Name == fn.Name.Name {
				found = fn.Name.Name
			}
		case *ast.SelectorExpr:
			// Method self-call: r.method(...) on the receiver.
			if target.Sel.Name == fn.Name.Name && fn.Recv != nil {
				found = fn.Name.Name
			}
		}
		return found == ""
	})
	return found
}
