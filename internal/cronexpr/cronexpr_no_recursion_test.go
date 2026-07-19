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
	"io/fs"
	"strings"
	"testing"
)

func TestPackageHasNoDirectRecursion(t *testing.T) {
	fileSet := token.NewFileSet()
	pkgs, err := parser.ParseDir(fileSet, ".", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package source: %v", err)
	}

	pkg, ok := pkgs["cronexpr"]
	if !ok {
		t.Fatalf("package cronexpr not found in parsed dirs %v", pkgs)
	}

	inspected := 0
	for path, file := range pkg.Files {
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
