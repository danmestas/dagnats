// Package lint holds project-rule checks the standard Go toolchain does
// not perform. CLAUDE.md forbids recursion, but gofmt, go vet, and
// staticcheck all pass on recursive code -- the rule was advisory until
// something enforced it (issue #560).
//
// Ousterhout note: one exported function hiding the whole pipeline --
// walk, parse, build the call graph, find cycles. Callers get a list of
// violations and never see an AST. The alternative, exporting the graph
// so callers could search it themselves, would push the hard part
// (deciding what counts as a self-call) up to every caller.
package lint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// AllowMarker opts a function out of the recursion check. It must appear
// in the function's doc comment, followed by a justification.
const AllowMarker = "recursion:allow"

// Cycle is one recursion violation. Funcs lists the participating
// functions in call order; a single entry means direct self-recursion.
type Cycle struct {
	Funcs []string
	Pos   string
}

// String renders a cycle as a one-line diagnostic.
func (c Cycle) String() string {
	if len(c.Funcs) == 0 {
		panic("Cycle.String: Funcs must not be empty")
	}
	if c.Pos == "" {
		panic("Cycle.String: Pos must not be empty")
	}
	if len(c.Funcs) == 1 {
		return fmt.Sprintf("%s: %s calls itself", c.Pos, c.Funcs[0])
	}
	return fmt.Sprintf("%s: recursion cycle %s",
		c.Pos, strings.Join(append(c.Funcs, c.Funcs[0]), " -> "))
}

type function struct {
	name    string
	pos     string
	allowed bool
	calls   map[string]bool
}

// FindRecursion reports every recursion cycle under root, skipping
// functions whose doc comment carries AllowMarker. Test files are
// scanned too: the rule is about the code we write, not where it lives.
func FindRecursion(root string) ([]Cycle, error) {
	if root == "" {
		panic("FindRecursion: root must not be empty")
	}
	if AllowMarker == "" {
		panic("FindRecursion: AllowMarker must not be empty")
	}

	funcs := map[string]*function{}
	fileSet := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return skipDir(entry.Name())
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		return collectFile(fileSet, path, funcs)
	})
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", root, err)
	}
	return detectCycles(funcs), nil
}

// skipDir keeps the walk out of trees that are not our source: vendored
// or generated code would otherwise be judged by our rules.
func skipDir(name string) error {
	switch name {
	case ".git", "node_modules", "vendor", "testdata", ".claude":
		return filepath.SkipDir
	}
	return nil
}

func collectFile(
	fileSet *token.FileSet, path string, funcs map[string]*function,
) error {
	if fileSet == nil {
		panic("collectFile: fileSet must not be nil")
	}
	if funcs == nil {
		panic("collectFile: funcs must not be nil")
	}

	file, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	pkgDir := filepath.Dir(path)
	for _, decl := range file.Decls {
		decl, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || decl.Body == nil {
			continue
		}
		recv, recvVar := receiver(decl)
		key := qualify(pkgDir, recv, decl.Name.Name)
		existing, seen := funcs[key]
		if !seen {
			existing = &function{
				name:    displayName(recv, decl.Name.Name),
				pos:     fileSet.Position(decl.Pos()).String(),
				allowed: hasAllowMarker(decl.Doc),
				calls:   map[string]bool{},
			}
			funcs[key] = existing
		}
		addCallEdges(decl, pkgDir, recv, recvVar, existing.calls)
	}
	return nil
}

// addCallEdges records who this function calls. Two deliberate rules:
//
// A selector call counts only when its receiver expression IS the
// receiver variable. Matching on method name alone reads e.Err.Error()
// inside Error() as self-recursion -- the false positive that made an
// earlier version of this check unusable (80 findings, 67 of them junk).
//
// Calls launched with `go` are not edges. A goroutine does not grow the
// caller's stack, so it cannot produce the unbounded growth this rule
// exists to prevent.
func addCallEdges(
	decl *ast.FuncDecl,
	pkgDir string,
	recv string,
	recvVar string,
	calls map[string]bool,
) {
	if decl == nil || decl.Body == nil {
		panic("addCallEdges: decl must have a body")
	}
	if calls == nil {
		panic("addCallEdges: calls must not be nil")
	}

	spawned := map[ast.Node]bool{}
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		if statement, isGo := node.(*ast.GoStmt); isGo && statement.Call != nil {
			spawned[statement.Call] = true
		}
		return true
	})

	ast.Inspect(decl.Body, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall || spawned[call] {
			return true
		}
		switch target := call.Fun.(type) {
		case *ast.Ident:
			calls[qualify(pkgDir, "", target.Name)] = true
		case *ast.SelectorExpr:
			ident, isIdent := target.X.(*ast.Ident)
			if isIdent && recvVar != "" && ident.Name == recvVar {
				calls[qualify(pkgDir, recv, target.Sel.Name)] = true
			}
		}
		return true
	})
}

func detectCycles(funcs map[string]*function) []Cycle {
	if funcs == nil {
		panic("detectCycles: funcs must not be nil")
	}
	if AllowMarker == "" {
		panic("detectCycles: AllowMarker must not be empty")
	}

	keys := make([]string, 0, len(funcs))
	for key := range funcs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	const (
		unvisited = 0
		active    = 1
		done      = 2
	)
	state := map[string]int{}
	seen := map[string]bool{}
	var found []Cycle

	// Iterative DFS with an explicit stack -- this file may not recurse.
	for _, start := range keys {
		if state[start] != unvisited {
			continue
		}
		path := []string{start}
		frames := []frame{{key: start, edges: edgesOf(funcs, start)}}
		state[start] = active
		for len(frames) > 0 {
			top := &frames[len(frames)-1]
			if top.next >= len(top.edges) {
				state[top.key] = done
				frames = frames[:len(frames)-1]
				path = path[:len(path)-1]
				continue
			}
			edge := top.edges[top.next]
			top.next++
			if state[edge] == active {
				if cycle, ok := cycleFrom(funcs, path, edge, seen); ok {
					found = append(found, cycle)
				}
				continue
			}
			if state[edge] == done {
				continue
			}
			state[edge] = active
			path = append(path, edge)
			frames = append(frames, frame{key: edge, edges: edgesOf(funcs, edge)})
		}
	}
	sort.Slice(found, func(i, j int) bool {
		return found[i].Pos < found[j].Pos
	})
	return found
}

type frame struct {
	key   string
	edges []string
	next  int
}

// cycleFrom builds the Cycle closing at target, or reports ok=false when
// every participant is allowlisted or the cycle was already recorded.
func cycleFrom(
	funcs map[string]*function, path []string, target string,
	seen map[string]bool,
) (Cycle, bool) {
	if len(path) == 0 {
		panic("cycleFrom: path must not be empty")
	}
	if seen == nil {
		panic("cycleFrom: seen must not be nil")
	}

	at := -1
	for i, key := range path {
		if key == target {
			at = i
			break
		}
	}
	if at < 0 {
		return Cycle{}, false
	}
	members := path[at:]
	// One allowlisted participant clears the cycle: the marker documents
	// a deliberate choice, and every member is part of the same decision.
	names := make([]string, 0, len(members))
	for _, key := range members {
		if funcs[key].allowed {
			return Cycle{}, false
		}
		names = append(names, funcs[key].name)
	}
	fingerprint := strings.Join(sorted(names), "|")
	if seen[fingerprint] {
		return Cycle{}, false
	}
	seen[fingerprint] = true
	return Cycle{Funcs: names, Pos: funcs[members[0]].pos}, true
}

func edgesOf(funcs map[string]*function, key string) []string {
	if funcs == nil {
		panic("edgesOf: funcs must not be nil")
	}
	if key == "" {
		panic("edgesOf: key must not be empty")
	}

	current := funcs[key]
	if current == nil {
		return nil
	}
	edges := make([]string, 0, len(current.calls))
	for callee := range current.calls {
		if _, known := funcs[callee]; known {
			edges = append(edges, callee)
		}
	}
	sort.Strings(edges)
	return edges
}

func hasAllowMarker(doc *ast.CommentGroup) bool {
	if AllowMarker == "" {
		panic("hasAllowMarker: AllowMarker must not be empty")
	}
	if doc == nil {
		return false
	}
	return strings.Contains(doc.Text(), AllowMarker)
}

func receiver(decl *ast.FuncDecl) (typeName string, varName string) {
	if decl == nil {
		panic("receiver: decl must not be nil")
	}
	if decl.Name == nil {
		panic("receiver: decl.Name must not be nil")
	}

	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return "", ""
	}
	field := decl.Recv.List[0]
	if len(field.Names) > 0 {
		varName = field.Names[0].Name
	}
	return baseTypeName(field.Type), varName
}

func baseTypeName(expr ast.Expr) string {
	for {
		switch typed := expr.(type) {
		case *ast.StarExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.IndexListExpr:
			expr = typed.X
		case *ast.Ident:
			return typed.Name
		default:
			return ""
		}
	}
}

func qualify(pkgDir, recv, name string) string {
	return pkgDir + "|" + recv + "." + name
}

func displayName(recv, name string) string {
	if recv == "" {
		return name
	}
	return recv + "." + name
}

func sorted(values []string) []string {
	out := append([]string{}, values...)
	sort.Strings(out)
	return out
}

// ModuleRoot returns the directory holding go.mod at or above start.
func ModuleRoot(start string) (string, error) {
	if start == "" {
		panic("ModuleRoot: start must not be empty")
	}
	if AllowMarker == "" {
		panic("ModuleRoot: package must be initialized")
	}

	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", start, err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod at or above %s", start)
		}
		dir = parent
	}
}
