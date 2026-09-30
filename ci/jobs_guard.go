package ci

// jobs_guard.go screens a jobs: document before the parser walks it. The
// parser reads the yaml.Node tree directly, and yaml.v3 enforces neither its
// alias-expansion limit nor duplicate-key rejection on a Node tree (both run
// only when decoding into Go values). Without this pass a small spec of
// nested aliases expands into gigabytes of job JSON, and `K: secret` then
// `K: plain` in one env block leaves one variable both a secret and a
// literal.

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// jobsNodeMax bounds the screening walk. It is far above any real spec
// (the HTTP layer caps a request at 256 KiB, and every node costs at least
// one byte of source), and it keeps the walk bounded for direct callers of
// CompileYAMLWith that apply no size cap of their own.
const jobsNodeMax = 1 << 20

// jobsDocumentDiagnostics rejects every alias and every duplicate mapping
// key in the document. With no aliases, the tree the parser walks is no
// larger than the source text, so the payload size bound can no longer be
// outrun by expansion.
func jobsDocumentDiagnostics(doc *yaml.Node) []Diagnostic {
	if doc == nil || doc.Kind != yaml.MappingNode {
		panic("jobsDocumentDiagnostics: doc must be a mapping node")
	}
	var diags []Diagnostic
	stack := []*yaml.Node{doc}
	for visited := 0; len(stack) > 0; visited++ {
		if visited >= jobsNodeMax {
			return addDiagnostic(diags, diagAt(doc, "spec", fmt.Sprintf(
				"spec has more than %d YAML nodes", jobsNodeMax)))
		}
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if node.Kind == yaml.AliasNode {
			diags = addDiagnostic(diags, diagAt(node, "spec",
				"aliases (*"+node.Value+") are not supported in a jobs: spec"))
			continue
		}
		if node.Kind == yaml.MappingNode {
			diags = duplicateKeyDiagnostics(node, diags)
		}
		stack = append(stack, node.Content...)
	}
	if len(stack) != 0 {
		panic("jobsDocumentDiagnostics: walk ended with nodes unvisited")
	}
	return diags
}

// duplicateKeyDiagnostics reports every repeat of a key within one mapping,
// at the repeat, so the first declaration reads as the intended one.
func duplicateKeyDiagnostics(node *yaml.Node, diags []Diagnostic) []Diagnostic {
	if node == nil || node.Kind != yaml.MappingNode {
		panic("duplicateKeyDiagnostics: node must be a mapping node")
	}
	if len(node.Content)%2 != 0 {
		panic("duplicateKeyDiagnostics: a mapping holds key/value pairs")
	}
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind == yaml.ScalarNode && seen[key.Value] {
			diags = addDiagnostic(diags, diagAt(key, key.Value,
				fmt.Sprintf("key %q is declared twice in the same mapping", key.Value)))
		}
		seen[key.Value] = true
	}
	return diags
}
