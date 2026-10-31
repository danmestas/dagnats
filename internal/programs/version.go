package programs

import (
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/program"
)

// rangeComparatorsMax bounds a Manifest.Dagnats range; real ones have
// two (">=0.0.35 <0.1.0").
const rangeComparatorsMax = 8

// checkVersion refuses a program whose Dagnats range excludes the
// server version. An empty range admits every version, and so does a
// server version that is not MAJOR.MINOR.PATCH ("dev", an unstamped
// build), so local builds never lock programs out.
func checkVersion(expr, serverVersion string) error {
	server, ok := parseVersion(serverVersion)
	if expr == "" || !ok {
		return nil
	}
	comparators := strings.Fields(expr)
	if len(comparators) > rangeComparatorsMax {
		return refused(program.RefusedVersionIncompatible,
			"range %q has more than %d comparators", expr, rangeComparatorsMax)
	}
	for _, comparator := range comparators {
		op, text := splitOperator(comparator)
		bound, ok := parseVersion(text)
		if !ok {
			return refused(program.RefusedVersionIncompatible,
				"range %q: %q is not MAJOR.MINOR.PATCH", expr, text)
		}
		if !compareHolds(op, compareVersions(server, bound)) {
			return refused(program.RefusedVersionIncompatible,
				"program wants dagnats %s; this is %s", expr, serverVersion)
		}
	}
	return nil
}

func splitOperator(comparator string) (op, version string) {
	for _, candidate := range []string{">=", "<=", ">", "<", "="} {
		if rest, ok := strings.CutPrefix(comparator, candidate); ok {
			return candidate, rest
		}
	}
	return "=", comparator
}

func compareHolds(op string, order int) bool {
	switch op {
	case ">=":
		return order >= 0
	case "<=":
		return order <= 0
	case ">":
		return order > 0
	case "<":
		return order < 0
	case "=":
		return order == 0
	}
	panic("programs.compareHolds: unknown operator " + op)
}

// parseVersion reads "[v]MAJOR.MINOR.PATCH", ignoring any "-pre" or
// "+build" suffix.
func parseVersion(text string) ([3]int, bool) {
	text = strings.TrimPrefix(text, "v")
	if cut := strings.IndexAny(text, "-+"); cut >= 0 {
		text = text[:cut]
	}
	parts := strings.Split(text, ".")
	var out [3]int
	if len(parts) != len(out) {
		return out, false
	}
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return out, false
		}
		out[i] = value
	}
	return out, true
}

func compareVersions(a, b [3]int) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
