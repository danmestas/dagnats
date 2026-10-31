package program

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// Bounds on a manifest. They also bound the permission list a manifest
// generates, which the server evaluates on every publish.
const (
	nameLenMin         = 2
	nameLenMax         = 32
	taskTypeCountMax   = 128
	workflowCountMax   = 256
	streamCountMax     = 16
	bucketCountMax     = 32
	readCountMax       = 32
	protocolCountMax   = 16
	dottedLenMax       = 128
	versionLenMax      = 64
	consoleLabelLenMax = 64
	consoleURLLenMax   = 2048
	bucketHistoryMax   = 64
)

// engineSubjectRoots are the first subject tokens the engine's streams
// and signals own. A program named after one would be granted "root.>".
var engineSubjectRoots = []string{
	"api", "dagnats", "dead", "event", "history", "journal", "logs",
	"scheduled", "sleep", "stream", "sticky", "task", "telemetry", "trigger",
}

// reservedNames shadow JetStream's own stream prefixes: a program named
// "kv" would own streams "KV_*", which are every bucket's stream.
var reservedNames = []string{"kv", "obj"}

// Validate checks names, prefixes, sizes, and that Protocols are not
// under the program's own prefix or an engine root. Pure; used by
// registration, by Use, and by a program's own tests. Whether a protocol
// falls under another registered program's prefix needs the registry and
// is checked at registration.
func Validate(m Manifest) error {
	if err := validateName(m.Name); err != nil {
		return fmt.Errorf("program: manifest: %w", err)
	}
	errs := []error{validateCounts(m)}
	errs = append(errs, validateTaskTypes(m.Name, m.TaskTypes)...)
	errs = append(errs, validateWorkflows(m.Name, m.Workflows)...)
	errs = append(errs, validateStreams(m.Name, m.Streams)...)
	errs = append(errs, validateBuckets(m.Name, m.Buckets)...)
	errs = append(errs, validateReads(m.Name, m.Reads)...)
	errs = append(errs, validateProtocols(m.Name, m.Protocols)...)
	errs = append(errs, validateLabels(m))
	if len(errs) < 2 {
		panic("program.Validate: every rule group must report")
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("program: manifest %q: %w", m.Name, err)
	}
	return nil
}

// ValidateName checks only a manifest's Name, the first rule Validate
// applies. Registration uses it to tell name_invalid apart from the
// other manifest errors.
func ValidateName(name string) error {
	return validateName(name)
}

func validateName(name string) error {
	if len(name) < nameLenMin || len(name) > nameLenMax {
		return fmt.Errorf("name %q must be %d to %d bytes", name, nameLenMin, nameLenMax)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		lower := c >= 'a' && c <= 'z'
		if lower || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return fmt.Errorf("name %q must match [a-z][a-z0-9]*", name)
	}
	if reason := reservedBy(name); reason != "" {
		return fmt.Errorf("name %q is reserved: %s", name, reason)
	}
	return nil
}

// reservedBy names the engine resource a program name would shadow, or
// returns "" when the name is free. Derived from what SetupAll
// provisions, so a new engine stream or bucket is covered automatically.
func reservedBy(name string) string {
	if name == "" {
		panic("program.reservedBy: name must not be empty")
	}
	if slices.Contains(engineSubjectRoots, name) {
		return "engine subject root " + name + ".>"
	}
	if slices.Contains(reservedNames, name) {
		return "JetStream stream prefix " + strings.ToUpper(name) + "_"
	}
	upper := strings.ToUpper(name)
	for _, s := range natsutil.ProvisionedStreamNames() {
		if s == upper || strings.HasPrefix(s, upper+"_") {
			return "engine stream " + s
		}
	}
	for _, b := range natsutil.ProvisionedKVBucketNames() {
		if b == name || strings.HasPrefix(b, name+"_") {
			return "engine bucket " + b
		}
	}
	return ""
}

func validateCounts(m Manifest) error {
	limits := []struct {
		what  string
		count int
		max   int
	}{
		{"task types", len(m.TaskTypes), taskTypeCountMax},
		{"workflows", len(m.Workflows), workflowCountMax},
		{"streams", len(m.Streams), streamCountMax},
		{"buckets", len(m.Buckets), bucketCountMax},
		{"reads", len(m.Reads), readCountMax},
		{"protocols", len(m.Protocols), protocolCountMax},
	}
	var errs []error
	for _, l := range limits {
		if l.count > l.max {
			errs = append(errs, fmt.Errorf("%d %s, max %d", l.count, l.what, l.max))
		}
	}
	return errors.Join(errs...)
}

func validateTaskTypes(name string, types []string) []error {
	if name == "" {
		panic("program.validateTaskTypes: name must not be empty")
	}
	var errs []error
	durables := map[string]string{}
	for _, t := range types {
		if err := underPrefix("task type", t, name+"."); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := dag.ValidTaskType(t); err != nil {
			errs = append(errs, err)
			continue
		}
		durable := consumername.NameFor(t, "")
		if prior, ok := durables[durable]; ok {
			errs = append(errs, fmt.Errorf(
				"task types %q and %q share worker consumer %s", prior, t, durable))
			continue
		}
		durables[durable] = t
	}
	if len(durables) > len(types) {
		panic("program.validateTaskTypes: more durables than task types")
	}
	return errs
}

func validateWorkflows(name string, workflows []string) []error {
	var errs []error
	seen := map[string]bool{}
	for _, w := range workflows {
		if err := underPrefix("workflow", w, name+"."); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := validDotted("workflow", w); err != nil {
			errs = append(errs, err)
			continue
		}
		if seen[w] {
			errs = append(errs, fmt.Errorf("workflow %q declared twice", w))
		}
		seen[w] = true
	}
	return errs
}

func validateStreams(name string, streams []StreamSpec) []error {
	prefix := strings.ToUpper(name) + "_"
	var errs []error
	seen := map[string]bool{}
	for _, s := range streams {
		if !suffixMatches(s.Name, prefix, 'A', 'Z') {
			errs = append(errs, fmt.Errorf(
				"stream %q must be %q followed by A-Z, 0-9 or _", s.Name, prefix))
			continue
		}
		if seen[s.Name] {
			errs = append(errs, fmt.Errorf("stream %q declared twice", s.Name))
		}
		seen[s.Name] = true
		errs = append(errs, validateStreamSize(s), validateStreamSubjects(name, s))
	}
	return errs
}

func validateStreamSize(s StreamSpec) error {
	switch {
	case s.MaxBytesFraction < 0 || s.MaxBytesFraction > 1:
		return fmt.Errorf("stream %q fraction %v outside [0, 1]", s.Name, s.MaxBytesFraction)
	case s.MaxBytesFloor < 0:
		return fmt.Errorf("stream %q floor %d is negative", s.Name, s.MaxBytesFloor)
	case s.MaxBytesFraction == 0 && s.MaxBytesFloor == 0:
		return fmt.Errorf("stream %q needs a size: a fraction, a floor, or both", s.Name)
	case s.MaxAge < 0:
		return fmt.Errorf("stream %q max age %v is negative", s.Name, s.MaxAge)
	}
	return nil
}

func validateStreamSubjects(name string, s StreamSpec) error {
	if len(s.Subjects) == 0 {
		return fmt.Errorf("stream %q needs at least one subject", s.Name)
	}
	for _, subject := range s.Subjects {
		tokens := strings.Split(subject, ".")
		if tokens[0] != name || len(tokens) < 2 || len(subject) > dottedLenMax {
			return fmt.Errorf("stream %q subject %q is not under %s.>", s.Name, subject, name)
		}
		for i, token := range tokens[1:] {
			last := i == len(tokens)-2
			if token == "*" || (token == ">" && last) {
				continue
			}
			if token == "" || strings.IndexFunc(token, notTokenRune) >= 0 {
				return fmt.Errorf("stream %q subject %q has an invalid token", s.Name, subject)
			}
		}
	}
	return nil
}

func validateBuckets(name string, buckets []BucketSpec) []error {
	prefix := name + "_"
	var errs []error
	seen := map[string]bool{}
	for _, b := range buckets {
		switch {
		case !suffixMatches(b.Name, prefix, 'a', 'z'):
			errs = append(errs, fmt.Errorf(
				"bucket %q must be %q followed by a-z, 0-9 or _", b.Name, prefix))
		case seen[b.Name]:
			errs = append(errs, fmt.Errorf("bucket %q declared twice", b.Name))
		case b.MaxBytes <= 0:
			errs = append(errs, fmt.Errorf("bucket %q max_bytes must be positive", b.Name))
		case b.History > bucketHistoryMax:
			errs = append(errs, fmt.Errorf(
				"bucket %q history %d, max %d", b.Name, b.History, bucketHistoryMax))
		case b.TTL < 0:
			errs = append(errs, fmt.Errorf("bucket %q ttl %v is negative", b.Name, b.TTL))
		}
		seen[b.Name] = true
	}
	return errs
}

// validateReads checks each read grant is "<owner>_<rest>" with a valid
// owner name other than the program itself, and not an engine bucket.
func validateReads(name string, reads []string) []error {
	var errs []error
	seen := map[string]bool{}
	engine := natsutil.ProvisionedKVBucketNames()
	for _, r := range reads {
		owner, _, found := strings.Cut(r, "_")
		switch {
		case slices.Contains(engine, r):
			errs = append(errs, fmt.Errorf("read %q is an engine bucket", r))
		case !found || validateName(owner) != nil || !suffixMatches(r, owner+"_", 'a', 'z'):
			errs = append(errs, fmt.Errorf(
				"read %q must be another program's bucket, \"<program>_<rest>\"", r))
		case owner == name:
			errs = append(errs, fmt.Errorf(
				"read %q is under the program's own prefix %s_ (already full access)", r, name))
		case seen[r]:
			errs = append(errs, fmt.Errorf("read %q listed twice", r))
		}
		seen[r] = true
	}
	return errs
}

func validateProtocols(name string, protocols []string) []error {
	var errs []error
	for _, p := range protocols {
		if err := validDotted("protocol", p); err != nil || p[0] == '_' {
			errs = append(errs, fmt.Errorf(
				"protocol %q must be dotted tokens of A-Z, a-z, 0-9, _ or -", p))
			continue
		}
		root, _, _ := strings.Cut(p, ".")
		if root == name {
			errs = append(errs, fmt.Errorf(
				"protocol %q is under the program's own prefix %s.>", p, name))
			continue
		}
		if slices.Contains(engineSubjectRoots, root) {
			errs = append(errs, fmt.Errorf("protocol %q is under reserved root %s.>", p, root))
		}
	}
	return errs
}

func validateLabels(m Manifest) error {
	var errs []error
	if len(m.Version) > versionLenMax || len(m.Dagnats) > versionLenMax {
		errs = append(errs, fmt.Errorf("version strings are limited to %d bytes", versionLenMax))
	}
	if c := m.Console; c != nil {
		if c.Label == "" || len(c.Label) > consoleLabelLenMax {
			errs = append(errs, fmt.Errorf("console label must be 1 to %d bytes", consoleLabelLenMax))
		}
		if c.URL == "" || len(c.URL) > consoleURLLenMax {
			errs = append(errs, fmt.Errorf("console url must be 1 to %d bytes", consoleURLLenMax))
		}
	}
	return errors.Join(errs...)
}

func underPrefix(kind, value, prefix string) error {
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("%s %q is not under prefix %q", kind, value, prefix)
	}
	if len(value) == len(prefix) {
		return fmt.Errorf("%s %q has nothing after prefix %q", kind, value, prefix)
	}
	return nil
}

// validDotted accepts subject-safe names: non-empty dot-separated tokens
// of letters, digits, '_' and '-'. No wildcards, no '$'.
func validDotted(kind, s string) error {
	if s == "" || len(s) > dottedLenMax {
		return fmt.Errorf("%s %q must be 1 to %d bytes", kind, s, dottedLenMax)
	}
	for _, token := range strings.Split(s, ".") {
		if token == "" || strings.IndexFunc(token, notTokenRune) >= 0 {
			return fmt.Errorf("%s %q has an empty or invalid token", kind, s)
		}
	}
	return nil
}

func notTokenRune(r rune) bool {
	return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
		r >= '0' && r <= '9' || r == '_' || r == '-')
}

// suffixMatches reports whether s is prefix followed by one or more
// bytes in [lo, hi], digits or '_'.
func suffixMatches(s, prefix string, lo, hi byte) bool {
	if lo > hi {
		panic("program.suffixMatches: empty letter range")
	}
	rest, ok := strings.CutPrefix(s, prefix)
	if !ok || rest == "" || len(s) > dottedLenMax {
		return false
	}
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if (c < lo || c > hi) && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
