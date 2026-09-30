package ci

// jobs.go parses the Actions-shaped spec (#728): a top-level jobs: key with
// needs and shell run: steps. It walks the yaml.Node tree directly rather
// than decoding into structs so every diagnostic keeps the line and column
// of the offending key or value, and so a bad job never hides its siblings.
// The compile half lives in jobs_compile.go.

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"gopkg.in/yaml.v3"
)

const (
	// jobTimeoutMinutesMax mirrors GitHub Actions' six-hour job limit. The
	// engine enforces no step-timeout ceiling of its own (dag.Validate only
	// requires a workflow timeout for hard-sticky workflows), so this is the
	// compiler's own bound: a typo like 3000000 must not become a step that
	// never times out in practice.
	jobTimeoutMinutesMax = 360

	expressionsUnsupported = "expressions are not supported; " +
		"only ${{ secrets.NAME }} as an env value"
)

// secretExpression matches ${{ secrets.NAME }} as the ENTIRE value of an env
// entry, tolerating the whitespace GitHub tolerates inside the braces.
var secretExpression = regexp.MustCompile(
	`^\$\{\{\s*secrets\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$`,
)

// envKeyPattern is the env var grammar, shared with secret names. A key such
// as "A=B" would reach a worker's exec environment as A=B=value and set A,
// overriding whatever A was bound to, so anything else is refused.
var envKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// jobIDPattern is the GitHub Actions job id grammar. A job id becomes a
// step ID, appears in needs, and is echoed in every diagnostic about it.
var jobIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// unsupportedJobsKeys are Actions keys this spec does not implement. They
// are named as unsupported, not as typos, at every level they appear.
var unsupportedJobsKeys = map[string]string{
	"uses":              "this spec only runs shell commands",
	"if":                "this spec has no conditional execution",
	"strategy":          "this spec has no matrix expansion",
	"matrix":            "this spec has no matrix expansion",
	"container":         "this spec runs commands directly on the worker",
	"services":          "this spec runs commands directly on the worker",
	"runs-on":           "this spec runs every job on the repository's worker group",
	"outputs":           "this spec passes no outputs between jobs",
	"permissions":       "this spec has no token permissions",
	"concurrency":       "this spec has no concurrency groups",
	"defaults":          "set env or working-directory on the job instead",
	"shell":             "this spec runs every step with the worker's shell",
	"continue-on-error": "a failed step fails its job",
}

var (
	jobsTopAllowed = []string{"on", "env", "jobs", "name"}
	jobsJobAllowed = []string{"name", "needs", "timeout-minutes", "retries", "env",
		"environment", "working-directory", "steps"}
	jobsStepAllowed = []string{"name", "run", "env", "working-directory"}
)

// jobsKnown builds the known-key set unknownFieldDiagnostics needs: the
// allowed keys plus the unsupported ones, so a key like runs-on is reported
// once, as unsupported, and never again as an unknown field.
func jobsKnown(allowed []string) map[string]bool {
	if len(allowed) == 0 {
		panic("jobsKnown: allowed must not be empty")
	}
	known := make(map[string]bool, len(allowed)+len(unsupportedJobsKeys))
	for _, key := range allowed {
		known[key] = true
	}
	for key := range unsupportedJobsKeys {
		known[key] = true
	}
	if len(known) < len(unsupportedJobsKeys) {
		panic("jobsKnown: the known set must include every unsupported key")
	}
	return known
}

// envValues is one env block after secret extraction: plain entries, plus
// secretNames, the keys that held ${{ secrets.KEY }}. A secret is always
// bound to the env var of its own name, so the key IS the secret's name.
// The secret value never exists here; only its name does. keyNodes keeps
// each key's node so cross-level conflicts can be reported in place.
type envValues struct {
	field       string
	plain       map[string]string
	secretNames map[string]bool
	keyNodes    map[string]*yaml.Node
}

func newEnvValues() envValues {
	return envValues{
		plain: map[string]string{}, secretNames: map[string]bool{},
		keyNodes: map[string]*yaml.Node{},
	}
}

type parsedStep struct {
	name       string
	run        string
	workingDir string
	env        envValues
}

type parsedJob struct {
	idNode     *yaml.Node
	needsKey   *yaml.Node
	needs      []string
	name       string
	timeout    time.Duration
	retries    int
	env        envValues
	environ    string
	workingDir string
	steps      []parsedStep
	ok         bool
}

type parsedJobs struct {
	on   json.RawMessage
	env  envValues
	jobs map[string]*parsedJob
	// jobsKey positions diagnostics about the jobs as a whole.
	jobsKey *yaml.Node
	// ids lists every declared job id, including jobs that failed to parse,
	// so a needs reference to a malformed job is not reported as unknown.
	ids []string
}

// mappingKey returns the key node for key within mapping node, or nil.
func mappingKey(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		panic("mappingKey: node must be a mapping node")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i]
		}
	}
	return nil
}

func diagAt(node *yaml.Node, field, message string) Diagnostic {
	if node == nil {
		panic("diagAt: node must not be nil")
	}
	if message == "" {
		panic("diagAt: message must not be empty")
	}
	return Diagnostic{Line: node.Line, Column: node.Column, Field: field, Message: message}
}

// jobsLevelKeys reports unsupported and unknown keys of one mapping level.
func jobsLevelKeys(
	node *yaml.Node, allowed []string, field string, diags []Diagnostic,
) []Diagnostic {
	if node == nil || node.Kind != yaml.MappingNode {
		panic("jobsLevelKeys: node must be a mapping node")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Tag == "!!merge" {
			diags = addDiagnostic(diags, diagAt(key, field,
				field+": merge keys are not supported in a jobs: spec"))
			continue
		}
		if reason, ok := unsupportedJobsKeys[key.Value]; ok {
			diags = addDiagnostic(diags, diagAt(key, field, fmt.Sprintf(
				"%s: `%s` is not supported: %s", field, key.Value, reason)))
		}
	}
	return unknownFieldDiagnostics(node, jobsKnown(allowed), field, diags)
}

// parseJobsString reads a scalar string field and rejects any ${{ in it.
func parseJobsString(
	node *yaml.Node, field string, diags []Diagnostic,
) (string, []Diagnostic) {
	if field == "" {
		panic("parseJobsString: field must not be empty")
	}
	if node.Kind != yaml.ScalarNode {
		return "", addDiagnostic(diags, diagAt(node, field, field+" must be a string"))
	}
	if strings.Contains(node.Value, "${{") {
		return "", addDiagnostic(diags, diagAt(node, field,
			field+": "+expressionsUnsupported))
	}
	return node.Value, diags
}

// parseJobsEnv reads an env mapping, moving each whole-value secret
// reference out of the values and into secretNames.
func parseJobsEnv(
	node *yaml.Node, field string, diags []Diagnostic,
) (envValues, []Diagnostic) {
	if field == "" {
		panic("parseJobsEnv: field must not be empty")
	}
	if node == nil {
		panic("parseJobsEnv: node must not be nil")
	}
	env := newEnvValues()
	env.field = field
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return env, diags
	}
	if node.Kind != yaml.MappingNode {
		return env, addDiagnostic(diags, diagAt(node, field, field+" must be a mapping"))
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		diags = parseJobsEnvEntry(&env, node.Content[i], node.Content[i+1], field, diags)
	}
	return env, diags
}

// parseJobsEnvEntry validates one key/value pair and files it as plain or
// secret.
func parseJobsEnvEntry(
	env *envValues, key, val *yaml.Node, field string, diags []Diagnostic,
) []Diagnostic {
	if env == nil || key == nil || val == nil {
		panic("parseJobsEnvEntry: env, key and val must not be nil")
	}
	if field == "" {
		panic("parseJobsEnvEntry: field must not be empty")
	}
	entry := field + "." + key.Value
	if strings.Contains(key.Value, "${{") {
		return addDiagnostic(diags, diagAt(key, field, field+": "+expressionsUnsupported))
	}
	if key.Kind != yaml.ScalarNode || !envKeyPattern.MatchString(key.Value) {
		return addDiagnostic(diags, diagAt(key, field, fmt.Sprintf(
			"%s: env var name %q must match %s", field, key.Value, envKeyPattern)))
	}
	if val.Kind != yaml.ScalarNode {
		return addDiagnostic(diags, diagAt(val, entry, entry+" must be a string"))
	}
	env.keyNodes[key.Value] = key
	m := secretExpression.FindStringSubmatch(val.Value)
	if m == nil {
		if strings.Contains(val.Value, "${{") {
			return addDiagnostic(diags, diagAt(val, entry, entry+": "+expressionsUnsupported))
		}
		env.plain[key.Value] = val.Value
		return diags
	}
	// The job JSON lists secrets by NAME only, so a worker can bind a secret
	// to exactly one env var: its own name. KEY: ${{ secrets.OTHER }} would
	// silently export OTHER instead of KEY, and the step reading $KEY would
	// see nothing. Refuse it here rather than mis-bind later.
	if m[1] != key.Value {
		return addDiagnostic(diags, diagAt(val, entry, fmt.Sprintf(
			"%s: a secret must be bound to an env var of the same name; "+
				"write %s: ${{ secrets.%s }}", entry, m[1], m[1])))
	}
	env.secretNames[key.Value] = true
	return diags
}

// parseOn normalises on: to a map of event to filter and returns it as JSON
// without interpreting any event or filter. A bare event or a list of events
// becomes {event: {}}; a null filter (`push:`) becomes {} so consumers see
// one shape.
func parseOn(node *yaml.Node, diags []Diagnostic) (json.RawMessage, []Diagnostic) {
	if node == nil {
		panic("parseOn: node must not be nil")
	}
	if node.Kind == yaml.AliasNode {
		panic("parseOn: aliases are rejected before parsing")
	}
	events := map[string]interface{}{}
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!null" || node.Value == "" {
			return nil, addDiagnostic(diags, diagAt(node, "on", "on must not be empty"))
		}
		events[node.Value] = map[string]interface{}{}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode || item.Value == "" {
				return nil, addDiagnostic(diags, diagAt(item, "on",
					"on list entries must be event names"))
			}
			events[item.Value] = map[string]interface{}{}
		}
	case yaml.MappingNode:
		var decoded map[string]interface{}
		if err := node.Decode(&decoded); err != nil {
			return nil, addDiagnostic(diags, diagAt(node, "on", "on: "+err.Error()))
		}
		for event, filter := range decoded {
			if filter == nil {
				filter = map[string]interface{}{}
			}
			events[event] = filter
		}
	default:
		return nil, addDiagnostic(diags, diagAt(node, "on",
			"on must be an event name, a list of event names, or a mapping"))
	}
	if len(events) == 0 {
		return nil, addDiagnostic(diags, diagAt(node, "on", "on must name at least one event"))
	}
	raw, err := json.Marshal(events)
	if err != nil {
		return nil, addDiagnostic(diags, diagAt(node, "on",
			"on is not representable as JSON: "+err.Error()))
	}
	// "$", "{" and "}" are never escaped by encoding/json, so a plain
	// substring test on the encoded form finds an expression anywhere in on:.
	if strings.Contains(string(raw), "${{") {
		return nil, addDiagnostic(diags, diagAt(node, "on", "on: "+expressionsUnsupported))
	}
	return raw, diags
}

// parseJobsSpec parses the top-level mapping of a jobs: spec.
func parseJobsSpec(doc *yaml.Node) (parsedJobs, []Diagnostic) {
	if doc == nil || doc.Kind != yaml.MappingNode {
		panic("parseJobsSpec: doc must be a mapping node")
	}
	p := parsedJobs{jobs: map[string]*parsedJob{}}
	p.env = newEnvValues()
	var diags []Diagnostic
	diags = jobsLevelKeys(doc, jobsTopAllowed, "spec", diags)
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key, val := doc.Content[i], doc.Content[i+1]
		switch key.Value {
		case "on":
			p.on, diags = parseOn(val, diags)
		case "env":
			p.env, diags = parseJobsEnv(val, "env", diags)
		case "name":
			_, diags = parseJobsString(val, "name", diags)
		case "jobs":
			p.jobsKey = key
			diags = parseJobsMap(&p, val, diags)
		}
	}
	sort.Strings(p.ids)
	return p, diags
}

func parseJobsMap(p *parsedJobs, node *yaml.Node, diags []Diagnostic) []Diagnostic {
	if p == nil || p.jobs == nil {
		panic("parseJobsMap: p and its jobs map must not be nil")
	}
	if node == nil {
		panic("parseJobsMap: node must not be nil")
	}
	if node.Kind != yaml.MappingNode {
		return addDiagnostic(diags, diagAt(node, "jobs", "jobs must be a mapping of job id to job"))
	}
	if len(node.Content) == 0 {
		return addDiagnostic(diags, diagAt(node, "jobs", "spec has no jobs"))
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		idNode := node.Content[i]
		if _, dup := p.jobs[idNode.Value]; dup {
			// jobsDocumentDiagnostics rejects duplicate keys, and compile
			// stops before parsing when it reports anything.
			panic("parseJobsMap: duplicate job id reached the parser")
		}
		if idNode.Kind != yaml.ScalarNode || !jobIDPattern.MatchString(idNode.Value) {
			diags = addDiagnostic(diags, diagAt(idNode, "jobs", fmt.Sprintf(
				"job id %q must match %s", idNode.Value, jobIDPattern)))
			continue
		}
		var job *parsedJob
		job, diags = parseJob(idNode, node.Content[i+1], diags)
		p.jobs[idNode.Value] = job
		p.ids = append(p.ids, idNode.Value)
	}
	return diags
}

// parseJob parses one job mapping. The returned job has ok set only when it
// parsed without a diagnostic, which gates its size check at compile time.
func parseJob(
	idNode, node *yaml.Node, diags []Diagnostic,
) (*parsedJob, []Diagnostic) {
	if idNode == nil || node == nil {
		panic("parseJob: idNode and node must not be nil")
	}
	if !jobIDPattern.MatchString(idNode.Value) {
		panic("parseJob: the caller validates the job id")
	}
	job := &parsedJob{
		idNode: idNode, timeout: defaultCheckTimeout,
		env: newEnvValues(),
	}
	field := "jobs." + idNode.Value
	if node.Kind != yaml.MappingNode {
		return job, addDiagnostic(diags, diagAt(node, field, field+" must be a mapping"))
	}
	before := len(diags)
	diags = jobsLevelKeys(node, jobsJobAllowed, field, diags)
	for i := 0; i+1 < len(node.Content); i += 2 {
		diags = parseJobField(job, node.Content[i], node.Content[i+1], field, diags)
	}
	if mappingKey(node, "steps") == nil {
		diags = addDiagnostic(diags, diagAt(idNode, field, field+": job has no steps"))
	}
	job.ok = len(diags) == before
	return job, diags
}

func parseJobField(
	job *parsedJob, key, val *yaml.Node, field string, diags []Diagnostic,
) []Diagnostic {
	if job == nil || key == nil {
		panic("parseJobField: job and key must not be nil")
	}
	switch key.Value {
	case "name":
		job.name, diags = parseJobsString(val, field+".name", diags)
	case "needs":
		job.needsKey = key
		job.needs, diags = parseNeeds(val, field+".needs", diags)
	case "timeout-minutes":
		job.timeout, diags = parseTimeoutMinutes(val, field+".timeout-minutes", job.timeout, diags)
	case "retries":
		job.retries, diags = parseRetries(val, field+".retries", diags)
	case "env":
		job.env, diags = parseJobsEnv(val, field+".env", diags)
	case "environment":
		job.environ, diags = parseJobsString(val, field+".environment", diags)
	case "working-directory":
		job.workingDir, diags = parseJobsString(val, field+".working-directory", diags)
	case "steps":
		diags = parseSteps(job, val, field+".steps", diags)
	}
	return diags
}

func parseNeeds(node *yaml.Node, field string, diags []Diagnostic) ([]string, []Diagnostic) {
	if node == nil {
		panic("parseNeeds: node must not be nil")
	}
	if field == "" {
		panic("parseNeeds: field must not be empty")
	}
	if node.Kind == yaml.ScalarNode && node.Tag != "!!null" {
		return []string{node.Value}, diags
	}
	if node.Kind != yaml.SequenceNode {
		return nil, addDiagnostic(diags, diagAt(node, field,
			field+" must be a job id or a list of job ids"))
	}
	needs := make([]string, 0, len(node.Content))
	listed := make(map[string]bool, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			diags = addDiagnostic(diags, diagAt(item, field, field+" entries must be job ids"))
			continue
		}
		// A repeated need adds no ordering; keep one so a cycle through it
		// is reported once.
		if !listed[item.Value] {
			listed[item.Value] = true
			needs = append(needs, item.Value)
		}
	}
	return needs, diags
}

func parseTimeoutMinutes(
	node *yaml.Node, field string, fallback time.Duration, diags []Diagnostic,
) (time.Duration, []Diagnostic) {
	if node == nil {
		panic("parseTimeoutMinutes: node must not be nil")
	}
	if fallback <= 0 {
		panic("parseTimeoutMinutes: fallback must be positive")
	}
	var minutes int
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Decode(&minutes) != nil {
		return fallback, addDiagnostic(diags, diagAt(node, field,
			field+" must be a positive integer number of minutes"))
	}
	if minutes < 1 || minutes > jobTimeoutMinutesMax {
		return fallback, addDiagnostic(diags, diagAt(node, field, fmt.Sprintf(
			"%s must be between 1 and %d minutes (got %d)",
			field, jobTimeoutMinutesMax, minutes)))
	}
	return time.Duration(minutes) * time.Minute, diags
}

func parseRetries(node *yaml.Node, field string, diags []Diagnostic) (int, []Diagnostic) {
	if node == nil {
		panic("parseRetries: node must not be nil")
	}
	if field == "" {
		panic("parseRetries: field must not be empty")
	}
	var retries int
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Decode(&retries) != nil {
		return 0, addDiagnostic(diags, diagAt(node, field, field+" must be an integer"))
	}
	if retries < 0 {
		return 0, addDiagnostic(diags, diagAt(node, field,
			fmt.Sprintf("%s must not be negative (%d)", field, retries)))
	}
	if retries > dag.RetryAttemptCountMax {
		return 0, addDiagnostic(diags, diagAt(node, field, fmt.Sprintf(
			"%s must be at most %d (got %d)", field, dag.RetryAttemptCountMax, retries)))
	}
	return retries, diags
}

func parseSteps(
	job *parsedJob, node *yaml.Node, field string, diags []Diagnostic,
) []Diagnostic {
	if job == nil {
		panic("parseSteps: job must not be nil")
	}
	if node.Kind != yaml.SequenceNode {
		return addDiagnostic(diags, diagAt(node, field, field+" must be a list of steps"))
	}
	if len(node.Content) == 0 {
		return addDiagnostic(diags, diagAt(node, field, field+": job has no steps"))
	}
	for i, stepNode := range node.Content {
		var step parsedStep
		step, diags = parseStep(job, stepNode, fmt.Sprintf("%s[%d]", field, i), diags)
		job.steps = append(job.steps, step)
	}
	return diags
}

func parseStep(
	job *parsedJob, node *yaml.Node, field string, diags []Diagnostic,
) (parsedStep, []Diagnostic) {
	if job == nil {
		panic("parseStep: job must not be nil")
	}
	step := parsedStep{env: newEnvValues()}
	if node.Kind != yaml.MappingNode {
		return step, addDiagnostic(diags, diagAt(node, field, field+" must be a mapping"))
	}
	diags = jobsLevelKeys(node, jobsStepAllowed, field, diags)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, val := node.Content[i], node.Content[i+1]
		switch key.Value {
		case "name":
			step.name, diags = parseJobsString(val, field+".name", diags)
		case "run":
			runBefore := len(diags)
			step.run, diags = parseJobsString(val, field+".run", diags)
			if len(diags) == runBefore && step.run == "" {
				diags = addDiagnostic(diags, diagAt(val, field, field+": step has no run"))
			}
		case "working-directory":
			step.workingDir, diags = parseJobsString(val, field+".working-directory", diags)
		case "env":
			step.env, diags = parseJobsEnv(val, field+".env", diags)
		}
	}
	if mappingKey(node, "run") == nil {
		diags = addDiagnostic(diags, diagAt(node, field, field+": step has no run"))
	}
	return step, diags
}
