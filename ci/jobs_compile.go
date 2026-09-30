package ci

// jobs_compile.go compiles a parsed jobs: spec (jobs.go) into a
// dag.WorkflowDef: one step per job, the job itself carried as JSON in
// StepDef.Metadata["ci.job"] for the worker that runs it.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"gopkg.in/yaml.v3"
)

const (
	// jobMetadataKey is the single Metadata key that carries the job.
	jobMetadataKey = "ci.job"
	// jobTaskSuffix follows the caller's namespace to form the task type.
	jobTaskSuffix = "job"
	// jobPayloadMaxBytes bounds the encoded job. It travels in every task
	// message for the job and in the stored def, so an oversized job fails
	// here, positioned, rather than at publish with nothing tying it to the
	// spec.
	jobPayloadMaxBytes = 64 * 1024
	// jobsWorkflowTimeoutMax caps the summed workflow timeout. The engine
	// sets a run deadline with time.Now().Add(timeout) and bounds neither,
	// so an unbounded sum (many jobs x long timeouts x high retries) could
	// overflow there. A year is far past any real spec's total and keeps
	// that arithmetic safe.
	jobsWorkflowTimeoutMax = 365 * 24 * time.Hour
)

// jobStepPayload and jobPayload define the wire shape of Metadata["ci.job"].
// The JSON keys are a contract with job runners; do not rename them.
type jobStepPayload struct {
	Name             string            `json:"name"`
	Run              string            `json:"run"`
	Env              map[string]string `json:"env"`
	WorkingDirectory string            `json:"working-directory"`
}

type jobPayload struct {
	Name             string            `json:"name"`
	Env              map[string]string `json:"env"`
	WorkingDirectory string            `json:"working-directory"`
	Environment      string            `json:"environment"`
	Secrets          []string          `json:"secrets"`
	Steps            []jobStepPayload  `json:"steps"`
}

// compileJobsDoc parses and compiles a spec already known to be jobs-shaped.
// Cross-job checks run even when parsing reported problems, so one pass
// surfaces every mistake; only the final build is gated on a clean parse.
func compileJobsDoc(
	name string, doc *yaml.Node, namespace string,
) (CompileResult, []Diagnostic) {
	if name == "" {
		panic("compileJobsDoc: name must not be empty")
	}
	if doc == nil || doc.Kind != yaml.MappingNode {
		panic("compileJobsDoc: doc must be a mapping node")
	}
	var diags []Diagnostic
	if checksKey := mappingKey(doc, "checks"); checksKey != nil {
		jobsKey := mappingKey(doc, "jobs")
		return CompileResult{}, addDiagnostic(diags, diagAt(jobsKey, "jobs",
			"a spec uses either jobs: or checks:, not both"))
	}
	parsed, diags := parseJobsSpec(doc)
	diags = namespaceDiagnostics(namespace, diags)
	diags = needsDiagnostics(parsed, diags)
	diags = cycleDiagnostics(parsed, diags)
	payloads, diags := jobPayloads(parsed, diags)
	if len(diags) > 0 {
		return CompileResult{}, diags
	}
	def := dag.WorkflowDef{
		Name: name, Version: workflowVersion, Steps: buildJobSteps(parsed, payloads, namespace),
	}
	def.Timeout = jobsWorkflowTimeout(parsed)
	if err := dag.Validate(def); err != nil {
		return CompileResult{}, addDiagnostic(diags, Diagnostic{
			Field: stepFieldFromError(err), Message: err.Error(),
		})
	}
	return CompileResult{Workflow: def, On: parsed.on}, nil
}

// namespaceDiagnostics requires a namespace that is legal both as a worker
// group (one subject token) and, with the job suffix, as a task type.
func namespaceDiagnostics(namespace string, diags []Diagnostic) []Diagnostic {
	if namespace == "" {
		return addDiagnostic(diags, Diagnostic{
			Field:   "task_namespace",
			Message: "a jobs: spec needs a task_namespace: the worker group its jobs run on",
		})
	}
	if err := dag.ValidWorkerGroup(namespace); err != nil {
		return addDiagnostic(diags, Diagnostic{
			Field: "task_namespace", Message: "invalid task_namespace: " + err.Error(),
		})
	}
	if err := dag.ValidTaskType(namespace + "." + jobTaskSuffix); err != nil {
		return addDiagnostic(diags, Diagnostic{
			Field: "task_namespace", Message: "invalid task_namespace: " + err.Error(),
		})
	}
	return diags
}

func needsDiagnostics(p parsedJobs, diags []Diagnostic) []Diagnostic {
	declared := make(map[string]bool, len(p.ids))
	for _, id := range p.ids {
		declared[id] = true
	}
	for _, id := range p.ids {
		job := p.jobs[id]
		for _, need := range job.needs {
			if !declared[need] {
				diags = addDiagnostic(diags, diagAt(job.needsKey, "jobs."+id+".needs",
					fmt.Sprintf("job %q needs %q, which is not a job", id, need)))
			}
		}
	}
	return diags
}

type dfsFrame struct {
	id   string
	next int
}

// cycleDiagnostics finds needs cycles with an explicit-stack DFS (no
// recursion) over sorted ids, so the reported cycles are deterministic. Each
// back edge is reported once, at the needs key of the job that closes it,
// with the path spelled out.
func cycleDiagnostics(p parsedJobs, diags []Diagnostic) []Diagnostic {
	if p.jobs == nil {
		panic("cycleDiagnostics: jobs must not be nil")
	}
	const (
		white, gray, black = 0, 1, 2
	)
	color := make(map[string]int, len(p.ids))
	edgeCount := 0
	for _, id := range p.ids {
		edgeCount += len(p.jobs[id].needs)
	}
	stepMax := 2*(len(p.ids)+edgeCount) + 1
	steps := 0
	for _, root := range p.ids {
		if color[root] != white {
			continue
		}
		stack := []dfsFrame{{id: root}}
		color[root] = gray
		for len(stack) > 0 {
			steps++
			if steps > stepMax {
				panic("cycleDiagnostics: internal invariant: traversal exceeded its bound")
			}
			top := &stack[len(stack)-1]
			needs := p.jobs[top.id].needs
			if top.next >= len(needs) {
				color[top.id] = black
				stack = stack[:len(stack)-1]
				continue
			}
			need := needs[top.next]
			top.next++
			target, known := p.jobs[need]
			if !known || target == nil {
				continue
			}
			switch color[need] {
			case white:
				color[need] = gray
				stack = append(stack, dfsFrame{id: need})
			case gray:
				diags = addDiagnostic(diags, cycleDiagnostic(p, stack, need))
			}
		}
	}
	return diags
}

// cycleDiagnostic names the path from the gray job back to itself.
func cycleDiagnostic(p parsedJobs, stack []dfsFrame, closing string) Diagnostic {
	start := 0
	for i, frame := range stack {
		if frame.id == closing {
			start = i
			break
		}
	}
	path := make([]string, 0, len(stack)-start+1)
	for _, frame := range stack[start:] {
		path = append(path, frame.id)
	}
	path = append(path, closing)
	job := p.jobs[stack[len(stack)-1].id]
	return diagAt(job.needsKey, "jobs."+stack[len(stack)-1].id+".needs",
		"needs cycle: "+strings.Join(path, " -> "))
}

// jobPayloads encodes each cleanly parsed job and enforces the size bound.
func jobPayloads(
	p parsedJobs, diags []Diagnostic,
) (map[string]string, []Diagnostic) {
	payloads := make(map[string]string, len(p.ids))
	for _, id := range p.ids {
		job := p.jobs[id]
		if !job.ok {
			continue
		}
		encoded, err := json.Marshal(buildJobPayload(p, job))
		if err != nil {
			diags = addDiagnostic(diags, diagAt(job.idNode, "jobs."+id, err.Error()))
			continue
		}
		if len(encoded) > jobPayloadMaxBytes {
			diags = addDiagnostic(diags, diagAt(job.idNode, "jobs."+id, fmt.Sprintf(
				"job %q encodes to %d bytes, over the %d byte limit",
				id, len(encoded), jobPayloadMaxBytes)))
			continue
		}
		payloads[id] = string(encoded)
	}
	return payloads, diags
}

// buildJobPayload merges workflow env under job env (job wins) and gathers
// the deduplicated, sorted secret names from every level.
func buildJobPayload(p parsedJobs, job *parsedJob) jobPayload {
	if job == nil {
		panic("buildJobPayload: job must not be nil")
	}
	env := make(map[string]string, len(p.env.plain)+len(job.env.plain))
	for key, val := range p.env.plain {
		env[key] = val
	}
	for key := range job.env.secretNames {
		delete(env, key)
	}
	for key, val := range job.env.plain {
		env[key] = val
	}
	// Secrets follow the same precedence as plain env: a job-level entry,
	// plain or secret, replaces a workflow-level one for the same key.
	// Without this, a workflow secret overridden by a job's plain value
	// would stay in secrets, and the worker would receive both the literal
	// and the secret for one variable, one silently clobbering the other.
	bound := make(map[string]string, len(p.env.secretNames)+len(job.env.secretNames))
	for key, name := range p.env.secretNames {
		bound[key] = name
	}
	for key := range job.env.plain {
		delete(bound, key)
	}
	for key, name := range job.env.secretNames {
		bound[key] = name
	}
	names := make(map[string]bool, len(bound)+len(job.secrets))
	for _, name := range bound {
		names[name] = true
	}
	for name := range job.secrets {
		names[name] = true
	}
	secrets := make([]string, 0, len(names))
	for name := range names {
		secrets = append(secrets, name)
	}
	sort.Strings(secrets)
	steps := make([]jobStepPayload, 0, len(job.steps))
	for _, step := range job.steps {
		steps = append(steps, jobStepPayload{
			Name: step.name, Run: step.run, Env: step.env, WorkingDirectory: step.workingDir,
		})
	}
	return jobPayload{
		Name: job.name, Env: env, WorkingDirectory: job.workingDir,
		Environment: job.environ, Secrets: secrets, Steps: steps,
	}
}

func buildJobSteps(p parsedJobs, payloads map[string]string, namespace string) []dag.StepDef {
	if len(payloads) != len(p.ids) {
		panic("buildJobSteps: every job must have a payload")
	}
	steps := make([]dag.StepDef, 0, len(p.ids))
	for _, id := range p.ids {
		job := p.jobs[id]
		deps := make([]string, len(job.needs))
		copy(deps, job.needs)
		steps = append(steps, dag.StepDef{
			ID: id, Type: dag.StepTypeNormal, Task: namespace + "." + jobTaskSuffix,
			WorkerGroup: namespace, Timeout: job.timeout, DependsOn: deps,
			Retry:    fixedRetryPolicy(job.retries),
			Metadata: map[string]string{jobMetadataKey: payloads[id]},
		})
	}
	return steps
}

// jobsWorkflowTimeout gives the workflow room for every job to run in
// sequence with all its retries; the checks shape's fixed 45m would cut a
// longer job off mid-run. It never drops below the checks default.
func jobsWorkflowTimeout(p parsedJobs) time.Duration {
	if len(p.ids) == 0 {
		panic("jobsWorkflowTimeout: a compiled spec has at least one job")
	}
	total := time.Duration(0)
	for _, id := range p.ids {
		job := p.jobs[id]
		if job.timeout <= 0 || job.retries < 0 {
			panic("jobsWorkflowTimeout: a clean job has a positive timeout and retries >= 0")
		}
		// One job's product fits: timeout <= 360m and retries are bounded
		// by dag.RetryAttemptCountMax, about 2.2e18ns. The SUM across jobs
		// is what can overflow, so saturate at the ceiling instead.
		per := job.timeout * time.Duration(1+job.retries)
		if per >= jobsWorkflowTimeoutMax || total >= jobsWorkflowTimeoutMax-per {
			return jobsWorkflowTimeoutMax
		}
		total += per
	}
	if total < workflowTimeout {
		return workflowTimeout
	}
	return total
}
