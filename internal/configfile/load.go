// internal/configfile/load.go
// Pure load + validate. No filesystem, no NATS — callers pass a
// reader. yaml.v3 line-number errors are surfaced verbatim so the
// operator can jump straight to the offending key.
package configfile

import (
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// maxConfigBytes bounds the file read so a runaway file cannot
// exhaust memory. 1 MiB is two orders of magnitude above any
// reasonable dagnats.yaml.
const maxConfigBytes = 1 << 20

// maxEntries bounds workflows + triggers per file. Matches the
// TriggerService maxActiveTriggers ceiling.
const maxEntries = 500

// Load reads YAML from r and returns the parsed ConfigFile. Unknown
// top-level keys (e.g. legacy server.Config fields) are tolerated so
// the same dagnats.yaml can carry both layers. Returns the
// yaml.v3 typed error verbatim on parse failure — its String()
// already includes the offending line.
func Load(r io.Reader) (ConfigFile, error) {
	if r == nil {
		panic("Load: r must not be nil")
	}

	limited := io.LimitReader(r, maxConfigBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("read config: %w", err)
	}
	if len(data) > maxConfigBytes {
		return ConfigFile{}, fmt.Errorf(
			"config file exceeds %d bytes", maxConfigBytes,
		)
	}

	var cfg ConfigFile
	dec := yaml.NewDecoder(bytesReader(data))
	// KnownFields(false): legacy server keys at the top level
	// (data_dir, http_addr, ...) must not break the load.
	dec.KnownFields(false)
	if err := dec.Decode(&cfg); err != nil {
		// io.EOF on an empty file is benign — the apply path
		// should treat it as "no workflows, no triggers".
		if err == io.EOF {
			return ConfigFile{}, nil
		}
		return ConfigFile{}, fmt.Errorf("yaml decode: %w", err)
	}

	if len(cfg.Workflows) > maxEntries {
		return ConfigFile{}, fmt.Errorf(
			"workflows exceeds max %d entries", maxEntries,
		)
	}
	if len(cfg.Triggers) > maxEntries {
		return ConfigFile{}, fmt.Errorf(
			"triggers exceeds max %d entries", maxEntries,
		)
	}

	return cfg, nil
}

// bytesReader avoids pulling in bytes.NewReader transitively —
// configfile is meant to stay light on imports.
func bytesReader(b []byte) io.Reader {
	return &byteSlice{b: b}
}

type byteSlice struct {
	b   []byte
	off int
}

func (s *byteSlice) Read(p []byte) (int, error) {
	if s.off >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.off:])
	s.off += n
	return n, nil
}

// Validate checks invariants on the parsed config. Returns the
// first violation found (callers loop fast; the operator iterates).
// Programmer-error invariants on the call site itself panic.
func Validate(cfg ConfigFile) error {
	if err := validateWorkflows(cfg.Workflows); err != nil {
		return err
	}
	if err := validateTriggers(cfg.Triggers); err != nil {
		return err
	}
	if err := validatePolicy(cfg.Policy); err != nil {
		return err
	}
	return crossValidate(cfg)
}

// validatePolicy enforces the control-plane grant invariants: both lists
// bounded at maxEntries (TigerStyle), no duplicates, no empty strings, and
// every promote entry present in grant (you cannot promote what you cannot
// reach). A nil policy is valid — deny-by-default needs no declaration.
func validatePolicy(p *PolicyYAML) error {
	if p == nil || p.ControlPlane == nil {
		return nil
	}
	cp := p.ControlPlane
	grant, err := validateGrantList("grant", cp.Grant)
	if err != nil {
		return err
	}
	if _, err := validateGrantList("promote", cp.Promote); err != nil {
		return err
	}
	for _, name := range cp.Promote {
		if _, ok := grant[name]; !ok {
			return fmt.Errorf(
				"policy.control_plane.promote %q not in grant", name,
			)
		}
	}
	return nil
}

// validateGrantList bounds the list, rejects empty strings and duplicates,
// and returns the names as a set for subset checks. The bound is a hard
// upper limit so a malformed file cannot allocate unboundedly.
func validateGrantList(field string, names []string) (map[string]struct{}, error) {
	if len(names) > maxEntries {
		return nil, fmt.Errorf(
			"policy.control_plane.%s exceeds max %d entries",
			field, maxEntries,
		)
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf(
				"policy.control_plane.%s contains an empty name", field,
			)
		}
		if _, dup := set[name]; dup {
			return nil, fmt.Errorf(
				"policy.control_plane.%s duplicate name %q", field, name,
			)
		}
		set[name] = struct{}{}
	}
	return set, nil
}

// validateWorkflows enforces the per-workflow invariants: name
// non-empty, ID-unique, at least one step.
func validateWorkflows(wfs []WorkflowYAML) error {
	if len(wfs) > maxEntries {
		panic("validateWorkflows: exceeds max bound")
	}
	seen := make(map[string]bool, len(wfs))
	for i, wf := range wfs {
		if wf.Name == "" {
			return fmt.Errorf(
				"workflows[%d]: name is required", i,
			)
		}
		if seen[wf.Name] {
			return fmt.Errorf(
				"workflows[%d]: duplicate name %q", i, wf.Name,
			)
		}
		seen[wf.Name] = true
		if len(wf.Steps) == 0 {
			return fmt.Errorf(
				"workflows[%d] (%s): steps cannot be empty",
				i, wf.Name,
			)
		}
		for j, step := range wf.Steps {
			if step.ID == "" {
				return fmt.Errorf(
					"workflows[%d].steps[%d]: id is required",
					i, j,
				)
			}
			if step.Task == "" {
				return fmt.Errorf(
					"workflows[%d].steps[%d] (%s): task is required",
					i, j, step.ID,
				)
			}
		}
	}
	return nil
}

// validateTriggers enforces per-trigger invariants. Exactly-one-of
// is asserted so a misconfigured file fails fast at parse time
// rather than at trigger.Validate after a KV round-trip.
func validateTriggers(trs []TriggerYAML) error {
	if len(trs) > maxEntries {
		panic("validateTriggers: exceeds max bound")
	}
	seen := make(map[string]bool, len(trs))
	for i, tr := range trs {
		if tr.ID == "" {
			return fmt.Errorf("triggers[%d]: id is required", i)
		}
		if seen[tr.ID] {
			return fmt.Errorf(
				"triggers[%d]: duplicate id %q", i, tr.ID,
			)
		}
		seen[tr.ID] = true
		if tr.WorkflowID == "" {
			return fmt.Errorf(
				"triggers[%d] (%s): workflow_id is required",
				i, tr.ID,
			)
		}
		if err := assertExactlyOneKind(i, tr); err != nil {
			return err
		}
	}
	return nil
}

// assertExactlyOneKind enforces the trigger.TriggerDef invariant
// that exactly one of cron/subject/webhook/http is set.
func assertExactlyOneKind(i int, tr TriggerYAML) error {
	count := 0
	if tr.Cron != nil {
		count++
	}
	if tr.Subject != nil {
		count++
	}
	if tr.Webhook != nil {
		count++
	}
	if tr.HTTP != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf(
			"triggers[%d] (%s): exactly one of cron/subject/"+
				"webhook/http must be set, got %d",
			i, tr.ID, count,
		)
	}
	return nil
}

// crossValidate ensures every trigger.WorkflowID references a
// workflow defined in the same file. Cross-file references are
// disallowed at the configfile layer — if an operator wants to
// trigger a KV-managed workflow from the file, they can add the
// workflow to the file too.
func crossValidate(cfg ConfigFile) error {
	wfNames := make(map[string]bool, len(cfg.Workflows))
	for _, wf := range cfg.Workflows {
		wfNames[wf.Name] = true
	}
	for i, tr := range cfg.Triggers {
		if !wfNames[tr.WorkflowID] {
			return fmt.Errorf(
				"triggers[%d] (%s): workflow_id %q is "+
					"not declared in the same file",
				i, tr.ID, tr.WorkflowID,
			)
		}
	}
	return nil
}
