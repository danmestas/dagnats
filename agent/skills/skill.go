// agent/skills/skill.go

// Package skills provides reusable workflow templates with pre-configured
// agent configs. A Skill bundles a WorkflowDef (the DAG) with a map of
// AgentConfigs (one per step that uses "agent-run" task type). Skills are
// stored in NATS KV and invoked by name to create workflow runs.
package skills

import (
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/agent"
)

// Skill is a reusable workflow template. The Workflow field defines
// the DAG structure. Configs maps step ID → AgentConfig for steps
// that use the "agent-run" task type.
type Skill struct {
	Name        string                        `json:"name"`
	Description string                        `json:"description"`
	Workflow    json.RawMessage               `json:"workflow"`
	Configs     map[string]agent.AgentConfig  `json:"configs"`
}

// ValidateSkill checks a Skill for structural errors.
func ValidateSkill(skill Skill) error {
	if skill.Name == "" {
		return fmt.Errorf("skill name must not be empty")
	}
	if skill.Description == "" {
		return fmt.Errorf(
			"skill %q: description must not be empty",
			skill.Name,
		)
	}
	if skill.Workflow == nil {
		return fmt.Errorf(
			"skill %q: workflow must not be nil",
			skill.Name,
		)
	}
	for stepID, cfg := range skill.Configs {
		if err := agent.ValidateConfig(cfg); err != nil {
			return fmt.Errorf(
				"skill %q, step %q: %w",
				skill.Name, stepID, err,
			)
		}
	}
	return nil
}

// MarshalSkill serializes a Skill to JSON.
func MarshalSkill(skill Skill) ([]byte, error) {
	return json.Marshal(skill)
}

// UnmarshalSkill deserializes a Skill from JSON.
func UnmarshalSkill(data []byte) (Skill, error) {
	var skill Skill
	if err := json.Unmarshal(data, &skill); err != nil {
		return Skill{}, fmt.Errorf("unmarshal skill: %w", err)
	}
	return skill, nil
}

// Registry holds available skills in memory. In production, skills
// are loaded from the "skills" KV bucket at startup and cached here.
type Registry struct {
	skills map[string]Skill
}

// NewRegistry creates an empty skill registry.
func NewRegistry() *Registry {
	return &Registry{skills: make(map[string]Skill)}
}

// Register adds a skill to the registry. Returns an error if
// validation fails.
func (r *Registry) Register(skill Skill) error {
	if err := ValidateSkill(skill); err != nil {
		return err
	}
	r.skills[skill.Name] = skill
	return nil
}

// Get retrieves a skill by name. Returns an error if not found.
func (r *Registry) Get(name string) (Skill, error) {
	skill, ok := r.skills[name]
	if !ok {
		return Skill{}, fmt.Errorf("skill %q not found", name)
	}
	return skill, nil
}

// Names returns all registered skill names.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.skills))
	for name := range r.skills {
		names = append(names, name)
	}
	return names
}
