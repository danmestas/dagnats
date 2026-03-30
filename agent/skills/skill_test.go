// agent/skills/skill_test.go
// Tests for skill validation, registry, and builtin skills.
// Methodology: Each test verifies positive behavior and negative space.

package skills

import (
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/agent"
)

func TestValidateSkill_Valid(t *testing.T) {
	skill := Skill{
		Name:        "test",
		Description: "A test skill.",
		Workflow:    json.RawMessage(`{"name":"test","version":"1","steps":[]}`),
		Configs: map[string]agent.AgentConfig{
			"step1": {
				Name: "s1", SystemPrompt: "p",
				Model: "m", Provider: "anthropic",
			},
		},
	}
	err := ValidateSkill(skill)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSkill_EmptyName(t *testing.T) {
	skill := Skill{
		Description: "d",
		Workflow:    json.RawMessage(`{}`),
	}
	err := ValidateSkill(skill)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestValidateSkill_InvalidConfig(t *testing.T) {
	skill := Skill{
		Name:        "test",
		Description: "d",
		Workflow:    json.RawMessage(`{}`),
		Configs: map[string]agent.AgentConfig{
			"step1": {Name: "s1"}, // Missing required fields.
		},
	}
	err := ValidateSkill(skill)
	if err == nil {
		t.Fatal("expected error for invalid agent config")
	}
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	skill := Skill{
		Name:        "test-skill",
		Description: "A test.",
		Workflow:    json.RawMessage(`{}`),
	}
	err := reg.Register(skill)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	got, err := reg.Get("test-skill")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Name != "test-skill" {
		t.Fatalf("expected 'test-skill', got %q", got.Name)
	}
}

func TestRegistry_GetNotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing skill")
	}
}

func TestMarshalUnmarshalSkill(t *testing.T) {
	skill := Skill{
		Name:        "roundtrip",
		Description: "Round trip test.",
		Workflow:    json.RawMessage(`{"name":"test"}`),
		Configs: map[string]agent.AgentConfig{
			"s1": {
				Name: "s1", SystemPrompt: "p",
				Model: "m", Provider: "anthropic",
			},
		},
	}
	data, err := MarshalSkill(skill)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	got, err := UnmarshalSkill(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Name != "roundtrip" {
		t.Fatalf("expected 'roundtrip', got %q", got.Name)
	}
	if len(got.Configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(got.Configs))
	}
}

func TestBuiltinSkills_Register(t *testing.T) {
	reg := NewRegistry()
	RegisterBuiltinSkills(reg)
	names := reg.Names()
	if len(names) < 2 {
		t.Fatalf("expected at least 2 builtin skills, got %d",
			len(names))
	}
	// Verify each builtin skill is retrievable.
	for _, name := range names {
		skill, err := reg.Get(name)
		if err != nil {
			t.Fatalf("get %q failed: %v", name, err)
		}
		if skill.Description == "" {
			t.Fatalf("skill %q has empty description", name)
		}
	}
}

func TestBuiltinSkills_ValidConfigs(t *testing.T) {
	skills := []Skill{
		ExploreCodebaseSkill(),
		CodeReviewSkill(),
	}
	for _, skill := range skills {
		err := ValidateSkill(skill)
		if err != nil {
			t.Fatalf("builtin skill %q validation failed: %v",
				skill.Name, err)
		}
		if len(skill.Configs) == 0 {
			t.Fatalf("skill %q has no configs", skill.Name)
		}
	}
}
