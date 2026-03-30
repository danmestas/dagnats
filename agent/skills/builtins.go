// agent/skills/builtins.go

// Pre-built skills for common engineering workflows. These serve as
// both useful defaults and examples of how to compose agent workflows.
package skills

import (
	"encoding/json"

	"github.com/danmestas/dagnats/agent"
)

// ExploreCodebaseSkill returns a skill for codebase exploration.
// Single-phase workflow: one agent loop step with search tools.
func ExploreCodebaseSkill() Skill {
	workflow := json.RawMessage(`{
		"name": "explore-codebase",
		"version": "1",
		"steps": [{
			"id": "explore",
			"task": "agent-run",
			"type": "agent_loop",
			"loop": {"max_iterations": 30, "max_duration": 300000000000}
		}]
	}`)
	return Skill{
		Name:        "explore-codebase",
		Description: "Explore a codebase to understand structure, " +
			"patterns, and key files.",
		Workflow: workflow,
		Configs: map[string]agent.AgentConfig{
			"explore": {
				Name: "explorer",
				SystemPrompt: "You are a codebase exploration agent. " +
					"Your job is to understand the structure, key " +
					"patterns, and important files in the codebase. " +
					"Use glob to find files, grep to search contents, " +
					"and read_file to understand implementations. " +
					"Produce a structured summary when done.",
				Model:    "claude-haiku-4-5-20251001",
				Provider: "anthropic",
				Tools:    []string{"glob", "grep", "read_file", "list_dir"},
				MaxTurns: 30,
			},
		},
	}
}

// CodeReviewSkill returns a multi-phase code review workflow.
// Four phases: explore → plan → implement → test.
func CodeReviewSkill() Skill {
	workflow := json.RawMessage(`{
		"name": "code-review",
		"version": "1",
		"steps": [
			{
				"id": "explore",
				"task": "agent-run",
				"type": "agent_loop",
				"loop": {"max_iterations": 20, "max_duration": 300000000000}
			},
			{
				"id": "plan",
				"task": "agent-run",
				"depends_on": ["explore"],
				"type": "agent_loop",
				"loop": {"max_iterations": 10, "max_duration": 180000000000}
			},
			{
				"id": "implement",
				"task": "agent-run",
				"depends_on": ["plan"],
				"type": "agent_loop",
				"loop": {"max_iterations": 50, "max_duration": 900000000000}
			},
			{
				"id": "test",
				"task": "agent-run",
				"depends_on": ["implement"],
				"type": "agent_loop",
				"loop": {"max_iterations": 30, "max_duration": 600000000000}
			}
		]
	}`)
	return Skill{
		Name: "code-review",
		Description: "Multi-phase code review: explore the codebase, " +
			"plan changes, implement fixes, run tests.",
		Workflow: workflow,
		Configs: map[string]agent.AgentConfig{
			"explore": {
				Name: "explore-reviewer",
				SystemPrompt: "You are exploring a codebase to prepare " +
					"for a code review. Find relevant files, understand " +
					"the architecture, and identify areas that need " +
					"attention. Produce a structured summary.",
				Model:    "claude-haiku-4-5-20251001",
				Provider: "anthropic",
				Tools:    []string{"glob", "grep", "read_file", "list_dir"},
				MaxTurns: 20,
			},
			"plan": {
				Name: "plan-reviewer",
				SystemPrompt: "Based on the exploration summary, create " +
					"a detailed plan for the code changes needed. " +
					"Identify specific files to modify, the nature of " +
					"each change, and the order of operations.",
				Model:    "claude-opus-4-6",
				Provider: "anthropic",
				Tools:    []string{"read_file", "glob", "grep"},
				MaxTurns: 10,
			},
			"implement": {
				Name: "implement-reviewer",
				SystemPrompt: "Execute the plan from the planning phase. " +
					"Make the code changes described. Use edit_file for " +
					"modifications and write_file for new files. Verify " +
					"each change with read_file after making it.",
				Model:    "claude-sonnet-4-6",
				Provider: "anthropic",
				Tools: []string{
					"read_file", "write_file", "edit_file",
					"bash", "glob", "grep",
				},
				MaxTurns: 50,
			},
			"test": {
				Name: "test-runner",
				SystemPrompt: "Run the test suite and verify the changes " +
					"from the implementation phase. Use bash to run tests. " +
					"If tests fail, read the error output and provide a " +
					"clear summary of what needs to be fixed.",
				Model:    "claude-sonnet-4-6",
				Provider: "anthropic",
				Tools:    []string{"bash", "read_file", "grep"},
				MaxTurns: 30,
			},
		},
	}
}

// RegisterBuiltinSkills adds the pre-built skills to the registry.
func RegisterBuiltinSkills(registry *Registry) {
	registry.Register(ExploreCodebaseSkill())
	registry.Register(CodeReviewSkill())
}
