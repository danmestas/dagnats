# Shell Completions

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (additive CLI feature)

## Problem

The CLI has 13+ commands with subcommands, flags, and arguments that
require workflow names and run IDs. There's no tab completion — users
must remember command names and copy-paste IDs. This is daily friction
for power users.

## Design

### 1. Static Completion Script Generation

New command:

```bash
dagnats completion bash   # Output bash completion script
dagnats completion zsh    # Output zsh completion script
dagnats completion fish   # Output fish completion script
```

Usage (add to shell profile):

```bash
# Bash
eval "$(dagnats completion bash)"

# Zsh
eval "$(dagnats completion zsh)"

# Fish
dagnats completion fish | source
```

### 2. What Gets Completed

**Static completions** (no NATS connection needed):

| Context | Completions |
|---------|-------------|
| `dagnats <TAB>` | workflow, run, trigger, dlq, workers, serve, init, config, status, logs, dev, trace, metrics, observe, sidecar |
| `dagnats run <TAB>` | start, status, cancel, cancel-all, bulk, retry-all, signal, list, events, inspect, watch, output, retry |
| `dagnats workflow <TAB>` | list, register, show, validate |
| `dagnats trigger <TAB>` | create, list, update, delete, enable, disable, test, history |
| `dagnats dlq <TAB>` | list, replay, watch |
| `dagnats completion <TAB>` | bash, zsh, fish |
| Any command with `--<TAB>` | Applicable flags for that command |

**Dynamic completions** (require NATS connection, best-effort):

| Context | Source |
|---------|--------|
| `dagnats run start <TAB>` | Workflow names from KV |
| `dagnats workflow show <TAB>` | Workflow names from KV |
| `dagnats run status <TAB>` | Recent run ID prefixes from KV |
| `dagnats run inspect <TAB>` | Recent run ID prefixes from KV |
| `dagnats trigger enable <TAB>` | Trigger IDs from KV |

Dynamic completions connect to NATS using the same resolution as
the CLI (env vars, config file). If connection fails, fall back to
no completions (no error output — silent failure is the shell
completion convention).

### 3. Implementation Approach

Since dagnats doesn't use a CLI framework (it's hand-rolled argument
parsing in `cli/root.go`), implement completion as a self-contained
module:

```go
// cli/completion.go

// GenerateBashCompletion writes a bash completion script to stdout.
func GenerateBashCompletion()

// GenerateZshCompletion writes a zsh completion script to stdout.
func GenerateZshCompletion()

// GenerateFishCompletion writes a fish completion script to stdout.
func GenerateFishCompletion()

// HandleCompletionRequest checks if the shell is requesting
// completions (via __DAGNATS_COMPLETE env var) and responds.
// Returns true if it handled a completion request.
func HandleCompletionRequest() bool
```

The generated scripts set up a completion function that either:
- Returns static completions for known command/subcommand positions
- Calls `dagnats __complete <args...>` for dynamic completions

The `__complete` hidden command:
- Parses the partial command line
- Determines context (which command, which argument position)
- For dynamic completions, connects to NATS and lists matching items
- Outputs one completion per line
- Exits silently on any error

### 4. Dynamic Completion Limits

- List at most 50 workflow names (sorted alphabetically)
- List at most 20 recent run IDs (most recent first)
- Connection timeout: 500ms (completions must be fast)
- Cache workflow names for 5 seconds in a temp file to avoid
  repeated NATS round-trips during rapid tab presses

### 5. Files Changed

| File | Change |
|------|--------|
| `cli/completion.go` (new) | Completion script generation and `__complete` handler |
| `cli/completion_test.go` (new) | Tests for static completions and `__complete` parsing |
| `cli/root.go` | Add `completion` command and `__complete` hidden command |

### 6. Notes

- Start with Bash and Zsh. Fish can be added later if there's demand.
- Do not pull in a CLI framework just for completions — keep it
  hand-rolled to match the existing style.
- The `__complete` command is hidden (not shown in help) — it's an
  implementation detail of the completion scripts.
