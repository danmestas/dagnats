# Getting Started Guide Revamp

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (documentation-only change)

## Problem

The getting-started guide is comprehensive once you're inside it, but has
two gaps:

1. **No install instructions.** The guide says "The `dagnats` CLI installed"
   as a prerequisite but doesn't explain how to install it. There's no
   `go install` one-liner, no Homebrew formula, and no pre-built binary
   download link.

2. **`dagnats init` is undiscoverable.** The scaffold command exists and
   works, but the getting-started guide never mentions it. Users manually
   create `workflow.json` and `main.go` instead of running
   `dagnats init my-project`.

3. **No zero-to-running tutorial.** The guide assumes the reader already
   has a Go project. There's no single path from "I just cloned the repo"
   to "I see my workflow complete."

## Design

### 1. Add Installation Section

Add a new section before "1. Start the Server":

```markdown
## Install

From source (requires Go 1.21+):

    go install github.com/danmestas/dagnats/cmd/dagnats@latest

Or build from a local clone:

    git clone https://github.com/danmestas/dagnats.git
    cd dagnats
    make build
    export PATH=$PWD/bin:$PATH

Verify:

    dagnats --version
```

### 2. Replace Manual File Creation with `dagnats init`

Replace the current "2. Define a Workflow" section (manual JSON creation)
with:

```markdown
## 2. Scaffold a Project

    dagnats init hello-world

This creates a `hello-world/` directory with:
- `workflow.json` — two-step pipeline (process → format)
- `main.go` — worker with handler stubs

To add a workflow to an existing project:

    dagnats init workflow etl-pipeline --steps=fetch,transform,load
```

Keep the JSON explanation but move it under a "### Understanding the
workflow definition" subsection so users see what `init` generated.

### 3. Restructure as Linear Tutorial

Reorganize the guide into a clear numbered path:

1. Install
2. Start the server (`dagnats serve`)
3. Scaffold (`dagnats init hello-world`)
4. Explore the generated files (explain workflow.json, main.go)
5. Register (`dagnats workflow register hello-world/workflow.json`)
6. Run the worker (`cd hello-world && go run .`)
7. Start a run (`dagnats run start hello-world '"World"' --watch`)
8. Inspect the result (`dagnats run output --last`)

### 4. Add "What Next?" Section

After the tutorial, add links to:
- Adding retries, timeouts, agent loops (already in the guide, just link)
- `dagnats init workflow` for adding workflows to existing projects
- `dagnats trigger create` for scheduling
- The examples/ directory

### 5. Files Changed

| File | Change |
|------|--------|
| `docs/getting-started.md` | Restructure per above |

No code changes required.
