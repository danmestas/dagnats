# Hello World Example

Two-step workflow: `greet` produces a greeting, `uppercase` transforms it.

## Run It

Terminal 1 — start the server:

```bash
dagnats serve
```

Terminal 2 — start the worker:

```bash
go run ./examples/hello-world/
```

Terminal 3 — register and run the workflow:

```bash
dagnats workflow register examples/hello-world/workflow.json
dagnats run start hello-world '"Alice"'
# Started: abc123...

dagnats run status abc123
dagnats run events abc123
dagnats run list --workflow=hello-world
```
