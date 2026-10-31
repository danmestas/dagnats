# Minimal Program Example

The smallest dagnats program: one task type, `min.upper`, served by the
worker `program.Attach` starts. It connects as its own NKey user, with
NATS permissions derived from its manifest by `program.PermissionsFor`,
and dials with `nats.CustomInboxPrefix("_INBOX.min")` so no other program
can read its replies.

## Run It

The test is the runnable form: it starts an embedded NATS server with
min's user, a real orchestrator, and runs `min.upper` to completion.

```bash
go test ./examples/program-min/
```

Against a server you run yourself, print the key and permissions to
bind (self-registration replaces this step in a later release):

```bash
go run ./examples/program-min/ -print-permissions -seed min.seed
NATS_URL=nats://127.0.0.1:4222 go run ./examples/program-min/ -seed min.seed
```

The seed file is created on first run with mode 0600 and never leaves
the machine; only its public key is printed.
