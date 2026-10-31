# @dagnats/bridge

A TypeScript client for the dagnats [HTTP bridge](../../docs/site/content/docs/workers/http-bridge.md),
with the ADR-026 agent journal replay rules. Node 22.19 or later, ESM, no runtime
dependencies (global `fetch`, SSE parsed by hand).

Not published to npm yet; build it from this directory with `npm ci && npm run build`.

## Use

```ts
import { BridgeClient, TaskContext, isTerminal } from "@dagnats/bridge";

const client = new BridgeClient("http://127.0.0.1:8080", process.env.DAGNATS_BRIDGE_TOKEN ?? "");

// Keep the worker registered; reconnects with backoff, stops on a 4xx.
const conn = client.connect({ worker: "w1", task_types: ["summarise"] });
conn.closed.catch((reason) => console.error("connection ended:", reason));

for (;;) {
  const items = await client.poll({ worker: "w1", task_types: ["summarise"], wait_ms: 20_000 });
  for (const item of items) {
    const ctx = new TaskContext(client, item);
    try {
      await ctx.complete(await run(ctx.input));
    } catch (err) {
      if (isTerminal(err)) continue; // fenced, finished or cancelled: the task is not ours any more
      await ctx.fail({ error: String(err), failure_type: "retriable" });
    }
  }
}
```

Every call takes an optional `{ signal, timeout_ms }`. The default timeout is 5 s, and a poll
gets its `wait_ms` plus 5 s.

### TaskContext

`complete(output)`, `fail({ error, failure_type?, retry_after_ms? })`, `checkpoint(data)`,
`loadCheckpoint()` (the data, or `undefined` when there is none), `heartbeat()`, `stream(chunk)`,
`journal.view()` (`{ claim, records }`) and `journal.append(type, data)`. `task` is the
delivery identity for `Journal.open`. The bridge's poll answer has no dispatch nonce, so
`task.dispatch_nonce` is empty until the first `journal.view()`.

### Errors

Told apart by status and body, so a fence and a cancelled run are not confused (both are `409`):

| Class | Wire |
|---|---|
| `Fenced` | `409 {"code":"fenced"}` |
| `RunCancelled` | `409`, plain text `run cancelled` |
| `Stale`, `Committed`, `RestartLimit` | `410` with that `code` |
| `NotProvisioned` | `503 {"code":"not_provisioned"}` |
| `BridgeError` | anything else; carries `status`, `body` and the JSON `code` if any |

All extend `BridgeError`. They surface from `journal.append` and `journal.view` as well as from
`complete`. `isTerminal(err)` is true for `Fenced`, `Stale`, `Committed`, `RestartLimit` and
`RunCancelled`, and false for network errors and 5xx.

### Journal

`@dagnats/bridge/journal` implements the replay rules over a `Log`; `Journal<M>` is generic over
the model output type and stores it as given.

```ts
import { BridgeLog, Journal } from "@dagnats/bridge/journal";

const journal = await Journal.open<MyMessage>(new BridgeLog(ctx.journal), ctx.task);
const { message, outcome } = await journal.model(() => callModel());
const { result } = await journal.tool("bash", { cmd: "ls" }, "unsafe", (key) => runBash(key));
```

`BridgeLog` serializes appends: one in flight at a time, in call order. `MemoryLog` is for tests.
Record `data` shapes are the Go ones, so `dagnats journal check` and Go workers read the same
journal. Not yet here: `continue` and `inputs.place`.

## Develop

```
npm ci
npm run typecheck
npm test
```

`npm test` builds `test/devserver` (a Go program, so Go is needed) and the integration tests start
it as a real in-process dev-mode server. `test/replay-fixtures.test.ts` runs the language-neutral
fixtures in [`journal/testdata/replay`](../../journal/testdata/replay) through `Journal`; those
files are the Go SDK's own, not copied.
