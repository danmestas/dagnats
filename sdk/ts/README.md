# @dagnats/bridge

A TypeScript client for the dagnats [HTTP bridge](../../docs/site/content/docs/workers/http-bridge.md),
with the ADR-026 agent journal replay rules. Node 22.19 or later, ESM, no runtime
dependencies (global `fetch`, SSE parsed by hand).

## Install

dagnats does not publish this package to npmjs or GitHub Packages. It ships as a GitHub release
asset, `dagnats-bridge-<version>.tgz`, listed in the release's `SHA256SUMS`:

```
npm install https://github.com/danmestas/dagnats/releases/download/<tag>/dagnats-bridge-<version>.tgz
```

`<tag>` is the dagnats release tag (`v0.0.37`) and `<version>` is the same without the `v`
(`0.0.37`). The tarball is compiled ESM plus `.d.ts` and has no dependencies.

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
`loadCheckpoint()` (`{ state, source }`, with the bridge's own `source`: `"journal"`, `"checkpoint"`
or `"payload"`; `undefined` when there is none), `heartbeat()`, `stream(chunk)`,
`journal.view()` (`{ claim, records }`) and `journal.append(type, data)`. `task` is the
delivery identity from the poll item (including `dispatch_nonce` and `task_type`) and goes to
`Journal.open`. A `checkpoint` is durable only once a later `continue` commits it.

### Errors

Told apart by status and body, so a fence and a cancelled run are not confused (both are `409`):

| Class | Wire |
|---|---|
| `Fenced` | `409 {"code":"fenced"}` |
| `RunCancelled` | `409`, plain text `run cancelled` |
| `Stale`, `Committed`, `RestartLimit` | `410` with that `code` |
| `StateTooLarge`, `BodyTooLarge` | `413` with that `code`; `StateTooLarge` carries `maxBytes` and `bytes` |
| `NotProvisioned` | `503 {"code":"not_provisioned"}` |
| `BridgeError` | anything else; carries `status`, `body` and the JSON `code` if any |

All extend `BridgeError`. They surface from `journal.append` and `journal.view` as well as from
`complete`. `isTerminal(err)` is true for `Fenced`, `Stale`, `Committed`, `RestartLimit` and
`RunCancelled`, and false for network errors, 5xx and the 413s.

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
