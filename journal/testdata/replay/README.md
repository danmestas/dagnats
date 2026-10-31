# Replay fixtures

Each file is one case of the ADR-026 replay rules, written so a client
in any language can check that it decides what the Go SDK decides. The
rules themselves are stated in the wire-protocol reference, "Replay
rules for clients". `TestReplayFixtures` (`journal/replay_fixture_test.go`)
replays every file through `journal.Open`, `Tool` and `Model` on a real
server and fails when Go disagrees with the file.

## Format

```json
{
  "description": "what the case shows",
  "task": {"run_id", "step_id", "iteration", "attempt", "dispatch_nonce", "delivered", "stream_seq"},
  "records": [{"seq", "type", "iteration", "attempt", "dispatch_nonce", "delivered", "stream_seq", "index", "data"}],
  "calls": [
    {"kind": "tool", "name", "input", "replay": "safe|unsafe", "output", "error"},
    {"kind": "model", "output"}
  ],
  "expect": [{"outcome", "result", "output"}],
  "appended": [{"type", "data"}]
}
```

- `task` is the delivery the client is: the poll item's identity.
- `records` is what `journal.view` answers for that task: this
  iteration's records before the task's claim, in order (the bridge
  also sends `time`, which replay never reads).
- `calls` are the handler's calls, in order. `output` and `error` are
  what the tool or the model returns if the rules let it run.
- `expect` has one entry per call. A tool's `outcome` is `ran` (the
  tool ran), `reused` (a recorded result answered it), `interrupted`
  (an interrupted result answered it) or `mismatch` (a replay
  mismatch: the case ends there); `result` is the `tool.result` body
  the handler gets. A model's `outcome` is `requested`, `reused` or
  `failed` (the request returned the call's `error`), and `output` is
  the response.
- `appended` is every record the replay wrote after the claim, in
  order: its `type` and `data`.

Compare JSON values, not text: canonical JSON with sorted keys.

## Changing a fixture

`go test ./journal -run TestReplayFixtures -update` rewrites `expect`
and `appended` from the Go behaviour. Review that diff like code: a
change to a fixture is a change to the rules every client follows.
