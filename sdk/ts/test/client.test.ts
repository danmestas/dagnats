// Methodology: unit tests of BridgeClient's request shape and bounds
// against a fake node:http bridge. Positive space: the wire bodies are the
// bridge's own field names (max_tasks, timeout_ms, base64 log data) and the
// bearer token is sent. Negative space: every call is bounded by a default
// timeout and by the caller's signal, and an empty token sends no header.

import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, test } from "node:test";
import { BridgeClient, Fenced } from "../src/index.ts";

const servers: http.Server[] = [];
afterEach(async () => {
  for (const s of servers.splice(0)) {
    s.closeAllConnections();
    await new Promise((r) => s.close(r));
  }
});

interface Seen {
  url: string;
  auth: string | undefined;
  body: unknown;
}

async function fake(respond: (req: Seen, res: http.ServerResponse) => void): Promise<{ url: string; seen: Seen[] }> {
  const seen: Seen[] = [];
  const server = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (d: Buffer) => (raw += d.toString()));
    req.on("end", () => {
      const s = { url: req.url ?? "", auth: req.headers.authorization, body: raw === "" ? undefined : JSON.parse(raw) };
      seen.push(s);
      respond(s, res);
    });
  });
  servers.push(server);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, seen };
}

test("poll sends the bridge's field names and returns the items", async () => {
  const f = await fake((_, res) => res.end(JSON.stringify([{ task_id: "r.s", run_id: "r", step_id: "s", delivered: 1 }])));
  const items = await new BridgeClient(f.url, "tok").poll({ worker: "w", task_types: ["a"], max: 3, wait_ms: 1234 });
  assert.deepEqual(f.seen[0]!.body, { task_types: ["a"], max_tasks: 3, timeout_ms: 1234 });
  assert.equal(f.seen[0]!.auth, "Bearer tok");
  assert.equal(items[0]!.task_id, "r.s");
});

test("an empty token sends no Authorization header", async () => {
  const f = await fake((_, res) => res.end("[]"));
  await new BridgeClient(f.url, "").poll({ worker: "w", task_types: ["a"] });
  assert.equal(f.seen[0]!.auth, undefined);
});

test("resolve posts the action with its fields; a 409 fenced body throws Fenced", async () => {
  const f = await fake((_, res) => res.writeHead(409).end('{"error":"journal: fenced","code":"fenced"}\n'));
  const client = new BridgeClient(f.url, "");
  await assert.rejects(client.resolve("r.s", "complete", { output: { a: 1 } }), Fenced);
  assert.equal(f.seen[0]!.url, "/v1/tasks/r.s/resolve");
  assert.deepEqual(f.seen[0]!.body, { output: { a: 1 }, action: "complete" });
});

test("logs base64-encodes the data and tags the stream", async () => {
  const f = await fake((_, res) => res.end());
  await new BridgeClient(f.url, "").logs("r.s", ["hi", { stream: "err", data: "oops" }]);
  assert.equal(f.seen[0]!.url, "/v1/tasks/r.s/logs");
  assert.deepEqual(f.seen[0]!.body, {
    chunks: [
      { stream: "out", data: Buffer.from("hi").toString("base64") },
      { stream: "err", data: Buffer.from("oops").toString("base64") },
    ],
  });
});

test("a call that outlives its timeout rejects with TimeoutError", async () => {
  const f = await fake(() => {});
  const err = await new BridgeClient(f.url, "").resolve("r.s", "heartbeat", {}, { timeout_ms: 100 }).then(
    () => undefined,
    (e: unknown) => e,
  );
  assert.equal((err as Error).name, "TimeoutError");
});

test("a caller's signal aborts a call that would otherwise wait", async () => {
  const f = await fake(() => {});
  const controller = new AbortController();
  const pending = new BridgeClient(f.url, "").resolve("r.s", "heartbeat", {}, { signal: controller.signal });
  setTimeout(() => controller.abort(), 50);
  await assert.rejects(pending, (e: unknown) => (e as Error).name === "AbortError");
});
