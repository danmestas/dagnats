// Methodology: unit tests of Connection's reconnect policy against a fake
// node:http bridge, because a real server cannot be told to drop a stream
// or answer 5xx. Positive space: a dropped stream, a 5xx and a silent
// stream (no heartbeat within idle_timeout_ms) are all retried, with the
// backoff resetting after a connection that held. Negative space: a 4xx
// ends it at once with the status, and close() stops all retrying. Every
// wait is bounded.

import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, test } from "node:test";
import { BridgeClient, BridgeError } from "../src/index.ts";

type Handler = (hit: number, res: http.ServerResponse) => void;

const servers: http.Server[] = [];
afterEach(async () => {
  for (const s of servers.splice(0)) {
    s.closeAllConnections();
    await new Promise((r) => s.close(r));
  }
});

async function fake(handler: Handler): Promise<{ url: string; hits: () => number }> {
  let hits = 0;
  const server = http.createServer((req, res) => {
    req.resume();
    handler(++hits, res);
  });
  servers.push(server);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, hits: () => hits };
}

const sse = (res: http.ServerResponse) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.write("event: heartbeat\ndata: \n\n");
};

const fast = { backoff_base_ms: 10, backoff_max_ms: 40 };

async function until(cond: () => boolean, what: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

test("a 4xx stops reconnecting and closed rejects with the status", async () => {
  const f = await fake((_, res) => res.writeHead(400).end("task_types is required\n"));
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await assert.rejects(conn.closed, (e: unknown) => e instanceof BridgeError && e.status === 400);
  await assert.rejects(conn.ready, BridgeError);
  await new Promise((r) => setTimeout(r, 150));
  assert.equal(f.hits(), 1, "a 4xx must not be retried");
});

test("429 and 408 are retried, 429 waiting out Retry-After; other 4xx still stop", async () => {
  const f = await fake((hit, res) => {
    if (hit === 1) return void res.writeHead(429, { "retry-after": "1" }).end("slow down");
    if (hit === 2) return void res.writeHead(408).end("timeout");
    sse(res);
  });
  const started = Date.now();
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await conn.ready;
  assert.equal(f.hits(), 3);
  assert.ok(Date.now() - started >= 900, "Retry-After: 1 was not honoured");
  conn.close();
  await assert.rejects(conn.closed);

  const refused = await fake((_, res) => res.writeHead(403).end("no"));
  const stopped = new BridgeClient(refused.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await assert.rejects(stopped.closed, (e: unknown) => e instanceof BridgeError && e.status === 403);
});

test("a 5xx is retried with backoff until the stream opens", async () => {
  const f = await fake((hit, res) => (hit < 3 ? res.writeHead(500).end("boom") : sse(res)));
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await conn.ready;
  assert.equal(f.hits(), 3);
  conn.close();
  await assert.rejects(conn.closed);
});

test("a dropped stream reconnects", async () => {
  const f = await fake((hit, res) => {
    sse(res);
    if (hit === 1) res.end();
  });
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await until(() => f.hits() >= 2, "a reconnect after the stream ended");
  conn.close();
  await assert.rejects(conn.closed);
});

test("a stream with no heartbeat within idle_timeout_ms is dropped and reconnected", async () => {
  const f = await fake((_, res) => res.writeHead(200, { "content-type": "text/event-stream" }).write(": open\n\n"));
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], idle_timeout_ms: 100, ...fast });
  await until(() => f.hits() >= 2, "a reconnect after the silent stream");
  conn.close();
  await assert.rejects(conn.closed);
});

test("close stops reconnecting", async () => {
  const f = await fake((_, res) => res.writeHead(500).end("boom"));
  const conn = new BridgeClient(f.url, "t").connect({ worker: "w", task_types: ["x"], ...fast });
  await until(() => f.hits() >= 1, "the first attempt");
  conn.close();
  await assert.rejects(conn.closed, /closed/);
  // A request already on the wire when close() ran may still reach the server; let it land.
  await new Promise((r) => setTimeout(r, 50));
  const seen = f.hits();
  await new Promise((r) => setTimeout(r, 150));
  assert.equal(f.hits(), seen, "no attempts after close");
});

test("the connect body and the bearer token are what the bridge expects", async () => {
  let body = "";
  let auth = "";
  const server = http.createServer((req, res) => {
    auth = req.headers.authorization ?? "";
    req.on("data", (d: Buffer) => (body += d.toString()));
    req.on("end", () => sse(res));
  });
  servers.push(server);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const conn = new BridgeClient(url, "secret").connect({ worker: "w1", task_types: ["a", "b"], max_tasks: 2, worker_group: "gpu" });
  await conn.ready;
  assert.deepEqual(JSON.parse(body), { worker_id: "w1", task_types: ["a", "b"], max_tasks: 2, worker_group: "gpu" });
  assert.equal(auth, "Bearer secret");
  conn.close();
  await assert.rejects(conn.closed);
});
