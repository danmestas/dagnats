// Test harness: starts the in-process dagnats dev server (test/devserver,
// built by `npm test`'s pretest) as a child process and offers the REST
// calls a test needs to put work on it. The child exits when its stdin
// closes, so a crashed test run cannot strand a server.

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

const binary = fileURLToPath(new URL("./.bin/devserver", import.meta.url));
const startTimeoutMs = 30_000;

export interface DevServer {
  url: string;
  /** A second bridge on the same NATS; also takes POST /_test/publish. */
  secondURL: string;
  stop(): Promise<void>;
}

export async function startDevServer(): Promise<DevServer> {
  if (!existsSync(binary)) throw new Error(`${binary} is missing; run \`npm test\` so pretest builds it`);
  const child: ChildProcessWithoutNullStreams = spawn(binary, [], { stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (d: Buffer) => (stderr += d.toString()));
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));

  const line = await new Promise<string>((resolve, reject) => {
    let buf = "";
    const timer = setTimeout(() => reject(new Error(`dev server did not start in time: ${stderr}`)), startTimeoutMs);
    child.stdout.on("data", (d: Buffer) => {
      buf += d.toString();
      const nl = buf.indexOf("\n");
      if (nl >= 0) {
        clearTimeout(timer);
        resolve(buf.slice(0, nl));
      }
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`dev server exited early (${code}): ${stderr}`));
    });
  });
  const info = JSON.parse(line) as { url: string; second_url: string };
  return {
    url: info.url,
    secondURL: info.second_url,
    async stop() {
      child.stdin.end();
      const killer = setTimeout(() => child.kill("SIGTERM"), 15_000);
      await exited;
      clearTimeout(killer);
    },
  };
}

async function rest(url: string, method: string, body?: unknown): Promise<unknown> {
  const res = await fetch(url, {
    method,
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${url}: ${res.status} ${text}`);
  return text === "" ? undefined : JSON.parse(text);
}

let workflowSeq = 0;

/** Registers a one-step workflow for taskType and starts a run of it; returns the run id. */
export async function startOneStepRun(srv: DevServer, taskType: string, input: unknown = {}): Promise<string> {
  const name = `sdkts-${taskType}-${++workflowSeq}-${Date.now()}`;
  await rest(`${srv.url}/workflows`, "POST", {
    name,
    version: "1.0",
    steps: [{ id: "step", task: taskType }],
  });
  const res = (await rest(`${srv.url}/runs`, "POST", { workflow: name, input })) as { run_id: string };
  return res.run_id;
}

export async function cancelRun(srv: DevServer, runId: string): Promise<void> {
  await rest(`${srv.url}/runs/${runId}/cancel`, "POST", {});
}

export async function runStatus(srv: DevServer, runId: string): Promise<string> {
  const run = (await rest(`${srv.url}/runs/${runId}`, "GET")) as { status: string };
  return run.status;
}

/** Waits, bounded, until the run reaches status. */
export async function waitForStatus(srv: DevServer, runId: string, status: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    last = await runStatus(srv, runId);
    if (last === status) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`run ${runId} is ${last}, wanted ${status}`);
}

/** Puts a hand-made task (`state` is base64, as Go encodes []byte) on the queue through the second bridge; a repeat is a dispatch resend. */
export async function publishTask(
  srv: DevServer,
  taskType: string,
  payload: { run_id: string; step_id: string; dispatch_nonce: string; state?: string },
): Promise<void> {
  await rest(`${srv.secondURL}/_test/publish?type=${taskType}`, "POST", payload);
}
