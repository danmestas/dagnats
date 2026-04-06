"""DagNats HTTP bridge worker example in Python.

Connects to the DagNats HTTP bridge and processes "uppercase" tasks.
Demonstrates the full worker lifecycle: connect, poll, resolve.
Uses only the requests library for HTTP calls.
"""

import json
import os
import sys
import threading
import time

import requests

BRIDGE_URL = os.environ.get("DAGNATS_BRIDGE_URL", "http://localhost:8080")
WORKER_ID = os.environ.get("DAGNATS_WORKER_ID", "python-worker-1")
BRIDGE_TOKEN = os.environ.get("DAGNATS_BRIDGE_TOKEN", "")
TASK_TYPES = ["uppercase"]
POLL_TIMEOUT_MS = 30000
RECONNECT_DELAY_SEC = 5
MAX_RECONNECT_DELAY_SEC = 60


def headers():
    """Build common request headers."""
    h = {"Content-Type": "application/json"}
    if BRIDGE_TOKEN:
        h["Authorization"] = f"Bearer {BRIDGE_TOKEN}"
    return h


def connect():
    """Register worker via SSE connection (runs in background thread).

    The bridge sends heartbeat events every 25 seconds. This thread
    reads and discards them to keep the connection alive. Returns the
    response object so the caller can close it on shutdown.
    """
    body = {
        "worker_id": WORKER_ID,
        "task_types": TASK_TYPES,
        "max_tasks": 1,
    }
    resp = requests.post(
        f"{BRIDGE_URL}/v1/workers/connect",
        headers=headers(),
        json=body,
        stream=True,
        timeout=None,
    )
    resp.raise_for_status()
    print(f"[connect] registered as {WORKER_ID}")
    return resp


def drain_sse(resp):
    """Read and discard SSE heartbeats until the connection closes."""
    try:
        for line in resp.iter_lines():
            if line:
                decoded = line.decode("utf-8", errors="replace")
                if decoded.startswith("event:"):
                    event_type = decoded[len("event:"):].strip()
                    print(f"[sse] {event_type}")
    except requests.exceptions.ConnectionError:
        pass
    finally:
        resp.close()


def poll_tasks():
    """Long-poll the bridge for available tasks.

    Returns a list of task dicts, or an empty list on timeout.
    """
    body = {
        "task_types": TASK_TYPES,
        "max_tasks": 1,
        "timeout_ms": POLL_TIMEOUT_MS,
    }
    resp = requests.post(
        f"{BRIDGE_URL}/v1/tasks/poll",
        headers=headers(),
        json=body,
        timeout=(POLL_TIMEOUT_MS / 1000) + 10,
    )
    resp.raise_for_status()
    return resp.json()


def resolve_complete(task_id, output):
    """Mark a task as successfully completed."""
    body = {"action": "complete", "output": output}
    resp = requests.post(
        f"{BRIDGE_URL}/v1/tasks/{task_id}/resolve",
        headers=headers(),
        json=body,
        timeout=10,
    )
    resp.raise_for_status()
    print(f"[resolve] completed {task_id}")


def resolve_fail(task_id, error_msg):
    """Mark a task as failed."""
    body = {
        "action": "fail",
        "error": error_msg,
        "failure_type": "permanent",
    }
    resp = requests.post(
        f"{BRIDGE_URL}/v1/tasks/{task_id}/resolve",
        headers=headers(),
        json=body,
        timeout=10,
    )
    resp.raise_for_status()
    print(f"[resolve] failed {task_id}: {error_msg}")


def handle_task(task):
    """Process a single uppercase task."""
    task_id = task["task_id"]
    raw_input = task.get("input")
    print(f"[task] received {task_id}: {raw_input}")

    try:
        if isinstance(raw_input, str):
            text = raw_input
        else:
            text = json.dumps(raw_input) if raw_input else ""
        result = text.upper()
        resolve_complete(task_id, result)
    except Exception as exc:
        resolve_fail(task_id, str(exc))


def run():
    """Main loop: connect, then poll and process tasks forever."""
    reconnect_delay = RECONNECT_DELAY_SEC

    while True:
        try:
            sse_resp = connect()
            sse_thread = threading.Thread(
                target=drain_sse, args=(sse_resp,), daemon=True,
            )
            sse_thread.start()
            reconnect_delay = RECONNECT_DELAY_SEC

            print(f"[poll] waiting for tasks ({TASK_TYPES})...")
            while True:
                tasks = poll_tasks()
                for task in tasks:
                    handle_task(task)

        except KeyboardInterrupt:
            print("\nShutting down...")
            sys.exit(0)
        except Exception as exc:
            print(f"[error] {exc}")
            print(
                f"[reconnect] retrying in {reconnect_delay}s..."
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * 2, MAX_RECONNECT_DELAY_SEC,
            )


if __name__ == "__main__":
    run()
