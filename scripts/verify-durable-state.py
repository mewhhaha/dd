#!/usr/bin/env python3
"""Exercise acknowledged KV and memory transactions across SIGKILL in a disposable store."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def request(port, path, *, worker=None, payload=None, method=None):
    headers = {"Authorization": "Bearer crash-recovery-probe"}
    if worker:
        headers["Host"] = f"{worker}.example.com"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode()
    command = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", body, headers, method=method
    )
    with urllib.request.urlopen(command, timeout=10) as response:
        return json.loads(response.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, default=Path("target/debug/dd_server"))
    arguments = parser.parse_args()
    binary = arguments.server.resolve(strict=True)
    public_port, private_port = free_port(), free_port()
    source = """
export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === '/memory') {
      const memory = env.MEMORY.get('entity');
      if (request.method === 'PUT') {
        return Response.json(await memory.atomic((tx) => {
          const count = (tx.get('count') ?? 0) + 42;
          tx.put('count', count);
          return { count, committed: true };
        }, { idempotencyKey: 'increment' }));
      }
      if (request.method === 'DELETE') {
        await memory.atomic((tx) => tx.delete('count'));
      }
      return Response.json(await memory.atomic((tx) => tx.get('count')));
    }
    if (request.method === 'PUT') {
      await env.STATE.put('record', { committed: true, count: 42 });
    }
    if (request.method === 'DELETE') await env.STATE.delete('record');
    return Response.json(await env.STATE.get('record'));
  },
};
"""
    with tempfile.TemporaryDirectory(prefix="dd-crash-recovery-") as directory:
        with open(Path(directory) / "server.log", "w+") as log:
            server = None
            try:
                for generation in range(5):
                    server = subprocess.Popen(
                        [str(binary), "--bind-public-addr", f"127.0.0.1:{public_port}",
                         "--bind-private-addr", f"127.0.0.1:{private_port}",
                         "--private-token", "crash-recovery-probe"],
                        cwd=directory, stdout=log, stderr=log,
                        env={**os.environ, "DD_OTEL_ENABLED": "false"},
                    )
                    deadline = time.monotonic() + 20
                    while True:
                        if server.poll() is not None or time.monotonic() >= deadline:
                            log.seek(0)
                            raise RuntimeError(f"server failed to become ready:\n{log.read()}")
                        try:
                            request(private_port, "/readyz")
                            break
                        except (urllib.error.URLError, TimeoutError):
                            time.sleep(0.05)
                    if generation == 0:
                        for worker in ["writer", "neighbor"]:
                            request(private_port, "/v1/deploy", payload={
                                "name": worker, "source": source,
                                "config": {"public": True, "bindings": [
                                    {"type": "kv", "binding": "STATE"},
                                    {"type": "memory", "binding": "MEMORY"},
                                ]},
                            })
                        assert request(public_port, "/", worker="writer", method="PUT") == {
                            "committed": True, "count": 42
                        }
                    elif generation == 1:
                        assert request(public_port, "/", worker="writer") == {
                            "committed": True, "count": 42
                        }, "acknowledged value was lost after SIGKILL"
                        assert request(public_port, "/", worker="writer", method="DELETE") is None
                    elif generation == 2:
                        assert request(public_port, "/", worker="writer") is None, "acknowledged deletion was lost"
                        assert request(public_port, "/memory", worker="writer", method="PUT") == {
                            "count": 42, "committed": True
                        }
                    elif generation == 3:
                        assert request(public_port, "/memory", worker="writer") == 42, "acknowledged memory transaction was lost"
                        assert request(public_port, "/memory", worker="writer", method="PUT") == {
                            "count": 42, "committed": True
                        }, "idempotent command executed again after restart"
                        assert request(public_port, "/memory", worker="writer", method="DELETE") is None
                    else:
                        assert request(public_port, "/memory", worker="writer") is None, "acknowledged memory deletion was lost"
                        assert request(public_port, "/", worker="neighbor") is None, "worker KV namespaces overlap"
                        assert request(public_port, "/memory", worker="neighbor") is None, "worker memory namespaces overlap"
                    server.kill()
                    server.wait(timeout=10)
                    server = None
            finally:
                if server is not None:
                    server.kill()
                    server.wait(timeout=10)
    print("PASS: acknowledged KV/memory writes and deletes survive SIGKILL; commands replay once and worker namespaces remain isolated")


if __name__ == "__main__":
    main()
