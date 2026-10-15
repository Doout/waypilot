"""Real MCP state reuse at a fixed connected-client and concurrent-request target."""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import resource
import sys
import time
from contextlib import ExitStack
from pathlib import Path

import psutil
from net import HTTP, MCP

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from state_support import ROOT, Process, StateDemo


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


async def run(args):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, max(soft, 8192)), hard))
    phases, samples = [], []
    with ExitStack() as stack:
        peer = stack.enter_context(
            Process([os.environ["WAYPILOT_PYTHON"], str(ROOT / "load/peer.py")], dict(os.environ))
        )
        upstream = "http://" + peer.ready["listen"]
        demo = stack.enter_context(
            StateDemo(
                args.binary,
                upstream=upstream,
                upstream_calls=args.upstream_calls,
                environment={"RUST_LOG": "warn"},
            )
        )
        port = int(demo.base.rsplit(":", 1)[1])
        control = HTTP("127.0.0.1", int(peer.ready["listen"].rsplit(":", 1)[1]))
        admin = HTTP("127.0.0.1", port, demo.admin)
        clients = [MCP(HTTP("127.0.0.1", port, demo.token)) for _ in range(args.clients)]
        process = psutil.Process(demo.service.process.pid)
        slots = asyncio.Semaphore(32)

        async def initialize(client):
            async with slots:
                await client.initialize()

        try:
            await asyncio.gather(*(initialize(client) for client in clients))
            status, _, stats = await admin.request("/admin/stats")
            assert status == 200 and stats["sessions"] == args.clients, stats
            gate = asyncio.Semaphore(args.concurrency)
            active, peak = 0, 0

            async def invoke(client, value):
                nonlocal active, peak
                async with gate:
                    active += 1
                    peak = max(peak, active)
                    started = time.perf_counter_ns()
                    try:
                        reply = await client.call("waypilot__state", {})
                        assert "result" in reply, reply
                        result = reply["result"]["structuredContent"]

                        def fingerprint(text):
                            return hashlib.sha256(
                                json.dumps(text, sort_keys=True).encode()
                            ).hexdigest()

                        assert result["outputs"]["left-index"]["structuredContent"][
                            "fingerprint"
                        ] == fingerprint(value)
                        assert result["outputs"]["right-index"]["structuredContent"][
                            "fingerprint"
                        ] == fingerprint("b")
                        return (time.perf_counter_ns() - started) / 1e6
                    finally:
                        active -= 1

            for name, update, expected in [
                ("cold", {}, "a"),
                ("warm", {}, "a"),
                ("version_only_change", {"key": "a", "touch": True}, "a"),
                ("value_change", {"key": "a", "value": "new"}, "new"),
            ]:
                if update:
                    assert (await control.request("/control", {"state": update}))[0] == 200
                _, _, before = await control.request("/stats")
                started = time.monotonic()
                # All clients participate over two waves at the full target.
                latencies = []
                for offset in range(0, len(clients), args.concurrency):
                    wave = await asyncio.gather(
                        *(
                            invoke(client, expected)
                            for client in clients[offset : offset + args.concurrency]
                        ),
                        return_exceptions=True,
                    )
                    failures = [str(value) for value in wave if isinstance(value, BaseException)]
                    assert not failures, {"count": len(failures), "examples": failures[:3]}
                    latencies.extend(wave)
                    # Each workflow consumes one entry token, four tool tokens and six
                    # validation tokens. Refill between full-concurrency bursts.
                    await asyncio.sleep(len(wave) * 11 / 10000)

                elapsed = time.monotonic() - started
                _, _, after = await control.request("/stats")
                business = sum(
                    after["by_tool"].get(tool, 0) - before["by_tool"].get(tool, 0)
                    for tool in ["state_read", "dependency_index", "source_index"]
                )
                expected_calls = {
                    "cold": 4,
                    "warm": 0,
                    "version_only_change": 2,
                    "value_change": 2,
                }[name]
                assert business == expected_calls, (name, business, expected_calls)
                phases.append(
                    {
                        "phase": name,
                        "calls": len(latencies),
                        "elapsed_seconds": elapsed,
                        "throughput_per_second": len(latencies) / elapsed,
                        "latency_ms": {
                            "p50": percentile(latencies, 0.5),
                            "p95": percentile(latencies, 0.95),
                            "p99": percentile(latencies, 0.99),
                        },
                        "business_calls": business,
                        "validation_calls": after["by_tool"].get("state_version", 0)
                        - before["by_tool"].get("state_version", 0),
                        "peak_upstream_calls": after["peak"],
                    }
                )
                samples.append({"rss_bytes": process.memory_info().rss, "fds": process.num_fds()})
            assert peak == args.concurrency
            output = {
                "environment": {
                    "platform": platform.platform(),
                    "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
                },
                "methodology": {
                    "connected_clients": args.clients,
                    "concurrency_limit": args.concurrency,
                    "upstream_call_limit": args.upstream_calls,
                    "peak_pending_calls": peak,
                    "pacing": "full-concurrency bursts with token refill between waves; elapsed throughput includes pacing",
                    "workload": "four barrier-separated phases; all connected clients call the same two-branch workflow; shared exact identity scope; external changes between phases; no model calls; no mid-phase mutation",
                },
                "correctness": "every response checked against independent fingerprints; no stale results or unexpected failures",
                "phases": phases,
                "resource_samples": samples,
            }
        except BaseException:
            for label, child in [("peer", peer), ("gateway", demo.service)]:
                child.log.seek(0)
                print(label + ": " + child.log.read()[-5000:], file=sys.stderr)
            raise
        finally:

            async def close(client):
                async with slots:
                    await client.close()

            await asyncio.gather(*(close(client) for client in clients), return_exceptions=True)
            await control.close()
            await admin.close()
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=500)
    parser.add_argument("--upstream-calls", type=int, default=64)
    parser.add_argument("--output", default="target/state-scale.json")
    args = parser.parse_args()
    assert 1 <= args.concurrency <= args.clients <= 1000
    output = asyncio.run(run(args))
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(output, indent=2) + "\n")
    print(
        json.dumps(
            {
                "state_scale": "passed",
                "clients": args.clients,
                "concurrency": args.concurrency,
                "raw": str(destination),
            }
        )
    )


if __name__ == "__main__":
    main()
