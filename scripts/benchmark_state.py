"""Compare the checked-out candidate with an exact baseline binary on mutable MCP state."""

import argparse
import hashlib
import json
import math
import platform
import time
from contextlib import ExitStack
from pathlib import Path

import psutil
from state_support import Client, StateDemo


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--read-ms", type=int, default=20)
    parser.add_argument("--change-every", type=int, default=5)
    args = parser.parse_args()
    assert 10 <= args.samples <= 2000 and 0 <= args.read_ms <= 1000 and args.change_every > 0
    modes = {
        "baseline_ttl": (args.baseline, False, 60000),
        "baseline_uncached": (args.baseline, False, 0),
        "candidate_ttl": (args.binary, False, 60000),
        "candidate_validated": (args.binary, True, 60000),
    }
    rows = {name: {"trials": [], "stale_results": 0, "peak_rss_bytes": 0} for name in modes}
    with ExitStack() as stack:
        peers, clients, processes, before, cpu_before = {}, {}, {}, {}, {}
        for name, (binary, validated, ttl) in modes.items():
            peer = stack.enter_context(StateDemo(binary, validated=validated, ttl=ttl))
            for key in ["a", "b"]:
                peer.mutate(key=key, read_ms=args.read_ms)
            peers[name] = peer
            clients[name] = Client(peer.base, peer.token)
            clients[name].call("waypilot__state", {})
            processes[name] = psutil.Process(peer.service.process.pid)
            cpu_before[name] = sum(processes[name].cpu_times()[:2])
            before[name] = peer.stats()
        current = "a"
        for index in range(args.samples):
            changed = index % args.change_every == 0
            if changed:
                current = f"revision-{index}"
                for peer in peers.values():
                    peer.mutate(key="a", value=current)
            order = list(modes) if index % 2 == 0 else list(reversed(modes))
            for name in order:
                started = time.perf_counter_ns()
                result = clients[name].call("waypilot__state", {})["structuredContent"]
                elapsed = (time.perf_counter_ns() - started) / 1000
                actual = result["outputs"]
                correct = actual["left-index"]["structuredContent"]["fingerprint"] == fingerprint(
                    current
                ) and actual["right-index"]["structuredContent"]["fingerprint"] == fingerprint("b")
                if name in ["candidate_validated", "baseline_uncached"]:
                    assert correct, (name, index, result)
                row = rows[name]
                row["stale_results"] += not correct
                row["peak_rss_bytes"] = max(
                    row["peak_rss_bytes"], processes[name].memory_info().rss
                )
                row["trials"].append(
                    {
                        "index": index,
                        "changed": changed,
                        "elapsed_us": elapsed,
                        "correct": correct,
                        "steps": {step["id"]: step["disposition"] for step in result["steps"]},
                    }
                )
        for name, row in rows.items():
            values = [trial["elapsed_us"] for trial in row["trials"]]
            row["latency_us"] = {
                "p50": percentile(values, 0.5),
                "p95": percentile(values, 0.95),
                "p99": percentile(values, 0.99),
            }
            row["gateway_cpu_seconds"] = sum(processes[name].cpu_times()[:2]) - cpu_before[name]
            stats = peers[name].stats()
            row["upstream_calls"] = stats["upstream_calls"] - before[name]["upstream_calls"]
            row["state_checks"] = stats.get("state_checks", 0) - before[name].get("state_checks", 0)
            row["business_calls"] = row["upstream_calls"] - row["state_checks"]
            row["invalidated_entries"] = stats["cache"].get("invalidated", 0) - before[name][
                "cache"
            ].get("invalidated", 0)
            clients[name].close()

    def sha(binary):
        return hashlib.sha256(Path(binary).read_bytes()).hexdigest()

    print(
        json.dumps(
            {
                "environment": {
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "baseline_sha256": sha(args.baseline),
                    "candidate_sha256": sha(args.binary),
                },
                "methodology": {
                    "comparison": "same-binary controls"
                    if Path(args.binary).resolve() == Path(args.baseline).resolve()
                    else "separate baseline binary",
                    "samples": args.samples,
                    "read_ms": args.read_ms,
                    "change_every": args.change_every,
                    "workload": "owned loopback MCP HTTP; two reads and two descendants; mutate one resource outside the gateway; alternate mode order; initialized sessions",
                    "model_calls": 0,
                    "timing": "end-to-end client HTTP/JSON; cold startup excluded",
                    "resources": "one gateway and one peer per mode; equal configured limits; calls issued sequentially across modes; peak RSS sampled after each call; CPU is gateway process only",
                    "correctness": "baseline TTL modes deliberately expose stale reuse; baseline_uncached and candidate_validated must match the independent value oracle on every trial",
                    "limits": "latency comparison is workload-specific; no shared-state transaction or external service performance claim",
                },
                "cases": rows,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
