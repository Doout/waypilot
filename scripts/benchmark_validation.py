"""Compare identical agent observations/effects with single, batched and guarded validation."""

import argparse
import asyncio
import hashlib
import json
import platform
from pathlib import Path

import psutil
from benchmark_agents import Sandbox, measure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default="target/release/waypilot")
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--runs", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--delay-ms", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--legacy-schemas", action="store_true")
    args = parser.parse_args()
    if not (
        2 <= args.runs <= 9000
        and 1 <= args.concurrency <= 32
        and 1 <= args.rounds <= 10
        and 0 <= args.delay_ms <= 1000
    ):
        parser.error("runs 2..9000, concurrency 1..32, rounds 1..10, delay-ms 0..1000 required")
    modes = [
        ("uncached", False, False, False),
        ("single", True, False, False),
        ("batch", True, True, False),
        ("guarded_single", True, False, True),
        ("guarded_batch", True, True, True),
    ]
    if args.baseline_binary:
        modes.insert(0, ("previous_single", True, False, False))
    results = []
    oracle = None
    for iteration in range(args.rounds):
        current = {}
        for label, reuse, batch, guarded in modes if iteration % 2 == 0 else reversed(modes):
            binary = str(args.baseline_binary) if label == "previous_single" else args.binary
            with Sandbox(
                binary,
                reuse=reuse,
                batch_validation=batch,
                delay_ms=args.delay_ms,
                environment={},
                compiled_schemas=not args.legacy_schemas,
            ) as sandbox:
                gateway, peer = (
                    psutil.Process(sandbox.service.process.pid),
                    psutil.Process(sandbox.peer_process.process.pid),
                )
                before, peer_before = gateway.cpu_times(), peer.cpu_times()
                result = asyncio.run(
                    measure(
                        sandbox,
                        args.runs,
                        args.concurrency,
                        guard_observations=guarded,
                        compiled_input_schemas=not args.legacy_schemas,
                    )
                )
                after, peer_after = gateway.cpu_times(), peer.cpu_times()
                result.update(
                    gateway_cpu_seconds=after.user + after.system - before.user - before.system,
                    peer_cpu_seconds=peer_after.user
                    + peer_after.system
                    - peer_before.user
                    - peer_before.system,
                    drained_rss_bytes=gateway.memory_info().rss,
                    drained_fds=gateway.num_fds(),
                )
                actual = (result.pop("summaries"), result.pop("evidence_digests"))
                if oracle is None:
                    oracle = actual
                if actual != oracle:
                    raise RuntimeError("agent decisions or structured observations differ")
                current[label] = result
                print(
                    json.dumps(
                        {
                            "round": iteration + 1,
                            "mode": label,
                            "p50_ms": result["p50_ms"],
                            "p95_ms": result["p95_ms"],
                            "upstream_calls": result["total_upstream_calls"],
                        }
                    ),
                    flush=True,
                )
        for left, right in [("single", "batch"), ("guarded_single", "guarded_batch")]:
            if current[left]["state_checks"] != current[right]["state_checks"]:
                raise RuntimeError("batching changed the number of validated observations")
            if current[right]["total_upstream_calls"] >= current[left]["total_upstream_calls"]:
                raise RuntimeError("batching did not reduce upstream RPCs")
        results.append(current)
    report = {
        "status": "passed",
        "output_equivalence": True,
        "equal_checks_within_guard_mode": True,
        "runs_per_mode_per_round": args.runs,
        "concurrency": args.concurrency,
        "owned_tool_delay_ms": args.delay_ms,
        "rounds": results,
        "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
        "baseline_binary_sha256": hashlib.sha256(args.baseline_binary.read_bytes()).hexdigest()
        if args.baseline_binary
        else None,
        "platform": platform.platform(),
        "model_calls": 0,
        "compiled_schemas": not args.legacy_schemas,
        "limits": [
            "Owned loopback tools with 1 ms validators; no arbitrary remote performance claim",
            "Guarded modes perform additional validation before and after the note call",
            "Validation points do not make the remote write atomic",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
