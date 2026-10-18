"""Latency ablations with identical outputs, state checks and effect counts."""

import argparse
import asyncio
import hashlib
import json
import platform
import sys
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.runner import compiled_input_schema
from benchmark_agents import Sandbox, measure


async def sample_memory(process, stop, samples):
    while not stop.is_set():
        samples.append({"rss_bytes": process.memory_info().rss, "fds": process.num_fds()})
        try:
            await asyncio.wait_for(stop.wait(), 0.05)
        except TimeoutError:
            pass


async def trial(sandbox, runs, concurrency, guarded, compiled_inputs=False, reuse_sessions=False):
    process = psutil.Process(sandbox.service.process.pid)
    stop, samples = asyncio.Event(), []
    sampler = asyncio.create_task(sample_memory(process, stop, samples))
    try:
        result = await measure(
            sandbox,
            runs,
            concurrency,
            guard_observations=guarded,
            compiled_input_schemas=compiled_inputs,
            reuse_sessions=reuse_sessions,
        )
        until = time.monotonic() + 15
        while True:
            stats = await asyncio.to_thread(sandbox.stats)
            if (
                stats["sessions"] == 0
                and stats["active_requests"] == 0
                and stats["cleanup"]["pending"] == 0
            ):
                break
            if time.monotonic() >= until:
                raise RuntimeError("sessions or cleanup did not drain")
            await asyncio.sleep(0.01)
        if stats["observation_bytes"] or stats["cleanup"]["errors"] or stats["cleanup"]["dropped"]:
            raise RuntimeError("receipt or cleanup resources did not drain")
        result.update(
            peak_rss_bytes=max(s["rss_bytes"] for s in samples),
            peak_fds=max(s["fds"] for s in samples),
            final=stats,
        )
        return result
    finally:
        stop.set()
        await sampler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--catalog-binary")
    parser.add_argument("--admission-binary")
    parser.add_argument("--runs", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--delay-ms", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--profile", action="append", help="Restrict to named profiles")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (
        2 <= args.runs <= 9000
        and 1 <= args.concurrency <= 32
        and 1 <= args.rounds <= 6
        and 0 <= args.delay_ms <= 1000
    ):
        parser.error("runs 2..9000, concurrency 1..32, rounds 1..6, delay-ms 0..1000 required")
    # Every guarded profile has the same read contract and validation points.
    profiles = {
        "uncached": dict(binary=args.baseline, reuse=False),
        "baseline": dict(binary=args.baseline),
        "compiled_uncached": dict(
            binary=args.baseline, reuse=False, compiled_schemas=True, compiled_inputs=True
        ),
        "compiled_client": dict(binary=args.baseline, compiled_inputs=True),
        "pooled_uncached": dict(
            binary=args.baseline,
            reuse=False,
            compiled_schemas=True,
            compiled_inputs=True,
            reuse_sessions=True,
        ),
        "pooled_both": dict(
            binary=args.baseline, compiled_schemas=True, compiled_inputs=True, reuse_sessions=True
        ),
        "pooled_parallel": dict(
            binary=args.binary,
            validation_parallelism=4,
            compiled_schemas=True,
            compiled_inputs=True,
            reuse_sessions=True,
        ),
        "pooled_16": dict(
            binary=args.binary,
            validation_parallelism=4,
            upstream_calls=16,
            compiled_schemas=True,
            compiled_inputs=True,
            reuse_sessions=True,
        ),
        "admission16_legacy": dict(
            binary=args.admission_binary or args.binary, validation_parallelism=4, upstream_calls=16
        ),
        "admission_compiled": dict(
            binary=args.admission_binary or args.binary,
            validation_parallelism=4,
            compiled_schemas=True,
            compiled_inputs=True,
        ),
        "admission16_compiled": dict(
            binary=args.admission_binary or args.binary,
            validation_parallelism=4,
            upstream_calls=16,
            compiled_schemas=True,
            compiled_inputs=True,
        ),
        "admission_pooled": dict(
            binary=args.admission_binary or args.binary,
            validation_parallelism=4,
            compiled_schemas=True,
            compiled_inputs=True,
            reuse_sessions=True,
        ),
        "admission16_pooled": dict(
            binary=args.admission_binary or args.binary,
            validation_parallelism=4,
            upstream_calls=16,
            compiled_schemas=True,
            compiled_inputs=True,
            reuse_sessions=True,
        ),
        "catalog": dict(binary=args.catalog_binary or args.binary),
        "parallel": dict(binary=args.binary, validation_parallelism=4),
        "admit_8": dict(binary=args.baseline, upstream_calls=8),
        "admit_16": dict(binary=args.baseline, upstream_calls=16),
        "admit_64": dict(binary=args.baseline, upstream_calls=64),
        "compiled_peer": dict(binary=args.baseline, compiled_schemas=True),
        "compiled_both": dict(binary=args.baseline, compiled_schemas=True, compiled_inputs=True),
        "parallel_compiled": dict(
            binary=args.binary,
            validation_parallelism=4,
            compiled_schemas=True,
            compiled_inputs=True,
        ),
        "parallel_admit16": dict(binary=args.binary, validation_parallelism=4, upstream_calls=16),
        "best_16": dict(
            binary=args.binary,
            validation_parallelism=4,
            upstream_calls=16,
            compiled_schemas=True,
            compiled_inputs=True,
        ),
        "combined_8": dict(
            binary=args.binary, validation_parallelism=4, upstream_calls=8, compiled_schemas=True
        ),
        "combined_16": dict(
            binary=args.binary, validation_parallelism=4, upstream_calls=16, compiled_schemas=True
        ),
        "combined_64": dict(
            binary=args.binary, validation_parallelism=4, upstream_calls=64, compiled_schemas=True
        ),
    }
    names = args.profile or [
        name for name in profiles if args.admission_binary or not name.startswith("admission")
    ]
    if not args.admission_binary and any(name.startswith("admission") for name in names):
        parser.error("admission experiments require --admission-binary")
    if len(names) != len(set(names)) or any(name not in profiles for name in names):
        parser.error("unknown or duplicate profile")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "running",
        "runs_per_profile_per_round": args.runs,
        "agent_concurrency": args.concurrency,
        "business_delay_ms": args.delay_ms,
        "platform": platform.platform(),
        "model_calls": 0,
        "session_reuse_contract": "Owned workbench only; tools must not depend on MCP session history. Each worker owns one client and rotates after 12 runs before the receipt cap.",
        "rounds": [],
        "profiles": profiles,
        "binary_sha256": {
            str(p): hashlib.sha256(Path(p).read_bytes()).hexdigest()
            for p in {
                args.binary,
                args.baseline,
                args.catalog_binary or args.binary,
                args.admission_binary or args.binary,
            }
        },
        "measurement": {
            "p50/p95/p99": "Agent run including discovery, after MCP client initialization and admission",
            "batch_completion_p95_ms": "All agents released together; includes waiting for the client concurrency slot, MCP initialization and run; wall time includes teardown",
            "peer_optimization": "Compiled schemas preserve argument validation and affect every served RPC; compare separately from gateway changes",
        },
    }
    oracle = expected_checks = None
    try:
        for iteration in range(args.rounds):
            current = {}
            for label in names if iteration % 2 == 0 else reversed(names):
                settings = dict(profiles[label])
                binary = settings.pop("binary")
                reuse = settings.pop("reuse", True)
                settings.setdefault("compiled_schemas", False)
                compiled_inputs = settings.pop("compiled_inputs", False)
                reuse_sessions = settings.pop("reuse_sessions", False)
                compiled_input_schema.cache_clear()
                begin = time.perf_counter()
                with Sandbox(
                    binary,
                    reuse=reuse,
                    batch_validation=reuse,
                    delay_ms=args.delay_ms,
                    environment={},
                    **settings,
                ) as sandbox:
                    startup = time.perf_counter() - begin
                    gateway, peer = (
                        psutil.Process(sandbox.service.process.pid),
                        psutil.Process(sandbox.peer_process.process.pid),
                    )
                    before, peer_before = gateway.cpu_times(), peer.cpu_times()
                    driver_started = time.process_time()
                    result = asyncio.run(
                        trial(
                            sandbox,
                            args.runs,
                            args.concurrency,
                            reuse,
                            compiled_inputs,
                            reuse_sessions,
                        )
                    )
                    result["driver_cpu_seconds"] = time.process_time() - driver_started
                    result["driver_rss_bytes"] = psutil.Process().memory_info().rss
                    after, peer_after = gateway.cpu_times(), peer.cpu_times()
                    result.update(
                        gateway_cpu_seconds=after.user + after.system - before.user - before.system,
                        peer_cpu_seconds=peer_after.user
                        + peer_after.system
                        - peer_before.user
                        - peer_before.system,
                        startup_seconds=startup,
                    )
                    actual = (result.pop("summaries"), result.pop("evidence_digests"))
                    if oracle is None:
                        oracle = actual
                    if actual != oracle:
                        raise RuntimeError("business observations or agent decisions differ")
                    if reuse:
                        if expected_checks is None:
                            expected_checks = (
                                result["state_checks"],
                                result["validation_calls"],
                                result["business_calls"],
                                result["effect_calls"],
                            )
                        if (
                            result["state_checks"],
                            result["validation_calls"],
                            result["business_calls"],
                            result["effect_calls"],
                        ) != expected_checks:
                            raise RuntimeError("profile changed required work or controls")
                    current[label] = result
                    print(
                        json.dumps(
                            {
                                "round": iteration + 1,
                                "profile": label,
                                **{
                                    key: result[key]
                                    for key in [
                                        "p50_ms",
                                        "p95_ms",
                                        "p99_ms",
                                        "batch_completion_p95_ms",
                                        "completed_agents_per_second",
                                        "validation_calls",
                                        "peer_cpu_seconds",
                                    ]
                                },
                            }
                        ),
                        flush=True,
                    )
            report["rounds"].append(current)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        report.update(
            status="passed", output_equivalence=True, equal_validation_and_effect_counts=True
        )
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
