"""Compare sequential HR agents, preparation, validated reuse and learned speculative reads."""

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
from benchmark_agents import percentile
from hr.runner import run_agent
from hr.sandbox import Sandbox, connect


async def wave(sandbox, start, runs, concurrency, think_ms, wrong_every=0):
    semaphore = asyncio.Semaphore(concurrency)
    submitted = time.perf_counter_ns()

    async def one(index):
        async with semaphore:
            async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
                report = await run_agent(
                    client,
                    f"E-{start + index:06d}",
                    kind="benefits"
                    if wrong_every and index % wrong_every == wrong_every - 1
                    else "leave",
                    think_ms=think_ms,
                    guarded=sandbox.mode != "off",
                )
            report["completion_ms"] = (time.perf_counter_ns() - submitted) / 1e6
            return report

    return await asyncio.gather(*(one(i) for i in range(runs)))


async def drain(sandbox):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        stats = await asyncio.to_thread(sandbox.stats)
        if (
            stats["sessions"]
            == stats["active_requests"]
            == stats["speculation"]["active"]
            == stats["cleanup"]["pending"]
            == 0
        ):
            assert stats["observation_bytes"] == 0 and stats["cache"]["flights"] == 0
            assert stats["cleanup"]["errors"] == stats["cleanup"]["dropped"] == 0
            return stats
        await asyncio.sleep(0.02)
    raise RuntimeError("HR resources did not drain")


async def trial(args, mode):
    with Sandbox(
        args.binary,
        mode,
        args.tool_ms,
        slots=args.slots,
        environment={},
        max_in_flight=max(64, args.concurrency + args.slots + 8),
        timeout_ms=30000,
    ) as sandbox:
        # Disjoint employees: training does not populate measurement cache keys.
        await wave(sandbox, 1, args.training_runs, 1, 0)
        await drain(sandbox)
        before = sandbox.peer_stats()
        stats_before = sandbox.stats()
        gateway, peer = (
            psutil.Process(sandbox.service.process.pid),
            psutil.Process(sandbox.peer_process.process.pid),
        )
        gcpu, pcpu = gateway.cpu_times(), peer.cpu_times()
        samples = []
        stop = asyncio.Event()

        async def sample():
            while not stop.is_set():
                samples.append((gateway.memory_info().rss, gateway.num_fds()))
                try:
                    await asyncio.wait_for(stop.wait(), 0.05)
                except TimeoutError:
                    pass

        sampler = asyncio.create_task(sample())
        driver_started = time.process_time()
        started = time.perf_counter()
        try:
            reports = await wave(
                sandbox, 10000, args.runs, args.concurrency, args.think_ms, args.wrong_every
            )
            elapsed = time.perf_counter() - started
            final = await drain(sandbox)
        finally:
            stop.set()
            await sampler
        after = sandbox.peer_stats()
        assert after["case_count"] - before["case_count"] == args.runs
        cases = [
            {k: v for k, v in row.items() if k != "case_id"}
            for row in after["cases"][before["case_count"] :]
        ]
        expected = [
            {
                "employee_id": r["employee_id"],
                "kind": r["decision"],
                "days": 5 if r["kind"] == "leave" else 0,
            }
            for r in reports
        ]
        assert sorted(cases, key=lambda r: r["employee_id"]) == sorted(
            expected, key=lambda r: r["employee_id"]
        )
        for r in reports:
            number = int(r["employee_id"][2:])
            expected_decision = (
                "benefits_guidance"
                if r["kind"] == "benefits"
                else ("needs_review" if number % 3 == 0 else "leave_guidance")
            )
            assert r["decision"] == expected_decision
        ga, pa = gateway.cpu_times(), peer.cpu_times()
        calls = {
            name: count - before["calls"].get(name, 0) for name, count in after["calls"].items()
        }
        canonical = [r["observations"] for r in reports]
        return {
            "mode": mode,
            "driver_cpu_seconds": time.process_time() - driver_started,
            "p50_ms": percentile([r["elapsed_ms"] for r in reports], 0.5),
            "p95_ms": percentile([r["elapsed_ms"] for r in reports], 0.95),
            "p99_ms": percentile([r["elapsed_ms"] for r in reports], 0.99),
            "completion_p95_ms": percentile([r["completion_ms"] for r in reports], 0.95),
            "agents_per_second": args.runs / elapsed,
            "calls": calls,
            "total_upstream_rpcs": sum(calls.values()),
            "exact_effects": len(cases),
            "speculation": {
                k: v - stats_before["speculation"][k] for k, v in final["speculation"].items()
            },
            "gateway_cpu_seconds": ga.user + ga.system - gcpu.user - gcpu.system,
            "peer_cpu_seconds": pa.user + pa.system - pcpu.user - pcpu.system,
            "peak_rss_bytes": max(r[0] for r in samples),
            "peak_fds": max(r[1] for r in samples),
            "final": final,
            "tool_wait_ms": sum(
                t["elapsed_ms"] for r in reports for t in r["timings"] if t["phase"] == "tool"
            ),
            "agent_think_ms": sum(
                t["elapsed_ms"]
                for r in reports
                for t in r["timings"]
                if t["phase"] == "agent_think"
            ),
            "output_digest": hashlib.sha256(
                json.dumps(canonical, sort_keys=True).encode()
            ).hexdigest(),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--runs", type=int, default=32)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--think-ms", type=int, default=200)
    parser.add_argument("--tool-ms", type=int, default=250)
    parser.add_argument("--wrong-every", type=int, default=5)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--training-runs", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--mode", action="append", choices=["off", "cache", "prewarm", "speculate"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (
        2 <= args.runs <= 9000
        and 1 <= args.concurrency <= 500
        and 0 <= args.think_ms <= 2000
        and 0 <= args.tool_ms <= 1000
        and 0 <= args.wrong_every <= 1000
        and 1 <= args.slots <= 32
        and 1 <= args.rounds <= 6
        and 0 <= args.training_runs <= 32
    ):
        parser.error("invalid workload bounds")
    report = {
        "status": "running",
        "methodology": vars(args)
        | {
            "output": str(args.output),
            "training_agents": args.training_runs,
            "training_ids_disjoint": True,
            "shared_upstream_call_budget": 64,
            "request_timeout_ms": 30000,
            "max_in_flight": max(64, args.concurrency + args.slots + 8),
            "model_calls": 0,
            "prediction": "learned next-successful-call frequency; authored argument bindings and explicit read/disclosure contracts",
        },
        "platform": platform.platform(),
        "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
        "rounds": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    oracle = None
    try:
        for iteration in range(args.rounds):
            current = {}
            modes = args.mode or ["off", "cache", "prewarm", "speculate"]
            for mode in modes if iteration % 2 == 0 else reversed(modes):
                result = asyncio.run(trial(args, mode))
                if oracle is None:
                    oracle = result["output_digest"]
                assert oracle == result["output_digest"], "HR observations or decisions differ"
                current[mode] = result
                print(
                    json.dumps(
                        {
                            "round": iteration + 1,
                            **{
                                k: v
                                for k, v in result.items()
                                if k not in ["final", "output_digest"]
                            },
                        }
                    ),
                    flush=True,
                )
            report["rounds"].append(current)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        report.update(status="passed", output_equivalence=True, exact_effects=True)
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
