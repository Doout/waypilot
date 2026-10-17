"""Compare sequential HR agents, preparation, validated reuse and learned speculative reads."""

import argparse
import asyncio
import hashlib
import json
import platform
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx2
import psutil
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from benchmark_agents import percentile
from hr.runner import run_agent
from hr.sandbox import Sandbox, connect
from hr.workbench import read_delay


class Direct:
    """Keep the same agent plan while calling unprefixed tools on the peer itself."""

    def __init__(self, client, slots):
        self.client = client
        self.slots = slots

    async def call_tool(self, name, arguments):
        if not name.startswith("hr__"):
            raise ValueError("unexpected HR tool prefix")
        async with self.slots:
            return await self.client.call_tool(name.removeprefix("hr__"), arguments)


@asynccontextmanager
async def connection(sandbox, direct, slots):
    if direct:
        # No gateway token is sent to the upstream service.
        async with httpx2.AsyncClient(timeout=20) as http:
            async with Client(
                streamable_http_client(sandbox.peer + "/mcp", http_client=http),
                read_timeout_seconds=20,
            ) as client:
                yield Direct(client, slots)
    else:
        async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
            yield client


async def wave(sandbox, start, runs, concurrency, think_ms, wrong_every=0, *, direct=False):
    semaphore = asyncio.Semaphore(concurrency)
    # Match the gateway's 64 active tools/call slots, including at high client load.
    upstream_slots = asyncio.Semaphore(64)
    submitted = time.perf_counter_ns()

    async def one(index):
        async with semaphore:
            started = time.perf_counter_ns()
            async with connection(sandbox, direct, upstream_slots) as client:
                report = await run_agent(
                    client,
                    f"E-{start + index:06d}",
                    kind="benefits"
                    if wrong_every and index % wrong_every == wrong_every - 1
                    else "leave",
                    think_ms=think_ms,
                    guarded=not direct and sandbox.mode != "off",
                )
            report["session_elapsed_ms"] = (time.perf_counter_ns() - started) / 1e6
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
        "off" if mode == "direct" else "speculate" if mode == "speculate_batch" else mode,
        args.tool_ms,
        slots=args.slots,
        environment={},
        batch_validation=mode == "speculate_batch",
        state_ms=args.state_ms,
        read_delays=dict(args.read_ms),
        duration_ms=args.prediction_ms,
        max_in_flight=max(64, args.concurrency + args.slots + 8),
        timeout_ms=30000,
    ) as sandbox:
        # Disjoint employees: training does not populate measurement cache keys.
        await wave(sandbox, 1, args.training_runs, 1, 0, direct=mode == "direct")
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
                sandbox,
                10000,
                args.runs,
                args.concurrency,
                args.think_ms,
                args.wrong_every,
                direct=mode == "direct",
            )
            elapsed = time.perf_counter() - started
            final = await drain(sandbox)
        finally:
            stop.set()
            await sampler
        after = sandbox.peer_stats()
        if mode == "direct":
            assert final["requests"] == final["upstream_calls"] == 0, (
                "direct traffic reached gateway"
            )
        assert after["active"] == 0
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
        tool_times = {}
        for report in reports:
            for timing in report["timings"]:
                if timing["phase"] == "tool":
                    tool_times.setdefault(timing["tool"], []).append(timing["elapsed_ms"])
        return {
            "mode": mode,
            "driver_cpu_seconds": time.process_time() - driver_started,
            "p50_ms": percentile([r["elapsed_ms"] for r in reports], 0.5),
            "p95_ms": percentile([r["elapsed_ms"] for r in reports], 0.95),
            "p99_ms": percentile([r["elapsed_ms"] for r in reports], 0.99),
            "session_p50_ms": percentile([r["session_elapsed_ms"] for r in reports], 0.5),
            "session_p95_ms": percentile([r["session_elapsed_ms"] for r in reports], 0.95),
            "session_p99_ms": percentile([r["session_elapsed_ms"] for r in reports], 0.99),
            "completion_p95_ms": percentile([r["completion_ms"] for r in reports], 0.95),
            "agents_per_second": args.runs / elapsed,
            "wall_seconds": elapsed,
            "calls": calls,
            "total_upstream_rpcs": sum(calls.values()),
            "state_checks": final["state_checks"] - stats_before["state_checks"],
            "state_batches": final["state_batches"] - stats_before["state_batches"],
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
            "tool_waits": {
                name: {
                    "calls": len(values),
                    "p50_ms": percentile(values, 0.5),
                    "p95_ms": percentile(values, 0.95),
                }
                for name, values in tool_times.items()
            },
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
    parser.add_argument("--read-ms", type=read_delay, action="append", default=[])
    parser.add_argument("--prediction-ms", type=int, default=2000)
    parser.add_argument("--state-ms", type=int, default=1)
    parser.add_argument("--wrong-every", type=int, default=5)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--training-runs", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--mode",
        action="append",
        choices=["direct", "off", "cache", "prewarm", "speculate", "speculate_batch"],
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (
        2 <= args.runs <= 9000
        and 1 <= args.concurrency <= 500
        and 0 <= args.think_ms <= 5000
        and 0 <= args.tool_ms <= 5000
        and 100 <= args.prediction_ms <= 5000
        and 0 <= args.state_ms <= 1000
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
            "direct": "Official MCP SDK connects to peer /mcp; idle gateway receives no requests; direct client caps active tools/call at 64",
            "guards": "Direct and off have no observation receipts; other modes enforce state-v1 and receipt guards",
            "latency": "p50/p95/p99 cover agent execution with think pauses; session percentiles also include SDK initialization and shutdown; completion includes client queueing",
            "prediction": "learned next-successful-call frequency; authored argument bindings and explicit read/disclosure contracts",
        },
        "platform": platform.platform(),
        "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
        "sources_sha256": {
            name: hashlib.sha256(
                (Path(__file__).resolve().parents[1] / name).read_bytes()
            ).hexdigest()
            for name in [
                "scripts/benchmark_hr.py",
                "demos/hr/sandbox.py",
                "demos/hr/runner.py",
                "demos/hr/workbench.py",
            ]
        },
        "rounds": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    oracle = None
    try:
        for iteration in range(args.rounds):
            current = {}
            modes = args.mode or [
                "direct",
                "off",
                "cache",
                "prewarm",
                "speculate",
                "speculate_batch",
            ]
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
