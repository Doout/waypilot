"""Continuous paired HR agents: direct MCP, guarded reuse, and cold automatic learning."""

import argparse
import asyncio
import hashlib
import json
import math
import platform
import random
import resource
import sys
import time
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from benchmark_hr import connection, drain
from hr.runner import META
from hr.sandbox import Sandbox
from mcp_demo import Process, http

ROOT = Path(__file__).resolve().parents[1]
MODES = ["direct", "guarded", "auto"]
PHASES = ["leave-heavy", "benefits-drift", "mixed-refresh", "leave-recovery", "bursty", "mixed"]


class Continuity:
    """Reject suspend, wall-clock jumps and long runner stalls in qualified runs."""

    def __init__(self, wall, monotonic):
        self.wall, self.monotonic, self.previous = wall, monotonic, monotonic
        self.max_clock_skew = self.max_sample_gap = 0.0

    def sample(self, wall, monotonic):
        elapsed = monotonic - self.monotonic
        skew = wall - self.wall - elapsed
        gap = monotonic - self.previous
        self.previous = monotonic
        self.max_clock_skew = max(self.max_clock_skew, abs(skew))
        self.max_sample_gap = max(self.max_sample_gap, gap)
        return {
            "wall_elapsed_seconds": wall - self.wall,
            "active_elapsed_seconds": elapsed,
            "wall_monotonic_skew_seconds": skew,
            "sample_gap_seconds": gap,
            "interrupted": abs(skew) > 2 or gap > 10,
        }


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def plan(seconds, rate, seed, pause_scale=1):
    """Freeze arrivals, branches, pauses, aliases and updates before any mode executes."""
    rng, due, episodes = random.Random(seed), 0.0, []
    while True:
        # Six-second bursts and seven-second quiet periods repeat independently of phase.
        due += rng.expovariate(rate * 4)
        if due >= seconds:
            break
        position = due % 120
        multiplier = 4 if position < 6 else 0.05 if 70 <= position < 77 else 0.8
        if rng.random() > multiplier / 4:
            continue
        phase = min(5, int(due / seconds * 6))
        index = len(episodes)
        number = 100000 + index
        alias = rng.random() < 0.2
        employee = f"E-{500000 + index if alias else number:06d}"
        choice = rng.random()
        leave_fraction = [0.87, 0.16, 0.45, 0.88, 0.65, 0.5][phase]
        kind = "profile" if choice < 0.05 else "leave" if choice < leave_fraction else "benefits"
        if rng.random() < 0.1 and kind != "profile":
            kind = "combined"
        pauses = []
        for _ in range(6):
            draw = rng.random()
            milliseconds = (
                0
                if draw < 0.1
                else rng.uniform(40, 180)
                if draw < 0.3
                else rng.uniform(250, 1700)
                if draw < 0.88
                else rng.uniform(2500, 7000)
            )
            pauses.append(milliseconds * pause_scale)
        episodes.append(
            {
                "index": index,
                "due_seconds": due,
                "phase": PHASES[phase],
                "requested": f"E-{number:06d}",
                "employee": employee,
                "kind": kind,
                "days": rng.choice([1, 3, 5, 10, 21]),
                "pauses_ms": pauses,
                "order": "parallel"
                if rng.random() < 0.1
                else "policy_first"
                if rng.random() < 0.15
                else "standard",
                "refresh": kind in {"leave", "combined"} and rng.random() < 0.15,
                "new_balance": rng.choice([0, 4, 12, 24]),
            }
        )
    assert 6 <= len(episodes) <= 9000, "choose a duration/rate yielding 6..9000 episodes per mode"
    return episodes


def read_oracle(name, employee, days):
    number = int(employee[2:])
    common = {"employee_id": employee, "region": "EU" if number % 2 == 0 else "US"}
    return {
        "employee": dict(common, status="active", department="engineering"),
        "leave_balance": dict(common, days=days),
        "leave_policy": dict(common, approval_required=True, max_days=20),
        "benefit_options": dict(common, plans=["standard", "extended"]),
    }[name]


async def episode(client, sandbox, spec, guarded):
    observations, receipts, waits, think, dispositions = [], [], [], [], Counter()
    employee = spec["employee"]
    balance = 8 if int(employee[2:]) % 3 else 2
    started = time.monotonic()

    async def call(name, write=False, decision=None):
        arguments = {"employee_id": spec["requested"] if name == "employee" else employee}
        if write:
            arguments.update(kind=decision, days=spec["days"] if spec["kind"] != "benefits" else 0)
        metadata = {"require": list(receipts)} if write else {"capture": True}
        before = time.monotonic()
        result = (
            await client.call_tool(
                "hr__" + name, arguments, **({"meta": {META: metadata}} if guarded else {})
            )
        ).model_dump(by_alias=True)
        waits.append({"tool": name, "ms": (time.monotonic() - before) * 1000})
        assert not result.get("isError"), (name, result)
        value = {k: v for k, v in result["structuredContent"].items() if k != "case_id"}
        expected = arguments if write else read_oracle(name, employee, balance)
        assert value == expected, (spec["index"], name, value, expected)
        observations.append({"tool": name, "value": value})
        meta = result.get("_meta") or {}
        if guarded and not write:
            receipt = meta.get(META, {}).get("receipt")
            assert receipt, "missing guarded observation receipt"
            receipts.append(receipt)
        dispositions[meta.get("io.waypilot/runtime", {}).get("disposition", "direct")] += 1

    async def pause(index):
        before = time.monotonic()
        await asyncio.sleep(spec["pauses_ms"][index] / 1000)
        think.append((time.monotonic() - before) * 1000)

    async def leave_reads():
        if spec["order"] == "parallel":
            async with asyncio.TaskGroup() as group:
                group.create_task(call("leave_balance"))
                group.create_task(call("leave_policy"))
        else:
            names = ["leave_balance", "leave_policy"]
            if spec["order"] == "policy_first":
                names.reverse()
            await call(names[0])
            await pause(1)
            await call(names[1])

    await call("employee")
    await pause(0)
    if spec["kind"] in {"leave", "combined"}:
        await leave_reads()
    if spec["kind"] in {"benefits", "combined"}:
        await call("benefit_options")
    if spec["refresh"]:
        # A separate control connection updates the external system after earlier observations.
        await asyncio.to_thread(sandbox.mutate, employee_id=employee, days=spec["new_balance"])
        balance = spec["new_balance"]
        receipts.clear()
        await pause(2)
        await call("employee")
        await leave_reads()
        if spec["kind"] == "combined":
            await call("benefit_options")
    decision = None
    if spec["kind"] != "profile":
        decision = (
            "benefits_guidance"
            if spec["kind"] == "benefits"
            else (
                "leave_guidance"
                if balance >= spec["days"] and spec["days"] <= 20
                else "needs_review"
            )
        )
        await pause(3)
        await call("record_case", write=True, decision=decision)
    # Parallel completion order is not a semantic difference; preserve duplicate observations.
    canonical = sorted(observations, key=lambda row: json.dumps(row, sort_keys=True))
    return {
        "index": spec["index"],
        "phase": spec["phase"],
        "kind": spec["kind"],
        "decision": decision,
        "refresh": spec["refresh"],
        "order": spec["order"],
        "agent_ms": (time.monotonic() - started) * 1000,
        "tool_wait_ms": sum(w["ms"] for w in waits),
        "think_ms": sum(think),
        "tool_waits": waits,
        "dispositions": dict(dispositions),
        "digest": hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest(),
    }


def latency(rows):
    return {
        field: {
            f"p{int(q * 100)}": percentile([row[field] for row in rows], q)
            for q in [0.5, 0.95, 0.99]
        }
        for field in ["agent_ms", "session_ms", "scheduled_completion_ms", "tool_wait_ms"]
    }


async def run(args):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    episodes = plan(args.seconds, args.rate, args.seed, args.pause_scale)
    modes = MODES + (["baseline_auto"] if args.baseline else [])
    atomic_json(args.output_dir / "plan.json", episodes)
    output = {
        "status": "running",
        "methodology": {
            "seconds": args.seconds,
            "rate_base": args.rate,
            "seed": args.seed,
            "episodes_per_mode": len(episodes),
            "concurrency_per_mode": args.concurrency,
            "upstream_slots_per_mode": 64,
            "training_agents": 0,
            "model_calls": 0,
            "protocol": "2025-11-25",
            "trace_ratio": args.trace_ratio,
            "pause_scale": args.pause_scale,
            "require_uninterrupted": args.require_uninterrupted,
            "comparison": "Concurrent isolated peers, identical seeded plans and call budgets; direct has no gateway state/receipt guards; guarded and auto share those controls. All share the host.",
            "timing": "Agent includes actual thinking pauses; session adds handshake/close; scheduled completion includes client queueing. Arrival-cohort windows separate workload drift from learning age.",
            "limits": "Owned synthetic HR over loopback HTTP; no LLM, vendor, WAN/TLS, or distributed snapshot. Per-query deterministic service-time variation includes 1% long reads and 10% fast reads.",
        },
        "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
        "baseline_binary_sha256": hashlib.sha256(Path(args.baseline).read_bytes()).hexdigest()
        if args.baseline
        else None,
        "platform": platform.platform(),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                Path(__file__),
                ROOT / "load/hr_soak_peer.py",
                ROOT / "load/soak_collector.py",
                ROOT / "demos/hr/sandbox.py",
                ROOT / "demos/hr/workbench.py",
                ROOT / "scripts/benchmark_hr.py",
            ]
        },
        "windows": [],
        "errors": [],
        "modes": {},
    }
    atomic_json(args.output_dir / "results.json", output)
    with ExitStack() as stack:
        collector = stack.enter_context(
            Process([sys.executable, str(ROOT / "load/soak_collector.py")], None)
        )
        collector_url = "http://" + collector.ready["listen"]
        sandboxes = {}
        for mode in modes:
            environment = {
                "OTEL_SERVICE_NAME": "hr-soak-" + mode,
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": collector_url + "/v1/traces",
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
                "OTEL_TRACES_SAMPLER": "traceidratio",
                "OTEL_TRACES_SAMPLER_ARG": str(args.trace_ratio),
            }
            sandboxes[mode] = stack.enter_context(
                Sandbox(
                    args.baseline if mode == "baseline_auto" else args.binary,
                    "off" if mode == "direct" else "prewarm" if mode == "guarded" else "speculate",
                    peer_script=ROOT / "load/hr_soak_peer.py",
                    state_ms=5,
                    environment=environment,
                    learning="auto" if mode in {"auto", "baseline_auto"} else None,
                    batch_validation=mode != "direct",
                    duration_ms=5000,
                    slots=8,
                    max_in_flight=max(128, args.concurrency + 8),
                    timeout_ms=30000,
                    min_samples=8,
                    confidence=75,
                )
            )
        processes = {mode: psutil.Process(sb.service.process.pid) for mode, sb in sandboxes.items()}
        peers = {
            mode: psutil.Process(sb.peer_process.process.pid) for mode, sb in sandboxes.items()
        }
        cpu_start = {mode: processes[mode].cpu_times() for mode in modes}
        peer_cpu_start = {mode: peers[mode].cpu_times() for mode in modes}
        rows = {mode: [] for mode in modes}
        samples, lags, active, peaks = [], [], Counter(), Counter()
        stop = asyncio.Event()
        semaphores = {mode: asyncio.Semaphore(args.concurrency) for mode in modes}
        slots = {mode: asyncio.Semaphore(64) for mode in modes}
        records = stack.enter_context((args.output_dir / "agents.jsonl").open("w", buffering=1))
        events = stack.enter_context((args.output_dir / "progress.jsonl").open("w", buffering=1))
        started = time.monotonic()
        clock = Continuity(time.time(), started)
        output["traffic_started_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        output["continuity"] = {
            "interruptions": [],
            "max_clock_skew_seconds": 0,
            "max_sample_gap_seconds": 0,
        }
        output["pids"] = {
            mode: {"gateway": processes[mode].pid, "peer": peers[mode].pid} for mode in modes
        }

        async def one(mode, spec):
            sandbox = sandboxes[mode]
            async with semaphores[mode]:
                active[mode] += 1
                peaks[mode] = max(peaks[mode], active[mode])
                try:
                    if spec["employee"] != spec["requested"]:
                        await asyncio.to_thread(
                            sandbox.mutate,
                            alias_id=spec["requested"],
                            canonical_id=spec["employee"],
                        )
                    before = time.monotonic()
                    async with connection(
                        sandbox, mode == "direct", slots[mode], "legacy"
                    ) as client:
                        initialized = time.monotonic()
                        protocol = (client.client if mode == "direct" else client).protocol_version
                        assert protocol == "2025-11-25", protocol
                        row = await episode(client, sandbox, spec, mode != "direct")
                        finished = time.monotonic()
                    row.update(
                        mode=mode,
                        due_seconds=spec["due_seconds"],
                        session_ms=(time.monotonic() - before) * 1000,
                        initialization_ms=(initialized - before) * 1000,
                        shutdown_ms=(time.monotonic() - finished) * 1000,
                        scheduled_completion_ms=(time.monotonic() - started - spec["due_seconds"])
                        * 1000,
                        client_queue_ms=(before - started - spec["due_seconds"]) * 1000,
                    )
                    rows[mode].append(row)
                    records.write(json.dumps(row) + "\n")
                except Exception as error:
                    output["errors"].append(
                        {"mode": mode, "index": spec["index"], "error": repr(error)}
                    )
                    raise
                finally:
                    active[mode] -= 1

        async def snapshot(final=False):
            elapsed = time.monotonic() - started
            event = {"elapsed_seconds": elapsed, "final": final, "modes": {}}
            for mode, sandbox in sandboxes.items():
                stats = await asyncio.to_thread(sandbox.stats)
                peer = (
                    await asyncio.to_thread(
                        http, sandbox.peer + "/control?summary=1", sandbox.tokens["control"]
                    )
                )[2]
                rss = processes[mode].memory_info().rss
                assert rss < args.max_rss_mb * 1024 * 1024, (mode, "RSS bound", rss)
                assert stats["cleanup"]["errors"] == stats["cleanup"]["dropped"] == 0
                assert stats["rejected_capacity"] == 0, (mode, "capacity rejected")
                assert stats["cache"]["entries"] <= 512
                assert stats["cache"]["bytes"] <= 16 * 1024 * 1024
                recent = [
                    row
                    for row in rows[mode]
                    if elapsed - args.report_seconds <= row["due_seconds"] < elapsed
                ]
                event["modes"][mode] = {
                    "completed": len(rows[mode]),
                    "active_agents": active[mode],
                    "peak_active": peaks[mode],
                    "recent_completed": len(recent),
                    "recent_latency": latency(recent),
                    "gateway_rss_bytes": rss,
                    "gateway_fds": processes[mode].num_fds(),
                    "peer_rss_bytes": peers[mode].memory_info().rss,
                    "stats": stats,
                    "peer": peer,
                    "rules": (await asyncio.to_thread(sandbox.predictions))
                    if mode in {"auto", "baseline_auto"}
                    else None,
                }
            event["host"] = {
                "cpu_percent": psutil.cpu_percent(),
                "available_memory_bytes": psutil.virtual_memory().available,
            }
            output["windows"].append(event)
            events.write(json.dumps(event) + "\n")
            atomic_json(
                args.output_dir / "traces.json",
                (await asyncio.to_thread(http, collector_url + "/stats", ""))[2],
            )
            atomic_json(args.output_dir / "results.json", output)
            print(
                json.dumps(
                    {
                        "elapsed_seconds": round(elapsed, 1),
                        "completed": {m: len(rows[m]) for m in modes},
                        "active": dict(active),
                        "auto_rules": Counter(
                            r["status"] for r in event["modes"]["auto"]["rules"]["rules"]
                        ),
                    }
                ),
                flush=True,
            )

        async def monitor():
            next_report = 0
            while not stop.is_set():
                sample = {"elapsed_seconds": time.monotonic() - started}
                sample["clock"] = clock.sample(time.time(), time.monotonic())
                output["continuity"].update(
                    max_clock_skew_seconds=clock.max_clock_skew,
                    max_sample_gap_seconds=clock.max_sample_gap,
                )
                if sample["clock"]["interrupted"]:
                    if not output["continuity"]["interruptions"]:
                        output["continuity"]["interruptions"].append(sample["clock"])
                    assert not args.require_uninterrupted, (
                        "run interrupted by host sleep, clock discontinuity or runner stall"
                    )
                for mode in modes:
                    sample[mode] = {
                        "rss_bytes": processes[mode].memory_info().rss,
                        "fds": processes[mode].num_fds(),
                        "active_agents": active[mode],
                    }
                    assert sample[mode]["rss_bytes"] < args.max_rss_mb * 1024 * 1024
                samples.append(sample)
                if time.monotonic() >= next_report:
                    await snapshot()
                    next_report = time.monotonic() + args.report_seconds
                try:
                    await asyncio.wait_for(stop.wait(), 2)
                except TimeoutError:
                    pass

        try:
            async with asyncio.TaskGroup() as group:
                sampler = group.create_task(monitor())
                tasks = []
                for spec in episodes:
                    due = started + spec["due_seconds"]
                    await asyncio.sleep(max(0, due - time.monotonic()))
                    lags.append(max(0, (time.monotonic() - due) * 1000))
                    # Rotate lane dispatch order to avoid systematically favoring one path.
                    offset = spec["index"] % len(modes)
                    order = modes[offset:] + modes[:offset]
                    for mode in order:
                        tasks.append(group.create_task(one(mode, spec)))
                await asyncio.sleep(max(0, started + args.seconds - time.monotonic()))
                await asyncio.gather(*tasks)
                stop.set()
                await sampler
            finals = {mode: await drain(sb) for mode, sb in sandboxes.items()}
            assert percentile(lags, 0.99) < 250, "generator p99 lag exceeded 250ms"
            for mode, sandbox in sandboxes.items():
                assert len(rows[mode]) == len(episodes)
                peer = await asyncio.to_thread(sandbox.peer_stats)
                expected = Counter()
                for spec in episodes:
                    if spec["kind"] == "profile":
                        continue
                    balance = (
                        spec["new_balance"]
                        if spec["refresh"]
                        else (8 if int(spec["employee"][2:]) % 3 else 2)
                    )
                    decision = (
                        "benefits_guidance"
                        if spec["kind"] == "benefits"
                        else (
                            "leave_guidance"
                            if balance >= spec["days"] and spec["days"] <= 20
                            else "needs_review"
                        )
                    )
                    expected[
                        (
                            spec["employee"],
                            decision,
                            spec["days"] if spec["kind"] != "benefits" else 0,
                        )
                    ] += 1
                actual = Counter(
                    (case["employee_id"], case["kind"], case["days"]) for case in peer["cases"]
                )
                assert actual == expected, (mode, "missing or duplicate effects")
                assert peer["active"] == 0
                if mode == "direct":
                    assert finals[mode]["requests"] == finals[mode]["upstream_calls"] == 0
                steady = next(
                    (
                        window["modes"][mode]
                        for window in output["windows"]
                        if window["elapsed_seconds"] >= min(300, args.seconds / 2)
                    ),
                    None,
                )
                if steady:
                    assert processes[mode].num_fds() <= steady["gateway_fds"] + 128, (
                        mode,
                        "FD growth",
                    )
                    assert (
                        processes[mode].memory_info().rss
                        <= steady["gateway_rss_bytes"] + 64 * 1024 * 1024
                    ), (mode, "RSS growth")
                ca, cb = processes[mode].cpu_times(), cpu_start[mode]
                pa, pb = peers[mode].cpu_times(), peer_cpu_start[mode]
                output["modes"][mode] = {
                    "completed_agents": len(rows[mode]),
                    "exact_effects": sum(actual.values()),
                    "latency": latency(rows[mode]),
                    "by_phase": {
                        phase: latency([row for row in rows[mode] if row["phase"] == phase])
                        for phase in PHASES
                    },
                    "by_kind": {
                        kind: latency([row for row in rows[mode] if row["kind"] == kind])
                        for kind in ["leave", "benefits", "combined", "profile"]
                    },
                    "arrival_cohorts": [
                        {
                            "start_seconds": start,
                            "completed": len(
                                cohort := [
                                    row
                                    for row in rows[mode]
                                    if start <= row["due_seconds"] < start + args.report_seconds
                                ]
                            ),
                            "latency": latency(cohort),
                        }
                        for start in range(0, args.seconds, args.report_seconds)
                    ],
                    "gateway_cpu_seconds": ca.user + ca.system - cb.user - cb.system,
                    "peer_cpu_seconds": pa.user + pa.system - pb.user - pb.system,
                    "peak_rss_bytes": max(s[mode]["rss_bytes"] for s in samples),
                    "peak_fds": max(s[mode]["fds"] for s in samples),
                    "peak_active_agents": peaks[mode],
                    "peer": {
                        key: value for key, value in peer.items() if key not in {"cases", "events"}
                    },
                    "final": finals[mode],
                }
            if args.seconds >= 600:
                assert finals["auto"]["speculation"]["started"] > 0
                assert finals["auto"]["speculation"]["selected"] > 0
            indexed = {mode: {row["index"]: row for row in rows[mode]} for mode in modes}
            paired = []
            for spec in episodes:
                current = {mode: indexed[mode][spec["index"]] for mode in modes}
                assert len({row["digest"] for row in current.values()}) == 1, (
                    spec,
                    "mode output differs",
                )
                paired.append(
                    {
                        "phase": spec["phase"],
                        "kind": spec["kind"],
                        "saved_ms_vs_direct": current["direct"]["agent_ms"]
                        - current["auto"]["agent_ms"],
                        "saved_ms_vs_guarded": current["guarded"]["agent_ms"]
                        - current["auto"]["agent_ms"],
                        **(
                            {
                                "saved_ms_vs_baseline_auto": current["baseline_auto"]["agent_ms"]
                                - current["auto"]["agent_ms"]
                            }
                            if args.baseline
                            else {}
                        ),
                    }
                )
            atomic_json(args.output_dir / "paired.json", paired)
            output.update(
                status="passed",
                output_equivalence=True,
                exact_effects=True,
                actual_seconds=time.monotonic() - started,
                scheduled_traffic_seconds=args.seconds,
                scheduler_lag_p99_ms=percentile(lags, 0.99),
                wall_elapsed_seconds=time.time() - clock.wall,
            )
            await asyncio.sleep(2)  # Allow the final OTLP batch to arrive after all calls drain.
            await snapshot(final=True)
            traces = (await asyncio.to_thread(http, collector_url + "/stats", ""))[2]
            if args.trace_ratio:
                assert traces["spans"].get("hr-soak-auto", 0) > 0
                assert traces["spans"].get("hr-soak-guarded", 0) > 0
            atomic_json(args.output_dir / "resources.json", samples)
        except BaseException as error:
            output.update(
                status="failed", error=repr(error), actual_seconds=time.monotonic() - started
            )
            for mode, sb in sandboxes.items():
                try:
                    output["modes"][mode] = {"stats": sb.stats(), "rules": sb.predictions()}
                except Exception as snapshot_error:
                    output["modes"][mode] = {"snapshot_error": repr(snapshot_error)}
            atomic_json(args.output_dir / "resources.json", samples)
            raise
        finally:
            # Preserve diagnostics on successful trials as well as failures.
            for mode, sandbox in sandboxes.items():
                for name, process in [("gateway", sandbox.service), ("peer", sandbox.peer_process)]:
                    process.log.flush()
                    process.log.seek(0)
                    (args.output_dir / f"{mode}-{name}.log").write_text(process.log.read())
            atomic_json(args.output_dir / "results.json", output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument(
        "--baseline", help="Also run a frozen previous predictor against the identical plan"
    )
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--rate", type=float, default=2)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20261018)
    parser.add_argument("--trace-ratio", type=float, default=0.1)
    parser.add_argument("--report-seconds", type=int, default=60)
    parser.add_argument("--max-rss-mb", type=int, default=512)
    parser.add_argument("--require-uninterrupted", action="store_true")
    parser.add_argument(
        "--pause-scale",
        type=float,
        default=1,
        help="Smoke tests only; use 1 for reported workloads",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    assert 10 <= args.seconds <= 14400 and 0 < args.rate <= 20
    assert 1 <= args.concurrency <= 500 and 0 <= args.trace_ratio <= 1
    assert args.report_seconds > 0 and 0 <= args.pause_scale <= 2
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(max(soft, 8192), hard), hard))
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
