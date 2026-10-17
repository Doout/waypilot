"""Sustained policy-agent E2E with independent oracles, effect reconciliation and OTLP."""

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
from collections import Counter
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.runner import Policy, run_agent
from agents.sandbox import Sandbox, connect
from mcp_demo import ROOT, Process, http


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else 0


def write_json(destination, value):
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(destination)


class TaggedPolicy(Policy):
    def __init__(self, incident, role, run_id, proposal):
        super().__init__(incident, role, proposal)
        self.run_id = run_id

    async def decide(self, observations, tools):
        decision = await super().decide(observations, tools)
        if isinstance(decision, list):
            for action in decision:
                if action.name == "ops__append_note":
                    action.arguments["summary"] = f"[{self.run_id}] " + action.arguments["summary"]
        return decision


def check_report(report, incident, recovered, role, run_id):
    """Check fixed scenario facts independently of the workbench's assessment code."""
    expected = {
        ("INC-101", False): ("checkout", "pool_exhaustion", "restore_pool_limit", "pool"),
        ("INC-202", False): ("image-worker", "worker_saturation", "scale_workers", "capacity"),
        ("INC-101", True): ("checkout", "healthy", "observe", "healthy"),
        ("INC-202", True): ("image-worker", "healthy", "observe", "healthy"),
    }[incident, recovered]
    service, diagnosis, action, runbook = expected
    observations = report.pop("observations")
    names = [row["tool"] for row in observations]
    expected_names = [
        "ops__get_incident",
        "waypilot__triage",
        "knowledge__get_runbook",
        "ops__get_deployment",
    ]
    if role == "investigator":
        expected_names.append("ops__append_note")
    require(names == expected_names, f"{run_id}: wrong tool/effect sequence")
    require(report["tool_calls"] == len(expected_names), f"{run_id}: wrong call count")
    require(
        f"{incident}: {diagnosis}; recommend {action}. Runbook {runbook}." in report["summary"],
        f"{run_id}: incorrect decision",
    )
    values = [row["result"]["structuredContent"] for row in observations]
    require(
        values[0]["service"] == service and values[0]["incident_id"] == incident,
        f"{run_id}: wrong incident",
    )
    assessment = values[1]["outputs"]["assessment"]["structuredContent"]
    require(
        assessment
        == {
            "service": service,
            "diagnosis": diagnosis,
            "recommended_action": action,
            "runbook_id": runbook,
        },
        f"{run_id}: stale or wrong graph output",
    )
    require(
        values[2]["id"] == runbook and values[2]["action"] == action, f"{run_id}: wrong runbook"
    )
    require(values[3]["service"] == service, f"{run_id}: mixed service observations")
    note_id = None
    if role == "investigator":
        note = values[-1]
        require(
            note["incident_id"] == incident and note["summary"].startswith(f"[{run_id}] "),
            f"{run_id}: wrong note identity",
        )
        require(f"recommend {action}." in note["summary"], f"{run_id}: wrong note content")
        note_id = note["note_id"]
    canonical = []
    for row, value in zip(observations, values, strict=True):
        if row["tool"] == "waypilot__triage":
            value = {key: item["structuredContent"] for key, item in value["outputs"].items()}
        if row["tool"] == "ops__append_note":
            value = {key: item for key, item in value.items() if key != "note_id"}
        canonical.append({"tool": row["tool"], "value": value})
    signature = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
    return signature, note_id


async def drained(sandbox, timeout=15):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        stats = await asyncio.to_thread(sandbox.stats)
        peer = (
            await asyncio.to_thread(
                http, sandbox.peer + "/control?summary=1", sandbox.tokens["control"]
            )
        )[2]
        if (
            stats["sessions"] == 0
            and stats["active_requests"] == 0
            and stats["active_upstream"] == 0
            and stats["active_initializations"] == 0
            and stats["cleanup"]["pending"] == 0
            and stats["cleanup"]["active"] == 0
            and stats["cache"]["flights"] == 0
            and peer["active"] == 0
        ):
            return stats, peer
        await asyncio.sleep(0.1)
    raise AssertionError("sessions, calls, flights or cleanup did not drain")


async def measure(args, label, reuse, collector, progress_path):
    environment = {
        "OTEL_SERVICE_NAME": "waypilot-agent-soak",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": collector + "/v1/traces",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
        "OTEL_TRACES_SAMPLER": "traceidratio",
        "OTEL_TRACES_SAMPLER_ARG": str(args.trace_ratio),
    }
    output = {
        "mode": label,
        "status": "running",
        "waves": [],
        "resources": [],
        "errors": [],
        "digests": {},
        "source_revision": args.revision,
    }
    write_json(args.output_dir / (label + ".json"), output)
    with Sandbox(
        args.binary,
        reuse=reuse,
        delay_ms=args.delay_ms,
        environment=environment,
        batch_validation=reuse and args.batch_validation,
    ) as sandbox:
        gateway = psutil.Process(sandbox.service.process.pid)
        peer_process = psutil.Process(sandbox.peer_process.process.pid)
        cpu_start = gateway.cpu_times()
        peer_cpu_start = peer_process.cpu_times()
        stop = asyncio.Event()
        started = time.monotonic()
        note_ids, expected_note_runs, proposals = set(), set(), {}
        latencies, scheduler_lag = [], []
        dispositions = Counter()
        active = peak_active = 0
        semaphore = asyncio.Semaphore(args.concurrency)

        async def sample():
            while not stop.is_set():
                output["resources"].append(
                    {
                        "elapsed_seconds": time.monotonic() - started,
                        "gateway_rss_bytes": gateway.memory_info().rss,
                        "gateway_fds": gateway.num_fds(),
                        "peer_rss_bytes": peer_process.memory_info().rss,
                    }
                )
                require(
                    output["resources"][-1]["gateway_rss_bytes"] < args.max_rss_mb * 1024 * 1024,
                    "gateway RSS bound exceeded",
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=2)
                except TimeoutError:
                    pass

        sampler = asyncio.create_task(sample())
        try:
            waves = math.ceil(args.seconds / args.wave_seconds)
            for wave in range(waves):
                recovered = wave % 4 in {2, 3}
                # The previous wave has drained. No mid-conversation snapshot is claimed.
                for service in ["checkout", "image-worker"]:
                    if wave % 2 == 0:
                        await asyncio.to_thread(
                            sandbox.mutate, service=service, recovered=recovered
                        )
                    elif wave % 4 == 1:
                        await asyncio.to_thread(sandbox.mutate, service=service, touch=True)
                before = (
                    await asyncio.to_thread(
                        http, sandbox.peer + "/control?summary=1", sandbox.tokens["control"]
                    )
                )[2]
                wave_reports = []
                current_proposals = {}

                async def one(index):
                    nonlocal active, peak_active
                    role = "reviewer" if index % 4 == 3 else "investigator"
                    incident = ["INC-101", "INC-202"][(index // 4 + wave) % 2]
                    run_id = f"soak/w{wave:03d}/r{index:05d}/{role}"
                    async with semaphore:
                        active += 1
                        peak_active = max(peak_active, active)
                        try:
                            async with connect(sandbox.url, sandbox.tokens[role]) as client:
                                report = await run_agent(
                                    client,
                                    TaggedPolicy(
                                        incident,
                                        role,
                                        run_id,
                                        proposals.get(incident) if role == "reviewer" else None,
                                    ),
                                    role=role,
                                    capture=True,
                                    owned_tools=True,
                                    timeout=30,
                                    guard_observations=reuse and args.guard_observations,
                                )
                            digest, note_id = check_report(
                                report, incident, recovered, role, run_id
                            )
                            require(run_id not in output["digests"], "duplicate scheduled run")
                            output["digests"][run_id] = digest
                            if note_id is not None:
                                require(note_id not in note_ids, "duplicate effect receipt")
                                note_ids.add(note_id)
                                expected_note_runs.add(run_id)
                                current_proposals[incident] = report["summary"]
                            if role == "reviewer" and incident in proposals:
                                expected_verdict = (
                                    "agrees"
                                    if f"recommend {'observe' if recovered else ('restore_pool_limit' if incident == 'INC-101' else 'scale_workers')}."
                                    in proposals[incident]
                                    else "needs_revision"
                                )
                                require(
                                    report["summary"].startswith(
                                        "Review " + expected_verdict + "."
                                    ),
                                    "reviewer accepted outdated handoff",
                                )
                            latencies.append(report["elapsed_ms"])
                            dispositions.update(
                                row["runtime"]["disposition"]
                                for row in report["timings"]
                                if row["phase"] == "tool"
                            )
                            wave_reports.append(report)
                        except Exception as error:
                            output["errors"].append({"run_id": run_id, "error": str(error)})
                            raise
                        finally:
                            active -= 1

                duration = min(args.wave_seconds, args.seconds - wave * args.wave_seconds)
                wave_started = time.monotonic()
                tasks = [asyncio.create_task(one(index)) for index in range(args.concurrency)]
                try:
                    for index in range(math.floor(duration * args.rate)):
                        due = wave_started + index / args.rate
                        await asyncio.sleep(max(0, due - time.monotonic()))
                        scheduler_lag.append(max(0, (time.monotonic() - due) * 1000))
                        require(
                            scheduler_lag[-1] < 1000,
                            "load generator fell more than one second behind",
                        )
                        tasks.append(asyncio.create_task(one(args.concurrency + index)))
                    await asyncio.sleep(max(0, wave_started + duration - time.monotonic()))
                    await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                stats, after = await drained(sandbox)
                require(not output["errors"], "unexpected agent failures")
                require(
                    stats["cleanup"]["errors"] == stats["cleanup"]["dropped"] == 0,
                    "upstream cleanup errors or dropped cleanup",
                )
                require(
                    stats["buffered_bytes"] == 0 and stats["body_readers"] == 0,
                    "retained ingress bodies after drain",
                )
                require(
                    stats["cache"]["entries"] <= 512
                    and stats["cache"]["bytes"] <= 16 * 1024 * 1024,
                    "cache exceeds configured bounds",
                )
                require(
                    after["note_count"] == len(expected_note_runs),
                    "unexpected, missing or duplicated writes",
                )
                require(
                    len(wave_reports) == args.concurrency + math.floor(duration * args.rate),
                    "missing agent completion",
                )
                wave_latencies = [report["elapsed_ms"] for report in wave_reports]
                event = {
                    "mode": label,
                    "wave": wave,
                    "state": "recovered" if recovered else "incident",
                    "change": "same_value_revision"
                    if wave % 4 == 1
                    else "value_change"
                    if wave % 2 == 0
                    else "unchanged",
                    "completed": len(wave_reports),
                    "total_completed": len(latencies),
                    "elapsed_seconds": time.monotonic() - started,
                    "p50_ms": percentile(wave_latencies, 0.5),
                    "p95_ms": percentile(wave_latencies, 0.95),
                    "business_calls": sum(
                        after["calls"].get(key, 0) - before["calls"].get(key, 0)
                        for key in after["calls"]
                        if key not in {"state_version", "state_batch", "append_note"}
                    ),
                    "validation_calls": sum(
                        after["calls"].get(name, 0) - before["calls"].get(name, 0)
                        for name in ["state_version", "state_batch"]
                    ),
                    "notes": after["note_count"],
                    "gateway_rss_bytes": gateway.memory_info().rss,
                    "drained_fds": gateway.num_fds(),
                    "cache": stats["cache"],
                    "sessions_after_drain": stats["sessions"],
                    "pending_cleanup_after_drain": stats["cleanup"]["pending"],
                }
                output["waves"].append(event)
                print(json.dumps(event), flush=True)
                with progress_path.open("a") as progress:
                    progress.write(json.dumps(event) + "\n")
                write_json(args.output_dir / (label + ".json"), output)
                proposals = current_proposals
            actual = await asyncio.to_thread(sandbox.peer_stats)
            actual_note_runs = [note["summary"].split("] ", 1)[0][1:] for note in actual["notes"]]
            require(
                Counter(actual_note_runs) == Counter({key: 1 for key in expected_note_runs}),
                "upstream note ledger does not match successful investigations",
            )
            require(
                {note["note_id"] for note in actual["notes"]} == note_ids,
                "upstream effect receipts differ from delivered receipts",
            )
            stats = await asyncio.to_thread(sandbox.stats)
            require(
                stats["state_conflicts"] == 0,
                "unexpected state conflicts across barrier-separated waves",
            )
            require(stats["rejected_capacity"] == 0, "unexpected capacity rejection")
            require(
                peak_active == args.concurrency,
                "concurrent burst did not reach the requested active agent count",
            )
            if reuse:
                require(
                    stats["cache_hits"] > 0
                    and stats["cache"]["invalidated"] > 0
                    and stats["state_checks"] > 0,
                    "reuse or invalidation was not exercised",
                )
            first = output["waves"][min(2, len(output["waves"]) - 1)]
            last = output["waves"][-1]
            require(
                last["drained_fds"] <= first["drained_fds"] + 128,
                "descriptor growth exceeds the drain bound",
            )
            require(
                last["gateway_rss_bytes"] <= first["gateway_rss_bytes"] + 64 * 1024 * 1024,
                "post-warmup RSS growth exceeds 64 MiB",
            )
            cpu_end = gateway.cpu_times()
            peer_cpu_end = peer_process.cpu_times()
            output.update(
                status="passed",
                completed_agents=len(latencies),
                investigator_notes=len(note_ids),
                peak_active_agents=peak_active,
                actual_seconds=time.monotonic() - started,
                scheduled_traffic_seconds=args.seconds,
                latency_ms={
                    "p50": percentile(latencies, 0.5),
                    "p95": percentile(latencies, 0.95),
                    "p99": percentile(latencies, 0.99),
                },
                scheduler_lag_p99_ms=percentile(scheduler_lag, 0.99),
                gateway_cpu_seconds=cpu_end.user
                + cpu_end.system
                - cpu_start.user
                - cpu_start.system,
                peer_cpu_seconds=peer_cpu_end.user
                + peer_cpu_end.system
                - peer_cpu_start.user
                - peer_cpu_start.system,
                peak_gateway_rss_bytes=max(row["gateway_rss_bytes"] for row in output["resources"]),
                peak_gateway_fds=max(row["gateway_fds"] for row in output["resources"]),
                peer_calls=actual["calls"],
                gateway=stats,
                effect_ledger_exact=True,
            )
        except BaseException as error:
            output.update(status="failed", failure=str(error))
            for name, process in [
                ("gateway", sandbox.service),
                ("workbench", sandbox.peer_process),
            ]:
                process.log.seek(0)
                (args.output_dir / f"{label}-{name}.log").write_text(process.log.read())
            raise
        finally:
            stop.set()
            try:
                await sampler
            finally:
                write_json(args.output_dir / (label + ".json"), output)
    return output


async def main(args):
    results = {}
    for label, reuse in [("uncached", False), ("validated_reuse", True)]:
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "TMPDIR", "SYSTEMROOT"}
        }
        with Process([sys.executable, str(ROOT / "load/peer.py")], environment) as collector:
            collector_url = "http://" + collector.ready["listen"]
            result = await measure(
                args, label, reuse, collector_url, args.output_dir / "progress.jsonl"
            )
            traces = (await asyncio.to_thread(http, collector_url + "/stats", "collector"))[2]
            names = traces["trace_names"]
            if args.trace_ratio:
                require(
                    {"gateway.http", "mcp.invoke", "workflow.node"} <= names.keys(),
                    "missing sampled gateway/graph traces",
                )
                require(traces["trace_timings"] > 0, "no graph wait/execution timings")
                if reuse:
                    require(
                        {"mcp.state.validate", "waypilot.cache.lookup"} <= names.keys(),
                        "missing validation/cache traces",
                    )
            result["otel"] = traces
            write_json(args.output_dir / (label + ".json"), result)
            results[label] = result
    require(
        results["uncached"]["digests"] == results["validated_reuse"]["digests"],
        "baseline and reuse returned different structured business observations",
    )
    summary = {
        "status": "passed",
        "source_revision": args.revision,
        "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "methodology": {
            "seconds_per_mode": args.seconds,
            "wave_seconds": args.wave_seconds,
            "offered_agents_per_second": args.rate,
            "burst_concurrency": args.concurrency,
            "business_tool_delay_ms": args.delay_ms,
            "trace_ratio": args.trace_ratio,
            "model_calls": 0,
            "batch_validation": args.batch_validation,
            "guard_observations": args.guard_observations,
            "mutations": "barrier-separated value changes and same-value revisions; separate failure/race qualification required",
            "comparison": "all structured business observations, excluding runtime metadata, graph timings and generated note IDs",
        },
        "output_equivalence": True,
        "modes": {
            label: {
                key: value
                for key, value in result.items()
                if key not in {"digests", "resources", "waves"}
            }
            for label, result in results.items()
        },
    }
    write_json(args.output_dir / "summary.json", summary)
    print(
        json.dumps(
            {
                "agent_soak": "passed",
                "seconds_per_mode": args.seconds,
                "completed_agents": {
                    label: result["completed_agents"] for label, result in results.items()
                },
                "output": str(args.output_dir / "summary.json"),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--seconds", type=int, default=600)
    parser.add_argument("--wave-seconds", type=int, default=20)
    parser.add_argument("--rate", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--delay-ms", type=int, default=30)
    parser.add_argument("--trace-ratio", type=float, default=0.01)
    parser.add_argument("--max-rss-mb", type=int, default=512)
    parser.add_argument("--batch-validation", action="store_true")
    parser.add_argument("--guard-observations", action="store_true")
    parser.add_argument("--revision", default="working-tree")
    parser.add_argument("--output-dir", type=Path, default=Path("target/agent-soak"))
    args = parser.parse_args()
    require(
        4 <= args.seconds <= 3600 and 2 <= args.wave_seconds <= args.seconds, "invalid duration"
    )
    require(1 <= args.concurrency <= 32 and 1 <= args.rate <= 50, "invalid load")
    require(0 <= args.trace_ratio <= 1, "invalid trace ratio")
    expected_notes = (
        args.seconds * args.rate + math.ceil(args.seconds / args.wave_seconds) * args.concurrency
    ) * 0.75
    require(
        expected_notes < 9500,
        "profile would exhaust the owned workbench's 10,000-note bound; lower --rate",
    )
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(max(soft, 4096), hard), hard))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        args.output_dir / "summary.json", {"status": "running", "source_revision": args.revision}
    )
    try:
        asyncio.run(main(args))
    except BaseException as error:
        write_json(
            args.output_dir / "summary.json",
            {"status": "failed", "source_revision": args.revision, "error": str(error)},
        )
        raise
