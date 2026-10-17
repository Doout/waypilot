"""Equal-work policy-agent baseline versus state-validated reuse on owned services."""

import argparse
import asyncio
import hashlib
import json
import math
import platform
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.runner import Policy, run_agent
from agents.sandbox import Sandbox, connect


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


async def measure(
    sandbox,
    runs,
    concurrency,
    *,
    guard_observations=False,
    compiled_input_schemas=False,
    reuse_sessions=False,
):
    semaphore = asyncio.Semaphore(concurrency)
    queued_ms, lifetime_ms = [], []
    sessions_started = 0
    submitted = time.perf_counter_ns()

    async def invoke(client, incident):
        return await run_agent(
            client,
            Policy(incident, "investigator"),
            role="investigator",
            owned_tools=True,
            capture=True,
            guard_observations=guard_observations,
            compiled_input_schemas=compiled_input_schemas,
        )

    async def one(index, persistent=None):
        nonlocal sessions_started
        async with semaphore:
            queued_ms.append((time.perf_counter_ns() - submitted) / 1e6)
            incident = "INC-101" if index % 2 == 0 else "INC-202"
            if persistent is None:
                sessions_started += 1
                async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                    report = await invoke(client, incident)
            else:
                report = await invoke(persistent, incident)
            lifetime_ms.append((time.perf_counter_ns() - submitted) / 1e6)
            expected = "restore_pool_limit" if incident == "INC-101" else "scale_workers"
            if expected not in report["summary"]:
                raise RuntimeError("incorrect incident decision")
            evidence = []
            for row in report.pop("observations"):
                value = row["result"]["structuredContent"]
                if row["tool"] == "waypilot__triage":
                    value = {
                        name: output["structuredContent"]
                        for name, output in value["outputs"].items()
                    }
                if row["tool"] == "ops__append_note":
                    value = {key: item for key, item in value.items() if key != "note_id"}
                evidence.append({"tool": row["tool"], "value": value})
            report["evidence_digest"] = hashlib.sha256(
                json.dumps(evidence, sort_keys=True).encode()
            ).hexdigest()
            return report

    started = time.perf_counter()
    if reuse_sessions:
        # Owned workbench contract only. A worker owns its SDK context and never
        # shares a client between concurrent agents. Rotate before receipt limits.
        reports = [None] * runs

        async def worker(slot):
            nonlocal sessions_started
            indices = iter(range(slot, runs, concurrency))
            while (first := next(indices, None)) is not None:
                sessions_started += 1
                async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                    reports[first] = await one(first, client)
                    for _ in range(11):
                        index = next(indices, None)
                        if index is None:
                            break
                        reports[index] = await one(index, client)

        await asyncio.gather(*(worker(slot) for slot in range(min(runs, concurrency))))
    else:
        reports = await asyncio.gather(*(one(index) for index in range(runs)))
    elapsed = time.perf_counter() - started
    durations = [report["elapsed_ms"] for report in reports]
    calls = sandbox.peer_stats()["calls"]
    if calls.get("append_note") != runs:
        raise RuntimeError("missing or duplicated incident effects")
    return {
        "p50_ms": percentile(durations, 0.5),
        "p95_ms": percentile(durations, 0.95),
        "p99_ms": percentile(durations, 0.99),
        "wall_seconds": elapsed,
        "queue_p95_ms": percentile(queued_ms, 0.95),
        "batch_completion_p50_ms": percentile(lifetime_ms, 0.5),
        "batch_completion_p95_ms": percentile(lifetime_ms, 0.95),
        "completed_agents_per_second": runs / elapsed,
        "agent_tool_calls": sum(report["tool_calls"] for report in reports),
        "model_calls": 0,
        "mcp_sessions_started": sessions_started,
        "business_calls": sum(
            count
            for tool, count in calls.items()
            if tool not in {"state_version", "state_batch", "append_note"}
        ),
        "validation_calls": calls.get("state_version", 0) + calls.get("state_batch", 0),
        "state_checks": sandbox.stats()["state_checks"],
        "effect_calls": calls.get("append_note", 0),
        "total_upstream_calls": sum(calls.values()),
        "per_tool": calls,
        "summaries": [report["summary"] for report in reports],
        "evidence_digests": [report["evidence_digest"] for report in reports],
        "dispositions": dict(
            Counter(
                timing["runtime"].get("disposition", "unknown")
                for report in reports
                for timing in report["timings"]
                if timing["phase"] == "tool"
            )
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default="target/release/waypilot")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--delay-ms", type=int, default=30)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--legacy-schemas", action="store_true")
    parser.add_argument(
        "--reuse-first", action="store_true", help="Reverse the mode order for a second measurement"
    )
    args = parser.parse_args()
    if (
        not 2 <= args.runs <= 10000
        or not 1 <= args.concurrency <= 32
        or not 0 <= args.delay_ms <= 1000
    ):
        parser.error("runs must be 2..10000, concurrency 1..32, delay-ms 0..1000")
    results = {}
    modes = [("uncached", False), ("validated_reuse", True)]
    if args.reuse_first:
        modes.reverse()
    for label, reuse in modes:
        with Sandbox(
            args.binary,
            reuse=reuse,
            delay_ms=args.delay_ms,
            compiled_schemas=not args.legacy_schemas,
        ) as sandbox:
            results[label] = asyncio.run(
                measure(
                    sandbox,
                    args.runs,
                    args.concurrency,
                    compiled_input_schemas=not args.legacy_schemas,
                )
            )
    if results["uncached"].pop("summaries") != results["validated_reuse"].pop("summaries"):
        raise RuntimeError("baseline and candidate agent decisions differ")
    if results["uncached"].pop("evidence_digests") != results["validated_reuse"].pop(
        "evidence_digests"
    ):
        raise RuntimeError("baseline and candidate tool observations differ")
    results.update(
        {
            "output_equivalence": True,
            "runs_per_mode": args.runs,
            "concurrency": args.concurrency,
            "owned_tool_delay_ms": args.delay_ms,
            "planner": "deterministic policy; no LLM",
            "order": [label for label, _reuse in modes],
            "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
            "platform": platform.platform(),
            "python": platform.python_version(),
        }
    )
    encoded = json.dumps(results, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    main()
