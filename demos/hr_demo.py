"""Delayed HR agent demo with learned, explicitly eligible next-read speculation."""

import argparse
import asyncio
import json
from pathlib import Path

from hr.runner import run_agent
from hr.sandbox import Sandbox, connect
from hr.workbench import read_delay


async def run(args, sandbox):
    if not args.cold:
        for number in range(1, 5):
            async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
                await run_agent(
                    client,
                    f"E-{number:06d}",
                    think_ms=args.think_ms if args.learning else 0,
                    guarded=args.mode != "off",
                )
    learned = sandbox.predictions()
    if args.approve_learned:
        for rule in learned["rules"]:
            if rule["status"] == "pending_approval":
                assert sandbox.decide(rule["id"])[0] == 200
    async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
        report = await run_agent(
            client,
            args.employee_id,
            kind=args.kind,
            think_ms=args.think_ms,
            guarded=args.mode != "off",
        )
    return {
        "agent": report,
        "learned_before_measurement": learned,
        "learned_after_measurement": sandbox.predictions(),
        "gateway": sandbox.stats(),
        "upstream": sandbox.peer_stats(),
        "model_calls": 0,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default="target/debug/waypilot")
    parser.add_argument(
        "--mode", choices=["off", "cache", "prewarm", "speculate"], default="speculate"
    )
    parser.add_argument("--employee-id", default="E-000101")
    parser.add_argument("--kind", choices=["leave", "benefits"], default="leave")
    parser.add_argument("--think-ms", type=int, default=200)
    parser.add_argument("--tool-ms", type=int, default=250)
    parser.add_argument("--read-ms", type=read_delay, action="append", default=[])
    parser.add_argument("--prediction-ms", type=int, default=2000)
    parser.add_argument(
        "--resolved-id", help="Synthetic directory maps --employee-id to this canonical ID"
    )
    parser.add_argument("--state-ms", type=int, default=1)
    parser.add_argument("--batch-validation", action="store_true")
    parser.add_argument("--cold", action="store_true")
    parser.add_argument("--learning", choices=["observe", "approval", "auto"])
    parser.add_argument(
        "--approve-learned",
        action="store_true",
        help="Approve discovered candidates before the measured demo agent",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not (
        0 <= args.think_ms <= 5000
        and 0 <= args.tool_ms <= 5000
        and 0 <= args.state_ms <= 1000
        and 100 <= args.prediction_ms <= 5000
    ):
        parser.error("invalid delay")
    with Sandbox(
        args.binary,
        args.mode,
        args.tool_ms,
        learning=args.learning,
        state_ms=args.state_ms,
        batch_validation=args.batch_validation,
        duration_ms=args.prediction_ms,
        read_delays=dict(args.read_ms),
    ) as sandbox:
        if args.resolved_id:
            sandbox.mutate(alias_id=args.employee_id, canonical_id=args.resolved_id)
        result = asyncio.run(run(args, sandbox))
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
