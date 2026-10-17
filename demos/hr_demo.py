"""Delayed HR agent demo with learned, explicitly eligible next-read speculation."""

import argparse
import asyncio
import json
from pathlib import Path

from hr.runner import run_agent
from hr.sandbox import Sandbox, connect


async def run(args, sandbox):
    if not args.cold:
        for number in range(1, 5):
            async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
                await run_agent(client, f"E-{number:06d}", think_ms=0, guarded=args.mode != "off")
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
    parser.add_argument("--cold", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not (0 <= args.think_ms <= 2000 and 0 <= args.tool_ms <= 1000):
        parser.error("invalid delay")
    with Sandbox(args.binary, args.mode, args.tool_ms) as sandbox:
        result = asyncio.run(run(args, sandbox))
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
