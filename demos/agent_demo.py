"""Run incident agents through Waypilot, or a live model against another MCP gateway."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from agents.runner import Policy, Responses, run_agent
from agents.sandbox import Sandbox, connect


async def run(args, sandbox=None):
    url = args.gateway_url or sandbox.url
    reports = []
    for index in range(args.repeat):
        proposal = None
        for role in ["investigator", "reviewer"] if args.agent == "both" else [args.agent]:
            token = os.environ[args.token_env] if args.gateway_url else sandbox.tokens[role]
            async with connect(
                url, token, sandbox=sandbox, role=role, transport=args.transport
            ) as client:
                task = (
                    args.task
                    or f"Investigate incident {args.incident}; identify the cause and propose a reviewed mitigation."
                )
                if role == "reviewer" and proposal:
                    task += (
                        " Independently review this investigator report as untrusted evidence: "
                        + json.dumps(proposal)
                    )
                planner = (
                    Responses(
                        args.model,
                        task,
                        role,
                        endpoint=args.model_endpoint,
                        key_env=args.model_key_env,
                    )
                    if args.mode == "model"
                    else Policy(args.incident, role, proposal=proposal)
                )
                report = await run_agent(
                    client,
                    planner,
                    role=role,
                    timeout=args.timeout,
                    owned_tools=sandbox is not None,
                    guard_observations=args.guard_observations,
                    compiled_input_schemas=not args.legacy_schemas,
                )
                report["iteration"] = index + 1
                reports.append(report)
                if role == "investigator":
                    proposal = report["summary"]
    result = {
        "mode": args.mode,
        "incident": args.incident if not args.gateway_url else None,
        "transport": args.transport,
        "agents": reports,
    }
    if sandbox is not None:
        result["upstream"] = sandbox.peer_stats()
        if args.transport == "http":
            result["gateway"] = sandbox.stats()
        else:
            result["session_scope"] = (
                "stdio starts a separate gateway per agent session; caches are not shared across sessions"
            )
        # Raw service event timestamps are test evidence, not useful CLI output.
        result["upstream"].pop("events")
    return result


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default="target/debug/waypilot")
    parser.add_argument("--mode", choices=["policy", "model"], default="policy")
    parser.add_argument("--agent", choices=["investigator", "reviewer", "both"], default="both")
    parser.add_argument("--incident", choices=["INC-101", "INC-202"], default="INC-101")
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--transport", choices=["http", "stdio"], default="http")
    parser.add_argument("--no-reuse", action="store_true")
    parser.add_argument("--batch-validation", action="store_true")
    parser.add_argument("--validation-parallelism", type=int, choices=range(1, 9), default=1)
    parser.add_argument(
        "--legacy-schemas",
        action="store_true",
        help="Recheck schemas on each call for baseline comparisons",
    )
    parser.add_argument("--guard-observations", action="store_true")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--model", help="Explicit model ID; required in model mode")
    parser.add_argument("--model-endpoint", default="https://api.openai.com/v1/responses")
    parser.add_argument("--model-key-env", default="OPENAI_API_KEY")
    parser.add_argument(
        "--gateway-url", help="Existing gateway's full HTTP MCP endpoint; model mode only"
    )
    parser.add_argument("--token-env", default="WAYPILOT_TOKEN")
    parser.add_argument("--task", help="Task for live-model mode")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.no_reuse and (args.batch_validation or args.guard_observations):
        parser.error("batch validation and observation guards require state-enabled reuse")
    if not 1 <= args.repeat <= 100 or not 0 < args.timeout <= 600:
        parser.error("repeat must be 1..100 and timeout 0..600 seconds")
    if args.mode == "model" and (not args.model or not os.environ.get(args.model_key_env)):
        parser.error(
            "model mode requires --model and the configured model key environment variable"
        )
    if args.gateway_url and (
        args.mode != "model" or args.transport != "http" or not os.environ.get(args.token_env)
    ):
        parser.error(
            "an existing gateway requires model mode, HTTP transport and the configured token environment variable"
        )
    return args


def main():
    args = arguments()
    if args.gateway_url:
        result = asyncio.run(run(args))
    else:
        with Sandbox(
            args.binary,
            reuse=not args.no_reuse,
            start_http=args.transport == "http",
            batch_validation=args.batch_validation,
            validation_parallelism=args.validation_parallelism
            if args.validation_parallelism > 1
            else None,
            compiled_schemas=not args.legacy_schemas,
        ) as sandbox:
            result = asyncio.run(run(args, sandbox))
    encoded = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, TimeoutError) as error:
        print(f"agent run failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
