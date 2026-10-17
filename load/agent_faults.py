"""Controlled failures during real agent calls; assert fail-closed release and exact effects."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from agent_soak import TaggedPolicy, check_report, drained, require, write_json
from agents.runner import run_agent
from agents.sandbox import Sandbox, connect
from mcp.shared.exceptions import MCPError
from mcp_demo import Client, http


async def control(sandbox, path, body=None):
    status, _, result = await asyncio.to_thread(
        http, sandbox.base + path, sandbox.tokens["admin"], body
    )
    require(status == 200, "operator control failed")
    return result


async def counts(sandbox):
    return (
        await asyncio.to_thread(
            http, sandbox.peer + "/control?summary=1", sandbox.tokens["control"]
        )
    )[2]


async def wait_call(sandbox, name, before):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = await counts(sandbox)
        if value["calls"].get(name, 0) > before:
            return
        await asyncio.sleep(0.002)
    raise AssertionError("controlled call was not observed at the upstream: " + name)


async def agent(sandbox, run_id):
    async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
        return await run_agent(
            client,
            TaggedPolicy("INC-101", "investigator", run_id, None),
            role="investigator",
            capture=True,
            owned_tools=True,
        )


def codes(error):
    if isinstance(error, MCPError):
        return [error.code]
    return [code for child in getattr(error, "exceptions", []) for code in codes(child)]


async def failed(task, expected):
    try:
        await task
    except Exception as error:
        actual = codes(error)
        require(
            any(code in expected for code in actual),
            f"wrong failure class: {type(error).__name__}, codes={actual}, expected={sorted(expected)}",
        )
        return actual
    raise AssertionError("protected agent unexpectedly completed")


async def qualification(args):
    results = []
    for case in [
        "mutation_during_read",
        "revocation_during_graph",
        "read_error",
        "deadline_in_graph",
        "cancel_before_write",
        "cancel_after_write",
    ]:
        with Sandbox(
            args.binary,
            delay_ms=120,
            timeout_ms=250 if case == "deadline_in_graph" else 10000,
            environment={},
        ) as sandbox:
            observed_codes = []
            for index in range(args.iterations):
                await control(sandbox, "/admin/invalidate", {})
                if case == "revocation_during_graph":
                    await control(
                        sandbox, "/admin/users/update", {"id": "investigator", "enabled": True}
                    )
                await asyncio.to_thread(sandbox.mutate, recovered=False)
                if case == "read_error":
                    await asyncio.to_thread(sandbox.mutate, fail_next="get_metrics")
                before = await counts(sandbox)
                task = asyncio.create_task(agent(sandbox, f"fault/{case}/{index}"))
                recovered = False
                try:
                    target = "append_note" if case == "cancel_after_write" else "get_metrics"
                    await wait_call(sandbox, target, before["calls"].get(target, 0))
                    if case == "mutation_during_read":
                        recovered = index % 2 == 1
                        await asyncio.to_thread(
                            sandbox.mutate, touch=True, **({"recovered": True} if recovered else {})
                        )
                        observed_codes.extend(await failed(task, {-32010}))
                    elif case == "revocation_during_graph":
                        await control(
                            sandbox, "/admin/users/update", {"id": "investigator", "enabled": False}
                        )
                        observed_codes.extend(await failed(task, {-32003, -32800}))
                    elif case == "read_error":
                        observed_codes.extend(await failed(task, {-32002}))
                    elif case == "deadline_in_graph":
                        observed_codes.extend(await failed(task, {-32008}))
                    else:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        else:
                            raise AssertionError("cancellation did not cancel the agent")
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await drained(sandbox)
                after = await counts(sandbox)
                issued = 1 if case == "cancel_after_write" else 0
                require(
                    after["note_count"] - before["note_count"] == issued,
                    "failed/cancelled agent wrote an unexpected number of notes",
                )
                require(
                    after["calls"].get("append_note", 0) - before["calls"].get("append_note", 0)
                    == issued,
                    "failed effect was replayed",
                )
                require(
                    after["calls"].get("get_metrics", 0) - before["calls"].get("get_metrics", 0)
                    == 1,
                    "read was silently replayed",
                )
                if case == "revocation_during_graph":
                    await control(
                        sandbox, "/admin/users/update", {"id": "investigator", "enabled": True}
                    )
                if case == "deadline_in_graph":
                    async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                        healthy = (
                            await client.call_tool("ops__get_metrics", {"service": "checkout"})
                        ).model_dump(by_alias=True)
                        require(
                            not healthy.get("isError"), "gateway did not recover after deadline"
                        )
                else:
                    run_id = f"recovery/{case}/{index}"
                    report = await agent(sandbox, run_id)
                    check_report(report, "INC-101", recovered, "investigator", run_id)
                    require(
                        (await counts(sandbox))["note_count"] == after["note_count"] + 1,
                        "fresh invocation did not produce exactly one new effect",
                    )
                await drained(sandbox)
            result = {
                "case": case,
                "iterations": args.iterations,
                "rpc_error_codes": sorted(set(observed_codes)),
                "passed": True,
                "peer": await counts(sandbox),
                "gateway": await asyncio.to_thread(sandbox.stats),
            }
            if case == "cancel_after_write":
                actual = await asyncio.to_thread(sandbox.peer_stats)
                expected = {
                    f"{prefix}/{case}/{index}"
                    for prefix in ["fault", "recovery"]
                    for index in range(args.iterations)
                }
                observed = [note["summary"].split("] ", 1)[0][1:] for note in actual["notes"]]
                require(
                    len(observed) == len(set(observed)) and set(observed) == expected,
                    "uncertain-effect ledger contains duplicates or missing writes",
                )
                result["uncertain_effects"] = args.iterations
                result["remote_rollback_claimed"] = False
            results.append(result)
            write_json(args.output, {"status": "running", "checks": results})
            print(
                json.dumps({"agent_fault": case, "iterations": args.iterations, "passed": True}),
                flush=True,
            )
    with Sandbox(args.binary, environment={}) as sandbox:
        client = await asyncio.to_thread(Client, sandbox.base, sandbox.tokens["investigator"])
        try:
            before = await counts(sandbox)
            status, _, result = await asyncio.to_thread(
                http,
                sandbox.base + "/v1/prewarm",
                sandbox.tokens["investigator"],
                {"servers": ["ops", "knowledge"]},
                extra=client.headers,
            )
            require(status == 200 and result["tool_calls"] == 0, "prewarm issued a tool call")
            after = await counts(sandbox)
            require(
                before["calls"] == after["calls"] and after["note_count"] == 0,
                "prewarm performed an effect or business read",
            )
            results.append(
                {"case": "prewarm_without_tool_execution", "passed": True, "response": result}
            )
        finally:
            await asyncio.to_thread(client.close)
        await drained(sandbox)
    return {
        "status": "passed",
        "iterations_per_case": args.iterations,
        "checks": results,
        "model_calls": 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--output", type=Path, default=Path("target/agent-faults.json"))
    args = parser.parse_args()
    require(1 <= args.iterations <= 100, "iterations must be 1..100")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, {"status": "running", "checks": []})
    try:
        write_json(args.output, asyncio.run(qualification(args)))
    except BaseException as error:
        result = json.loads(args.output.read_text())
        result.update(status="failed", error=str(error))
        write_json(args.output, result)
        raise
