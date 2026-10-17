"""Delayed policy agent: every next action remains an explicit MCP call."""

import asyncio
import time

META = "io.waypilot/observations"


async def run_agent(
    client, employee_id, *, kind="leave", think_ms=200, guarded=True, days=5, on_gap=None
):
    observations, timings, receipts = [], [], []
    started = time.perf_counter_ns()

    async def call(tool, arguments, write=False):
        before = time.perf_counter_ns()
        meta = {META: {"require": receipts}} if write else {META: {"capture": True}}
        kwargs = {"meta": meta} if guarded else {}
        result = (await client.call_tool("hr__" + tool, arguments, **kwargs)).model_dump(
            by_alias=True
        )
        if result.get("isError"):
            raise RuntimeError("HR tool failed: " + tool)
        handle = result.get("_meta", {}).get(META, {}).get("receipt")
        if guarded and not write:
            if not handle:
                raise RuntimeError("missing HR observation receipt")
            receipts.append(handle)
        observations.append({"tool": tool, "value": result["structuredContent"]})
        timings.append(
            {
                "phase": "tool",
                "tool": tool,
                "elapsed_ms": (time.perf_counter_ns() - before) / 1e6,
                "request_ns": before,
                "runtime": result.get("_meta", {}).get("io.waypilot/runtime", {}),
            }
        )
        return result["structuredContent"]

    async def think(after):
        before = time.perf_counter_ns()
        if on_gap:
            await on_gap(after)
        await asyncio.sleep(think_ms / 1000)
        timings.append(
            {
                "phase": "agent_think",
                "after": after,
                "elapsed_ms": (time.perf_counter_ns() - before) / 1e6,
            }
        )

    profile = await call("employee", {"employee_id": employee_id})
    await think("employee")
    if kind == "leave":
        balance = await call("leave_balance", {"employee_id": profile["employee_id"]})
        await think("leave_balance")
        policy = await call("leave_policy", {"employee_id": profile["employee_id"]})
        decision = (
            "leave_guidance"
            if balance["days"] >= days and days <= policy["max_days"]
            else "needs_review"
        )
    else:
        await call("benefit_options", {"employee_id": profile["employee_id"]})
        decision = "benefits_guidance"
    await think("decision")
    await call(
        "record_case",
        {"employee_id": employee_id, "kind": decision, "days": days if kind == "leave" else 0},
        write=True,
    )
    canonical = [
        {"tool": row["tool"], "value": {k: v for k, v in row["value"].items() if k != "case_id"}}
        for row in observations
    ]
    return {
        "employee_id": employee_id,
        "kind": kind,
        "decision": decision,
        "observations": canonical,
        "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
        "timings": timings,
    }
