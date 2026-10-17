"""Exercise automatic discovery and rule management over real MCP calls."""

import asyncio
import json
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from benchmark_hr import drain
from hr.runner import run_agent
from hr.sandbox import Sandbox, connect
from mcp_demo import Client, Process, http

BINARY = str(Path(sys.argv[1]).resolve())


def call(client, name, employee):
    return client.request(
        "tools/call", {"name": "hr__" + name, "arguments": {"employee_id": employee}}
    )


def wait_for(predicate):
    until = time.monotonic() + 5
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("expected learning event did not arrive")


class PreparedClient:
    """Correctness barrier: wait for a dispatched prediction, including over stdio.

    Benchmarks use the ordinary SDK client and never wait at this barrier.
    """

    def __init__(self, client, sandbox):
        self.client, self.sandbox = client, sandbox

    async def call_tool(self, *args, **kwargs):
        result = await self.client.call_tool(*args, **kwargs)
        value = result.model_dump(by_alias=True)
        predicted = value.get("_meta", {}).get("io.waypilot/runtime", {}).get("predicted_tool")
        if predicted:
            employee = value["structuredContent"]["employee_id"]
            await asyncio.to_thread(
                wait_for,
                lambda: any(
                    e["tool"] == predicted.removeprefix("hr__")
                    and e["employee_id"] == employee
                    and e["phase"] == "end"
                    for e in self.sandbox.peer_stats()["events"]
                ),
            )
        return result


def train(sandbox, start=1, count=4, think_ms=40, wrong_every=0):
    async def run():
        reports = []
        for index in range(count):
            async with connect(sandbox.url, sandbox.tokens["agent"]) as client:
                reports.append(
                    await run_agent(
                        PreparedClient(client, sandbox),
                        f"E-{start + index:06d}",
                        kind="benefits"
                        if wrong_every and index % wrong_every == wrong_every - 1
                        else "leave",
                        think_ms=think_ms,
                    )
                )
        return reports

    reports = asyncio.run(run())
    asyncio.run(drain(sandbox))
    return reports


def balance_rule(sandbox):
    return next(
        r
        for r in sandbox.predictions()["rules"]
        if r["after"] == "hr__employee" and r["tool"] == "hr__leave_balance"
    )


class LearningTests(unittest.TestCase):
    def test_approval_cli_and_rejection_without_authored_patterns(self):
        with Sandbox(BINARY, learning="approval", delay_ms=80, batch_validation=True) as sandbox:
            self.assertNotIn("[[speculation.rules]]", sandbox.config.read_text())
            train(sandbox)
            rule = balance_rule(sandbox)
            self.assertEqual(rule["status"], "pending_approval")
            self.assertGreaterEqual(rule["samples"], 4)
            self.assertGreater(rule["estimated_net_saving_ms"], 10)
            self.assertEqual(sandbox.stats()["speculation"]["started"], 0)
            self.assertEqual(
                http(sandbox.base + "/admin/predictions", sandbox.tokens["agent"])[0], 401
            )
            self.assertEqual(sandbox.decide("unknown")[0], 404)
            command = [BINARY, "admin", "--url", sandbox.base, "--token-env", "HR_ADMIN"]
            reply = subprocess.run(
                command + ["--action", "approve", "--rule", rule["id"]],
                env=sandbox.env,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue(json.loads(reply.stdout)["persistent"])
            reports = train(sandbox, start=101, count=2)
            self.assertTrue(
                any(
                    t.get("runtime", {}).get("disposition") in ["joined", "reused"]
                    for r in reports
                    for t in r["timings"]
                )
            )
            self.assertGreater(balance_rule(sandbox)["reused"], 0)
            self.assertEqual(sandbox.decide(rule["id"], False)[0], 200)
            sandbox.learning_mode("auto")
            train(sandbox, start=201, count=3)
            self.assertEqual(balance_rule(sandbox)["status"], "rejected")
            report = sandbox.predictions()
            self.assertEqual(report["context_bytes"], 0)
            self.assertNotIn("E-000", json.dumps(report))
            self.assertFalse(any(r["tool"] == "hr__record_case" for r in report["rules"]))

    def test_automatic_mode_uses_returned_parameters_and_holdout_calls(self):
        with Sandbox(BINARY, learning="auto", delay_ms=80, batch_validation=True) as sandbox:
            for n in range(1, 13):
                sandbox.mutate(alias_id=f"E-{n:06d}", canonical_id=f"E-{9000 + n:06d}")
            reports = train(sandbox, count=12)
            rule = balance_rule(sandbox)
            self.assertEqual(rule["status"], "active")
            self.assertGreater(rule["reused"], 0)
            self.assertGreater(rule["holdouts"], 0)
            self.assertEqual(
                rule["arguments"]["/employee_id"]["sources"],
                [{"result": True, "pointer": "/structuredContent/employee_id"}],
            )
            self.assertTrue(all(r["employee_id"].startswith("E-009") for r in reports))
            self.assertEqual(sandbox.peer_stats()["case_count"], 12)

    def test_observe_mode_and_missing_grant_never_speculate(self):
        for mode, grants in [("observe", True), ("auto", False)]:
            with (
                self.subTest(mode=mode, grants=grants),
                Sandbox(BINARY, learning=mode, grants=grants, delay_ms=40) as sandbox,
            ):
                train(sandbox, count=5)
                self.assertEqual(sandbox.stats()["speculation"]["started"], 0)
                if grants:
                    self.assertEqual(balance_rule(sandbox)["status"], "observing")
                else:
                    self.assertEqual(sandbox.predictions()["rules"], [])

    def test_rejection_cancels_running_prediction_and_blocks_new_ones(self):
        with Sandbox(BINARY, learning="approval", delay_ms=50, duration_ms=5000) as sandbox:
            train(sandbox)
            rule = balance_rule(sandbox)
            sandbox.decide(rule["id"])
            sandbox.mutate(hold_tool="leave_balance")
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                before_started = sandbox.stats()["speculation"]["started"]
                for number in range(101, 106):
                    employee = f"E-{number:06d}"
                    call(client, "employee", employee)
                    if sandbox.stats()["speculation"]["started"] > before_started:
                        break
                    # This randomized holdout must complete normally before the next opportunity.
                    sandbox.mutate(hold_tool=None)
                    self.assertIn("result", call(client, "leave_balance", employee))
                    sandbox.mutate(hold_tool="leave_balance")
                wait_for(lambda: sandbox.stats()["speculation"]["active"] == 1)
                sandbox.decide(rule["id"], False)
                wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0)
                sandbox.mutate(hold_tool=None)
                before = sandbox.stats()["speculation"]["selected"]
                self.assertIn("result", call(client, "leave_balance", employee))
                self.assertEqual(sandbox.stats()["speculation"]["selected"], before)
                self.assertEqual(balance_rule(sandbox)["status"], "rejected")
            finally:
                sandbox.mutate(hold_tool=None)
                client.close()
            asyncio.run(drain(sandbox))

    def test_branch_drift_retires_old_path_and_learns_new_path(self):
        with Sandbox(
            BINARY,
            learning="auto",
            delay_ms=35,
            learning_options={"window": 8, "evaluation_samples": 3, "recovery_samples": 4},
        ) as sandbox:
            train(sandbox, count=4)
            train(sandbox, start=101, count=12, wrong_every=1)
            rules = sandbox.predictions()["rules"]
            self.assertNotEqual(balance_rule(sandbox)["status"], "active")
            benefits = next(r for r in rules if r["tool"] == "hr__benefit_options")
            self.assertEqual(benefits["status"], "active")
            self.assertGreater(benefits["reused"], 0)
            self.assertLess(sandbox.stats()["speculation"]["unused"], 8)
            self.assertEqual(sandbox.peer_stats()["case_count"], 16)

    def test_overlap_does_not_invent_a_causal_sequence(self):
        with Sandbox(BINARY, learning="observe", delay_ms=20) as sandbox:
            for n in range(1, 5):
                client = Client(sandbox.base, sandbox.tokens["agent"])
                sandbox.mutate(hold_tool="employee")
                try:
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        future = pool.submit(call, client, "employee", f"E-{n:06d}")
                        wait_for(lambda: sandbox.peer_stats()["active"] > 0)
                        # Separate HTTP request IDs on the same session; completion order is not dependency.
                        second = pool.submit(call, client, "leave_balance", f"E-{n:06d}")
                        wait_for(lambda: sandbox.stats()["active_requests"] >= 2)
                        sandbox.mutate(hold_tool=None)
                        self.assertIn("result", second.result(timeout=5))
                        self.assertIn("result", future.result(timeout=5))
                finally:
                    sandbox.mutate(hold_tool=None)
                    client.close()
            self.assertEqual(sandbox.predictions()["rules"], [])
            self.assertEqual(sandbox.predictions()["context_bytes"], 0)

    def test_approval_survives_restart_but_evidence_must_retrain(self):
        with Sandbox(BINARY, learning="approval", delay_ms=35) as sandbox:
            train(sandbox)
            rule = balance_rule(sandbox)
            sandbox.decide(rule["id"])
            sandbox.service.__exit__(None, None, None)
            sandbox.service = sandbox.stack.enter_context(
                Process([BINARY, "serve", "--config", str(sandbox.config)], sandbox.env)
            )
            sandbox.base = "http://" + sandbox.service.ready["listen"]
            sandbox.url = sandbox.base + "/mcp"
            self.assertEqual(sandbox.predictions()["rules"], [])
            train(sandbox, start=101)
            self.assertEqual(balance_rule(sandbox)["id"], rule["id"])
            self.assertEqual(balance_rule(sandbox)["status"], "active")

    def test_changed_remote_state_reexecutes_learned_read_and_guards_write(self):
        with Sandbox(BINARY, learning="approval", delay_ms=40, batch_validation=True) as sandbox:
            train(sandbox)
            sandbox.decide(balance_rule(sandbox)["id"])
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                before = sandbox.stats()["speculation"]["started"]
                for number in range(101, 106):
                    employee = f"E-{number:06d}"
                    call(client, "employee", employee)
                    if sandbox.stats()["speculation"]["started"] > before:
                        break
                    call(client, "leave_balance", employee)
                wait_for(
                    lambda: any(
                        e["tool"] == "leave_balance"
                        and e["employee_id"] == employee
                        and e["phase"] == "end"
                        for e in sandbox.peer_stats()["events"]
                    )
                )
                sandbox.mutate(employee_id=employee, days=1)
                fresh = client.request(
                    "tools/call",
                    {
                        "name": "hr__leave_balance",
                        "arguments": {"employee_id": employee},
                        "_meta": {"io.waypilot/observations": {"capture": True}},
                    },
                )["result"]
                self.assertEqual(fresh["structuredContent"]["days"], 1)
                self.assertEqual(fresh["_meta"]["io.waypilot/runtime"]["disposition"], "executed")
                receipt = fresh["_meta"]["io.waypilot/observations"]["receipt"]
                sandbox.mutate(employee_id=employee, touch=True)
                response = client.request(
                    "tools/call",
                    {
                        "name": "hr__record_case",
                        "arguments": {"employee_id": employee, "kind": "needs_review", "days": 5},
                        "_meta": {"io.waypilot/observations": {"require": [receipt]}},
                    },
                )
                self.assertEqual(response["error"]["code"], -32010)
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_failed_calls_break_discovery_sequence(self):
        with Sandbox(BINARY, learning="observe", delay_ms=1) as sandbox:
            for number in range(1, 5):
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    employee = f"E-{number:06d}"
                    call(client, "employee", employee)
                    self.assertIn("error", call(client, "unknown_tool", employee))
                    call(client, "leave_balance", employee)
                finally:
                    client.close()
            self.assertEqual(sandbox.predictions()["rules"], [])
            self.assertEqual(sandbox.predictions()["context_bytes"], 0)

    def test_stdio_ingress_discovers_and_runs_patterns(self):
        async def run(sandbox):
            async with connect(
                sandbox.url,
                sandbox.tokens["agent"],
                sandbox=sandbox,
                role="hr-agent",
                transport="stdio",
            ) as client:
                reports = [
                    await run_agent(PreparedClient(client, sandbox), f"E-{n:06d}", think_ms=40)
                    for n in range(1, 7)
                ]
                return reports

        with Sandbox(BINARY, learning="auto", delay_ms=60) as sandbox:
            # The stdio service owns the same durable state while the HTTP service is stopped.
            sandbox.service.close()
            reports = asyncio.run(run(sandbox))
            self.assertTrue(
                any(
                    t.get("runtime", {}).get("disposition") in ["joined", "reused"]
                    for r in reports
                    for t in r["timings"]
                )
            )
            self.assertEqual(sandbox.peer_stats()["case_count"], 6)

    def test_generation_change_resets_evidence_and_zero_gap_does_not_enable(self):
        with Sandbox(BINARY, learning="auto", delay_ms=40) as sandbox:
            train(sandbox)
            self.assertTrue(sandbox.predictions()["rules"])
            self.assertEqual(
                http(sandbox.base + "/admin/invalidate", sandbox.tokens["admin"], {})[0], 200
            )
            self.assertEqual(sandbox.predictions()["rules"], [])
            train(sandbox, start=101, think_ms=0)
            self.assertEqual(sandbox.stats()["speculation"]["active"], 0)
            self.assertTrue(all(r["status"] == "learning" for r in sandbox.predictions()["rules"]))


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]], verbosity=2)
