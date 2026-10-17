"""Real MCP HR lookahead tests with controlled branches, stale reads and dispatch boundaries."""

import asyncio
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from benchmark_hr import drain, wave
from hr.runner import run_agent
from hr.sandbox import Sandbox, connect
from mcp_demo import Client, Process, http

BINARY = str(Path(sys.argv[1]).resolve())
META = "io.waypilot/observations"


def train(sandbox):
    asyncio.run(wave(sandbox, 1, 4, 1, 0))
    asyncio.run(drain(sandbox))


def wait_for(predicate, timeout=5):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("controlled HR event did not arrive")


def call(client, tool, employee="E-000101", *, capture=False, require=()):
    args = {"employee_id": employee}
    if tool == "record_case":
        args.update(kind="leave_guidance", days=5)
    return client.request(
        "tools/call",
        {
            "name": "hr__" + tool,
            "arguments": args,
            "_meta": {META: {"capture": capture, "require": list(require)}},
        },
    )


def observed(sandbox, employee, phase="end"):
    return any(
        e["tool"] == "leave_balance" and e["employee_id"] == employee and e["phase"] == phase
        for e in sandbox.peer_stats()["events"]
    )


class HRTests(unittest.TestCase):
    def test_direct_benchmark_bypasses_gateway_with_the_same_agent_plan(self):
        results = []
        for direct in [True, False]:
            with Sandbox(BINARY, "off", delay_ms=0) as sandbox:
                reports = asyncio.run(wave(sandbox, 101, 2, 2, 0, direct=direct))
                results.append([r["observations"] for r in reports])
                self.assertEqual(sandbox.peer_stats()["case_count"], 2)
                self.assertTrue(all(r["session_elapsed_ms"] >= r["elapsed_ms"] for r in reports))
                if direct:
                    self.assertEqual(sandbox.stats()["requests"], 0)
                    self.assertEqual(sandbox.stats()["upstream_calls"], 0)
                else:
                    self.assertGreater(sandbox.stats()["requests"], 0)
                asyncio.run(drain(sandbox))
        self.assertEqual(*results)

    def test_batch_reduces_rpcs_with_identical_checks_results_and_effects(self):
        results = []
        for batch in [False, True]:
            with Sandbox(BINARY, "prewarm", delay_ms=0, batch_validation=batch) as sandbox:
                reports = asyncio.run(wave(sandbox, 101, 4, 2, 0))
                final = asyncio.run(drain(sandbox))
                stats = sandbox.peer_stats()
                self.assertEqual(stats["case_count"], 4)
                results.append((reports, stats, final))
        single, batch = results
        self.assertEqual(
            [r["observations"] for r in single[0]], [r["observations"] for r in batch[0]]
        )
        self.assertEqual(single[2]["state_checks"], batch[2]["state_checks"])
        self.assertGreater(batch[2]["state_batches"], 0)
        self.assertLess(sum(batch[1]["calls"].values()), sum(single[1]["calls"].values()))

    def test_direct_benchmark_respects_shared_upstream_call_budget(self):
        with Sandbox(BINARY, "off", delay_ms=10) as sandbox:
            reports = asyncio.run(wave(sandbox, 101, 65, 65, 0, direct=True))
            self.assertEqual(len(reports), 65)
            self.assertEqual(sandbox.peer_stats()["case_count"], 65)
            self.assertLessEqual(sandbox.peer_stats()["peak_active"], 64)
            self.assertEqual(sandbox.stats()["requests"], 0)

    def test_batch_guards_speculated_reads_before_the_final_write(self):
        with Sandbox(BINARY, delay_ms=1, batch_validation=True) as sandbox:
            train(sandbox)
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                receipts = []
                for tool in ["employee", "leave_balance", "leave_policy"]:
                    result = call(client, tool, capture=True)["result"]
                    receipts.append(result["_meta"][META]["receipt"])
                sandbox.mutate(employee_id="E-000101", touch=True)
                result = call(client, "record_case", require=receipts)
                self.assertEqual(result["error"]["code"], -32010)
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
                self.assertGreater(sandbox.stats()["state_batches"], 0)
                self.assertGreater(sandbox.stats()["speculation"]["selected"], 0)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_bad_batch_blocks_write_without_single_validation_fallback(self):
        with Sandbox(BINARY, "cache", delay_ms=0, batch_validation=True) as sandbox:
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                receipts = [
                    call(client, tool, capture=True)["result"]["_meta"][META]["receipt"]
                    for tool in ["employee", "leave_balance", "leave_policy"]
                ]
                for mode in ["missing", "duplicate", "changed", "error"]:
                    with self.subTest(mode=mode):
                        sandbox.mutate(
                            batch_mode=mode, fail_next="state_batch" if mode == "error" else None
                        )
                        before = sandbox.peer_stats()["calls"].copy()
                        result = call(client, "record_case", require=receipts)
                        after = sandbox.peer_stats()
                        self.assertEqual(result["error"]["code"], -32010)
                        self.assertEqual(after["case_count"], 0)
                        self.assertNotIn("record_case", after["calls"])
                        self.assertEqual(
                            after["calls"].get("state_version"), before.get("state_version")
                        )
                        self.assertEqual(
                            after["calls"]["state_batch"], before.get("state_batch", 0) + 1
                        )
                sandbox.mutate(batch_mode="reorder")
                self.assertIn("result", call(client, "record_case", require=receipts))
                self.assertEqual(sandbox.peer_stats()["case_count"], 1)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_batch_mutation_and_rate_exhaustion_block_effect_dispatch(self):
        for control in ["mutation", "rate"]:
            with (
                self.subTest(control=control),
                Sandbox(
                    BINARY,
                    "cache",
                    delay_ms=0,
                    batch_validation=True,
                    rate=1 if control == "rate" else 10000,
                    burst=11 if control == "rate" else 10000,
                ) as sandbox,
            ):
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    receipts = [
                        call(client, tool, capture=True)["result"]["_meta"][META]["receipt"]
                        for tool in ["employee", "leave_balance", "leave_policy"]
                    ]
                    if control == "mutation":
                        sandbox.mutate(employee_id="E-000101", days=1)
                    result = call(client, "record_case", require=receipts)
                    self.assertEqual(
                        result["error"]["code"], -32010 if control == "mutation" else -32029
                    )
                    self.assertNotIn("record_case", sandbox.peer_stats()["calls"])
                    self.assertEqual(sandbox.peer_stats()["case_count"], 0)
                finally:
                    client.close()
                asyncio.run(drain(sandbox))

    def test_held_batch_honors_revocation_cancellation_deadline_and_mutation(self):
        for control in ["revoke", "cancel", "deadline", "mutation"]:
            with (
                self.subTest(control=control),
                Sandbox(
                    BINARY,
                    "cache",
                    delay_ms=0,
                    batch_validation=True,
                    timeout_ms=1000 if control == "deadline" else 10000,
                ) as sandbox,
            ):
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    receipts = [
                        call(client, tool, capture=True)["result"]["_meta"][META]["receipt"]
                        for tool in ["employee", "leave_balance", "leave_policy"]
                    ]
                    sandbox.mutate(hold_tool="state_batch")
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pending = pool.submit(
                            client.request,
                            "tools/call",
                            {
                                "name": "hr__record_case",
                                "arguments": {
                                    "employee_id": "E-000101",
                                    "kind": "leave_guidance",
                                    "days": 5,
                                },
                                "_meta": {META: {"require": receipts}},
                            },
                            99,
                        )
                        wait_for(
                            lambda: any(
                                e["tool"] == "state_batch" and e["phase"] == "start"
                                for e in sandbox.peer_stats()["events"]
                            )
                        )
                        if control == "revoke":
                            self.assertEqual(
                                http(
                                    sandbox.base + "/admin/users/update",
                                    sandbox.tokens["admin"],
                                    {"id": "hr-agent", "enabled": False},
                                )[0],
                                200,
                            )
                        elif control == "cancel":
                            self.assertEqual(
                                http(
                                    sandbox.base + "/mcp",
                                    sandbox.tokens["agent"],
                                    {
                                        "jsonrpc": "2.0",
                                        "method": "notifications/cancelled",
                                        "params": {"requestId": 99},
                                    },
                                    extra=client.headers,
                                )[0],
                                202,
                            )
                        elif control == "mutation":
                            sandbox.mutate(employee_id="E-000101", touch=True)
                            sandbox.mutate(hold_tool=None)
                        result = pending.result(timeout=5)
                        self.assertIn(
                            result["error"]["code"],
                            {
                                "revoke": [-32003, -32800],
                                "cancel": [-32800],
                                "deadline": [-32008],
                                "mutation": [-32010],
                            }[control],
                        )
                        self.assertNotIn("record_case", sandbox.peer_stats()["calls"])
                finally:
                    sandbox.mutate(hold_tool=None)
                    client.close()
                asyncio.run(drain(sandbox))

    def test_learns_next_read_with_current_employee_and_never_precalls_write(self):
        with Sandbox(BINARY, delay_ms=10) as sandbox:
            client = Client(sandbox.base, sandbox.tokens["agent"])
            self.assertIn("result", call(client, "employee"))
            self.assertEqual(sandbox.stats()["speculation"]["started"], 0)
            client.close()
            train(sandbox)
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                employee = "E-000108"
                cases = sandbox.peer_stats()["case_count"]
                receipts = []
                for tool in ["employee", "leave_balance", "leave_policy"]:
                    if tool != "employee":
                        if tool == "leave_balance":
                            wait_for(lambda: observed(sandbox, employee))
                        else:
                            wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0)
                    reply = call(client, tool, employee, capture=True)["result"]
                    self.assertEqual(reply["structuredContent"]["employee_id"], employee)
                    receipts.append(reply["_meta"][META]["receipt"])
                    if tool != "employee":
                        self.assertIn(
                            reply["_meta"]["io.waypilot/runtime"]["disposition"],
                            ["reused", "joined"],
                        )
                    self.assertEqual(sandbox.peer_stats()["case_count"], cases)
                self.assertIn("result", call(client, "record_case", employee, require=receipts))
                self.assertEqual(sandbox.peer_stats()["case_count"], cases + 1)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_wrong_branch_is_discarded_and_does_not_hold_demand_connection(self):
        with Sandbox(BINARY, delay_ms=5) as sandbox:
            train(sandbox)
            sandbox.mutate(hold_tool="leave_balance")
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                before = sandbox.stats()["speculation"]["unused"]
                call(client, "employee")
                wait_for(lambda: observed(sandbox, "E-000101", "start"))
                # Completes while the guessed balance read is still held upstream.
                result = call(client, "benefit_options")["result"]
                self.assertIn("plans", result["structuredContent"])
                self.assertGreater(sandbox.stats()["speculation"]["unused"], before)
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
            finally:
                sandbox.mutate(hold_tool=None)
                client.close()
            asyncio.run(drain(sandbox))

    def test_mutation_after_prefetch_recomputes_and_stale_receipt_blocks_write(self):
        with Sandbox(BINARY, delay_ms=5) as sandbox:
            train(sandbox)
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                call(client, "employee")
                wait_for(lambda: observed(sandbox, "E-000101"))
                wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0)
                sandbox.mutate(employee_id="E-000101", days=1)
                fresh = call(client, "leave_balance", capture=True)["result"]
                self.assertEqual(fresh["structuredContent"]["days"], 1)
                self.assertEqual(fresh["_meta"]["io.waypilot/runtime"]["disposition"], "executed")
                handle = fresh["_meta"][META]["receipt"]
                sandbox.mutate(employee_id="E-000101", touch=True)
                reply = call(client, "record_case", require=[handle])
                self.assertEqual(reply["error"]["code"], -32010)
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_no_disclosure_grant_means_no_speculative_calls(self):
        with Sandbox(BINARY, delay_ms=1, grants=False) as sandbox:
            train(sandbox)
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                call(client, "employee")
            finally:
                client.close()
            asyncio.run(drain(sandbox))
            self.assertEqual(sandbox.stats()["speculation"]["started"], 0)

    def test_close_and_revocation_stop_inflight_speculation(self):
        for revoke in [False, True]:
            with (
                self.subTest(revoke=revoke),
                Sandbox(BINARY, delay_ms=1, duration_ms=5000) as sandbox,
            ):
                train(sandbox)
                sandbox.mutate(hold_tool="leave_balance")
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    call(client, "employee")
                    wait_for(lambda: observed(sandbox, "E-000101", "start"))
                    if revoke:
                        status, _, _ = http(
                            sandbox.base + "/admin/users/update",
                            sandbox.tokens["admin"],
                            {"id": "hr-agent", "enabled": False},
                        )
                        self.assertEqual(status, 200)
                    else:
                        client.close()
                    wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0, timeout=3)
                    self.assertEqual(sandbox.peer_stats()["case_count"], 4)
                finally:
                    sandbox.mutate(hold_tool=None)
                    if not revoke:
                        client.close()
                asyncio.run(drain(sandbox))

    def test_speculation_honors_duration_and_original_source_deadline(self):
        for timeout, duration in [(5000, 100), (500, 2000)]:
            with (
                self.subTest(timeout=timeout, duration=duration),
                Sandbox(BINARY, delay_ms=1, timeout_ms=timeout, duration_ms=duration) as sandbox,
            ):
                train(sandbox)
                sandbox.mutate(hold_tool="leave_balance")
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    before = sandbox.stats()["speculation"]["errors"]
                    call(client, "employee")
                    # Setup consumes the same budget; expiry before dispatch is valid.
                    wait_for(lambda: sandbox.stats()["speculation"]["errors"] > before, timeout=1.5)
                    wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0)
                    self.assertGreater(sandbox.stats()["speculation"]["errors"], before)
                    self.assertEqual(sandbox.peer_stats()["case_count"], 4)
                finally:
                    sandbox.mutate(hold_tool=None)
                    client.close()
                asyncio.run(drain(sandbox))

    def test_cancelled_demand_cancels_its_selected_prediction(self):
        with Sandbox(BINARY, delay_ms=1, duration_ms=5000) as sandbox:
            train(sandbox)
            sandbox.mutate(hold_tool="leave_balance")
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                call(client, "employee")
                wait_for(lambda: observed(sandbox, "E-000101", "start"))
                before_selected = sandbox.stats()["speculation"]["selected"]
                with ThreadPoolExecutor(max_workers=1) as pool:
                    request = pool.submit(
                        client.request,
                        "tools/call",
                        {"name": "hr__leave_balance", "arguments": {"employee_id": "E-000101"}},
                        99,
                    )
                    wait_for(
                        lambda: (
                            sandbox.stats()["active_requests"] == 2
                            and sandbox.stats()["speculation"]["selected"] > before_selected
                        )
                    )
                    status, _, _ = http(
                        sandbox.base + "/mcp",
                        sandbox.tokens["agent"],
                        {
                            "jsonrpc": "2.0",
                            "method": "notifications/cancelled",
                            "params": {"requestId": 99},
                        },
                        extra=client.headers,
                    )
                    self.assertEqual(status, 202)
                    self.assertEqual(request.result(timeout=5)["error"]["code"], -32800)
                    wait_for(lambda: sandbox.stats()["speculation"]["active"] == 0, timeout=3)
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
            finally:
                sandbox.mutate(hold_tool=None)
                client.close()
            asyncio.run(drain(sandbox))

    def test_speculation_consumes_rate_budget_before_read_dispatch(self):
        with Sandbox(BINARY, delay_ms=0, min_samples=2, rate=1, burst=16) as sandbox:
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                for employee in ["E-000001", "E-000002"]:
                    self.assertIn("result", call(client, "employee", employee))
                    self.assertIn("result", call(client, "leave_balance", employee))
                self.assertIn("result", call(client, "employee", "E-000003"))
                wait_for(lambda: sandbox.stats()["speculation"]["errors"] > 0)
                self.assertEqual(sandbox.peer_stats()["calls"]["leave_balance"], 2)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_capacity_skips_predictions_and_all_agents_still_finish(self):
        with Sandbox(BINARY, delay_ms=1, max_in_flight=1) as sandbox:
            train(sandbox)
            reports = asyncio.run(wave(sandbox, 101, 4, 1, 0))
            self.assertEqual(len(reports), 4)
            self.assertEqual(sandbox.peer_stats()["case_count"], 8)
            self.assertGreater(sandbox.stats()["speculation"]["skipped"], 0)
            self.assertEqual(sandbox.stats()["speculation"]["started"], 0)
            asyncio.run(drain(sandbox))

    def test_policy_generation_resets_learning(self):
        with Sandbox(BINARY, delay_ms=1) as sandbox:
            train(sandbox)
            http(sandbox.base + "/admin/invalidate", sandbox.tokens["admin"], {})
            before = sandbox.stats()["speculation"]["started"]
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                call(client, "employee")
            finally:
                client.close()
            self.assertEqual(sandbox.stats()["speculation"]["started"], before)
            asyncio.run(drain(sandbox))

    def test_repeated_branch_misses_disable_nomination(self):
        with Sandbox(BINARY, delay_ms=1) as sandbox:
            train(sandbox)
            before = sandbox.stats()["speculation"]["started"]
            for number in range(301, 305):
                client = Client(sandbox.base, sandbox.tokens["agent"])
                try:
                    call(client, "employee", f"E-{number:06d}")
                    call(client, "benefit_options", f"E-{number:06d}")
                finally:
                    client.close()
            asyncio.run(drain(sandbox))
            self.assertEqual(sandbox.stats()["speculation"]["started"] - before, 2)

    def test_failed_guess_does_not_fail_the_later_explicit_read(self):
        with Sandbox(BINARY, delay_ms=1) as sandbox:
            train(sandbox)
            sandbox.mutate(fail_next="leave_balance")
            client = Client(sandbox.base, sandbox.tokens["agent"])
            try:
                before = sandbox.stats()["speculation"]["errors"]
                call(client, "employee")
                wait_for(lambda: sandbox.stats()["speculation"]["errors"] > before)
                result = call(client, "leave_balance")["result"]
                self.assertEqual(result["structuredContent"]["employee_id"], "E-000101")
                self.assertEqual(result["_meta"]["io.waypilot/runtime"]["disposition"], "executed")
                self.assertEqual(sandbox.peer_stats()["case_count"], 4)
            finally:
                client.close()
            asyncio.run(drain(sandbox))

    def test_official_sdk_stdio_ingress_uses_the_same_speculation_path(self):
        async def run(sandbox):
            async with connect(
                sandbox.url,
                sandbox.tokens["agent"],
                sandbox=sandbox,
                role="hr-agent",
                transport="stdio",
            ) as client:
                for employee in range(1, 5):
                    await run_agent(client, f"E-{employee:06d}", think_ms=0)

                async def prepared(after):
                    target = {"employee": "leave_balance", "leave_balance": "leave_policy"}.get(
                        after
                    )
                    if target:
                        await asyncio.to_thread(
                            wait_for,
                            lambda: any(
                                event["tool"] == target
                                and event["employee_id"] == "E-000101"
                                and event["phase"] == "end"
                                for event in sandbox.peer_stats()["events"]
                            ),
                        )

                report = await run_agent(client, "E-000101", think_ms=0, on_gap=prepared)
                reads = [
                    t
                    for t in report["timings"]
                    if t.get("tool") in {"leave_balance", "leave_policy"}
                ]
                self.assertTrue(
                    all(t["runtime"]["disposition"] in {"reused", "joined"} for t in reads)
                )

        with Sandbox(BINARY, delay_ms=5) as sandbox:
            asyncio.run(run(sandbox))
            self.assertEqual(sandbox.peer_stats()["case_count"], 5)

    def test_write_prediction_contract_is_rejected(self):
        def invalid(command, env):
            if "serve" in command:
                config = Path(command[-1])
                config.write_text(
                    config.read_text().replace(
                        'tool = "hr__leave_balance"', 'tool = "hr__record_case"'
                    )
                )
            return Process(command, env)

        with patch("hr.sandbox.Process", side_effect=invalid):
            with self.assertRaisesRegex(RuntimeError, "explicit tool contract"):
                Sandbox(BINARY, delay_ms=1)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]], verbosity=2)
