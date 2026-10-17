"""Live incident-agent regression tests. Model transport uses an owned fake provider."""

import asyncio
import http.server
import json
import os
import sys
import threading
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.runner import Action, Policy, Responses, run_agent
from agents.sandbox import Sandbox, connect
from mcp_demo import http as gateway_http

BINARY = str(Path(sys.argv[1]).resolve()) if len(sys.argv) > 1 else "target/debug/waypilot"


async def investigate(
    sandbox, incident="INC-101", role="investigator", transport="http", proposal=None
):
    async with connect(
        sandbox.url, sandbox.tokens[role], sandbox=sandbox, role=role, transport=transport
    ) as client:
        return await run_agent(
            client, Policy(incident, role, proposal), role=role, capture=True, owned_tools=True
        )


def graph(report):
    return next(
        row["result"]["structuredContent"]
        for row in report["observations"]
        if row["tool"] == "waypilot__triage"
    )


class AgentTests(unittest.TestCase):
    def test_reviewer_rejects_outdated_handoff(self):
        with Sandbox(BINARY) as sandbox:
            proposal = asyncio.run(investigate(sandbox))["summary"]
            sandbox.mutate(recovered=True)
            review = asyncio.run(investigate(sandbox, role="reviewer", proposal=proposal))
            self.assertTrue(review["summary"].startswith("Review needs_revision."))
            self.assertIn("recommend observe", review["summary"])
            self.assertEqual(len(sandbox.peer_stats()["notes"]), 1)

    def test_reuse_branch_invalidation_and_replanning(self):
        with Sandbox(BINARY) as sandbox:
            cold = asyncio.run(investigate(sandbox))
            warm = asyncio.run(investigate(sandbox))
            self.assertEqual(cold["summary"], warm["summary"])
            self.assertEqual({row["disposition"] for row in graph(warm)["steps"]}, {"reused"})
            sandbox.mutate(touch=True)
            touched = asyncio.run(investigate(sandbox))
            self.assertEqual(cold["summary"], touched["summary"])
            self.assertEqual(
                {row["id"]: row["disposition"] for row in graph(touched)["steps"]},
                {
                    "metrics": "executed",
                    "assessment": "executed",
                    "deployment": "reused",
                    "runbooks": "reused",
                },
            )
            sandbox.mutate(recovered=True)
            recovered = asyncio.run(investigate(sandbox))
            self.assertIn("recommend observe", recovered["summary"])
            self.assertIn("Runbook healthy", recovered["summary"])
            stats = sandbox.peer_stats()
            self.assertEqual(len(stats["notes"]), 4)
            self.assertEqual(len({note["note_id"] for note in stats["notes"]}), 4)
            self.assertEqual(stats["calls"]["append_note"], 4)
            self.assertGreater(stats["calls"]["state_version"], 0)
            self.assertGreater(sandbox.stats()["cache"]["invalidated"], 0)
            self.assertGreaterEqual(stats["peak_active"], 2)
            metrics = [row for row in stats["events"] if row["tool"] == "get_metrics"]
            searches = [row for row in stats["events"] if row["tool"] == "search_runbooks"]
            self.assertTrue(
                any(
                    max(a["start_ns"], b["start_ns"]) < min(a["end_ns"], b["end_ns"])
                    for a in metrics
                    for b in searches
                ),
                "independent graph branches did not overlap at the peer",
            )

    def test_second_incident_and_read_only_reviewer(self):
        with Sandbox(BINARY) as sandbox:
            report = asyncio.run(investigate(sandbox, "INC-202", "reviewer"))
            self.assertIn("recommend scale_workers", report["summary"])
            self.assertEqual(sandbox.peer_stats()["notes"], [])

            async def denied():
                async with connect(sandbox.url, sandbox.tokens["reviewer"]) as client:
                    self.assertNotIn(
                        "ops__append_note",
                        {tool.name for tool in (await client.list_tools()).tools},
                    )
                    with self.assertRaises(Exception):
                        await client.call_tool(
                            "ops__append_note", {"incident_id": "INC-202", "summary": "forbidden"}
                        )

            asyncio.run(denied())
            self.assertEqual(sandbox.peer_stats()["notes"], [])
            self.assertEqual(sandbox.peer_stats()["calls"].get("append_note", 0), 0)

    def test_sdk_stdio_ingress(self):
        with Sandbox(BINARY) as sandbox:
            report = asyncio.run(investigate(sandbox, transport="stdio"))
            self.assertIn("pool_exhaustion", report["summary"])
            self.assertEqual(len(sandbox.peer_stats()["notes"]), 1)

    def test_uncached_baseline_equivalent_with_effects(self):
        summaries = []
        counts = []
        for reuse in [False, True]:
            with Sandbox(BINARY, reuse=reuse) as sandbox:
                reports = [asyncio.run(investigate(sandbox)) for _ in range(2)]
                summaries.append([report["summary"] for report in reports])
                calls = sandbox.peer_stats()["calls"]
                self.assertEqual(calls["append_note"], 2)
                counts.append(calls)
        self.assertEqual(*summaries)
        self.assertLess(counts[1]["get_metrics"], counts[0]["get_metrics"])
        self.assertGreater(counts[1]["state_version"], 0)
        self.assertEqual(counts[0].get("state_version", 0), 0)

    def test_failed_read_never_reaches_write(self):
        with Sandbox(BINARY) as sandbox:
            sandbox.mutate(fail_next="get_metrics")
            with self.assertRaises(Exception):
                asyncio.run(investigate(sandbox))
            self.assertEqual(sandbox.peer_stats()["notes"], [])
            self.assertEqual(sandbox.peer_stats()["calls"]["get_metrics"], 1)

    def test_live_revocation_on_warm_path(self):
        with Sandbox(BINARY) as sandbox:
            asyncio.run(investigate(sandbox))
            status, _, _ = gateway_http(
                sandbox.base + "/admin/users/update",
                sandbox.tokens["admin"],
                {"id": "investigator", "enabled": False},
            )
            self.assertEqual(status, 200)
            before = sandbox.peer_stats()["calls"]
            with self.assertRaises(Exception):
                asyncio.run(investigate(sandbox))
            self.assertEqual(sandbox.peer_stats()["calls"], before)

    def test_concurrent_agents(self):
        with Sandbox(BINARY, delay_ms=10) as sandbox:

            async def many():
                return await asyncio.gather(
                    *(
                        investigate(sandbox, "INC-101" if index % 2 == 0 else "INC-202")
                        for index in range(16)
                    )
                )

            reports = asyncio.run(many())
            self.assertEqual(len(reports), 16)
            self.assertTrue(all(report["tool_calls"] == 5 for report in reports))
            notes = sandbox.peer_stats()["notes"]
            self.assertEqual(len(notes), 16)
            self.assertEqual(
                Counter(note["incident_id"] for note in notes), {"INC-101": 8, "INC-202": 8}
            )
            self.assertTrue(
                all(
                    "restore_pool_limit" in report["summary"]
                    if index % 2 == 0
                    else "scale_workers" in report["summary"]
                    for index, report in enumerate(reports)
                )
            )

    def test_deadline_prevents_later_write(self):
        with Sandbox(BINARY, delay_ms=500, timeout_ms=200) as sandbox:
            with self.assertRaises(Exception):
                asyncio.run(investigate(sandbox))
            self.assertEqual(sandbox.peer_stats()["notes"], [])


class GuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_timeout_cancels_planning(self):
        class Client:
            async def list_tools(self):
                class Catalog:
                    def model_dump(self, **_kwargs):
                        return {"tools": []}

                return Catalog()

        class SlowPlanner:
            cancelled = False

            async def decide(self, *_args):
                try:
                    await asyncio.Event().wait()
                finally:
                    self.cancelled = True

        planner = SlowPlanner()
        with self.assertRaises(TimeoutError):
            await run_agent(Client(), planner, role="reviewer", timeout=0.02)
        self.assertTrue(planner.cancelled)

    async def test_failure_after_effect_never_replays_it(self):
        class FailingPlanner(Policy):
            async def decide(self, observations, tools):
                if any(row["tool"] == "ops__append_note" for row in observations):
                    raise RuntimeError("injected planner failure after write")
                return await super().decide(observations, tools)

        with Sandbox(BINARY) as sandbox:
            async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                with self.assertRaisesRegex(RuntimeError, "planner failure after write"):
                    await run_agent(
                        client,
                        FailingPlanner("INC-101", "investigator"),
                        role="investigator",
                        owned_tools=True,
                    )
            self.assertEqual(sandbox.peer_stats()["calls"]["append_note"], 1)
            self.assertEqual(len(sandbox.peer_stats()["notes"]), 1)

    async def test_whole_batch_validated_before_dispatch(self):
        class Client:
            calls = []

            async def list_tools(self):
                class Catalog:
                    def model_dump(self, **_kwargs):
                        return {"tools": [{"name": "write", "inputSchema": {"type": "object"}}]}

                return Catalog()

            async def call_tool(self, name, arguments):
                self.calls.append(name)

        class Planner:
            async def decide(self, *_args):
                return [Action("write", {}), Action("absent", {})]

        client = Client()
        with self.assertRaises(ValueError):
            await run_agent(client, Planner(), role="investigator")
        self.assertEqual(client.calls, [])

    async def test_turn_and_call_budgets(self):
        with Sandbox(BINARY) as sandbox:
            async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                with self.assertRaisesRegex(RuntimeError, "tool-call budget"):
                    await run_agent(
                        client, Policy("INC-101", "investigator"), role="investigator", max_calls=1
                    )
                with self.assertRaisesRegex(RuntimeError, "turn budget"):
                    await run_agent(
                        client, Policy("INC-101", "investigator"), role="investigator", max_turns=1
                    )
            self.assertEqual(sandbox.peer_stats()["notes"], [])


class ModelTests(unittest.TestCase):
    def test_responses_wire_loop(self):
        requests = []

        class Provider(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(body)
                turn = len(requests)
                if turn == 1:
                    name, arguments = "ops__get_incident", {"incident_id": "INC-101"}
                elif turn == 2:
                    evidence = next(
                        json.loads(item["output"])["data"]
                        for item in body["input"]
                        if item.get("type") == "function_call_output"
                        and item["call_id"] == "call-1"
                    )
                    name, arguments = (
                        "waypilot__triage",
                        {"service": evidence["service"], "symptom": evidence["symptom"]},
                    )
                elif turn == 3:
                    evidence = json.loads(body["input"][-1]["output"])["data"]["outputs"][
                        "assessment"
                    ]["structuredContent"]
                    name, arguments = (
                        "ops__append_note",
                        {"incident_id": "INC-101", "summary": evidence["recommended_action"]},
                    )
                else:
                    name = None
                output = (
                    [
                        {
                            "type": "function_call",
                            "name": name,
                            "arguments": json.dumps(arguments),
                            "call_id": f"call-{turn}",
                            "id": f"fc-{turn}",
                        }
                    ]
                    if name
                    else [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": "Recommend restore_pool_limit from current evidence.",
                                }
                            ],
                        }
                    ]
                )
                if turn == 1:
                    output.insert(
                        0,
                        {
                            "type": "reasoning",
                            "id": "reasoning-1",
                            "summary": [],
                            "encrypted_content": "owned-opaque-reasoning",
                        },
                    )
                    output.append(
                        {
                            "type": "function_call",
                            "name": "knowledge__search_runbooks",
                            "arguments": json.dumps({"symptom": "errors"}),
                            "call_id": "call-1b",
                            "id": "fc-1b",
                        }
                    )
                encoded = json.dumps(
                    {
                        "id": f"resp-{turn}",
                        "status": "completed",
                        "output": output,
                        "usage": {"input_tokens": 10, "output_tokens": 5},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, *_args):
                pass

        provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Provider)
        thread = threading.Thread(target=provider.serve_forever, daemon=True)
        thread.start()
        try:
            with (
                Sandbox(BINARY) as sandbox,
                patch.dict(os.environ, {"AGENT_TEST_MODEL_KEY": "owned-fake-provider-key"}),
            ):
                planner = Responses(
                    "owned-fake-model",
                    "Investigate INC-101",
                    "investigator",
                    endpoint=f"http://127.0.0.1:{provider.server_port}/v1/responses",
                    key_env="AGENT_TEST_MODEL_KEY",
                )

                async def run():
                    async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                        return await run_agent(client, planner, role="investigator")

                report = asyncio.run(run())
                self.assertEqual(report["planner"], "model")
                self.assertEqual(report["usage"], {"input_tokens": 40, "output_tokens": 20})
                self.assertEqual(len(sandbox.peer_stats()["notes"]), 1)
                self.assertEqual(requests[-1]["input"][-1]["call_id"], "call-3")
                self.assertTrue(all(request["store"] is False for request in requests))
                self.assertNotIn("state_version", json.dumps(requests[0]["tools"]))
                self.assertNotIn("owned-fake-provider-key", json.dumps(report))
                self.assertTrue(
                    any(
                        item.get("encrypted_content") == "owned-opaque-reasoning"
                        for item in requests[1]["input"]
                    )
                )
                self.assertEqual(
                    {
                        item["call_id"]
                        for item in requests[1]["input"]
                        if item.get("type") == "function_call_output"
                    },
                    {"call-1", "call-1b"},
                )
        finally:
            provider.shutdown()
            thread.join(timeout=5)
            provider.server_close()


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]], verbosity=2)
