"""Live batch-state validation and session-bound cross-call observation regressions."""

import asyncio
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.runner import Policy, run_agent
from agents.sandbox import Sandbox, connect
from mcp_demo import ROOT, Client, Process, http

BINARY = str(Path(sys.argv[1]).resolve())
META = "io.waypilot/observations"


def fixture(command, environment):
    if len(command) > 1 and command[1].endswith("agents/workbench.py"):
        command = [command[0], str(ROOT / "fixtures/agent_validation.py"), *command[2:]]
    return Process(command, environment)


def call(client, name="waypilot__triage", arguments=None, *, capture=False, require=()):
    return client.request(
        "tools/call",
        {
            "name": name,
            "arguments": arguments or {"service": "checkout", "symptom": "errors"},
            "_meta": {META: {"capture": capture, "require": list(require)}},
        },
    )


def receipt(reply):
    return reply["result"]["_meta"][META]["receipt"]


NOTE = {"incident_id": "INC-101", "summary": "test guarded note"}


class ValidationTests(unittest.TestCase):
    def test_batch_saves_rpcs_without_skipping_observation_checks(self):
        counts = []
        for batch in [False, True]:
            with Sandbox(BINARY, batch_validation=batch) as sandbox:
                client = Client(sandbox.base, sandbox.tokens["investigator"])
                try:
                    self.assertIn("result", call(client))
                    self.assertIn("result", call(client))
                    counts.append((sandbox.peer_stats()["calls"], sandbox.stats()["state_checks"]))
                    sandbox.mutate(touch=True)
                    reply = call(client)["result"]["structuredContent"]
                    self.assertEqual(
                        {row["id"]: row["disposition"] for row in reply["steps"]},
                        {
                            "metrics": "executed",
                            "deployment": "reused",
                            "runbooks": "reused",
                            "assessment": "executed",
                        },
                    )
                finally:
                    client.close()
        self.assertEqual(counts[0][1], counts[1][1])
        self.assertGreater(counts[1][0]["state_batch"], 0)
        self.assertLess(sum(counts[1][0].values()), sum(counts[0][0].values()))

    def test_batch_requires_grants_and_charges_each_observation(self):
        for batch in [False, True]:
            for burst, expected in [(12, -32029), (13, None)]:

                def limited(command, environment):
                    if "serve" in command:
                        config = Path(command[-1])
                        config.write_text(
                            config.read_text()
                            .replace("requests_per_second = 10000", "requests_per_second = 1")
                            .replace("burst = 10000", f"burst = {burst}")
                        )
                    return Process(command, environment)

                with (
                    self.subTest(batch=batch, burst=burst),
                    patch("agents.sandbox.Process", side_effect=limited),
                    Sandbox(BINARY, batch_validation=batch, delay_ms=0) as sandbox,
                ):
                    client = Client(sandbox.base, sandbox.tokens["investigator"])
                    try:
                        reply = call(client)
                        if expected is None:
                            self.assertIn("result", reply)
                        else:
                            self.assertEqual(reply["error"]["code"], expected)
                    finally:
                        client.close()

        def no_batch_grant(command, environment):
            if "serve" in command:
                config = Path(command[-1])
                config.write_text(
                    config.read_text()
                    .replace(', "ops__state_batch"', "")
                    .replace(', "knowledge__state_batch"', "")
                )
            return Process(command, environment)

        with (
            patch("agents.sandbox.Process", side_effect=no_batch_grant),
            Sandbox(BINARY, batch_validation=True) as sandbox,
        ):
            client = Client(sandbox.base, sandbox.tokens["investigator"])
            try:
                names = {row["name"] for row in client.request("tools/list")["result"]["tools"]}
                self.assertNotIn("ops__get_metrics", names)
                self.assertEqual(
                    call(client, "ops__get_metrics", {"service": "checkout"})["error"]["code"],
                    -32003,
                )
                self.assertEqual(sandbox.peer_stats()["calls"], {})
            finally:
                client.close()

    def test_malformed_observation_request_never_dispatches_effect(self):
        with Sandbox(BINARY, batch_validation=True) as sandbox:
            client = Client(sandbox.base, sandbox.tokens["investigator"])
            try:
                for meta in [
                    None,
                    [],
                    "invalid",
                    {META: None},
                    {META: {"require": ["short"]}},
                    {META: {"capture": 1}},
                    {META: {"extra": True}},
                ]:
                    reply = client.request(
                        "tools/call", {"name": "ops__append_note", "arguments": NOTE, "_meta": meta}
                    )
                    self.assertEqual(reply["error"]["code"], -32602)
                self.assertEqual(sandbox.peer_stats()["calls"], {})
            finally:
                client.close()

    def test_changed_graph_observation_blocks_later_effect_even_with_same_value(self):
        for batch in [False, True]:
            for change in [{"touch": True}, {"recovered": True}]:
                with (
                    self.subTest(batch=batch, change=change),
                    Sandbox(BINARY, batch_validation=batch) as sandbox,
                ):
                    client = Client(sandbox.base, sandbox.tokens["investigator"])
                    try:
                        token = receipt(call(client, capture=True))
                        sandbox.mutate(**change)
                        self.assertEqual(
                            call(client, "ops__append_note", NOTE, require=[token])["error"][
                                "code"
                            ],
                            -32010,
                        )
                        self.assertEqual(sandbox.peer_stats()["note_count"], 0)
                        self.assertNotIn("append_note", sandbox.peer_stats()["calls"])
                        self.assertGreater(sandbox.stats()["cache"]["invalidated"], 0)
                    finally:
                        client.close()

    def test_unchanged_duplicate_receipts_produce_one_effect(self):
        with Sandbox(BINARY, batch_validation=True) as sandbox:
            client = Client(sandbox.base, sandbox.tokens["investigator"])
            try:
                token = receipt(call(client, capture=True))
                self.assertIn(
                    "result", call(client, "ops__append_note", NOTE, require=[token, token])
                )
                self.assertEqual(sandbox.peer_stats()["note_count"], 1)
                self.assertEqual(sandbox.peer_stats()["calls"]["append_note"], 1)
            finally:
                client.close()

    def test_forged_cross_session_cross_user_and_stale_generation_receipts_fail(self):
        with Sandbox(BINARY, batch_validation=True) as sandbox:
            clients = [
                Client(sandbox.base, sandbox.tokens[role])
                for role in ["investigator", "investigator", "reviewer"]
            ]
            try:
                token = receipt(call(clients[0], capture=True))
                for client, handle in [
                    (clients[0], "0" * 64),
                    (clients[1], token),
                    (clients[2], token),
                ]:
                    self.assertEqual(
                        call(
                            client,
                            "ops__get_incident",
                            {"incident_id": "INC-101"},
                            require=[handle],
                        )["error"]["code"],
                        -32010,
                    )
                self.assertEqual(
                    http(sandbox.base + "/admin/invalidate", sandbox.tokens["admin"], {})[0], 200
                )
                self.assertEqual(
                    call(clients[0], "ops__append_note", NOTE, require=[token])["error"]["code"],
                    -32010,
                )
                self.assertEqual(sandbox.peer_stats()["note_count"], 0)
            finally:
                for client in clients:
                    client.close()

    def test_bad_batch_replies_fail_closed_without_fallback(self):
        with (
            patch("agents.sandbox.Process", side_effect=fixture),
            Sandbox(BINARY, batch_validation=True) as sandbox,
        ):
            client = Client(sandbox.base, sandbox.tokens["investigator"])
            try:
                for mode in ["duplicate", "missing", "changed", "error"]:
                    sandbox.mutate(batch_mode=mode)
                    before = sandbox.peer_stats()["calls"].get("state_batch", 0)
                    self.assertEqual(call(client)["error"]["code"], -32010, mode)
                    self.assertEqual(sandbox.peer_stats()["calls"]["state_batch"], before + 1)
                sandbox.mutate(batch_mode="reorder")
                self.assertIn("result", call(client))
            finally:
                client.close()

    def test_blocked_batch_honors_deadline_revocation_and_cancellation(self):
        from concurrent.futures import ThreadPoolExecutor

        for control in ["deadline", "revoke", "cancel", "mutation"]:
            with (
                self.subTest(control=control),
                patch("agents.sandbox.Process", side_effect=fixture),
                Sandbox(BINARY, batch_validation=True, timeout_ms=2000) as sandbox,
            ):
                client = Client(sandbox.base, sandbox.tokens["investigator"])
                token = receipt(call(client, capture=True))
                sandbox.mutate(block_batch=True)
                try:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pending = pool.submit(
                            client.request,
                            "tools/call",
                            {
                                "name": "ops__append_note",
                                "arguments": NOTE,
                                "_meta": {META: {"require": [token]}},
                            },
                            99,
                        )
                        until = time.monotonic() + 5
                        while (
                            not sandbox.peer_stats()["batch_pending"] and time.monotonic() < until
                        ):
                            time.sleep(0.005)
                        self.assertGreater(sandbox.peer_stats()["batch_pending"], 0)
                        if control == "revoke":
                            http(
                                sandbox.base + "/admin/users/update",
                                sandbox.tokens["admin"],
                                {"id": "investigator", "enabled": False},
                            )
                        elif control == "cancel":
                            self.assertEqual(
                                http(
                                    sandbox.base + "/mcp",
                                    sandbox.tokens["investigator"],
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
                            sandbox.mutate(touch=True)
                            sandbox.mutate(block_batch=False)
                        reply = pending.result(timeout=5)
                        self.assertIn(
                            reply["error"]["code"],
                            {
                                "deadline": [-32008],
                                "revoke": [-32003, -32800],
                                "cancel": [-32800],
                                "mutation": [-32010],
                            }[control],
                        )
                        self.assertNotIn("append_note", sandbox.peer_stats()["calls"])
                finally:
                    sandbox.mutate(block_batch=False)
                    client.close()

    def test_post_effect_conflict_reports_uncertainty_without_replay(self):
        from concurrent.futures import ThreadPoolExecutor

        with (
            patch("agents.sandbox.Process", side_effect=fixture),
            Sandbox(BINARY, batch_validation=True) as sandbox,
        ):
            client = Client(sandbox.base, sandbox.tokens["investigator"])
            token = receipt(call(client, capture=True))
            sandbox.mutate(block_note_reply=True)
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(call, client, "ops__append_note", NOTE, require=[token])
                    until = time.monotonic() + 5
                    while not sandbox.peer_stats()["note_pending"] and time.monotonic() < until:
                        time.sleep(0.005)
                    self.assertGreater(sandbox.peer_stats()["note_pending"], 0)
                    self.assertEqual(sandbox.peer_stats()["note_count"], 1)
                    sandbox.mutate(touch=True, block_note_reply=False)
                    self.assertEqual(pending.result(timeout=5)["error"]["code"], -32010)
                    self.assertEqual(sandbox.peer_stats()["calls"]["append_note"], 1)
                    self.assertEqual(sandbox.peer_stats()["note_count"], 1)
            finally:
                sandbox.mutate(block_note_reply=False)
                client.close()

    def test_global_observation_budget_releases_on_session_close(self):
        def small_budget(command, environment):
            if "serve" in command:
                config = Path(command[-1])
                config.write_text(
                    config.read_text().replace("[limits]", "[limits]\nobservation_bytes = 2048", 1)
                )
            return Process(command, environment)

        with (
            patch("agents.sandbox.Process", side_effect=small_budget),
            Sandbox(BINARY, batch_validation=True) as sandbox,
        ):
            left = Client(sandbox.base, sandbox.tokens["investigator"])
            right = Client(sandbox.base, sandbox.tokens["investigator"])
            try:
                self.assertIn(
                    "result",
                    call(left, "ops__get_incident", {"incident_id": "INC-101"}, capture=True),
                )
                self.assertIn(
                    "result",
                    call(left, "ops__get_incident", {"incident_id": "INC-101"}, capture=True),
                )
                self.assertEqual(
                    call(right, "ops__get_incident", {"incident_id": "INC-101"}, capture=True)[
                        "error"
                    ]["code"],
                    -32029,
                )
                self.assertLessEqual(sandbox.stats()["observation_bytes"], 2048)
                left.close()
                self.assertIn(
                    "result",
                    call(right, "ops__get_incident", {"incident_id": "INC-101"}, capture=True),
                )
            finally:
                right.close()

    def test_independent_server_validations_overlap_before_any_effect(self):
        for parallelism in [1, 4]:
            with (
                self.subTest(parallelism=parallelism),
                patch("agents.sandbox.Process", side_effect=fixture),
                Sandbox(
                    BINARY,
                    batch_validation=True,
                    validation_parallelism=parallelism,
                    timeout_ms=2000,
                ) as sandbox,
            ):
                client = Client(sandbox.base, sandbox.tokens["investigator"])
                token = receipt(call(client, capture=True))
                sandbox.mutate(validation_rendezvous=True)
                try:
                    reply = call(client, "ops__append_note", NOTE, require=[token])
                    if parallelism == 1:
                        self.assertEqual(reply["error"]["code"], -32008)
                        self.assertEqual(sandbox.peer_stats()["note_count"], 0)
                    else:
                        self.assertIn("result", reply)
                        self.assertEqual(
                            sandbox.peer_stats()["validation_arrivals"], ["knowledge", "ops"]
                        )
                        self.assertEqual(sandbox.peer_stats()["note_count"], 1)
                finally:
                    sandbox.mutate(validation_rendezvous=False)
                    client.close()

    def test_parallel_validation_preserves_all_failure_boundaries(self):
        original = Sandbox

        def parallel(*args, **kwargs):
            kwargs["validation_parallelism"] = 4
            return original(*args, **kwargs)

        with patch(__name__ + ".Sandbox", side_effect=parallel):
            self.test_bad_batch_replies_fail_closed_without_fallback()
            self.test_blocked_batch_honors_deadline_revocation_and_cancellation()
            self.test_changed_graph_observation_blocks_later_effect_even_with_same_value()
            self.test_post_effect_conflict_reports_uncertainty_without_replay()
            self.test_batch_requires_grants_and_charges_each_observation()

    def test_compiled_client_schemas_track_schema_changes_and_reject_bad_batches(self):
        import check_agents
        from agents.runner import compiled_input_schema, validate_input
        from check_agents import GuardTests
        from jsonschema import ValidationError

        compiled_input_schema.cache_clear()
        schema = {
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
        }
        validate_input({"value": 1}, schema, True)
        with self.assertRaises(ValidationError):
            validate_input({"value": "invalid"}, schema, True)
        schema["properties"]["value"]["type"] = "string"
        with self.assertRaises(ValidationError):
            validate_input({"value": 1}, schema, True)
        referenced = {"$defs": {"value": {"type": "integer"}}, "$ref": "#/$defs/value"}
        with patch(
            "agents.runner.compiled_input_schema",
            side_effect=AssertionError("reference was cached"),
        ):
            validate_input(1, referenced, True)
            validate_input(1, {"type": "integer", "description": "x" * 17000}, True)
            with self.assertRaises(ValidationError):
                validate_input("wrong", referenced, True)
        original = check_agents.run_agent

        async def cached(*args, **kwargs):
            return await original(*args, **kwargs, compiled_input_schemas=True)

        with patch("check_agents.run_agent", side_effect=cached):
            GuardTests(
                "test_whole_batch_validated_before_dispatch"
            ).test_whole_batch_validated_before_dispatch()

    def test_persistent_clients_rotate_before_receipt_capacity(self):
        from benchmark_agents import measure

        with Sandbox(BINARY, batch_validation=True, validation_parallelism=4) as sandbox:
            result = asyncio.run(
                measure(
                    sandbox,
                    26,
                    1,
                    guard_observations=True,
                    compiled_input_schemas=True,
                    reuse_sessions=True,
                )
            )
            self.assertEqual(result["mcp_sessions_started"], 3)
            self.assertEqual(result["effect_calls"], 26)
            self.assertEqual(sandbox.stats()["sessions"], 0)
            self.assertEqual(sandbox.stats()["observation_bytes"], 0)

    def test_compiled_peer_schemas_preserve_validation_and_effects(self):
        from agents.workbench import Workbench

        for compiled in ["0", "1"]:
            with patch.dict("os.environ", {"WORKBENCH_COMPILED_SCHEMAS": compiled}):
                peer = Workbench(delay_ms=0)
            for tool, args in [
                ("append_note", {}),
                ("append_note", dict(NOTE, extra=True)),
                ("get_metrics", {"service": None}),
                ("state_batch", {"version": 1, "observations": []}),
            ]:
                self.assertTrue(asyncio.run(peer.call(tool, args))["isError"])
            self.assertEqual(peer.notes, [])
            result = asyncio.run(peer.call("append_note", NOTE))
            self.assertFalse(result["isError"])
            self.assertEqual(len(peer.notes), 1)

    def test_guarded_sdk_agents_http_stdio_and_concurrent(self):
        async def run(sandbox, transport="http"):
            async with connect(
                sandbox.url, sandbox.tokens["investigator"], sandbox=sandbox, transport=transport
            ) as client:
                return await run_agent(
                    client,
                    Policy("INC-101", "investigator"),
                    role="investigator",
                    owned_tools=True,
                    guard_observations=True,
                )

        for transport in ["http", "stdio"]:
            with Sandbox(BINARY, batch_validation=True, start_http=transport == "http") as sandbox:
                report = asyncio.run(run(sandbox, transport))
                self.assertIn("restore_pool_limit", report["summary"])
                self.assertEqual(sandbox.peer_stats()["note_count"], 1)
        with Sandbox(BINARY, batch_validation=True) as sandbox:

            async def concurrent():
                return await asyncio.gather(*(run(sandbox) for _ in range(16)))

            reports = asyncio.run(concurrent())
            self.assertEqual(len(reports), 16)
            self.assertEqual(sandbox.peer_stats()["note_count"], 16)

    def test_missing_receipt_support_stops_agent_before_any_write(self):
        with Sandbox(BINARY, reuse=False) as sandbox:

            async def run():
                async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                    await run_agent(
                        client,
                        Policy("INC-101", "investigator"),
                        role="investigator",
                        owned_tools=True,
                        guard_observations=True,
                    )

            try:
                asyncio.run(run())
            except Exception as error:

                def messages(value):
                    return str(value) + " ".join(
                        messages(child) for child in getattr(value, "exceptions", [])
                    )

                self.assertIn("gateway did not retain the required observation", messages(error))
            else:
                self.fail("guarded agent silently fell back without receipts")
            self.assertEqual(sandbox.peer_stats()["note_count"], 0)

    def test_guarded_agent_rejects_inter_call_change_and_fresh_run_recovers(self):
        with Sandbox(BINARY, batch_validation=True) as sandbox:

            class ChangeBeforeWrite(Policy):
                async def decide(self, observations, tools):
                    if len(observations) == 4:
                        await asyncio.to_thread(sandbox.mutate, recovered=True)
                    return await super().decide(observations, tools)

            async def run(planner):
                async with connect(sandbox.url, sandbox.tokens["investigator"]) as client:
                    return await run_agent(
                        client,
                        planner,
                        role="investigator",
                        owned_tools=True,
                        guard_observations=True,
                    )

            def codes(error):
                return ([error.code] if hasattr(error, "code") else []) + [
                    code for child in getattr(error, "exceptions", []) for code in codes(child)
                ]

            try:
                asyncio.run(run(ChangeBeforeWrite("INC-101", "investigator")))
            except Exception as error:
                self.assertIn(-32010, codes(error))
            else:
                self.fail("stale agent wrote its note")
            self.assertEqual(sandbox.peer_stats()["note_count"], 0)
            report = asyncio.run(run(Policy("INC-101", "investigator")))
            self.assertIn("recommend observe", report["summary"])
            self.assertEqual(sandbox.peer_stats()["note_count"], 1)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]], verbosity=2)
