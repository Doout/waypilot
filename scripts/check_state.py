"""Real MCP state validation, branch invalidation and control regressions."""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from state_support import Client, StateDemo, http


def steps(result):
    return {row["id"]: row["disposition"] for row in result["structuredContent"]["steps"]}


def wait_check(demo, before):
    until = time.monotonic() + 3
    while demo.mutate()["a"]["checks"] == before and time.monotonic() < until:
        time.sleep(0.005)
    assert demo.mutate()["a"]["checks"] > before


def main():
    binary = sys.argv[1]
    checks = []
    for transport in ["json", "sse", "stdio"]:
        with StateDemo(binary, transport) as demo:
            client = Client(demo.base, demo.token)
            first = client.call("waypilot__state", {})
            assert set(steps(first).values()) == {"executed"}
            assert set(steps(client.call("waypilot__state", {})).values()) == {"reused"}
            demo.mutate(
                key="a", touch=True
            )  # Same value, new version: downstream arguments are identical.
            changed = client.call("waypilot__state", {})
            assert steps(changed) == {
                "left": "executed",
                "left-index": "executed",
                "right": "reused",
                "right-index": "reused",
            }, steps(changed)
            assert changed["structuredContent"]["outputs"] == first["structuredContent"]["outputs"]
            demo.mutate(key="a", value="updated")
            newer = client.call("waypilot__state", {})
            assert (
                newer["structuredContent"]["outputs"]["left-index"]
                != first["structuredContent"]["outputs"]["left-index"]
            )
            assert (
                newer["structuredContent"]["outputs"]["right-index"]
                == first["structuredContent"]["outputs"]["right-index"]
            )
            restricted = Client(demo.base, demo.restricted)
            assert restricted.request("tools/list")["result"]["tools"] == []
            before = demo.stats()["upstream_calls"]
            reply = restricted.request(
                "tools/call", {"name": "owned__state_read", "arguments": {"key": "a"}}
            )
            assert reply["error"]["code"] == -32003
            assert demo.stats()["upstream_calls"] == before
            checks.extend(
                [
                    transport + "_version_only_branch_invalidation",
                    transport + "_external_update",
                    transport + "_validator_grant",
                ]
            )
            if transport != "stdio":
                for target in ["read_mode", "check_mode"]:
                    for mode in ["missing", "malformed", "error", "oversized"]:
                        demo.invalidate()
                        demo.mutate(**{target: mode})
                        reply = client.request(
                            "tools/call", {"name": "owned__state_read", "arguments": {"key": "a"}}
                        )
                        assert "error" in reply, reply
                        demo.mutate(**{target: "valid"})
                checks.append(transport + "_invalid_witnesses")
                demo.invalidate()
                demo.mutate(read_mode="wrong_resource")
                assert "error" in client.request(
                    "tools/call", {"name": "owned__state_read", "arguments": {"key": "a"}}
                )
                demo.mutate(read_mode="valid")
                demo.invalidate()
                demo.mutate(change_after_read=True)
                reply = client.request("tools/call", {"name": "waypilot__state", "arguments": {}})
                assert reply["error"]["code"] == -32010, reply
                demo.mutate(change_after_read=False)
                recovered = client.call("waypilot__state", {})
                assert steps(recovered)["left"] == "executed"
                checks.append(transport + "_change_before_release")
                demo.invalidate()
                demo.mutate(check_ms=30)
                timed = client.call("owned__state_read", {"key": "a"})
                assert timed["_meta"]["io.waypilot/runtime"]["elapsed_us"] >= 50000
                demo.mutate(check_ms=0)
                checks.append(transport + "_elapsed_includes_release_validation")
                demo.invalidate()
                demo.mutate(check_ms=500)
                before = demo.mutate()["a"]["checks"]
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(
                        client.request,
                        "tools/call",
                        {"name": "owned__state_read", "arguments": {"key": "a"}},
                        9001,
                    )
                    wait_check(demo, before)
                    assert (
                        http(
                            demo.base + "/mcp",
                            demo.token,
                            {
                                "jsonrpc": "2.0",
                                "method": "notifications/cancelled",
                                "params": {"requestId": 9001},
                            },
                            extra=client.headers,
                        )[0]
                        == 202
                    )
                    assert pending.result()["error"]["code"] == -32800
                demo.mutate(check_ms=0)
                checks.append(transport + "_cancel_validation")
            restricted.close()
            client.close()
    with StateDemo(binary, timeout=250) as demo:
        client = Client(demo.base, demo.token)
        demo.mutate(check_ms=700)
        started = time.monotonic()
        reply = client.request(
            "tools/call", {"name": "owned__state_read", "arguments": {"key": "a"}}
        )
        assert "error" in reply and time.monotonic() - started < 1
        assert demo.mutate()["a"]["reads"] == 0
        checks.append("original_deadline_includes_validation")
        client.close()
    with StateDemo(binary) as demo:
        client = Client(demo.base, demo.token)
        demo.mutate(check_ms=500)
        before = demo.mutate()["a"]["checks"]
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                client.request,
                "tools/call",
                {"name": "owned__state_read", "arguments": {"key": "a"}},
            )
            wait_check(demo, before)
            assert (
                http(
                    demo.base + "/admin/users/update",
                    demo.admin,
                    {"id": "reader", "enabled": False},
                )[0]
                == 200
            )
            assert "error" in pending.result()
        assert demo.mutate()["a"]["reads"] == 0
        checks.append("revocation_during_validation")
    with StateDemo(binary, rate=(1, 2)) as demo:
        client = Client(demo.base, demo.token)
        reply = client.request(
            "tools/call", {"name": "owned__state_read", "arguments": {"key": "a"}}
        )
        assert reply["error"]["code"] == -32029, reply
        assert demo.mutate()["a"]["reads"] == 1
        checks.append("validation_consumes_rate_budget")
        client.close()
    print(json.dumps({"state_regressions": "passed", "count": len(checks), "checks": checks}))


if __name__ == "__main__":
    main()
