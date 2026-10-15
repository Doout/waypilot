"""Live HTTP/stdio gateway regression checks with owned MCP peers."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import time
import subprocess
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from mcp_demo import Client, Demo, http, run


def raw(demo, data, headers=None):
    request = urllib.request.Request(demo.base + "/mcp", data=data, headers={"Authorization": "Bearer " + demo.token, "Content-Type": "application/json", "Accept": "application/json, text/event-stream", **(headers or {})})
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.read()


def wait_for_call(demo, before):
    deadline = time.monotonic() + 5
    while demo.stats()["upstream_calls"] == before and time.monotonic() < deadline:
        time.sleep(0.005)
    assert demo.stats()["upstream_calls"] == before + 1


def main():
    binary = sys.argv[1]
    python = os.environ["WAYPILOT_PYTHON"]
    checks = []
    result = run(binary, python)
    assert [case["upstream_calls"] for case in result["workflow"]] == [4, 0, 2]
    checks.extend(["stdio_branches", "parallel_join", "full_reuse", "partial_recompute", "prewarm_without_calls", "mutable_tool_not_reused"])
    for response in ["json", "sse"]:
        with Demo(binary, python, response=response) as demo:
            client = Client(demo.base, demo.token)
            before = demo.stats()["upstream_calls"]
            first = client.call("owned__echo", {"value": "wire-" + response})
            second = client.call("owned__echo", {"value": "wire-" + response})
            assert first["structuredContent"] == second["structuredContent"]
            assert second["_meta"]["io.waypilot/runtime"]["disposition"] == "reused"
            assert demo.stats()["upstream_calls"] - before == 1
            checks.append(response + "_upstream")
            warm_before, calls_before = demo.stats()["prewarms"], demo.stats()["upstream_calls"]
            predicted = Client(demo.base, demo.token)
            deadline = time.monotonic() + 5
            while demo.stats()["prewarms"] == warm_before and time.monotonic() < deadline:
                time.sleep(0.005)
            assert demo.stats()["prewarms"] == warm_before + 1
            assert demo.stats()["upstream_calls"] == calls_before
            predicted.close()
            checks.append(response + "_learned_prewarm")
            assert client.request("ping", request_id=0)["id"] == 0
            before = demo.stats()["upstream_calls"]
            for payload in [b'{"jsonrpc":"2.0","id":1,"method":"ping","method":"tools/call"}', b'[]', b'{"jsonrpc":"2.0","id":null,"method":"ping"}', b'{"jsonrpc":"2.0","id":1,"method":"ping","params":[]}']:
                assert raw(demo, payload, client.headers)[0] == 400
            assert raw(demo, b'x' * (1024 * 1024 + 1), client.headers)[0] == 413
            assert raw(demo, b'{"jsonrpc":"2.0","id":1,"method":"ping"}', {**client.headers, "MCP-Protocol-Version": "wrong"})[0] == 400
            assert demo.stats()["upstream_calls"] == before
            checks.append(response + "_protocol_limits")
            before = demo.stats()["upstream_calls"]
            for _ in range(2):
                assert client.call("owned__error", {})["isError"] is True
            assert demo.stats()["upstream_calls"] == before + 2, "execution error was retained"
            _, _, fixture_before = http(demo.upstream + "/stats", "fixture")
            lost = client.request("tools/call", {"name": "owned__lost_reply", "arguments": {}})
            assert lost["error"]["code"] == -32002
            _, _, fixture_after = http(demo.upstream + "/stats", "fixture")
            assert fixture_after["calls"] == fixture_before["calls"] + 1
            assert fixture_after["counter"] == fixture_before["counter"] + 1
            client.call("owned__echo", {"value": "after-lost-reply"})
            checks.extend([response + "_errors_not_reused", response + "_uncertain_effect_not_replayed"])
            status, _, _ = http(demo.base + "/mcp", "invalid-token", {"jsonrpc": "2.0", "id": 1, "method": "initialize"})
            assert status == 401
            status, _, _ = http(demo.base + "/mcp", demo.token, {"jsonrpc": "2.0", "id": 1, "method": "initialize"}, extra={"Origin": "https://untrusted.example"})
            assert status == 403
            status, _, _ = http(demo.base + "/admin/stats", demo.token)
            assert status == 401
            bob = Client(demo.base, demo.bob)
            listed = bob.request("tools/list")["result"]["tools"]
            assert [tool["name"] for tool in listed] == ["owned__echo"]
            before = demo.stats()["upstream_calls"]
            denied = bob.request("tools/call", {"name": "owned__counter", "arguments": {}})
            assert denied["error"]["code"] == -32003
            assert demo.stats()["upstream_calls"] == before
            bob.call("owned__echo", {"value": "wire-" + response})
            assert demo.stats()["upstream_calls"] == before + 1, "cross-identity cache hit"
            status, _, _ = http(demo.base + "/mcp", demo.bob, {"jsonrpc": "2.0", "id": 900, "method": "ping"}, extra=client.headers)
            assert status == 404, "session was not bound to its authenticated user"
            limited = Client(demo.base, demo.limited)
            for index in range(2):
                limited.call("owned__echo", {"value": str(index)})
            limited_reply = limited.request("tools/call", {"name": "owned__echo", "arguments": {"value": "third"}})
            assert limited_reply["error"]["code"] == -32029, limited_reply
            before = demo.stats()["upstream_calls"]
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(client.call, "owned__echo", {"value": "shared-" + response}, 1000 + index) for index in range(2)]
                values = [future.result()["structuredContent"] for future in futures]
            assert values[0] == values[1]
            assert demo.stats()["upstream_calls"] == before + 1
            status, _, _ = http(demo.base + "/admin/invalidate", demo.admin, {}, "POST")
            assert status == 200
            before = demo.stats()["upstream_calls"]
            client.call("owned__echo", {"value": "wire-" + response})
            assert demo.stats()["upstream_calls"] == before + 1
            before = demo.stats()["upstream_calls"]
            invalid = client.request("tools/call", {"name": "waypilot__inspect", "arguments": {"manifest": "missing source"}})
            assert "error" in invalid
            assert demo.stats()["upstream_calls"] == before
            checks.extend([response + "_auth", response + "_origin", response + "_tool_grants", response + "_identity_reuse", response + "_session_binding", response + "_rate", response + "_singleflight", response + "_invalidation", response + "_input_preflight"])
            # Cancel a request after its real upstream invocation has started.
            before = demo.stats()["upstream_calls"]
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(client.request, "tools/call", {"name": "owned__slow", "arguments": {"ms": 700}}, 9876)
                wait_for_call(demo, before)
                status, _, _ = http(demo.base + "/mcp", demo.token, {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 9876, "reason": "test cancellation"}}, extra=client.headers)
                assert status == 202
                assert pending.result()["error"]["code"] == -32800
            client.call("owned__echo", {"value": "after-cancellation"})
            # A fresh distinct invocation works; the canceled call was not replayed.
            assert demo.stats()["upstream_calls"] == before + 2
            checks.append(response + "_cancellation_without_replay")
            # Both an issued call and a same-server queued effect lose access.
            before = demo.stats()["upstream_calls"]
            with ThreadPoolExecutor(max_workers=2) as pool:
                issued = pool.submit(client.request, "tools/call", {"name": "owned__slow", "arguments": {"ms": 700}}, 9877)
                wait_for_call(demo, before)
                requests = demo.stats()["requests"]
                queued = pool.submit(client.request, "tools/call", {"name": "owned__counter", "arguments": {}}, 9878)
                deadline = time.monotonic() + 5
                while demo.stats()["requests"] < requests + 1 and not queued.done() and time.monotonic() < deadline:
                    time.sleep(0.005)
                assert demo.stats()["requests"] == requests + 1 and not queued.done()
                command = [str(Path(binary).resolve()), "admin", "--url", demo.base, "--token-env", "WAYPILOT_ADMIN_TOKEN"]
                users = json.loads(subprocess.run(command + ["--action", "users"], env=demo.env, capture_output=True, text=True, check=True, timeout=10).stdout)
                assert {user["id"] for user in users["users"]} == {"developer", "bob", "limited"}
                disabled = json.loads(subprocess.run(command + ["--action", "disable", "--user", "developer"], env=demo.env, capture_output=True, text=True, check=True, timeout=10).stdout)
                assert disabled["enabled"] is False and disabled["persistent"] is False
                assert "error" in issued.result()
                assert "error" in queued.result()
            assert demo.stats()["upstream_calls"] == before + 1, "queued effect was dispatched after revocation"
            checks.extend([response + "_admin_cli", response + "_queued_revocation"])
            status, _, _ = http(demo.base + "/mcp", demo.token, {"jsonrpc": "2.0", "id": 9999, "method": "tools/call", "params": {"name": "owned__echo", "arguments": {"value": "wire-" + response}}}, extra=client.headers)
            assert status == 401, "revoked caller received cached data"
            checks.append(response + "_revocation")
            bob.close()
            limited.close()
    with Demo(binary, python, request_timeout_ms=250) as demo:
        client = Client(demo.base, demo.token)
        before = demo.stats()["upstream_calls"]
        started = time.monotonic()
        status, _, reply = http(demo.base + "/mcp", demo.token, {"jsonrpc": "2.0", "id": 555, "method": "tools/call", "params": {"name": "owned__slow", "arguments": {"ms": 1500}}}, extra=client.headers)
        assert status in [200, 504] and "error" in reply
        assert time.monotonic() - started < 1.2
        assert demo.stats()["upstream_calls"] == before + 1
        client.call("owned__echo", {"value": "after-deadline"})
        assert demo.stats()["upstream_calls"] == before + 2
        client.close()
        checks.append("deadline_without_replay")
    print(json.dumps({"gateway_regressions": "passed", "checks": checks, "count": len(checks)}))


if __name__ == "__main__":
    main()
