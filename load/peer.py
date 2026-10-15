"""Asynchronous owned MCP peer. Failure injection is restricted to loopback tests."""

import asyncio
import json
import signal
import time
import uuid
from collections import Counter, deque


async def main():
    sessions, gates = {}, {}
    initialize_gate = asyncio.Event()
    initialize_gate.set()
    calls, active, effects = Counter(), Counter(), Counter()
    trace_names = Counter()
    trace_timings = 0
    audit = deque(maxlen=20000)
    totals = {"calls": 0, "active": 0, "peak": 0, "cancellations": 0, "deletions": 0}
    connections = set()
    tools = [
        {
            "name": name,
            "inputSchema": {
                "type": "object",
                "properties": {
                    "nonce": {"type": "string"},
                    "owner": {"type": "string"},
                },
                "required": ["nonce", "owner"],
            },
        }
        for name in ["echo", "hold", "cache_hold", "effect", "session"]
    ]

    async def dispatch(path, request, headers, method):
        nonlocal trace_timings
        if path == "/v1/traces":
            for resource_spans in request.get("resourceSpans", []):
                for scope in resource_spans.get("scopeSpans", []):
                    for span in scope.get("spans", []):
                        assert span.get("traceId") and span.get("spanId")
                        assert int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"])
                        trace_names[span["name"]] += 1
                        if span["name"] == "workflow.node":
                            fields = {attr["key"] for attr in span.get("attributes", [])}
                            if {
                                "node.dependency_wait_ns",
                                "node.scheduler_wait_ns",
                                "node.execution_ns",
                            } <= fields:
                                trace_timings += 1
            return 200, {}, {}
        if path == "/idle":
            return 200, {}, {"ok": True}
        if path == "/stats":
            return (
                200,
                {},
                dict(
                    totals,
                    trace_names=dict(trace_names),
                    trace_timings=trace_timings,
                    sessions=len(sessions),
                    active_by_phase=dict(active),
                    by_tool=dict(calls),
                    duplicate_effects=sum(value - 1 for value in effects.values()),
                ),
            )
        if path == "/audit":
            return 200, {}, list(audit)
        if path == "/control":
            if "pause_initialize" in request:
                initialize_gate.clear() if request["pause_initialize"] else initialize_gate.set()
            if "release" in request:
                gates.setdefault(request["release"], asyncio.Event()).set()
            return 200, {}, {"ok": True}
        assert path == "/mcp"
        session = headers.get("mcp-session-id")
        if method == "DELETE":
            for token in sessions.pop(session, {}).values():
                token.set()
            totals["deletions"] += 1
            return 204, {}, None
        if request.get("method") == "initialize":
            session = uuid.uuid4().hex
            sessions[session] = {}
            await initialize_gate.wait()
            return (
                200,
                {"MCP-Session-Id": session},
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "owned-async-peer", "version": "1"},
                    },
                },
            )
        if session not in sessions:
            return 404, {}, None
        if headers.get("mcp-protocol-version") != "2025-11-25":
            return 400, {}, None
        if "id" not in request:
            if request.get("method") == "notifications/cancelled":
                totals["cancellations"] += 1
                token = sessions[session].get(str(request["params"]["requestId"]))
                if token:
                    token.set()
            return 202, {}, None
        reply = {"jsonrpc": "2.0", "id": request["id"]}
        if request["method"] == "tools/list":
            return 200, {}, dict(reply, result={"tools": tools})
        assert request["method"] == "tools/call"
        args = request["params"]["arguments"]
        tool, phase = request["params"]["name"], args.get("phase", "default")
        token = asyncio.Event()
        sessions[session][str(request["id"])] = token
        calls[tool] += 1
        totals["calls"] += 1
        totals["active"] += 1
        totals["peak"] = max(totals["peak"], totals["active"])
        active[phase] += 1
        audit.append(
            {
                "nonce": args["nonce"],
                "owner": args["owner"],
                "tool": tool,
                "at": time.monotonic(),
            }
        )
        try:
            if tool == "effect":
                effects[args["nonce"]] += 1
                if args.get("drop"):
                    return None
            if tool in ["hold", "cache_hold"]:
                done = asyncio.create_task(gates.setdefault(phase, asyncio.Event()).wait())
            else:
                done = asyncio.create_task(asyncio.sleep(min(args.get("delay_ms", 0), 2000) / 1000))
            canceled = asyncio.create_task(token.wait())
            try:
                await asyncio.wait([done, canceled], return_when=asyncio.FIRST_COMPLETED)
                if token.is_set():
                    return (
                        200,
                        {},
                        dict(
                            reply,
                            error={"code": -32800, "message": "owned peer canceled"},
                        ),
                    )
            finally:
                done.cancel()
                canceled.cancel()
                await asyncio.gather(done, canceled, return_exceptions=True)
            result = {"nonce": args["nonce"], "owner": args["owner"]}
            if "value" in args:
                result["value"] = args["value"]
            if "left" in args or "right" in args:
                assert args["left"] == "left" and args["right"] == "right"
                result["joined"] = True
            if tool == "session":
                result["session"] = session
            return (
                200,
                {},
                dict(
                    reply,
                    result={
                        "content": [{"type": "text", "text": json.dumps(result)}],
                        "structuredContent": result,
                        "isError": False,
                    },
                ),
            )
        finally:
            totals["active"] -= 1
            active[phase] -= 1
            sessions.get(session, {}).pop(str(request["id"]), None)

    async def connection(reader, writer):
        connections.add(writer)
        try:
            while True:
                head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 120)
                lines = head.decode("latin1").split("\r\n")
                method, path, _ = lines[0].split()
                headers = dict(
                    (name.lower(), value)
                    for line in lines[1:]
                    if ": " in line
                    for name, value in [line.split(": ", 1)]
                )
                length = int(headers.get("content-length", "0"))
                assert 0 <= length <= (8 * 1024 * 1024 if path == "/v1/traces" else 1024 * 1024)
                body = await reader.readexactly(length)
                response = await dispatch(path, json.loads(body) if body else {}, headers, method)
                if response is None:
                    break
                status, headers, value = response
                data = b"" if value is None else json.dumps(value, separators=(",", ":")).encode()
                fields = {
                    "Content-Type": "application/json",
                    "Content-Length": str(len(data)),
                    **headers,
                }
                writer.write(
                    (
                        f"HTTP/1.1 {status} Response\r\n"
                        + "".join(f"{k}: {v}\r\n" for k, v in fields.items())
                        + "\r\n"
                    ).encode()
                    + data
                )
                await writer.drain()
                if path == "/idle":
                    await asyncio.sleep(0.05)
                    break
        except (asyncio.IncompleteReadError, ConnectionError, TimeoutError):
            pass
        finally:
            connections.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    server = await asyncio.start_server(connection, "127.0.0.1", 0, limit=65536, backlog=2048)
    print(
        json.dumps({"listen": f"127.0.0.1:{server.sockets[0].getsockname()[1]}"}),
        flush=True,
    )
    stop = asyncio.Event()
    for value in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(value, stop.set)
    async with server:
        await stop.wait()
    for writer in connections.copy():
        writer.close()


if __name__ == "__main__":
    asyncio.run(main())
