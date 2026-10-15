"""Owned, local MCP Streamable HTTP fixture. Supports JSON and SSE replies.

The counter intentionally claims readOnlyHint=true: the gateway must trust its
operator contract instead of inferring reusable behavior from this annotation.
"""
import argparse
import faulthandler
import hashlib
import http.server
import json
import socket
import socketserver
import threading
import time
import uuid
from state import State, TOOLS as STATE_TOOLS


TOOLS = [
    {"name": "echo", "description": "Return supplied immutable text", "inputSchema": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}, "annotations": {"readOnlyHint": True}},
    {"name": "counter", "description": "Increment server state; never reusable", "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": True}},
    {"name": "slow", "description": "Wait before returning", "inputSchema": {"type": "object", "properties": {"ms": {"type": "integer"}}, "required": ["ms"]}},
    {"name": "lost_reply", "description": "Apply an effect then drop the connection", "inputSchema": {"type": "object"}},
    {"name": "error", "description": "Return a tool execution error", "inputSchema": {"type": "object"}},
] + [
    {"name": name, "inputSchema": {"type": "object", "properties": {"value": {}}, "required": ["value"]}}
    for name in ["parse_manifest", "dependency_index", "parse_source", "source_index"]
]


def main():
    faulthandler.dump_traceback_later(10)
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--response", choices=["json", "sse"], default="json")
    parser.add_argument("--call-ms", type=int, default=0)
    args = parser.parse_args()
    state = State()
    lock = threading.Lock()
    sessions = set()
    counts = {"counter": 0, "calls": 0, "initialize": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def send_json(self, status, value, headers=None):
            encoded = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            if self.path == "/stats":
                with lock:
                    self.send_json(200, dict(counts))
            else:
                self.send_error(405)

        def do_DELETE(self):
            with lock:
                sessions.discard(self.headers.get("MCP-Session-Id"))
            self.send_response(204)
            self.end_headers()

        def do_POST(self):
            if self.path == "/state":
                length = int(self.headers.get("Content-Length", "0"))
                return self.send_json(200, state.control(json.loads(self.rfile.read(length))))
            if self.path != "/mcp":
                return self.send_error(404)
            length = int(self.headers.get("Content-Length", "0"))
            if length > 1024 * 1024:
                return self.send_error(413)
            request = json.loads(self.rfile.read(length))
            method = request.get("method")
            if method == "initialize":
                session = uuid.uuid4().hex
                with lock:
                    sessions.add(session)
                    counts["initialize"] += 1
                return self.send_json(200, {"jsonrpc": "2.0", "id": request["id"], "result": {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}}, "serverInfo": {"name": "waypilot-http-fixture", "version": "1"}}}, {"MCP-Session-Id": session})
            with lock:
                valid = self.headers.get("MCP-Session-Id") in sessions
            if not valid:
                return self.send_error(404)
            if self.headers.get("MCP-Protocol-Version") != "2025-11-25":
                return self.send_error(400)
            if "id" not in request:
                self.send_response(202)
                self.end_headers()
                return
            reply = {"jsonrpc": "2.0", "id": request["id"]}
            if method == "tools/list":
                reply["result"] = {"tools": TOOLS + STATE_TOOLS}
            elif method == "tools/call":
                params = request.get("params", {})
                name = params.get("name")
                arguments = params.get("arguments", {})
                with lock:
                    counts["calls"] += 1
                time.sleep(args.call_ms / 1000)
                if name in ["state_read", "state_version"]:
                    reply["result"] = state.call(name, arguments)
                    return self.send_json(200, reply) if args.response == "json" else self.send_sse(reply)
                if name == "echo" and isinstance(arguments.get("value"), str):
                    result = {"value": arguments["value"]}
                elif name == "counter":
                    with lock:
                        counts["counter"] += 1
                        result = {"count": counts["counter"]}
                elif name == "slow":
                    time.sleep(min(max(arguments.get("ms", 0), 0), 2000) / 1000)
                    result = {"done": True}
                elif name == "lost_reply":
                    with lock:
                        counts["counter"] += 1
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                elif name == "error":
                    result = {"error": "owned fixture error"}
                elif name in ["parse_manifest", "parse_source"]:
                    result = {"text": arguments["value"], "words": str(arguments["value"]).split()}
                elif name in ["dependency_index", "source_index"]:
                    result = {"fingerprint": hashlib.sha256(json.dumps(arguments["value"], sort_keys=True).encode()).hexdigest()}
                else:
                    reply["error"] = {"code": -32602, "message": "invalid tool or arguments"}
                    result = None
                if result is not None:
                    reply["result"] = {"content": [{"type": "text", "text": json.dumps(result)}], "structuredContent": result, "isError": name == "error"}
            elif method == "ping":
                reply["result"] = {}
            else:
                reply["error"] = {"code": -32601, "message": "unsupported method"}
            if args.response == "json":
                return self.send_json(200, reply)
            return self.send_sse(reply)

        def send_sse(self, reply):
            # Split UTF-8 and event framing across writes to exercise real streaming.
            body = (": heartbeat\r\n\r\ndata: " + json.dumps(reply) + "\r\n\r\n").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            for index in range(0, len(body), 17):
                self.wfile.write(body[index:index + 17])
                self.wfile.flush()

        def log_message(self, *_args):
            pass

    class LoopbackServer(http.server.ThreadingHTTPServer):
        def server_bind(self):
            # HTTPServer normally reverse-resolves the bound address. An owned
            # loopback fixture needs no DNS and must start independently of it.
            socketserver.TCPServer.server_bind(self)
            self.server_name = "localhost"
            self.server_port = self.server_address[1]

    server = LoopbackServer(("127.0.0.1", args.port), Handler)
    print(json.dumps({"listen": f"127.0.0.1:{server.server_port}"}), flush=True)
    faulthandler.cancel_dump_traceback_later()
    server.serve_forever()


if __name__ == "__main__":
    main()
