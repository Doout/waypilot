"""One-command real MCP demo. No LLM, GPU, model key or paid API is used."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import select
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def http(url, token, body=None, method=None, extra=None):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    headers.update(extra or {})
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try:
        response = urllib.request.urlopen(request, timeout=70)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read()
        reply = None
        if data:
            if response.headers.get("Content-Type", "").startswith("text/event-stream"):
                for event in data.decode().replace("\r\n", "\n").split("\n\n"):
                    payload = "\n".join(line[5:].lstrip(" ") for line in event.splitlines() if line.startswith("data:"))
                    if payload:
                        candidate = json.loads(payload)
                        if "result" in candidate or "error" in candidate:
                            reply = candidate
            else:
                reply = json.loads(data)
        return response.status, {name.lower(): value for name, value in response.headers.items()}, reply


class Client:
    def __init__(self, base, token):
        self.base, self.token, self.sequence = base, token, 1
        status, headers, reply = http(base + "/mcp", token, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "waypilot-demo", "version": "1"}}})
        assert status == 200 and "result" in reply, (status, reply)
        self.headers = {"MCP-Session-Id": headers["mcp-session-id"], "MCP-Protocol-Version": reply["result"]["protocolVersion"]}
        status, _, _ = http(base + "/mcp", token, {"jsonrpc": "2.0", "method": "notifications/initialized"}, extra=self.headers)
        assert status == 202, status

    def request(self, method, params=None, request_id=None):
        self.sequence += 1
        status, _, reply = http(self.base + "/mcp", self.token, {"jsonrpc": "2.0", "id": self.sequence if request_id is None else request_id, "method": method, "params": params or {}}, extra=self.headers)
        assert status == 200, (status, reply)
        return reply

    def call(self, name, arguments, request_id=None):
        reply = self.request("tools/call", {"name": name, "arguments": arguments}, request_id)
        assert "result" in reply, reply
        return reply["result"]

    def close(self):
        http(self.base + "/mcp", self.token, method="DELETE", extra=self.headers)


class Process:
    def __init__(self, command, environment):
        self.log = tempfile.TemporaryFile(mode="w+")
        self.process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=self.log, text=True)
        if not select.select([self.process.stdout], [], [], 20)[0]:
            self.log.seek(0)
            message = self.log.read()[-4000:]
            self.close()
            raise RuntimeError(f"service did not announce readiness: {message}")
        line = self.process.stdout.readline()
        if not line:
            self.log.seek(0)
            message = self.log.read()[-4000:]
            self.close()
            raise RuntimeError(f"service failed to start: {message}")
        self.ready = json.loads(line)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.process.stdout.close()
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class Demo:
    def __init__(self, binary, python, remote=False, response="sse", reuse=True, request_timeout_ms=None, rate_limit=None):
        self.stack = ExitStack()
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix="waypilot-demo-")))
        self.binary, self.python = str(Path(binary).resolve()), str(Path(python).resolve())
        self.token, self.admin, self.bob, self.limited = [secrets.token_urlsafe(32) for _ in range(4)]
        self.env = dict(os.environ, WAYPILOT_TOKEN=self.token, WAYPILOT_ADMIN_TOKEN=self.admin, WAYPILOT_BOB_TOKEN=self.bob, WAYPILOT_LIMITED_TOKEN=self.limited, WAYPILOT_PYTHON=self.python, RUST_LOG="error")
        try:
            fixture = self.stack.enter_context(Process([self.python, str(ROOT / "fixtures/http_server.py"), "--response", response], self.env))
            self.upstream = "http://" + fixture.ready["listen"]
            text = (ROOT / "examples/local.toml").read_text().replace("{config_dir}/../fixtures/tools.py", str(ROOT / "fixtures/tools.py"))
            if request_timeout_ms is not None:
                text = text.replace("request_timeout_ms = 10000", f"request_timeout_ms = {request_timeout_ms}")
            if rate_limit is not None:
                text = text.replace("requests_per_second = 50\nburst = 100", f"requests_per_second = {rate_limit[0]}\nburst = {rate_limit[1]}")
            text += f'''\n[[servers]]
id = "owned"
revision = "owned-http-v1"
prewarm = true
[servers.endpoint]
transport = "http"
url = {json.dumps(self.upstream + "/mcp")}
[servers.tools.echo]
reusable = true
cache_ttl_ms = 60000
[servers.tools.error]
reusable = true
cache_ttl_ms = 60000
'''
            # Extend the existing first user's grants rather than inventing a bypass.
            text = text.replace('tools = ["deps__*", "source__*", "waypilot__inspect"]', 'tools = ["deps__*", "source__*", "owned__*", "waypilot__inspect"' + (', "learn__*"' if remote else '') + ']')
            text += '''\n[[users]]
id = "bob"
tenant = "other"
token_env = "WAYPILOT_BOB_TOKEN"
tools = ["owned__echo"]
[[users]]
id = "limited"
tenant = "local"
token_env = "WAYPILOT_LIMITED_TOKEN"
tools = ["owned__echo"]
requests_per_second = 1
burst = 2
'''
            if remote:
                text += '''\n[[servers]]
id = "learn"
revision = "live-public-service-v1"
prewarm = true
[servers.endpoint]
transport = "http"
url = "https://learn.microsoft.com/api/mcp"
'''
                text = text.replace("request_timeout_ms = 10000", "request_timeout_ms = 60000")
            if not reuse:
                text = text.replace("reusable = true", "reusable = false").replace("cache_ttl_ms = 60000", "cache_ttl_ms = 0")
            self.config = self.directory / "gateway.toml"
            self.config.write_text(text)
            process = self.stack.enter_context(Process([self.binary, "serve", "--config", str(self.config), "--listen", "127.0.0.1:0"], self.env))
            self.base = "http://" + process.ready["listen"]
        except BaseException:
            self.stack.close()
            raise

    def stats(self):
        status, _, value = http(self.base + "/admin/stats", self.admin)
        assert status == 200
        return value

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.stack.close()


def run(binary, python, remote=False):
    with Demo(binary, python, remote=remote) as demo:
        client = Client(demo.base, demo.token)
        before = demo.stats()["upstream_calls"]
        status, _, prepared = http(demo.base + "/v1/prewarm", demo.token, {"servers": ["deps", "source"]}, extra=client.headers)
        assert status == 200 and prepared["tool_calls"] == 0, prepared
        assert demo.stats()["upstream_calls"] == before
        listed = client.request("tools/list")["result"]["tools"]
        inputs = {"manifest": "tokio serde", "source": "fn main() {}"}
        outcomes = []
        for name, arguments in [("cold", inputs), ("repeat", inputs), ("source_changed", dict(inputs, source="fn main() {} fn next() {}"))]:
            before = demo.stats()["upstream_calls"]
            started = time.perf_counter()
            result = client.call("waypilot__inspect", arguments)
            outcomes.append({"case": name, "elapsed_ms": round((time.perf_counter() - started) * 1000, 3), "upstream_calls": demo.stats()["upstream_calls"] - before, "steps": result["structuredContent"]["steps"]})
        assert [case["upstream_calls"] for case in outcomes] == [4, 0, 2], outcomes
        first = client.call("owned__counter", {})["structuredContent"]["count"]
        second = client.call("owned__counter", {})["structuredContent"]["count"]
        assert second == first + 1
        live = None
        if remote:
            candidates = [tool for tool in listed if tool["name"].startswith("learn__") and "search" in tool["name"]]
            assert candidates, "public service did not advertise a search tool"
            tool = candidates[0]
            properties = tool["inputSchema"].get("properties", {})
            arguments = {"query": "MCP Streamable HTTP transport"}
            if "question" in properties and "query" not in properties:
                arguments = {"question": arguments["query"]}
            if "intent" in properties:
                arguments["intent"] = "Find official documentation explaining MCP HTTP transport"
            result = client.call(tool["name"], arguments)
            assert result.get("content") and result.get("isError") is not True
            live = {"server": "https://learn.microsoft.com/api/mcp", "tool": tool["name"], "content_items": len(result["content"]), "disposition": result["_meta"]["io.waypilot/runtime"]["disposition"]}
        client.close()
        return {"transport": "real MCP over HTTP and stdio", "prewarm": prepared, "advertised_tools": [tool["name"] for tool in listed], "workflow": outcomes, "mutable_counter_not_reused": True, "third_party": live, "stats": demo.stats()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default=str(ROOT / "target/debug/waypilot"))
    parser.add_argument("--python", default=os.environ.get("WAYPILOT_PYTHON", os.sys.executable))
    parser.add_argument("--remote", action="store_true", help="Also call Microsoft Learn's public MCP service")
    args = parser.parse_args()
    print(json.dumps(run(args.binary, args.python, args.remote), indent=2))
