"""Owned peers and configurations shared by state regression tests and benchmarks."""

import json
import os
import secrets
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from mcp_demo import ROOT, Process, http
from mcp_demo import Client as Client


class StateDemo:
    def __init__(
        self,
        binary,
        transport="json",
        validated=True,
        ttl=60000,
        timeout=10000,
        environment=None,
        upstream=None,
        upstream_calls=512,
        rate=(10000, 10000),
        pure=False,
    ):
        self.stack = ExitStack()
        self.directory = Path(
            self.stack.enter_context(tempfile.TemporaryDirectory(prefix="waypilot-state-"))
        )
        self.token, self.admin, self.restricted = [secrets.token_urlsafe(32) for _ in range(3)]
        self.env = dict(
            os.environ,
            WAYPILOT_TOKEN=self.token,
            WAYPILOT_ADMIN_TOKEN=self.admin,
            WAYPILOT_RESTRICTED=self.restricted,
            RUST_LOG="error",
        )
        self.env.update(environment or {})
        self.transport = transport
        self.state_file = self.directory / "state.json"
        self.state_file.write_text(
            json.dumps({key: {"value": key, "revision": 0} for key in ["a", "b"]})
        )
        python = os.environ["WAYPILOT_PYTHON"]
        try:
            if transport == "stdio":
                endpoint = f'transport = "stdio"\ncommand = {json.dumps(python)}\nargs = {json.dumps([str(ROOT / "fixtures/state_stdio.py"), str(self.state_file)])}'
            elif upstream is not None:
                self.upstream = upstream
                endpoint = f'transport = "http"\nurl = {json.dumps(upstream + "/mcp")}'
            else:
                fixture = self.stack.enter_context(
                    Process(
                        [python, str(ROOT / "fixtures/http_server.py"), "--response", transport],
                        self.env,
                    )
                )
                self.upstream = "http://" + fixture.ready["listen"]
                endpoint = f'transport = "http"\nurl = {json.dumps(self.upstream + "/mcp")}'
            text = f"""listen = "127.0.0.1:0"
admin_token_env = "WAYPILOT_ADMIN_TOKEN"
max_sessions = 1100
max_in_flight = 600
request_timeout_ms = {timeout}
[limits]
upstream_calls = {upstream_calls}
[[users]]
id = "reader"
tenant = "local"
token_env = "WAYPILOT_TOKEN"
tools = ["owned__*", "waypilot__state"]
requests_per_second = {rate[0]}
burst = {rate[1]}
[[users]]
id = "restricted"
tenant = "local"
token_env = "WAYPILOT_RESTRICTED"
tools = ["owned__state_read"]
[[servers]]
id = "owned"
revision = "state-v1"
[servers.endpoint]
{endpoint}
[servers.tools.state_read]
reusable = true
cache_ttl_ms = {ttl}
"""
            if validated:
                text += 'state_tool = "state_version"\n'
            if pure:
                text += "speculative = true\n"
            for name in ["dependency_index", "source_index"]:
                text += f"[servers.tools.{name}]\nreusable = true\ncache_ttl_ms = {ttl}\n"
            text += """[[workflows]]
name = "state"
description = "Two stateful reads and their independently retained descendants"
input_schema = {type = "object"}
outputs = ["left-index", "right-index"]
"""
            for name, key, child in [
                ("left", "a", "dependency_index"),
                ("right", "b", "source_index"),
            ]:
                text += f'''[[workflows.steps]]
id = "{name}"
tool = "owned__state_read"
arguments.key = {{literal = "{key}"}}
[[workflows.steps]]
id = "{name}-index"
tool = "owned__{child}"
arguments.value = {{step = "{name}", pointer = "/structuredContent/value"}}
'''
            config = self.directory / "gateway.toml"
            self.config = config
            config.write_text(text)
            self.service = self.stack.enter_context(
                Process([str(Path(binary).resolve()), "serve", "--config", str(config)], self.env)
            )
            self.base = "http://" + self.service.ready["listen"]
        except BaseException:
            self.stack.close()
            raise

    def mutate(self, **arguments):
        if self.transport != "stdio":
            return http(self.upstream + "/state", "fixture", arguments)[2]
        state = json.loads(self.state_file.read_text())
        row = state[arguments.get("key", "a")]
        if arguments.get("touch") or "value" in arguments:
            row["revision"] += 1
        if "value" in arguments:
            row["value"] = arguments["value"]
        pending = self.state_file.with_suffix(".new")
        pending.write_text(json.dumps(state))
        pending.replace(self.state_file)
        return state

    def stats(self):
        return http(self.base + "/admin/stats", self.admin)[2]

    def invalidate(self):
        assert http(self.base + "/admin/invalidate", self.admin, {})[0] == 200

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stack.close()
