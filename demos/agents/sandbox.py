"""Temporary, loopback-only gateway and incident services; no model credentials in peers."""

import json
import os
import secrets
import sys
import tempfile
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path

import httpx2
from mcp import Client
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp_demo import ROOT, Process, http

from .runner import validate_endpoint


@asynccontextmanager
async def connect(url, token, *, sandbox=None, role="investigator", transport="http"):
    if transport == "stdio":
        if sandbox is None:
            raise ValueError("stdio ingress requires a local sandbox")
        parameters = StdioServerParameters(
            command=sandbox.binary,
            args=["serve-stdio", "--config", str(sandbox.config), "--user", role],
            env=sandbox.env,
        )
        async with Client(stdio_client(parameters), read_timeout_seconds=20) as client:
            yield client
    else:
        validate_endpoint(url)
        async with httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {token}"}, timeout=20
        ) as connection:
            async with Client(
                streamable_http_client(url, http_client=connection), read_timeout_seconds=20
            ) as client:
                yield client


class Sandbox:
    def __init__(
        self,
        binary,
        *,
        reuse=True,
        batch_validation=False,
        max_sessions=128,
        max_in_flight=64,
        upstream_calls=512,
        delay_ms=30,
        timeout_ms=10000,
        start_http=True,
        environment=None,
    ):
        self.stack = ExitStack()
        self.binary = str(Path(binary).resolve())
        self.tokens = {
            role: secrets.token_urlsafe(32)
            for role in ["investigator", "reviewer", "admin", "control"]
        }
        self.env = {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "SYSTEMROOT", "TMPDIR"}
        }
        self.env.update(
            {key: value for key, value in os.environ.items() if key.startswith("OTEL_")}
            if environment is None
            else environment
        )
        self.env.update(
            {
                "RUST_LOG": "error",
                "WORKBENCH_CONTROL_TOKEN": self.tokens["control"],
                "AGENT_INVESTIGATOR": self.tokens["investigator"],
                "AGENT_REVIEWER": self.tokens["reviewer"],
                "AGENT_ADMIN": self.tokens["admin"],
            }
        )
        try:
            directory = Path(
                self.stack.enter_context(tempfile.TemporaryDirectory(prefix="waypilot-agents-"))
            )
            peer = self.stack.enter_context(
                Process(
                    [
                        sys.executable,
                        str(ROOT / "demos/agents/workbench.py"),
                        "--delay-ms",
                        str(delay_ms),
                    ],
                    self.env,
                )
            )
            self.peer_process = peer
            self.peer = "http://" + peer.ready["listen"]
            text = f"""listen = "127.0.0.1:0"
admin_token_env = "AGENT_ADMIN"
max_sessions = {max_sessions}
max_in_flight = {max_in_flight}
request_timeout_ms = {timeout_ms}
[limits]
upstream_calls = {upstream_calls}
"""
            reads = [
                "ops__get_incident",
                "ops__get_metrics",
                "ops__get_deployment",
                "ops__state_version",
                "knowledge__search_runbooks",
                "knowledge__get_runbook",
                "knowledge__assess",
                "knowledge__state_version",
                "waypilot__triage",
            ]
            if batch_validation:
                reads += ["ops__state_batch", "knowledge__state_batch"]
            for role in ["investigator", "reviewer"]:
                grants = reads + (["ops__append_note"] if role == "investigator" else [])
                text += f'''[[users]]
id = "{role}"
tenant = "incident-lab"
token_env = "AGENT_{role.upper()}"
tools = {json.dumps(grants)}
requests_per_second = 10000
burst = 10000
'''
            for name, tools in [
                ("ops", ["get_incident", "get_metrics", "get_deployment"]),
                ("knowledge", ["search_runbooks", "get_runbook", "assess"]),
            ]:
                text += f'''[[servers]]
id = "{name}"
revision = "incident-workbench-v1"
prewarm = true
[servers.endpoint]
transport = "http"
url = "{self.peer}/mcp"
'''
                if reuse:
                    for tool in tools:
                        text += f"[servers.tools.{tool}]\nreusable = true\ncache_ttl_ms = 60000\n"
                        if tool != "assess":
                            text += 'state_tool = "state_version"\n'
                            if batch_validation:
                                text += 'state_batch_tool = "state_batch"\n'
            text += """[[workflows]]
name = "triage"
description = "Read live telemetry and deployment, search runbooks concurrently, then assess the evidence. No writes."
input_schema = {type = "object", properties = {service = {type = "string"}, symptom = {type = "string", enum = ["errors", "backlog"]}}, required = ["service", "symptom"], additionalProperties = false}
outputs = ["assessment", "runbooks"]
[[workflows.steps]]
id = "metrics"
tool = "ops__get_metrics"
arguments.service = {input = "/service"}
[[workflows.steps]]
id = "deployment"
tool = "ops__get_deployment"
arguments.service = {input = "/service"}
[[workflows.steps]]
id = "runbooks"
tool = "knowledge__search_runbooks"
arguments.symptom = {input = "/symptom"}
[[workflows.steps]]
id = "assessment"
tool = "knowledge__assess"
arguments.metrics = {step = "metrics", pointer = "/structuredContent"}
arguments.deployment = {step = "deployment", pointer = "/structuredContent"}
"""
            self.config = directory / "gateway.toml"
            self.config.write_text(text)
            self.base = self.url = None
            if start_http:
                gateway = self.stack.enter_context(
                    Process([self.binary, "serve", "--config", str(self.config)], self.env)
                )
                self.service = gateway
                self.base = "http://" + gateway.ready["listen"]
                self.url = self.base + "/mcp"
        except BaseException:
            self.stack.close()
            raise

    def peer_stats(self):
        status, _, result = http(self.peer + "/control", self.tokens["control"])
        if status != 200:
            raise RuntimeError("workbench stats unavailable")
        return result

    def mutate(self, **value):
        status, _, result = http(self.peer + "/control", self.tokens["control"], value)
        if status != 200:
            raise RuntimeError("workbench mutation failed")
        return result

    def stats(self):
        return http(self.base + "/admin/stats", self.tokens["admin"])[2]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stack.close()
