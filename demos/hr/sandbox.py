"""Temporary HR peer and real Waypilot gateway with explicit speculation contracts."""

import json
import os
import secrets
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path

from agents.sandbox import connect
from mcp_demo import ROOT, Process, http

from .workbench import READS

__all__ = ["Sandbox", "connect"]


class Sandbox:
    def __init__(
        self,
        binary,
        mode="speculate",
        delay_ms=250,
        *,
        slots=8,
        timeout_ms=10000,
        duration_ms=2000,
        min_samples=3,
        confidence=75,
        rate=10000,
        burst=10000,
        grants=True,
        environment=None,
        max_in_flight=64,
    ):
        self.binary = str(Path(binary).resolve())
        self.mode = mode
        self.stack = ExitStack()
        self.tokens = {key: secrets.token_urlsafe(32) for key in ["agent", "admin", "control"]}
        env = {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "SYSTEMROOT", "TMPDIR"}
        }
        env.update(
            {k: v for k, v in os.environ.items() if k.startswith("OTEL_")}
            if environment is None
            else environment
        )
        env.update(
            HR_AGENT=self.tokens["agent"],
            HR_ADMIN=self.tokens["admin"],
            WORKBENCH_CONTROL_TOKEN=self.tokens["control"],
            RUST_LOG="error",
        )
        try:
            directory = Path(
                self.stack.enter_context(tempfile.TemporaryDirectory(prefix="waypilot-hr-"))
            )
            self.peer_process = self.stack.enter_context(
                Process(
                    [
                        sys.executable,
                        str(ROOT / "demos/hr/workbench.py"),
                        "--delay-ms",
                        str(delay_ms),
                    ],
                    env,
                )
            )
            self.peer = "http://" + self.peer_process.ready["listen"]
            text = f'''listen = "127.0.0.1:0"
admin_token_env = "HR_ADMIN"
request_timeout_ms = {timeout_ms}
max_sessions = 2048
max_in_flight = {max_in_flight}
[limits]
body_readers = 512
upstream_calls = 64
upstream_initializations = 64
[[users]]
id = "hr-agent"
tenant = "hr-lab"
token_env = "HR_AGENT"
tools = ["hr__*"]
speculative_tools = {json.dumps(["hr__leave_balance", "hr__leave_policy"] if grants else [])}
requests_per_second = {rate}
burst = {burst}
[[servers]]
id = "hr"
revision = "hr-v1"
prewarm = {str(mode in ["prewarm", "speculate"]).lower()}
max_in_flight = 64
[servers.endpoint]
transport = "http"
url = "{self.peer}/mcp"
'''
            if mode != "off":
                for name in READS:
                    text += f"""[servers.tools.{name}]
reusable = true
cache_ttl_ms = 60000
state_tool = "state_version"
speculative = true
"""
            if mode == "speculate":
                text += f"""[speculation]
max_in_flight = {slots}
min_samples = {min_samples}
confidence_percent = {confidence}
max_duration_ms = {duration_ms}
[[speculation.rules]]
after = "hr__employee"
tool = "hr__leave_balance"
arguments.employee_id = {{result = "/structuredContent/employee_id"}}
[[speculation.rules]]
after = "hr__leave_balance"
tool = "hr__leave_policy"
arguments.employee_id = {{input = "/employee_id"}}
"""
            self.config = directory / "gateway.toml"
            self.config.write_text(text)
            self.env = env
            self.service = self.stack.enter_context(
                Process([str(Path(binary).resolve()), "serve", "--config", str(self.config)], env)
            )
            self.base = "http://" + self.service.ready["listen"]
            self.url = self.base + "/mcp"
        except BaseException:
            self.stack.close()
            raise

    def peer_stats(self):
        return http(self.peer + "/control", self.tokens["control"])[2]

    def mutate(self, **values):
        return http(self.peer + "/control", self.tokens["control"], values)[2]

    def stats(self):
        return http(self.base + "/admin/stats", self.tokens["admin"])[2]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stack.close()
