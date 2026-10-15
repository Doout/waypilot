"""Single-node MCP qualification: capacity, controls, failure recovery and open-loop load."""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import resource
import secrets
import signal
import socket
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

from net import HTTP, MCP

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from mcp_demo import ROOT, Process


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else 0


class Qualification:
    def __init__(self, args):
        self.args = args
        self.stack = ExitStack()
        self.directory = Path(
            self.stack.enter_context(tempfile.TemporaryDirectory(prefix="waypilot-scale-"))
        )
        self.env = dict(
            os.environ,
            WAYPILOT_ADMIN_TOKEN=secrets.token_urlsafe(32),
            WAYPILOT_A=secrets.token_urlsafe(32),
            WAYPILOT_B=secrets.token_urlsafe(32),
            WAYPILOT_LIMITED=secrets.token_urlsafe(32),
            RUST_LOG="warn",
        )
        for name in list(self.env):
            if name.startswith("OTEL_"):
                self.env.pop(name)
        self.collector = None
        self.collector_http = None
        if args.trace_ratio:
            self.collector = self.stack.enter_context(
                Process([sys.executable, str(ROOT / "load/peer.py")], self.env)
            )
            collector_port = int(self.collector.ready["listen"].split(":")[-1])
            self.collector_http = HTTP("127.0.0.1", collector_port)
            self.env.update(
                OTEL_SERVICE_NAME="waypilot",
                OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=f"http://127.0.0.1:{collector_port}/v1/traces",
                OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="http/json",
                OTEL_TRACES_SAMPLER="traceidratio",
                OTEL_TRACES_SAMPLER_ARG=str(args.trace_ratio),
            )
        self.peer = self.stack.enter_context(
            Process([sys.executable, str(ROOT / "load/peer.py")], self.env)
        )
        self.peer_port = int(self.peer.ready["listen"].split(":")[-1])
        self.control = HTTP("127.0.0.1", self.peer_port)
        self.service = None
        self.admin = None
        self.clients = []
        self.samples = []
        self.resources = []
        self.phases = []
        self.stop_sampling = asyncio.Event()
        self.sampling_paused = False
        self.last_wait_state = {}
        self.session_ttl = 180
        self.request_timeout_ms = 5000

    def config(self, upstream_limit):
        n = self.args.concurrency
        text = f"""listen = "127.0.0.1:0"
admin_token_env = "WAYPILOT_ADMIN_TOKEN"
state_path = "state.json"
request_timeout_ms = {self.request_timeout_ms}
max_sessions = {max(self.args.clients + 64, 128)}
max_in_flight = {n}
session_ttl_seconds = {self.session_ttl}
[limits]
max_connections = {max(self.args.clients + n + 256, 512)}
connection_idle_ms = 180000
body_readers = {n}
admin_requests = 16
request_bytes = 65536
buffered_bytes = 16777216
upstream_calls = {2 * n}
upstream_idle_per_host = {n}
cleanup_pending = {max(self.args.clients * 2, 512)}
shutdown_grace_ms = 2000
[[users]]
id = "alice"
tenant = "a"
token_env = "WAYPILOT_A"
tools = ["owned__*", "parallel__*", "waypilot__join"]
requests_per_second = 10000
burst = 10000
[[users]]
id = "bob"
tenant = "b"
token_env = "WAYPILOT_B"
tools = ["owned__*", "parallel__*", "waypilot__join"]
requests_per_second = 10000
burst = 10000
[[users]]
id = "limited"
tenant = "limited"
token_env = "WAYPILOT_LIMITED"
tools = ["owned__echo"]
requests_per_second = 1
burst = 4
[[servers]]
id = "owned"
revision = "scale-peer-v1"
max_in_flight = {upstream_limit}
[servers.endpoint]
transport = "http"
url = "http://127.0.0.1:{self.peer_port}/mcp"
[servers.tools.cache_hold]
reusable = true
cache_ttl_ms = 60000
[[servers]]
id = "parallel"
revision = "scale-peer-v1"
max_in_flight = {n}
[servers.endpoint]
transport = "http"
url = "http://127.0.0.1:{self.peer_port}/mcp"
[[workflows]]
name = "join"
description = "Two async branches followed by a dependent effect and collection"
input_schema = {{ type = "object", required = ["owner","nonce"] }}
outputs = ["joined"]
[[workflows.steps]]
id = "left"
tool = "owned__hold"
arguments = {{ owner = {{ input = "/owner" }}, nonce = {{ input = "/nonce" }}, phase = {{ literal = "workflow-left" }}, value = {{ literal = "left" }} }}
[[workflows.steps]]
id = "right"
tool = "parallel__hold"
arguments = {{ owner = {{ input = "/owner" }}, nonce = {{ input = "/nonce" }}, phase = {{ literal = "workflow-right" }}, value = {{ literal = "right" }} }}
[[workflows.steps]]
id = "joined"
tool = "owned__effect"
arguments = {{ owner = {{ input = "/owner" }}, nonce = {{ input = "/nonce" }}, phase = {{ literal = "workflow-join" }}, left = {{ step = "left", pointer = "/structuredContent/value" }}, right = {{ step = "right", pointer = "/structuredContent/value" }} }}
"""
        path = self.directory / "gateway.toml"
        path.write_text(text)
        return path

    async def start(self, upstream_limit):
        self.service = Process(
            [
                str(Path(self.args.binary).resolve()),
                "serve",
                "--config",
                str(self.config(upstream_limit)),
            ],
            self.env,
        )
        self.port = int(self.service.ready["listen"].split(":")[-1])
        self.admin = HTTP("127.0.0.1", self.port, self.env["WAYPILOT_ADMIN_TOKEN"])
        self.sampling_paused = False

    async def stats(self):
        status, _, value = await self.admin.request("/admin/stats")
        assert status == 200
        return value

    async def peer_stats(self):
        status, _, value = await self.control.request("/stats")
        assert status == 200
        return value

    async def until(self, predicate, timeout=4):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.005)
        raise AssertionError("condition did not become true before deadline")

    async def idle(self):
        async def ready():
            s, p = await asyncio.gather(self.stats(), self.peer_stats())
            return (
                s["active_requests"]
                == s["active_upstream"]
                == s["active_initializations"]
                == s["cleanup"]["pending"]
                == p["active"]
                == 0
            )

        await self.until(ready, timeout=6)
        stats = await self.stats()
        assert stats["cleanup"]["dropped"] == 0 and stats["cleanup"]["errors"] == 0, stats

    async def release(self, phase):
        status, _, _ = await self.control.request("/control", {"release": phase})
        assert status == 200

    async def clients_start(self, count):
        slots = asyncio.Semaphore(64)

        async def make(index):
            owner = "alice" if index < max(self.args.concurrency, int(count * 0.75)) else "bob"
            async with slots:
                client = MCP(
                    HTTP(
                        "127.0.0.1",
                        self.port,
                        self.env["WAYPILOT_A" if owner == "alice" else "WAYPILOT_B"],
                    )
                )
                await client.initialize()
                return owner, client

        self.clients = await asyncio.gather(*(make(index) for index in range(count)))
        assert (await self.stats())["sessions"] == count

    def arguments(self, owner, nonce, phase, **extra):
        return dict(owner=owner, nonce=nonce, phase=phase, **extra)

    async def wave(self, tool, phase, count=None, shared=False, **extra):
        count = count or self.args.concurrency
        jobs = []
        for index, (owner, client) in enumerate(self.clients[:count]):
            jobs.append(
                asyncio.create_task(
                    client.call(
                        "owned__" + tool,
                        self.arguments(
                            owner, "shared" if shared else f"{phase}-{index}", phase, **extra
                        ),
                    )
                )
            )
            # Establish concurrency with a declared short ramp. Instantaneously
            # opening hundreds of TCP sockets can overflow the owned peer's OS
            # accept queue (notably macOS's default 128-entry listen backlog).
            # All calls still have to reach the upstream barrier concurrently.
            if len(jobs) % 16 == 0 and getattr(self.args, "ramp_ms", 2):
                await asyncio.sleep(getattr(self.args, "ramp_ms", 2) / 1000)
        self.pending_jobs = jobs
        return jobs

    async def pending(self, phase, count):
        async def ready():
            s, p = await asyncio.gather(self.stats(), self.peer_stats())
            finished = []
            for job in getattr(self, "pending_jobs", []):
                if job.done():
                    try:
                        finished.append(job.result())
                    except Exception as error:
                        finished.append(repr(error))
            self.last_wait_state = {
                "phase": phase,
                "gateway": s,
                "peer": p,
                "finished": finished,
            }
            return (
                s["active_requests"] == self.args.concurrency
                and p["active_by_phase"].get(phase, 0) == count
            )

        await self.until(ready)

    async def initialization_cancel(self):
        client = MCP(HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"]))
        await client.initialize()
        await self.control.request("/control", {"pause_initialize": True})
        job = asyncio.create_task(
            client.call("owned__echo", self.arguments("alice", "abandoned", "init-cancel"))
        )

        async def started():
            return (await self.peer_stats())["sessions"] == 1

        await self.until(started)
        control = HTTP("127.0.0.1", self.port, client.http.token)
        status, _, _ = await control.request(
            "/mcp",
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": client.sequence},
            },
            headers=client.headers,
        )
        assert status == 202 and "error" in await job
        await self.control.request("/control", {"pause_initialize": False})
        await self.idle()
        assert (await self.peer_stats())["sessions"] == 0
        assert (await self.peer_stats())["calls"] == 0
        await client.close()
        await control.close()
        self.phases.append(
            {
                "name": "abandoned_initialization_cleanup",
                "tool_calls": 0,
                "orphan_sessions": 0,
            }
        )

    async def capacity(self):
        n = self.args.concurrency
        jobs = await self.wave("hold", "capacity")
        await self.pending("capacity", n)
        s = await self.stats()
        assert s["active_upstream"] == n and s["connections"] >= self.args.clients
        owner, extra = self.clients[n]
        rejected = await extra.call("owned__echo", self.arguments(owner, "overload", "capacity"))
        assert rejected["error"]["code"] == -32029, rejected
        await self.release("capacity")
        results = await asyncio.gather(*jobs)
        for index, value in enumerate(results):
            assert value["result"]["structuredContent"] == {
                "owner": self.clients[index][0],
                "nonce": f"capacity-{index}",
            }
        await self.idle()
        self.phases.append(
            {
                "name": "capacity_and_overload",
                "accepted_concurrent": n,
                "connected_clients": self.args.clients,
                "rejected_extra": True,
            }
        )

    async def workflow(self):
        before = (await self.peer_stats())["calls"]
        jobs = [
            asyncio.create_task(
                client.call("waypilot__join", {"owner": owner, "nonce": f"workflow-{index}"})
            )
            for index, (owner, client) in enumerate(self.clients[: self.args.concurrency])
        ]

        async def ready():
            s, p = await asyncio.gather(self.stats(), self.peer_stats())
            return (
                s["active_requests"] == self.args.concurrency
                and p["active_by_phase"].get("workflow-left", 0) == self.args.concurrency
                and p["active_by_phase"].get("workflow-right", 0) == self.args.concurrency
            )

        await self.until(ready)
        await self.release("workflow-left")
        await self.release("workflow-right")
        for index, value in enumerate(await asyncio.gather(*jobs)):
            output = value["result"]["structuredContent"]["outputs"]["joined"]["structuredContent"]
            assert output == {
                "owner": self.clients[index][0],
                "nonce": f"workflow-{index}",
                "joined": True,
            }, output
        assert (await self.peer_stats())["calls"] == before + 3 * self.args.concurrency
        await self.idle()
        self.phases.append(
            {
                "name": "parallel_workflow_and_dependent_effect",
                "concurrent_workflows": self.args.concurrency,
                "concurrent_branches": 2 * self.args.concurrency,
                "tool_calls": 3 * self.args.concurrency,
                "equivalent_outputs": True,
            }
        )

    async def body_pressure(self):
        writers = []
        try:
            for _ in range(self.args.concurrency):
                _, writer = await asyncio.open_connection("127.0.0.1", self.port)
                writer.write(
                    (
                        f"POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {self.env['WAYPILOT_A']}\r\nContent-Type: application/json\r\nContent-Length: 65536\r\n\r\n"
                    ).encode()
                )
                await writer.drain()
                writers.append(writer)

            async def full():
                return (await self.stats())["body_readers"] == self.args.concurrency

            await self.until(full)
            http = HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"])
            status, _, _ = await http.request(
                "/mcp",
                {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                headers=self.clients[0][1].headers,
            )
            assert status == 503
            await http.close()
        finally:
            for writer in writers:
                writer.close()
            await asyncio.gather(*(writer.wait_closed() for writer in writers))

        async def empty():
            return (await self.stats())["body_readers"] == 0

        await self.until(empty)
        self.phases.append(
            {
                "name": "body_admission_and_admin_reserve",
                "blocked_readers": self.args.concurrency,
                "excess_rejected": True,
                "admin_available": True,
            }
        )

    async def payload_pressure(self):
        count = self.args.concurrency
        jobs = await self.wave("hold", "payload", padding="x" * 60000)

        async def arrived():
            s, p = await asyncio.gather(self.stats(), self.peer_stats())
            done = sum(job.done() for job in jobs)
            assert s["buffered_bytes"] <= 16777216
            return done + p["active_by_phase"].get("payload", 0) == count

        await self.until(arrived)
        await self.release("payload")
        values = await asyncio.gather(*jobs)
        for index, value in enumerate(values):
            if "result" in value:
                owner = self.clients[index][0]
                assert value["result"]["structuredContent"] == {
                    "owner": owner,
                    "nonce": f"payload-{index}",
                }, value
        accepted = sum("result" in value for value in values)
        rejected = sum(value.get("http_status") == 503 for value in values)
        assert accepted + rejected == count
        if count >= 300:
            assert rejected > 0
        await self.idle()
        self.phases.append(
            {
                "name": "large_payload_byte_budget",
                "payload_bytes": 60000,
                "accepted": accepted,
                "rejected": rejected,
                "accounted_byte_limit": 16777216,
            }
        )

    async def churn(self):
        for cycle in range(3):
            await self.close_clients()
            await self.clients_start(self.args.clients)
            values = await asyncio.gather(
                *(
                    client.call(
                        "owned__session",
                        self.arguments(owner, f"churn-{cycle}-{index}", "churn"),
                    )
                    for index, (owner, client) in enumerate(self.clients[: self.args.concurrency])
                )
            )
            assert (
                len({value["result"]["structuredContent"]["session"] for value in values})
                == self.args.concurrency
            )
        self.phases.append(
            {
                "name": "session_churn",
                "cycles": 3,
                "sessions_per_cycle": self.args.clients,
                "remote_sessions_after_close": 0,
            }
        )

    async def rate_limit(self):
        clients = [
            MCP(HTTP("127.0.0.1", self.port, self.env["WAYPILOT_LIMITED"])) for _ in range(20)
        ]
        await asyncio.gather(*(client.initialize() for client in clients))
        start = time.monotonic()
        values = await asyncio.gather(
            *(
                client.call("owned__echo", self.arguments("limited", f"limited-{index}", "rate"))
                for index, client in enumerate(clients)
            )
        )
        assert time.monotonic() - start < 0.9, "rate test generator did not submit a single burst"
        assert sum("result" in value for value in values) == 4
        assert sum(value.get("error", {}).get("code") == -32029 for value in values) == 16
        await asyncio.gather(*(client.close() for client in clients))
        self.phases.append({"name": "concurrent_rate_bucket", "accepted": 4, "rate_limited": 16})

    async def cancellation(self):
        n = self.args.concurrency
        jobs = await self.wave("hold", "cancellation")
        await self.pending("cancellation", n)
        slots = asyncio.Semaphore(32)

        async def cancel(client):
            async with slots:
                http = HTTP("127.0.0.1", self.port, client.http.token)
                status, _, _ = await http.request(
                    "/mcp",
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/cancelled",
                        "params": {"requestId": client.sequence},
                    },
                    headers=client.headers,
                )
                assert status == 202
                await http.close()

        await asyncio.gather(*(cancel(client) for _, client in self.clients[:n]))
        assert all("error" in value for value in await asyncio.gather(*jobs))
        await self.idle()
        self.phases.append({"name": "cancellation_and_remote_cleanup", "calls": n, "replayed": 0})

    async def reuse(self):
        n = self.args.concurrency
        before = (await self.peer_stats())["calls"]
        jobs = await self.wave("cache_hold", "singleflight", shared=True)
        await self.pending("singleflight", 1)
        await self.release("singleflight")
        results = await asyncio.gather(*jobs)
        assert all(
            value["result"]["structuredContent"] == {"owner": "alice", "nonce": "shared"}
            for value in results
        )
        assert (await self.peer_stats())["calls"] == before + 1
        assert (
            sum(
                value["result"]["_meta"]["io.waypilot/runtime"]["disposition"] == "executed"
                for value in results
            )
            == 1
        )
        bob = next(client for owner, client in self.clients if owner == "bob")
        # Identical arguments across tenants must cause another upstream invocation.
        await self.release("cross-identity")
        value = await bob.call(
            "owned__cache_hold", self.arguments("alice", "shared", "singleflight")
        )
        assert "result" in value
        assert (await self.peer_stats())["calls"] == before + 2
        await self.idle()
        self.phases.append(
            {
                "name": "singleflight_and_identity",
                "callers": n,
                "same_identity_upstream_calls": 1,
                "cross_identity_extra_calls": 1,
            }
        )

    async def failures(self):
        count = min(64, self.args.concurrency)
        values = await asyncio.gather(
            *(
                client.call(
                    "owned__effect",
                    self.arguments(owner, f"fault-{index}", "fault", drop=True),
                )
                for index, (owner, client) in enumerate(self.clients[:count])
            )
        )
        assert all("error" in value for value in values)
        assert (await self.peer_stats())["duplicate_effects"] == 0
        after = await asyncio.gather(
            *(
                client.call(
                    "owned__session",
                    self.arguments(owner, f"after-fault-{index}", "recovery"),
                )
                for index, (owner, client) in enumerate(self.clients[:count])
            )
        )
        assert len({value["result"]["structuredContent"]["session"] for value in after}) == count
        await self.idle()
        self.phases.append(
            {
                "name": "uncertain_effect_and_session_isolation",
                "failed_effects": count,
                "duplicate_effects": 0,
                "isolated_sessions": count,
            }
        )

    async def deadlines(self):
        jobs = await self.wave("hold", "deadlines")
        await self.pending("deadlines", self.args.concurrency)
        results = await asyncio.gather(*jobs)
        assert all("error" in value for value in results)
        await self.idle()
        self.phases.append({"name": "deadline_recovery", "timed_out": len(results)})

    async def sample_resources(self):
        while not self.stop_sampling.is_set():
            if not self.sampling_paused and self.service and self.service.process.poll() is None:
                service = self.service
                pid = service.process.pid
                if sys.platform == "linux":
                    try:
                        status = Path(f"/proc/{pid}/status").read_text()
                        fds = len(list(Path(f"/proc/{pid}/fd").iterdir()))
                    except FileNotFoundError:
                        if (
                            self.sampling_paused
                            or service is not self.service
                            or service.process.poll() is not None
                        ):
                            continue
                        raise
                    if (
                        self.sampling_paused
                        or service is not self.service
                        or service.process.poll() is not None
                    ):
                        continue
                    memory = (
                        int(
                            next(
                                line.split()[1]
                                for line in status.splitlines()
                                if line.startswith("VmRSS:")
                            )
                        )
                        * 1024
                    )
                else:
                    process = await asyncio.create_subprocess_exec(
                        "ps",
                        "-o",
                        "rss=",
                        "-p",
                        str(pid),
                        stdout=asyncio.subprocess.PIPE,
                    )
                    output, _ = await process.communicate()
                    if not output.strip():
                        if (
                            self.sampling_paused
                            or service is not self.service
                            or service.process.poll() is not None
                        ):
                            continue
                        raise AssertionError("live gateway RSS sample is empty")
                    memory = int(output.strip()) * 1024
                    process = await asyncio.create_subprocess_exec(
                        "/usr/sbin/lsof",
                        "-nP",
                        "-p",
                        str(pid),
                        "-Ff",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    output, _ = await process.communicate()
                    fds = sum(
                        line[1:].isdigit() for line in output.splitlines() if line.startswith(b"f")
                    )
                if (
                    self.sampling_paused
                    or service is not self.service
                    or service.process.poll() is not None
                ):
                    continue
                try:
                    stats = await self.stats()
                except (OSError, asyncio.IncompleteReadError):
                    if (
                        self.sampling_paused
                        or service is not self.service
                        or service.process.poll() is not None
                    ):
                        continue
                    raise
                self.resources.append(
                    dict(at=time.monotonic(), pid=pid, rss_bytes=memory, fds=fds, **stats)
                )
            try:
                await asyncio.wait_for(self.stop_sampling.wait(), 0.5)
            except TimeoutError:
                pass

    async def soak(self):
        start = time.monotonic()
        slots = asyncio.Semaphore(self.args.concurrency)
        jobs = set()

        async def invoke(index, scheduled):
            async with slots:
                owner, client = self.clients[index % len(self.clients)]
                actual = time.monotonic()
                value = await client.call(
                    "owned__echo",
                    self.arguments(owner, f"soak-{index}", "soak", delay_ms=10),
                )
                end = time.monotonic()
                assert value["result"]["structuredContent"] == {
                    "owner": owner,
                    "nonce": f"soak-{index}",
                }, value
                self.samples.append(
                    {
                        "sequence": index,
                        "scheduled_ms": (scheduled - start) * 1000,
                        "lag_ms": (actual - scheduled) * 1000,
                        "latency_ms": (end - scheduled) * 1000,
                    }
                )

        total = int(self.args.seconds * self.args.rate)
        for index in range(total):
            scheduled = start + index / self.args.rate
            await asyncio.sleep(max(0, scheduled - time.monotonic()))
            job = asyncio.create_task(invoke(index, scheduled))
            jobs.add(job)
            if len(jobs) >= self.args.concurrency * 2:
                done, jobs = await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
                for job in done:
                    job.result()
        await asyncio.gather(*jobs)
        duration = time.monotonic() - start
        assert percentile([sample["lag_ms"] for sample in self.samples], 0.99) < 250, (
            "load generator could not sustain the offered rate"
        )
        self.phases.append(
            {
                "name": "open_loop_soak",
                "offered_rps": self.args.rate,
                "completed": total,
                "seconds": duration,
                "achieved_rps": total / duration,
                "generator_lag_p99_ms": percentile(
                    [sample["lag_ms"] for sample in self.samples], 0.99
                ),
                "latency_ms": {
                    label: percentile([sample["latency_ms"] for sample in self.samples], fraction)
                    for label, fraction in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]
                },
            }
        )
        await self.idle()

    async def close_clients(self):
        slots = asyncio.Semaphore(32)

        async def close(client):
            async with slots:
                await client.close()

        await asyncio.gather(*(close(client) for _, client in self.clients))
        self.clients = []
        await self.idle()
        assert (await self.stats())["sessions"] == 0
        assert (await self.peer_stats())["sessions"] == 0

    async def revoke_and_restart(self):
        await self.close_clients()
        self.sampling_paused = True
        await self.admin.close()
        self.service.close()
        self.service = None
        await self.start(min(16, self.args.concurrency))
        await self.clients_start(self.args.clients)
        jobs = await self.wave("hold", "revocation")
        await self.pending("revocation", min(16, self.args.concurrency))
        before = (await self.peer_stats())["calls"]
        status, _, reply = await self.admin.request(
            "/admin/users/update", {"id": "alice", "enabled": False}
        )
        assert status == 200 and reply["persistent"] is True
        assert all("error" in value for value in await asyncio.gather(*jobs))
        await self.idle()
        assert (await self.peer_stats())["calls"] == before, "revoked queued work was dispatched"
        await self.close_clients()
        self.sampling_paused = True
        await self.admin.close()
        self.service.close()
        self.service = None
        await self.start(self.args.concurrency)
        rejected = HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"])
        status, _, _ = await rejected.request(
            "/mcp", {"jsonrpc": "2.0", "id": 0, "method": "initialize"}
        )
        assert status == 401, "revocation was lost on restart"
        await rejected.close()
        status, _, reply = await self.admin.request(
            "/admin/users/update", {"id": "alice", "enabled": True}
        )
        assert status == 200 and reply["persistent"] is True
        client = MCP(HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"]))
        await client.initialize()
        value = await client.call("owned__echo", self.arguments("alice", "enabled", "restart"))
        assert "result" in value
        await client.close()
        await self.idle()
        self.phases.append(
            {
                "name": "queued_revocation_and_durable_restart",
                "queued": self.args.concurrency - min(16, self.args.concurrency),
                "revoked_dispatches": 0,
                "restart_preserved_revocation": True,
            }
        )

    async def expiry_and_shutdown(self):
        self.sampling_paused = True
        await self.admin.close()
        self.service.close()
        self.service = None
        self.session_ttl = 10
        self.request_timeout_ms = 15000
        await self.start(self.args.concurrency)
        client = MCP(HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"]))
        await client.initialize()
        job = asyncio.create_task(
            client.call("owned__hold", self.arguments("alice", "active-idle", "active-idle"))
        )

        async def active():
            return (await self.peer_stats())["active_by_phase"].get("active-idle", 0) == 1

        await self.until(active)
        await asyncio.sleep(10.3)
        assert (await self.stats())["sessions"] == 1, "active session expired"
        probe = HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"])
        status, _, reply = await probe.request(
            "/mcp",
            {"jsonrpc": "2.0", "id": "active-probe", "method": "ping"},
            headers=client.headers,
        )
        assert status == 200 and "result" in reply, "active session rejected after idle TTL"
        await probe.close()
        await self.release("active-idle")
        assert "result" in await job
        self.phases.append({"name": "active_session_survives_idle_ttl", "held_seconds": 10.3})

        async def expired():
            s, p = await asyncio.gather(self.stats(), self.peer_stats())
            return s["sessions"] == p["sessions"] == 0 and s["cleanup"]["pending"] == 0

        await self.until(expired, timeout=12)
        status, _, _ = await client.http.request(
            "/mcp",
            {"jsonrpc": "2.0", "id": 55, "method": "ping"},
            headers=client.headers,
        )
        assert status == 404
        await client.http.close()
        self.phases.append({"name": "idle_expiry_reclaims_remote_session", "expired_sessions": 1})
        client = MCP(HTTP("127.0.0.1", self.port, self.env["WAYPILOT_A"]))
        await client.initialize()
        job = asyncio.create_task(
            client.call("owned__hold", self.arguments("alice", "drain", "shutdown"))
        )

        async def issued():
            return (await self.peer_stats())["active_by_phase"].get("shutdown", 0) == 1

        await self.until(issued)
        # Fill a separate response path while its client stops reading. A
        # draining service must also stop blocked writes, not just request reads.
        slow_reader, slow_writer = await asyncio.open_connection("127.0.0.1", self.port, limit=1024)
        slow_writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        slow_writer.write(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n" * 10000)
        await asyncio.sleep(0.1)
        assert not slow_reader.at_eof()
        self.sampling_paused = True
        self.service.process.send_signal(signal.SIGTERM)

        async def draining():
            return (await self.stats())["draining"]

        await self.until(draining)
        await self.release("shutdown")
        assert "result" in await job
        await client.http.close()
        await self.admin.close()
        await asyncio.to_thread(self.service.process.wait, timeout=5)
        assert self.service.process.returncode == 0
        slow_writer.close()
        # The intentionally blocked reader can prevent wait_closed from flushing
        # its queued requests; abort that test connection after server exit.
        slow_writer.transport.abort()
        assert (await self.peer_stats())["sessions"] == 0
        self.phases.append(
            {
                "name": "graceful_shutdown",
                "accepted_call_completed": True,
                "nonreading_client_did_not_block_exit": True,
                "exit_status": 0,
            }
        )

    async def stdio_input_failure(self):
        # Exercise the error exit, not just clean EOF: active HTTP session
        # ownership must reach cleanup even if the JSONL reader fails.
        process = await asyncio.create_subprocess_exec(
            str(Path(self.args.binary).resolve()),
            "serve-stdio",
            "--config",
            str(self.directory / "gateway.toml"),
            "--user",
            "alice",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env,
        )

        async def send(value):
            process.stdin.write(json.dumps(value).encode() + b"\n")
            await process.stdin.drain()

        async def reply():
            return json.loads(await asyncio.wait_for(process.stdout.readline(), 5))

        try:
            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "qualification", "version": "1"},
                    },
                }
            )
            assert "result" in await reply()
            await send({"jsonrpc": "2.0", "method": "notifications/initialized"})
            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "owned__echo",
                        "arguments": self.arguments("alice", "stdio-error", "stdio-error"),
                    },
                }
            )
            assert "result" in await reply()
            assert (await self.peer_stats())["sessions"] == 1
            _, error = await asyncio.wait_for(
                process.communicate(b"x" * (1024 * 1024 + 1) + b"\n"), 6
            )
            assert process.returncode != 0 and b"max line length" in error, error.decode()
            assert (await self.peer_stats())["sessions"] == 0, "stdio error leaked a remote session"
            self.phases.append({"name": "stdio_input_failure_cleanup", "remote_sessions": 0})
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def run(self):
        sampler = None
        try:
            await self.start(self.args.concurrency)
            sampler = asyncio.create_task(self.sample_resources())
            # A peer may close an idle keep-alive socket without a Connection
            # header. Reconnect only when EOF is already known, before sending.
            await self.control.request("/idle")
            await asyncio.sleep(0.1)
            assert (await self.peer_stats())["calls"] == 0
            self.phases.append({"name": "test_client_idle_reconnect", "replayed_requests": 0})
            await self.initialization_cancel()
            await self.clients_start(self.args.clients)
            await self.capacity()
            await self.workflow()
            await self.body_pressure()
            await self.rate_limit()
            await self.payload_pressure()
            await self.cancellation()
            await self.reuse()
            await self.failures()
            await self.deadlines()
            await self.soak()
            await self.churn()
            await self.revoke_and_restart()
            stats = await self.stats()
            await self.expiry_and_shutdown()
            await self.stdio_input_failure()
            traces = None
            if self.collector_http:
                _, _, traces = await self.collector_http.request("/stats")
                assert {
                    "gateway.http",
                    "mcp.gateway.request",
                    "mcp.invoke",
                    "workflow.node",
                } <= traces["trace_names"].keys()
                assert traces["trace_timings"] > 0
            peak_rss = max(sample["rss_bytes"] for sample in self.resources)
            assert peak_rss < self.args.max_rss_mb * 1024 * 1024, (
                peak_rss,
                self.args.max_rss_mb,
            )
            result = {
                "passed": True,
                "environment": {
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "binary_sha256": hashlib.sha256(
                        Path(self.args.binary).read_bytes()
                    ).hexdigest(),
                    "binary": str(Path(self.args.binary).resolve()),
                },
                "target": {
                    "clients": self.args.clients,
                    "concurrent_calls": self.args.concurrency,
                    "seconds": self.args.seconds,
                    "offered_rps": self.args.rate,
                    "rss_limit_mb": self.args.max_rss_mb,
                    "wave_launch_batch": 16,
                    "wave_launch_pause_ms": getattr(self.args, "ramp_ms", 2),
                },
                "phases": self.phases,
                "resources": {
                    "peak_rss_bytes": peak_rss,
                    "peak_fds": max(sample["fds"] for sample in self.resources),
                    "samples": self.resources,
                },
                "tracing": {"ratio": self.args.trace_ratio, "exported": traces},
                "final_stats": stats,
                "trials": self.samples,
            }
            return result
        except BaseException:
            for name, process in [("gateway", self.service), ("peer", self.peer)]:
                if process:
                    process.log.seek(0)
                    self.last_wait_state[name + "_stderr"] = process.log.read()[-12000:]
            raise
        finally:
            self.stop_sampling.set()
            if sampler:
                await sampler
            if self.admin:
                await self.admin.close()
            if self.service:
                self.service.close()
            await self.control.close()
            if self.collector_http:
                await self.collector_http.close()
            self.stack.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=500)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument(
        "--ramp-ms",
        type=float,
        default=2,
        help="pause per 16 burst callers; 0 sends an unpaced connection burst",
    )
    parser.add_argument("--trace-ratio", type=float, default=0)
    parser.add_argument("--max-rss-mb", type=int, default=512)
    parser.add_argument("--output", type=Path, default=Path("target/scale-qualification.json"))
    args = parser.parse_args()
    assert 2 <= args.concurrency < args.clients <= 10000
    assert args.clients >= args.concurrency * 1.34
    assert 0 <= args.trace_ratio <= 1
    assert 0 <= args.ramp_ms <= 100
    assert 1 <= args.seconds <= 3600 and 1 <= args.rate <= 10000
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    needed = args.clients + args.concurrency + 1024
    if soft < needed:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(needed, hard), hard))
    assert resource.getrlimit(resource.RLIMIT_NOFILE)[0] >= needed, (
        "insufficient file descriptor limit"
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    qualification = Qualification(args)
    try:
        result = asyncio.run(qualification.run())
    except BaseException as error:
        args.output.write_text(
            json.dumps(
                {
                    "passed": False,
                    "error": repr(error),
                    "last_wait_state": qualification.last_wait_state,
                    "phases": qualification.phases,
                    "resources": qualification.resources,
                },
                indent=2,
            )
        )
        raise
    args.output.write_text(json.dumps(result, indent=2))
    print(
        json.dumps(
            {key: value for key, value in result.items() if key not in ["trials", "resources"]},
            indent=2,
        )
    )
    print(
        json.dumps(
            {
                "peak_rss_mb": result["resources"]["peak_rss_bytes"] / 1024 / 1024,
                "peak_fds": result["resources"]["peak_fds"],
                "raw": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
