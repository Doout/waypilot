"""Connected MCP clients retain graph observations, reject stale writes, and recover."""

import argparse
import asyncio
import hashlib
import json
import resource
import sys
import time
from pathlib import Path

import psutil
from net import HTTP, MCP

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.sandbox import Sandbox

META = "io.waypilot/observations"


async def run(args):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, max(soft, 8192)), hard))
    phases, resources = [], []
    with Sandbox(
        args.binary,
        batch_validation=True,
        max_sessions=args.clients + 16,
        max_in_flight=args.concurrency,
        timeout_ms=30000,
        upstream_calls=args.upstream_calls,
        environment={},
    ) as sandbox:
        port = int(sandbox.base.rsplit(":", 1)[1])
        clients = [
            MCP(HTTP("127.0.0.1", port, sandbox.tokens["investigator"]))
            for _ in range(args.clients)
        ]
        process = psutil.Process(sandbox.service.process.pid)
        stop = asyncio.Event()
        slots = asyncio.Semaphore(32)

        async def initialize(client):
            async with slots:
                await client.initialize()

        async def sample():
            while not stop.is_set():
                resources.append({"rss_bytes": process.memory_info().rss, "fds": process.num_fds()})
                assert resources[-1]["rss_bytes"] < 512 * 1024 * 1024, (
                    "gateway exceeded 512 MiB RSS"
                )
                try:
                    await asyncio.wait_for(stop.wait(), 0.2)
                except TimeoutError:
                    pass

        sampler = asyncio.create_task(sample())
        receipts = {}
        signatures = {}
        peak_pending = pending = 0

        async def phase(label, reading=False, stale=False):
            nonlocal pending, peak_pending
            before = await asyncio.to_thread(sandbox.peer_stats)
            latencies = []

            async def one(index):
                nonlocal pending, peak_pending
                pending += 1
                peak_pending = max(peak_pending, pending)
                started = time.monotonic()
                try:
                    client = clients[index]
                    incident, service, symptom = (
                        ("INC-101", "checkout", "errors")
                        if index % 2 == 0
                        else ("INC-202", "image-worker", "backlog")
                    )
                    if reading:
                        reply = await client.call(
                            "waypilot__triage",
                            {"service": service, "symptom": symptom},
                            meta={META: {"capture": True}},
                        )
                        assert "result" in reply, reply
                        result = reply["result"]
                        receipts[index] = result["_meta"][META]["receipt"]
                        outputs = {
                            name: value["structuredContent"]
                            for name, value in result["structuredContent"]["outputs"].items()
                        }
                        expected_action = (
                            "restore_pool_limit" if index % 2 == 0 else "scale_workers"
                        )
                        assert outputs["assessment"]["recommended_action"] == expected_action
                        signature = hashlib.sha256(
                            json.dumps(outputs, sort_keys=True).encode()
                        ).hexdigest()
                        if index in signatures:
                            assert signature == signatures[index], (
                                "same-value revision changed business result"
                            )
                        signatures[index] = signature
                    else:
                        reply = await client.call(
                            "ops__append_note",
                            {
                                "incident_id": incident,
                                "summary": f"[{label}/{index}] guarded observation",
                            },
                            meta={META: {"require": [receipts[index]]}},
                        )
                        if stale:
                            assert reply.get("error", {}).get("code") == -32010, reply
                        else:
                            assert "result" in reply and not reply["result"].get("isError"), reply
                            assert reply["result"]["structuredContent"]["incident_id"] == incident
                    latencies.append((time.monotonic() - started) * 1000)
                finally:
                    pending -= 1

            for offset in range(0, args.clients, args.concurrency):
                tasks = []
                try:
                    for start in range(offset, min(offset + args.concurrency, args.clients), 16):
                        tasks.extend(
                            asyncio.create_task(one(index))
                            for index in range(
                                start, min(start + 16, offset + args.concurrency, args.clients)
                            )
                        )
                        await asyncio.sleep(0.002)
                    await asyncio.gather(*tasks)
                finally:
                    await asyncio.gather(*tasks, return_exceptions=True)
                # Entry, business and validator tokens refill between bursts.
                await asyncio.sleep(len(tasks) * (13 if reading else 7) / 10000)
            after = await asyncio.to_thread(sandbox.peer_stats)
            writes = after["calls"].get("append_note", 0) - before["calls"].get("append_note", 0)
            assert writes == (0 if reading or stale else args.clients), (label, writes)
            stats = await asyncio.to_thread(sandbox.stats)
            assert stats["sessions"] == args.clients
            assert 0 < stats["observation_bytes"] <= 16 * 1024 * 1024
            values = sorted(latencies)
            row = {
                "phase": label,
                "responses": len(values),
                "writes": writes,
                "p50_ms": values[(len(values) - 1) // 2],
                "p95_ms": values[int((len(values) - 1) * 0.95)],
                "state_checks": stats["state_checks"],
                "state_batches": stats["state_batches"],
                "observation_bytes": stats["observation_bytes"],
            }
            phases.append(row)
            print(json.dumps(row), flush=True)

        try:
            await asyncio.gather(*(initialize(client) for client in clients))
            await phase("capture", reading=True)
            await phase("unchanged_write")
            for service in ["checkout", "image-worker"]:
                await asyncio.to_thread(sandbox.mutate, service=service, touch=True)
            await phase("stale_write", stale=True)
            await phase("recapture", reading=True)
            await phase("recovered_write")
            notes = (await asyncio.to_thread(sandbox.peer_stats))["notes"]
            expected = {
                f"[{label}/{index}] guarded observation"
                for label in ["unchanged_write", "recovered_write"]
                for index in range(args.clients)
            }
            assert (
                len(notes) == len(expected) and {note["summary"] for note in notes} == expected
            ), "missing or duplicated upstream effect"
        except BaseException as error:
            diagnostics = {
                "status": "failed",
                "error": str(error),
                "phases": phases,
                "gateway": await asyncio.to_thread(sandbox.stats),
                "peer": await asyncio.to_thread(sandbox.peer_stats),
                "resources": resources,
            }
            for name, service in [
                ("gateway_log", sandbox.service),
                ("peer_log", sandbox.peer_process),
            ]:
                service.log.seek(0)
                log = service.log.read()[-12000:]
                for token in sandbox.tokens.values():
                    log = log.replace(token, "<redacted>")
                diagnostics[name] = log
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(diagnostics, indent=2) + "\n")
            raise
        finally:

            async def close(client):
                async with slots:
                    await client.close()

            await asyncio.gather(*(close(client) for client in clients))
            stop.set()
            await sampler
        until = time.monotonic() + 15
        while True:
            stats = await asyncio.to_thread(sandbox.stats)
            if stats["sessions"] == 0 and stats["cleanup"]["pending"] == 0:
                break
            assert time.monotonic() < until, "sessions or upstream cleanup did not drain"
            await asyncio.sleep(0.05)
        assert stats["observation_bytes"] == 0 and stats["active_requests"] == 0
        assert stats["cleanup"]["errors"] == 0 and stats["cleanup"]["dropped"] == 0
        return {
            "status": "passed",
            "clients": args.clients,
            "upstream_calls_budget": args.upstream_calls,
            "max_pending_calls": peak_pending,
            "phases": phases,
            "exact_notes": len(notes),
            "peak_rss_bytes": max(row["rss_bytes"] for row in resources),
            "peak_fds": max(row["fds"] for row in resources),
            "final": stats,
            "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
            "limits": "Cooperative local HTTP fixture; mutation barriers and pending-request concurrency, no atomic remote write guarantee",
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=500)
    parser.add_argument("--upstream-calls", type=int, default=64)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.concurrency <= min(args.clients, 500) or not 1 <= args.clients <= 2000:
        parser.error("clients 1..2000; concurrency 1..min(clients,500) required")
    if not 1 <= args.upstream_calls <= 512:
        parser.error("upstream-calls 1..512 required")
    result = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
