"""Owned incident workbench, served by the official MCP SDK. No production access."""

import argparse
import asyncio
import json
import os
import secrets
import socket
import time
import uuid
from collections import Counter, deque
from copy import deepcopy

import uvicorn
from jsonschema import ValidationError, validate
from jsonschema.validators import validator_for
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from starlette.responses import JSONResponse
from starlette.routing import Route

STATE_META = "io.waypilot/state"
INCIDENTS = {
    "INC-101": {"service": "checkout", "symptom": "errors", "title": "Checkout 5xx after release"},
    "INC-202": {
        "service": "image-worker",
        "symptom": "backlog",
        "title": "Image processing queue growing",
    },
}
RUNBOOKS = {
    "pool": {
        "id": "pool",
        "title": "Connection pool exhaustion",
        "action": "restore_pool_limit",
        "checks": [
            "Compare pool utilization with the configured connection limit",
            "Prepare a reviewed configuration rollback",
        ],
    },
    "capacity": {
        "id": "capacity",
        "title": "Worker capacity",
        "action": "scale_workers",
        "checks": ["Check arrival and processing rates", "Prepare a reviewed worker scale-up"],
    },
    "healthy": {
        "id": "healthy",
        "title": "Recovered service",
        "action": "observe",
        "checks": ["Continue monitoring; no change needed"],
    },
}


def tool(name, description, properties, required=None, readonly=True):
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required or list(properties),
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": readonly, "destructiveHint": not readonly},
    }


STRING = {"type": "string", "minLength": 1, "maxLength": 200}
TOOLS = [
    tool(
        "get_incident", "Read incident details and the affected service.", {"incident_id": STRING}
    ),
    tool(
        "get_metrics",
        "Read the service's current error, pool and queue metrics.",
        {"service": STRING},
    ),
    tool(
        "get_deployment", "Read current and previous deployment configuration.", {"service": STRING}
    ),
    tool(
        "search_runbooks",
        "Find operational runbooks for errors or backlog.",
        {"symptom": {"enum": ["errors", "backlog"]}},
    ),
    tool(
        "get_runbook",
        "Read the complete runbook selected from an assessment.",
        {"id": {"enum": list(RUNBOOKS)}},
    ),
    tool(
        "assess",
        "Compare supplied metric and deployment observations; has no external reads or effects.",
        {"metrics": {"type": "object"}, "deployment": {"type": "object"}},
    ),
    tool(
        "append_note",
        "Append one local incident note. Does not deploy or modify any real system.",
        {"incident_id": STRING, "summary": {"type": "string", "minLength": 1, "maxLength": 4000}},
        readonly=False,
    ),
    tool(
        "state_batch",
        "Gateway-only state-batch-v1 validator; one snapshot for all requested observations.",
        {
            "version": {"const": 1},
            "observations": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": {
                    "type": "object",
                    "properties": {"id": STRING, "tool": STRING, "arguments": {"type": "object"}},
                    "required": ["id", "tool", "arguments"],
                    "additionalProperties": False,
                },
            },
        },
    ),
    tool(
        "state_version",
        "Gateway-only state-v1 validator for an eligible read.",
        {"tool": STRING, "arguments": {"type": "object"}},
    ),
]
CATALOG = {item["name"]: item for item in TOOLS}
STATEFUL = {"get_incident", "get_metrics", "get_deployment", "search_runbooks", "get_runbook"}


class Workbench:
    def __init__(self, delay_ms=30):
        self.delay_ms = delay_ms
        self.validators = None
        if os.environ.get("WORKBENCH_COMPILED_SCHEMAS", "1") == "1":
            self.validators = {}
            for name, definition in CATALOG.items():
                schema = definition["inputSchema"]
                implementation = validator_for(schema)
                implementation.check_schema(schema)
                self.validators[name] = implementation(schema)
        self.epoch = uuid.uuid4().hex
        self.revisions = Counter()
        self.metrics = {
            "checkout": {
                "service": "checkout",
                "error_rate": 0.18,
                "pool_utilization": 0.99,
                "queue_depth": 0,
            },
            "image-worker": {
                "service": "image-worker",
                "error_rate": 0.001,
                "pool_utilization": 0.2,
                "queue_depth": 12000,
            },
        }
        self.deployments = {
            "checkout": {
                "service": "checkout",
                "release": "checkout-42",
                "connection_limit": 5,
                "previous_connection_limit": 50,
                "workers": 4,
            },
            "image-worker": {
                "service": "image-worker",
                "release": "image-17",
                "connection_limit": 50,
                "previous_connection_limit": 50,
                "workers": 1,
            },
        }
        self.calls = Counter()
        self.notes = []
        self.events = deque(maxlen=256)
        self.active = self.peak = 0
        self.fail_next = set()

    def read(self, name, arguments):
        if name == "get_incident":
            key = arguments["incident_id"]
            return dict(INCIDENTS[key], incident_id=key), "incident:" + key
        if name == "get_metrics":
            return self.metrics[arguments["service"]], "metrics:" + arguments["service"]
        if name == "get_deployment":
            return self.deployments[arguments["service"]], "deployment:" + arguments["service"]
        if name == "search_runbooks":
            ids = (
                ["pool", "healthy"] if arguments["symptom"] == "errors" else ["capacity", "healthy"]
            )
            return {"matches": [RUNBOOKS[key] for key in ids]}, "runbooks"
        if name == "get_runbook":
            return RUNBOOKS[arguments["id"]], "runbooks"
        raise ValueError("tool has no state contract")

    def validate(self, name, arguments):
        if self.validators is None:
            validate(arguments, CATALOG[name]["inputSchema"])
        else:
            self.validators[name].validate(arguments)

    async def call(self, name, arguments):
        started = time.monotonic_ns()
        self.calls[name] += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            self.validate(name, arguments)
            if name in self.fail_next:
                self.fail_next.remove(name)
                raise ValueError("injected workbench failure")
            checking = name in {"state_version", "state_batch"}
            evidence = None
            batch = None
            if name == "state_batch":
                batch = []
                for observation in arguments["observations"]:
                    read_name, query = observation["tool"], observation["arguments"]
                    if read_name not in STATEFUL:
                        raise ValueError("unsupported batch target")
                    self.validate(read_name, query)
                    _, resource = self.read(read_name, query)
                    batch.append(
                        {
                            "id": observation["id"],
                            "state": {
                                "version": 1,
                                "resources": {resource: f"{self.epoch}:{self.revisions[resource]}"},
                            },
                        }
                    )
                value = {}
            elif name in STATEFUL or checking:
                read_name = arguments["tool"] if checking else name
                query = arguments["arguments"] if checking else arguments
                if read_name not in STATEFUL:
                    raise ValueError("unsupported state validator target")
                self.validate(read_name, query)
                # Capture value and its evidence without yielding; mutation is on this event loop.
                value, resource = self.read(read_name, query)
                value = {} if checking else deepcopy(value)
                evidence = {
                    "version": 1,
                    "resources": {resource: f"{self.epoch}:{self.revisions[resource]}"},
                }
            elif name == "assess":
                metrics, deployment = arguments["metrics"], arguments["deployment"]
                if metrics["service"] != deployment["service"]:
                    raise ValueError("observations refer to different services")
                if (
                    metrics["error_rate"] > 0.05
                    and metrics["pool_utilization"] > 0.95
                    and deployment["connection_limit"] < deployment["previous_connection_limit"]
                ):
                    diagnosis, action, runbook = "pool_exhaustion", "restore_pool_limit", "pool"
                elif metrics["queue_depth"] > 1000:
                    diagnosis, action, runbook = "worker_saturation", "scale_workers", "capacity"
                else:
                    diagnosis, action, runbook = "healthy", "observe", "healthy"
                value = {
                    "service": metrics["service"],
                    "diagnosis": diagnosis,
                    "recommended_action": action,
                    "runbook_id": runbook,
                }
            elif name == "append_note":
                if arguments["incident_id"] not in INCIDENTS:
                    raise ValueError("unknown incident")
                if len(self.notes) >= 10000:
                    raise ValueError("local note capacity reached")
                value = dict(arguments, note_id=f"note-{len(self.notes) + 1}")
                self.notes.append(deepcopy(value))
            else:
                raise ValueError("unknown tool")
            await asyncio.sleep((1 if checking else self.delay_ms) / 1000)
            result = {
                "content": [{"type": "text", "text": json.dumps(value)}],
                "structuredContent": value,
                "isError": False,
            }
            if batch is not None:
                result["_meta"] = {"io.waypilot/state-batch": {"version": 1, "observations": batch}}
            if evidence is not None:
                result["_meta"] = {STATE_META: evidence}
            return result
        except (KeyError, ValueError, TypeError, ValidationError) as error:
            # Synthetic data only; do not echo arbitrary supplied values in errors.
            return {
                "content": [
                    {"type": "text", "text": "invalid workbench call: " + type(error).__name__}
                ],
                "isError": True,
            }
        finally:
            self.active -= 1
            self.events.append({"tool": name, "start_ns": started, "end_ns": time.monotonic_ns()})

    def snapshot(self, summary=False):
        result = {
            "calls": dict(self.calls),
            "note_count": len(self.notes),
            "active": self.active,
            "peak_active": self.peak,
        }
        if not summary:
            result.update(notes=deepcopy(self.notes), events=list(self.events))
        return result

    def control(self, value):
        service = value.get("service", "checkout")
        if service not in self.metrics:
            raise ValueError("unknown service")
        if value.get("fail_next"):
            if value["fail_next"] not in CATALOG:
                raise ValueError("unknown tool")
            self.fail_next.add(value["fail_next"])
        if value.get("touch") or "recovered" in value:
            self.revisions["metrics:" + service] += 1
        if "recovered" in value:
            self.metrics[service]["error_rate"] = 0.001 if value["recovered"] else 0.18
            self.metrics[service]["pool_utilization"] = 0.2 if value["recovered"] else 0.99
            self.metrics[service]["queue_depth"] = 0 if value["recovered"] else 12000
        return {"revisions": dict(self.revisions)}


def server(workbench, definitions=TOOLS, name="waypilot-incident-workbench"):
    async def list_tools(_context, _params):
        return types.ListToolsResult(
            tools=[types.Tool.model_validate(item) for item in definitions]
        )

    async def call_tool(_context, params):
        return types.CallToolResult.model_validate(
            await workbench.call(params.name, params.arguments or {})
        )

    return Server(name, version="1", on_list_tools=list_tools, on_call_tool=call_tool)


async def main(args, workbench=None, definitions=TOOLS, name="waypilot-incident-workbench"):
    workbench = workbench or Workbench(args.delay_ms)
    mcp = server(workbench, definitions, name)
    if args.stdio:
        async with stdio_server() as (read, write):
            await mcp.run(read, write, mcp.create_initialization_options())
        return
    control_token = os.environ["WORKBENCH_CONTROL_TOKEN"]

    async def control(request):
        if not secrets.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + control_token
        ):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        if request.method == "GET":
            return JSONResponse(
                workbench.snapshot(summary=request.query_params.get("summary") == "1")
            )
        if len(await request.body()) > 4096:
            return JSONResponse({"error": "body too large"}, status_code=413)
        try:
            return JSONResponse(workbench.control(await request.json()))
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid control"}, status_code=400)

    app = mcp.streamable_http_app(
        json_response=True,
        custom_starlette_routes=[Route("/control", control, methods=["GET", "POST"])],
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", args.port))
        listener.listen(128)

        class ReadyServer(uvicorn.Server):
            async def startup(self, sockets=None):
                await super().startup(sockets)
                print(json.dumps({"listen": f"127.0.0.1:{listener.getsockname()[1]}"}), flush=True)

        await ReadyServer(uvicorn.Config(app, log_level="error", lifespan="on")).serve(
            sockets=[listener]
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=30)
    parser.add_argument("--stdio", action="store_true")
    asyncio.run(main(parser.parse_args()))
