"""Synthetic HR MCP service. No production HR access or personal data."""

import argparse
import asyncio
import json
import sys
import time
import uuid
from collections import Counter, deque
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agents.workbench import main, tool
from jsonschema.validators import validator_for

STRING = {"type": "string", "minLength": 1, "maxLength": 64}
READS = ["employee", "leave_balance", "leave_policy", "benefit_options"]


def read_delay(value):
    """CLI override for one synthetic read; writes and validators stay separate."""
    try:
        name, milliseconds = value.split("=", 1)
        milliseconds = int(milliseconds)
        if name not in READS or not 0 <= milliseconds <= 5000:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("expected HR_READ=MS with MS in 0..5000") from None
    return name, milliseconds


TOOLS = [tool(name, "Read synthetic HR " + name, {"employee_id": STRING}) for name in READS]
TOOLS += [
    tool(
        "record_case",
        "Append one local HR case; never eligible for speculation.",
        {
            "employee_id": STRING,
            "kind": {
                "type": "string",
                "enum": ["leave_guidance", "needs_review", "benefits_guidance"],
            },
            "days": {"type": "integer", "minimum": 0, "maximum": 30},
        },
        readonly=False,
    ),
    tool(
        "state_version",
        "Cooperative state-v1 validator.",
        {"tool": STRING, "arguments": {"type": "object"}},
    ),
    tool(
        "state_batch",
        "Cooperative state-batch-v1 validator; does not execute the reads.",
        {
            "version": {"const": 1},
            "observations": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": {
                    "type": "object",
                    "properties": {
                        "id": STRING,
                        "tool": STRING,
                        "arguments": {"type": "object"},
                    },
                    "required": ["id", "tool", "arguments"],
                    "additionalProperties": False,
                },
            },
        },
    ),
]


class HR:
    def __init__(self, delay_ms, state_ms=1, read_delays=None):
        self.delay_ms = delay_ms
        self.state_ms = state_ms
        self.read_delays = read_delays or {}
        self.epoch = uuid.uuid4().hex
        self.revisions = Counter()
        self.balances = {}
        self.aliases = {}
        self.calls = Counter()
        self.cases = []
        self.events = deque(maxlen=20000)
        self.active = self.peak = 0
        self.hold_tool = None
        self.release = asyncio.Event()
        self.release.set()
        self.fail_next = None
        self.batch_mode = "valid"
        self.validators = {}
        for definition in TOOLS:
            schema = definition["inputSchema"]
            implementation = validator_for(schema)
            implementation.check_schema(schema)
            self.validators[definition["name"]] = implementation(schema)

    def read(self, name, args):
        requested = args["employee_id"]
        employee = self.aliases.get(requested, requested) if name == "employee" else requested
        number = int(employee.removeprefix("E-"))
        region = "EU" if number % 2 == 0 else "US"
        common = {"employee_id": employee, "region": region}
        values = {
            "employee": dict(common, status="active", department="engineering"),
            "leave_balance": dict(common, days=self.balances.get(employee, 8 if number % 3 else 2)),
            "leave_policy": dict(common, approval_required=True, max_days=20),
            "benefit_options": dict(common, plans=["standard", "extended"]),
        }
        return values[name], f"{name}:{requested}"

    def read_delay_ms(self, name, arguments):
        return self.read_delays.get(name, self.delay_ms)

    async def call(self, name, arguments):
        started = time.monotonic_ns()
        self.calls[name] += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.events.append(
            {
                "phase": "start",
                "tool": name,
                "employee_id": arguments.get("employee_id"),
                "ns": started,
            }
        )
        try:
            self.validators[name].validate(arguments)
            if self.fail_next == name:
                self.fail_next = None
                raise ValueError("injected read error")
            state = None
            if name == "state_batch":
                if self.hold_tool == name:
                    await self.release.wait()
                await asyncio.sleep(self.state_ms / 1000)
                observations = []
                for observation in arguments["observations"]:
                    target, query = observation["tool"], observation["arguments"]
                    if target not in READS:
                        raise ValueError("invalid batch state target")
                    self.validators[target].validate(query)
                    _, resource = self.read(target, query)
                    observations.append(
                        {
                            "id": observation["id"],
                            "state": {
                                "version": 1,
                                "resources": {resource: f"{self.epoch}:{self.revisions[resource]}"},
                            },
                        }
                    )
                if self.batch_mode == "missing":
                    observations.pop()
                elif self.batch_mode == "duplicate":
                    observations[-1]["id"] = observations[0]["id"]
                elif self.batch_mode == "changed":
                    observations[0]["state"]["resources"] = {"wrong": "version"}
                elif self.batch_mode == "reorder":
                    observations.reverse()
                return {
                    "content": [],
                    "isError": False,
                    "_meta": {
                        "io.waypilot/state-batch": {"version": 1, "observations": observations}
                    },
                }
            elif name == "record_case":
                if len(self.cases) >= 10000:
                    raise ValueError("case capacity")
                value = dict(arguments, case_id=len(self.cases) + 1)
                self.cases.append(deepcopy(value))
            else:
                checking = name == "state_version"
                target = arguments["tool"] if checking else name
                query = arguments["arguments"] if checking else arguments
                if target not in READS:
                    raise ValueError("invalid state target")
                self.validators[target].validate(query)
                value, resource = self.read(target, query)
                state = {
                    "version": 1,
                    "resources": {resource: f"{self.epoch}:{self.revisions[resource]}"},
                }
                if checking:
                    value = {}
                if self.hold_tool == name:
                    await self.release.wait()
                delay = self.state_ms if checking else self.read_delay_ms(name, query)
                await asyncio.sleep(delay / 1000)
            result = {
                "content": [{"type": "text", "text": json.dumps(value)}],
                "structuredContent": value,
                "isError": False,
            }
            if state:
                result["_meta"] = {"io.waypilot/state": state}
            return result
        except Exception as error:
            return {
                "content": [{"type": "text", "text": "invalid HR call: " + type(error).__name__}],
                "isError": True,
            }
        finally:
            self.active -= 1
            self.events.append(
                {
                    "phase": "end",
                    "tool": name,
                    "employee_id": arguments.get("employee_id"),
                    "ns": time.monotonic_ns(),
                }
            )

    def snapshot(self, summary=False):
        result = {
            "calls": dict(self.calls),
            "case_count": len(self.cases),
            "active": self.active,
            "peak_active": self.peak,
        }
        if not summary:
            result.update(cases=deepcopy(self.cases), events=list(self.events))
        return result

    def control(self, value):
        if "alias_id" in value:
            alias, canonical = value["alias_id"], value["canonical_id"]
            int(alias.removeprefix("E-"))
            int(canonical.removeprefix("E-"))
            self.aliases[alias] = canonical
            self.revisions["employee:" + alias] += 1
        if "hold_tool" in value:
            self.hold_tool = value["hold_tool"]
            if self.hold_tool:
                self.release.clear()
            else:
                self.release.set()
        if "days" in value or value.get("touch"):
            employee = value["employee_id"]
            self.revisions["leave_balance:" + employee] += 1
            if "days" in value:
                self.balances[employee] = value["days"]
        if "fail_next" in value:
            self.fail_next = value["fail_next"]
        if "batch_mode" in value:
            self.batch_mode = value["batch_mode"]
        return {"updated": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=250)
    parser.add_argument("--state-ms", type=int, choices=range(0, 1001), default=1)
    parser.add_argument("--read-ms", type=read_delay, action="append", default=[])
    parser.add_argument("--stdio", action="store_true")
    args = parser.parse_args()
    asyncio.run(
        main(
            args,
            HR(args.delay_ms, args.state_ms, dict(args.read_ms)),
            TOOLS,
            "waypilot-hr-workbench",
        )
    )
