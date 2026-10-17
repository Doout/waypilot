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
]


class HR:
    def __init__(self, delay_ms):
        self.delay_ms = delay_ms
        self.epoch = uuid.uuid4().hex
        self.revisions = Counter()
        self.balances = {}
        self.calls = Counter()
        self.cases = []
        self.events = deque(maxlen=20000)
        self.active = self.peak = 0
        self.hold_tool = None
        self.release = asyncio.Event()
        self.release.set()
        self.fail_next = None
        self.validators = {}
        for definition in TOOLS:
            schema = definition["inputSchema"]
            implementation = validator_for(schema)
            implementation.check_schema(schema)
            self.validators[definition["name"]] = implementation(schema)

    def read(self, name, args):
        employee = args["employee_id"]
        number = int(employee.removeprefix("E-"))
        region = "EU" if number % 2 == 0 else "US"
        common = {"employee_id": employee, "region": region}
        values = {
            "employee": dict(common, status="active", department="engineering"),
            "leave_balance": dict(common, days=self.balances.get(employee, 8 if number % 3 else 2)),
            "leave_policy": dict(common, approval_required=True, max_days=20),
            "benefit_options": dict(common, plans=["standard", "extended"]),
        }
        return values[name], f"{name}:{employee}"

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
            if name == "record_case":
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
                await asyncio.sleep((1 if checking else self.delay_ms) / 1000)
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
        return {"updated": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=250)
    parser.add_argument("--stdio", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args, HR(args.delay_ms), TOOLS, "waypilot-hr-workbench"))
