"""State-v1 contract for the owned HTTP fixture; never used by the gateway itself."""

import threading
import time
import uuid

META = "io.waypilot/state"
TOOLS = [
    {"name": name, "inputSchema": {"type": "object"}} for name in ["state_read", "state_version"]
]


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.epoch = uuid.uuid4().hex
        self.resources = {
            name: {"revision": 0, "value": name, "checks": 0, "reads": 0} for name in ["a", "b"]
        }

    def control(self, arguments):
        with self.lock:
            state = self.resources[arguments.get("key", "a")]
            if arguments.get("touch") or "value" in arguments:
                state["revision"] += 1
            for key in [
                "value",
                "read_ms",
                "check_ms",
                "read_mode",
                "check_mode",
                "change_at",
                "change_after_read",
            ]:
                if key in arguments:
                    state[key] = arguments[key]
            return {key: dict(value) for key, value in self.resources.items()}

    def call(self, tool, arguments):
        query = arguments.get("arguments", {}) if tool == "state_version" else arguments
        key = query.get("key", "a")
        with self.lock:
            state = self.resources[key]
            checking = tool == "state_version"
            state["checks" if checking else "reads"] += 1
            if checking and state.get("change_at") == state["checks"]:
                state["revision"] += 1
            witness = {"version": 1, "resources": {key: f"{self.epoch}:{state['revision']}"}}
            result = {"value": state["value"]}
            delay = state.get("check_ms" if checking else "read_ms", 0)
            mode = state.get("check_mode" if checking else "read_mode", "valid")
            if not checking and state.get("change_after_read"):
                state["revision"] += 1
        time.sleep(delay / 1000)
        if mode == "malformed":
            witness["resources"][key] = 123
        if mode == "wrong_resource":
            witness["resources"] = {"other": "1"}
        if mode == "oversized":
            witness["resources"] = {str(index): "1" for index in range(65)}
        response = {
            "content": [{"type": "text", "text": "version" if checking else str(result)}],
            "structuredContent": {} if checking else result,
            "isError": mode == "error",
        }
        if mode != "missing":
            response["_meta"] = {META: witness}
        return response
