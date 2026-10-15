"""File-backed state-v1 MCP peer. Test updates use atomic file replacement."""

import hashlib
import json
import sys
from pathlib import Path

from state import META, TOOLS

state_file = Path(sys.argv[1])
for line in sys.stdin:
    request = json.loads(line)
    if "id" not in request:
        continue
    method = request.get("method")
    reply = {"jsonrpc": "2.0", "id": request["id"]}
    if method == "initialize":
        reply["result"] = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "state-file", "version": "1"},
        }
    elif method == "tools/list":
        reply["result"] = {
            "tools": TOOLS
            + [
                {"name": name, "inputSchema": {"type": "object"}}
                for name in ["dependency_index", "source_index"]
            ]
        }
    elif method == "tools/call":
        params = request["params"]
        name, arguments = params["name"], params["arguments"]
        meta = {}
        if name in ["state_read", "state_version"]:
            query = arguments.get("arguments", {}) if name == "state_version" else arguments
            key = query.get("key", "a")
            state = json.loads(state_file.read_text())[key]
            value = {"value": state["value"]} if name == "state_read" else {}
            meta = {META: {"version": 1, "resources": {key: str(state["revision"])}}}
        else:
            value = {
                "fingerprint": hashlib.sha256(
                    json.dumps(arguments["value"], sort_keys=True).encode()
                ).hexdigest()
            }
        reply["result"] = {
            "content": [{"type": "text", "text": json.dumps(value)}],
            "structuredContent": value,
            "isError": False,
            "_meta": meta,
        }
    else:
        reply["error"] = {"code": -32601, "message": "unsupported"}
    print(json.dumps(reply), flush=True)
