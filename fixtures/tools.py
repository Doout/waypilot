"""Local MCP fixture: explicit pure tools over immutable supplied content.

The delays model startup and remote I/O; this is not a production tool server.
Only JSON-RPC messages are written to stdout.
"""
import argparse
import hashlib
import json
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["dependencies", "source"], required=True)
    parser.add_argument("--boot-ms", type=int, default=60)
    parser.add_argument("--call-ms", type=int, default=15)
    args = parser.parse_args()
    time.sleep(args.boot_ms / 1000)
    names = (
        ["parse_manifest", "dependency_index"]
        if args.role == "dependencies"
        else ["parse_source", "source_index"]
    )
    initialized = False
    for line in sys.stdin:
        request = json.loads(line)
        method = request.get("method")
        if "id" not in request:
            if method == "notifications/initialized":
                initialized = True
            continue
        reply = {"jsonrpc": "2.0", "id": request["id"]}
        if method == "initialize":
            reply["result"] = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": f"waypilot-{args.role}", "version": "1"},
            }
        elif not initialized:
            reply["error"] = {"code": -32002, "message": "not initialized"}
        elif method == "tools/list":
            reply["result"] = {
                "tools": [
                    {
                        "name": name,
                        "inputSchema": {
                            "type": "object",
                            "properties": {"value": {}},
                            "required": ["value"],
                            "additionalProperties": False,
                        },
                        "annotations": {"readOnlyHint": True},
                    }
                    for name in names
                ]
            }
        elif method == "tools/call":
            params = request.get("params", {})
            name = params.get("name")
            arguments = params.get("arguments", {})
            if name not in names or set(arguments) != {"value"}:
                reply["error"] = {"code": -32602, "message": "invalid tool or arguments"}
            else:
                time.sleep(args.call_ms / 1000)
                value = arguments["value"]
                if name.startswith("parse_"):
                    result = {"text": value, "words": str(value).split()}
                else:
                    encoded = json.dumps(value, sort_keys=True).encode()
                    result = {"kind": args.role, "fingerprint": hashlib.sha256(encoded).hexdigest()}
                reply["result"] = {
                    "content": [{"type": "text", "text": json.dumps(result)}],
                    "structuredContent": result,
                    "isError": False,
                }
        else:
            reply["error"] = {"code": -32601, "message": "method not found"}
        print(json.dumps(reply), flush=True)


if __name__ == "__main__":
    main()
