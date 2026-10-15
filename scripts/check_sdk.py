"""Interoperability with the independent official MCP Python SDK (classic protocol)."""
import asyncio
import json
import os
from pathlib import Path
import sys

import httpx2
from mcp import Client as SDKClient
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from mcp_demo import Demo


async def check(demo):
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {demo.token}"}, timeout=30) as http:
        async with SDKClient(streamable_http_client(demo.base + "/mcp", http_client=http)) as client:
            tools = await client.list_tools()
            assert "waypilot__inspect" in {tool.name for tool in tools.tools}
            first = (await client.call_tool("owned__echo", {"value": "official-sdk-http"})).model_dump(by_alias=True)
            second = (await client.call_tool("owned__echo", {"value": "official-sdk-http"})).model_dump(by_alias=True)
            assert first["structuredContent"] == second["structuredContent"]
            assert second["_meta"]["io.waypilot/runtime"]["disposition"] == "reused"
            workflow = (await client.call_tool("waypilot__inspect", {"manifest": "serde", "source": "fn main() {}"})).model_dump(by_alias=True)
            assert workflow.get("isError") is not True
            assert len(workflow["structuredContent"]["outputs"]) == 2
    parameters = StdioServerParameters(command=demo.binary, args=["serve-stdio", "--config", str(demo.config), "--user", "developer"], env=demo.env)
    async with SDKClient(stdio_client(parameters)) as client:
        tools = await client.list_tools()
        assert "owned__counter" in {tool.name for tool in tools.tools}
        result = (await client.call_tool("owned__echo", {"value": "official-sdk-stdio"})).model_dump(by_alias=True)
        assert result["structuredContent"] == {"value": "official-sdk-stdio"}
    return {"sdk": "official mcp==2.0.1", "protocol": "negotiated classic 2025-11-25", "ingress": ["streamable_http", "stdio"], "upstream": ["streamable_http_sse", "stdio"], "workflow": "passed", "result_reuse": "passed"}


if __name__ == "__main__":
    with Demo(sys.argv[1], os.environ["WAYPILOT_PYTHON"]) as demo:
        print(json.dumps(asyncio.run(check(demo))))
