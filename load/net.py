"""Independent, bounded HTTP/1.1 JSON client for owned load-test peers."""

import asyncio
import json


class HTTP:
    def __init__(self, host, port, token=None):
        self.host, self.port, self.token = host, port, token
        self.reader = self.writer = None
        self.lock = asyncio.Lock()

    async def request(self, path, body=None, method=None, headers=None, timeout=30):
        async with self.lock, asyncio.timeout(timeout):
            if self.writer is not None and (self.writer.is_closing() or self.reader.at_eof()):
                await self.close()
            if self.writer is None:
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port, limit=65536
                )
            data = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
            fields = {
                "Host": f"{self.host}:{self.port}",
                "Accept": "application/json, text/event-stream",
                "Connection": "keep-alive",
                "Content-Length": str(len(data)),
            }
            if self.token:
                fields["Authorization"] = "Bearer " + self.token
            if body is not None:
                fields["Content-Type"] = "application/json"
            fields.update(headers or {})
            head = (
                f"{method or ('GET' if body is None else 'POST')} {path} HTTP/1.1\r\n"
                + "".join(f"{key}: {value}\r\n" for key, value in fields.items())
                + "\r\n"
            )
            self.writer.write(head.encode() + data)
            await self.writer.drain()
            head = await self.reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin1").split("\r\n")
            status = int(lines[0].split()[1])
            response_headers = dict(
                (name.lower(), value)
                for line in lines[1:]
                if ": " in line
                for name, value in [line.split(": ", 1)]
            )
            if "content-length" in response_headers:
                size = int(response_headers["content-length"])
                assert 0 <= size <= 16 * 1024 * 1024
                data = await self.reader.readexactly(size)
            elif response_headers.get("transfer-encoding") == "chunked":
                chunks, size = [], 0
                while True:
                    length = int((await self.reader.readline()).split(b";")[0], 16)
                    if not length:
                        assert await self.reader.readline() == b"\r\n"
                        break
                    size += length
                    assert size <= 16 * 1024 * 1024
                    chunks.append(await self.reader.readexactly(length))
                    assert await self.reader.readexactly(2) == b"\r\n"
                data = b"".join(chunks)
            elif status in (202, 204, 304):
                data = b""
            else:
                raise AssertionError(f"unframed HTTP response {status}")
            if response_headers.get("connection") == "close":
                await self.close()
            value = (
                json.loads(data)
                if data and "application/json" in response_headers.get("content-type", "")
                else None
            )
            return status, response_headers, value

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (OSError, ConnectionError):
                pass
            self.writer = self.reader = None


class MCP:
    def __init__(self, http):
        self.http, self.headers, self.sequence = http, {}, 0

    async def initialize(self):
        status, headers, result = await self.http.request(
            "/mcp",
            {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "independent-scale-client", "version": "1"},
                },
            },
        )
        assert status == 200 and result["id"] == 0 and "result" in result, (
            status,
            result,
        )
        self.headers = {
            "MCP-Session-Id": headers["mcp-session-id"],
            "MCP-Protocol-Version": result["result"]["protocolVersion"],
        }
        status, _, _ = await self.http.request(
            "/mcp",
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            headers=self.headers,
        )
        assert status == 202

    async def call(self, tool, arguments):
        self.sequence += 1
        identity = self.sequence
        status, _, reply = await self.http.request(
            "/mcp",
            {
                "jsonrpc": "2.0",
                "id": identity,
                "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            },
            headers=self.headers,
        )
        assert status in (200, 429, 503, 504), (status, reply)
        if status != 200:
            return dict(
                reply
                or {
                    "jsonrpc": "2.0",
                    "id": identity,
                    "error": {"code": -32029, "message": "HTTP admission rejected"},
                },
                http_status=status,
            )
        assert reply["id"] == identity, (status, reply)
        return reply

    async def close(self):
        if self.headers:
            await self.http.request("/mcp", method="DELETE", headers=self.headers)
        await self.http.close()
