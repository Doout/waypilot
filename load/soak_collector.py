"""Bounded local OTLP JSON timing histograms; no argument or result retention."""

import asyncio
import json
import math
import socket
from collections import Counter

import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route


def timing_attribute(attribute):
    key = attribute["key"].rsplit(".", 1)[-1]
    if key not in {"admission_us", "queue_us", "exchange_us"}:
        return None
    value = attribute["value"]
    number = value.get("intValue", value.get("doubleValue", value.get("stringValue")))
    try:
        number = float(number)
    except (TypeError, ValueError):
        return None
    return (key, number / 1000) if math.isfinite(number) and number >= 0 else None


async def main():
    histograms, totals = {}, Counter()

    def record(service, name, milliseconds):
        key = service + "/" + name
        if key not in histograms:
            assert len(histograms) < 256, "unexpected span cardinality"
            histograms[key] = {"count": 0, "sum_ms": 0.0, "max_ms": 0.0, "bins": [0] * 256}
        histogram = histograms[key]
        histogram["count"] += 1
        histogram["sum_ms"] += milliseconds
        histogram["max_ms"] = max(histogram["max_ms"], milliseconds)
        slot = min(255, max(0, math.ceil(math.log1p(milliseconds * 1000) / math.log(1.1))))
        histogram["bins"][slot] += 1

    async def ingest(request):
        data = await request.body()
        assert len(data) <= 8 * 1024 * 1024
        for resource in json.loads(data).get("resourceSpans", []):
            attributes = resource.get("resource", {}).get("attributes", [])
            service = next(
                item["value"]["stringValue"] for item in attributes if item["key"] == "service.name"
            )
            for scope in resource.get("scopeSpans", []):
                for span in scope.get("spans", []):
                    start, end = int(span["startTimeUnixNano"]), int(span["endTimeUnixNano"])
                    assert end >= start and span.get("traceId") and span.get("spanId")
                    record(service, span["name"], (end - start) / 1e6)
                    totals[service] += 1
                    for attribute in span.get("attributes", []):
                        timing = timing_attribute(attribute)
                        if timing:
                            record(service, span["name"] + "." + timing[0], timing[1])
        return JSONResponse({})

    async def stats(_request):
        return JSONResponse({"spans": dict(totals), "timings": histograms})

    app = Starlette(routes=[Route("/v1/traces", ingest, methods=["POST"]), Route("/stats", stats)])
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)

        class ReadyServer(uvicorn.Server):
            async def startup(self, sockets=None):
                await super().startup(sockets)
                print(json.dumps({"listen": f"127.0.0.1:{listener.getsockname()[1]}"}), flush=True)

        await ReadyServer(uvicorn.Config(app, log_level="error")).serve(sockets=[listener])


if __name__ == "__main__":
    asyncio.run(main())
