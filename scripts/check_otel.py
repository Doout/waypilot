"""Verify real OTLP/HTTP JSON span export against an ephemeral local collector."""
import http.server
import json
import os
import socketserver
from pathlib import Path
import subprocess
import sys
import threading


def main():
    spans = []

    class Collector(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            for resource in payload.get("resourceSpans", []):
                for scope in resource.get("scopeSpans", []):
                    spans.extend(scope.get("spans", []))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    binary = Path(sys.argv[1]).resolve()
    class LoopbackServer(http.server.HTTPServer):
        def server_bind(self):
            socketserver.TCPServer.server_bind(self)
            self.server_name = "localhost"
            self.server_port = self.server_address[1]

    collector = LoopbackServer(("127.0.0.1", 0), Collector)
    thread = threading.Thread(target=collector.serve_forever, daemon=True)
    thread.start()
    environment = dict(os.environ)
    environment.update(
        OTEL_SERVICE_NAME="waypilot",
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=f"http://127.0.0.1:{collector.server_port}/v1/traces",
        OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="http/json",
        OTEL_TRACES_SAMPLER="always_on",
        RUST_LOG="error",
    )
    try:
        process = subprocess.run([str(binary), "demo", "--samples", "1"], env=environment, capture_output=True, text=True, timeout=45, check=True)
        benchmark = json.loads(process.stdout)
        assert benchmark["output_equivalence"] is True
        assert benchmark["incremental"]["repeat"]["counts"]["calls"] == 0
        demo = Path(__file__).resolve().parents[1] / "demos/mcp_demo.py"
        process = subprocess.run([os.environ["WAYPILOT_PYTHON"], str(demo), str(binary)], env=environment, capture_output=True, text=True, timeout=45, check=True)
        live = json.loads(process.stdout)
        assert [case["upstream_calls"] for case in live["workflow"]] == [4, 0, 2]
        agent_demo = Path(__file__).resolve().parents[1] / "demos/agent_demo.py"
        agent_process = subprocess.run([os.environ.get("WAYPILOT_SDK_PYTHON", os.environ["WAYPILOT_PYTHON"]), str(agent_demo), str(binary), "--batch-validation", "--guard-observations", "--validation-parallelism", "4"], env=environment, capture_output=True, text=True, timeout=45, check=True)
        agent_run = json.loads(agent_process.stdout)
        assert len(agent_run["agents"]) == 4
        assert agent_run["upstream"]["calls"]["append_note"] == 2
        assert any(agent["summary"].startswith("Review agrees.") for agent in agent_run["agents"])
        hr_demo = Path(__file__).resolve().parents[1] / "demos/hr_demo.py"
        hr_process = subprocess.run([os.environ.get("WAYPILOT_SDK_PYTHON", os.environ["WAYPILOT_PYTHON"]), str(hr_demo), str(binary), "--think-ms", "20", "--tool-ms", "5"], env=environment, capture_output=True, text=True, timeout=45, check=True)
        hr_run = json.loads(hr_process.stdout)
        assert hr_run["gateway"]["speculation"]["started"] >= 2
        assert hr_run["upstream"]["case_count"] == 5
        from state_support import Client, StateDemo
        private_marker = "waypilot-otel-private-argument-and-result-probe"
        with StateDemo(binary, environment=environment) as state_demo:
            client = Client(state_demo.base, state_demo.token)
            client.call("waypilot__state", {})
            client.call("waypilot__state", {})
            state_demo.mutate(key="a", touch=True)
            client.call("waypilot__state", {})
            state_demo.mutate(key="a", value=private_marker)
            private_result = client.request("tools/call", {"name": "owned__state_read", "arguments": {"key": "a", "private_context": private_marker}, "_meta": {"io.waypilot/observations": {"capture": True}}})["result"]
            private_receipt = private_result["_meta"]["io.waypilot/observations"]["receipt"]
            assert private_result["structuredContent"]["value"] == private_marker
            client.close()
        serialized_spans = json.dumps(spans)
        assert all(secret not in serialized_spans for secret in ["E-000101", private_marker, private_receipt, state_demo.token, state_demo.admin]), "arguments, results or credentials leaked into trace fields"
        names = {span["name"] for span in spans}
        required = {"waypilot.run", "waypilot.fragment", "mcp.prewarm", "mcp.tool", "workflow.node", "gateway.http", "mcp.gateway.request", "mcp.prepare", "mcp.invoke", "mcp.state.validate", "mcp.state.validate_batch", "mcp.state.validate_set", "waypilot.cache.lookup", "mcp.speculate", "mcp.predict"}
        assert required <= names, f"missing spans: {required - names}; stderr: {process.stderr[-2000:]}"
        dispositions = {
            attr["value"].get("stringValue")
            for span in spans
            for attr in span.get("attributes", [])
            if attr["key"] == "disposition"
        }
        assert {"executed", "reused"} <= dispositions, dispositions
        assert all(int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"]) for span in spans)
        assert all(span.get("traceId") and span.get("spanId") for span in spans)
        assert any(span["name"] == "mcp.tool" and span.get("parentSpanId") for span in spans)
        gateway_traces = {span["traceId"] for span in spans if span["name"] == "gateway.http"}
        assert any(span["name"] == "mcp.invoke" and span["traceId"] in gateway_traces and span.get("parentSpanId") for span in spans)
        timings = {"node.dependency_wait_ns", "node.scheduler_wait_ns", "node.execution_ns"}
        timed_nodes = [span for span in spans if span["name"] == "workflow.node" and timings <= {attr["key"] for attr in span.get("attributes", [])}]
        assert timed_nodes, "graph node wait/execution timings were not exported"
        assert any(span["name"] == "mcp.invoke" and {"queue_us", "exchange_us"} <= {attr["key"] for attr in span.get("attributes", [])} for span in spans), "upstream queue/exchange timing missing"
        print(json.dumps({"otel_export": "passed", "spans": len(spans), "required_names": sorted(required), "dispositions": sorted(dispositions), "timed_graph_nodes": len(timed_nodes), "gateway_to_upstream_parentage": "passed"}))
    finally:
        collector.shutdown()
        thread.join(timeout=5)
        collector.server_close()


if __name__ == "__main__":
    main()
