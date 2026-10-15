"""Compare direct and gateway MCP calls over the same owned HTTP tool server."""
import argparse
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from mcp_demo import Client, Demo


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--samples", type=int, default=100)
    args = parser.parse_args()
    assert 10 <= args.samples <= 5000
    rows = []
    for mode in ["json", "sse"]:
        with Demo(args.binary, os.environ["WAYPILOT_PYTHON"], response=mode, reuse=False, rate_limit=(10000, 10000)) as demo:
            direct = Client(demo.upstream, "owned-fixture")
            gateway = Client(demo.base, demo.token)
            for _ in range(3):
                direct.call("echo", {"value": "benchmark"})
                gateway.call("owned__echo", {"value": "benchmark"})
            elapsed = {"direct": [], "gateway": []}
            trials = []
            before = demo.stats()["upstream_calls"]
            for index in range(args.samples):
                # Alternate order to reduce systematic warmup/drift bias.
                order = ["direct", "gateway"] if index % 2 == 0 else ["gateway", "direct"]
                outputs = {}
                trial = {}
                for case in order:
                    started = time.perf_counter_ns()
                    result = (direct if case == "direct" else gateway).call("echo" if case == "direct" else "owned__echo", {"value": "benchmark"})
                    trial[case + "_us"] = (time.perf_counter_ns() - started) / 1000
                    elapsed[case].append(trial[case + "_us"])
                    outputs[case] = result["structuredContent"]
                assert outputs["direct"] == outputs["gateway"]
                trials.append(trial)
            assert demo.stats()["upstream_calls"] - before == args.samples
            stats = {case: {"p50_us": percentile(values, 0.5), "p95_us": percentile(values, 0.95)} for case, values in elapsed.items()}
            rows.append({"mode": mode, "summary": stats, "p50_overhead_us": stats["gateway"]["p50_us"] - stats["direct"]["p50_us"], "gateway_upstream_calls": args.samples, "trials": trials})
            direct.close()
            gateway.close()
    print(json.dumps({"environment": {"platform": platform.platform(), "python": platform.python_version(), "binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()}, "methodology": {"samples_per_mode": args.samples, "reuse": False, "model_calls": 0, "workload": "same owned loopback MCP echo server and caller; initialized sessions; three warmup calls; alternating direct/gateway order", "timing": "includes caller HTTP/JSON handling; reports end-to-end proxy overhead, not isolated Rust dispatch cost", "tracing": "export off unless configured through OTEL environment"}, "output_equivalence": True, "cases": rows}, indent=2))


if __name__ == "__main__":
    main()
