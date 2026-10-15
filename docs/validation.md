# Initial local qualification

Recorded 2026-10-14 on macOS arm64, Apple M2 Max, Rust 1.99.0, Python 3.14.7. Dependency versions are recorded in `Cargo.lock`; Wayframe is pinned to `ec4d27224ec213eabc2eae9b474b522d1cab8874`.

`bash scripts/verify.sh` passed formatting, 3 unit tests, 9 live MCP integration tests, Clippy with warnings denied, Python syntax checks, and a local OTLP/HTTP JSON collector check. The collector received 190 spans, including execution, fragment disposition, MCP preparation/tool calls and framework node spans, with trace IDs, parent relationships and valid start/end timestamps.

Integration checks cover exact output equivalence, full/partial result reuse, tenant/subject isolation, revoked access before reuse and during I/O, generation/profile rejection, deadlines, preparation without tool calls, and overlapping-run rejection without disrupting the active run.

Release command: `cargo run --locked --release -- demo --samples 20`. Model reasoning is a simulated 100 ms gap, fixture startup adds 60 ms, and each tool adds 15 ms. Timings also include actual process startup and MCP transport costs.

| Case | Local p50 | Local p95 | Calls/trial | Processes/trial |
| --- | ---: | ---: | ---: | ---: |
| Serial cold | 475.045 ms | 489.219 ms | 4 | 2 |
| Parallel cold | 304.378 ms | 327.440 ms | 4 | 2 |
| Parallel with learned prewarming | 196.767 ms | 211.546 ms | 4 | 2 |

All 60 trials produced equivalent outputs. Two prior training runs used eight tool calls and two process starts; that work is reported separately. Prewarming is included in each measured end-to-end interval and does not reduce tool-call count.

The incremental demonstration issued four tool calls on its first run, zero for an identical repeat, and two after changing only the source input. The unchanged dependency branch was reused. These are single-case behavioral checks, not latency distributions.

Raw local output: ignored file `target/benchmark.json`, SHA-256 `c411841cfb85a9778072139a4181b28b6ee03fd0392582232f04b5bac5b79c52`. Subsequent CI runs publish their own raw benchmark artifact for each platform. This local fixture result does not establish real-model quality, production latency, broad MCP interoperability or speculative-execution performance. Automatic workflow mining and speculative tool invocation are not part of this qualification.
