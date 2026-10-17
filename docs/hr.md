# HR agents and next-read speculation

An HR agent reads an employee profile, pauses to interpret it, then chooses leave or
benefits guidance. Leave requests read a balance and policy before recording one local
case. Benefits requests take a different branch. Agent delays model time spent thinking;
they are timers, not measurements of a live LLM. The official MCP SDK carries real calls
through Waypilot to an owned service containing synthetic data.

```sh
cargo build --locked
# Use the SDK virtualenv from docs/agents.md.
target/sdk-venv/bin/python demos/hr_demo.py --think-ms 200 --tool-ms 250
# Branch away from the common leave path, or start without learned history.
target/sdk-venv/bin/python demos/hr_demo.py --kind benefits
target/sdk-venv/bin/python demos/hr_demo.py --cold
```

The demo first runs four ordinary training agents on other employees; each records a
synthetic case. It then reports the measured agent, phase timings, gateway counters and
peer events. `--cold` omits training. Every agent still chooses and issues every call.
No future branch hint is sent to the gateway.

## Gateway behavior

Candidate transitions and argument provenance are authored. The gateway learns their
frequency from successful calls, separately for each principal and policy generation.
The HR configuration requires three observations and 75% confidence before nominating
one next read. Arguments come from the current call or its result, never a retained
employee ID. Repeated branch misses lower confidence and stop nomination.

Speculation requires all three: a tool rule with `speculative = true`, state-validated
reuse, and an explicit user `speculative_tools` grant containing that exact tool name.
The operator's contract asserts an effect-free, session-independent read and permits
early argument disclosure. MCP annotations and observed patterns cannot establish this.
The owned `record_case` write has no eligible contract.

For an already configured eligible tool and user, the prediction configuration is:

```toml
[speculation]
max_in_flight = 8
min_samples = 3
confidence_percent = 75
max_duration_ms = 2000
[[speculation.rules]]
after = "hr__employee"
tool = "hr__leave_balance"
arguments.employee_id = {result = "/structuredContent/employee_id"}
```

Bindings choose one JSON pointer: `input` for the triggering arguments or `result` for
the returned result. Learning is in memory and resets on restart or policy generation
change. There are at most 16 templates, 16 KiB of argument values in each session’s learning context,
one pending nomination per session, and 32 globally active speculative jobs at most.

Predictions use isolated upstream sessions, a separate concurrency cap and the same
request, upstream, initialization, rate and cache budgets as demanded work. They do not
chain themselves. Their deadline cannot outlive the triggering request's original
deadline. Closing/revoking the session or cancelling a selected demand stops remaining
work. Capacity exhaustion skips nomination and leaves ordinary execution available.

A later actual call matches the exact tool and arguments, checks current state and
permissions, and can join or reuse the normal bounded cache entry. Changed versions
force fresh execution or fail closed. Earlier read receipts still guard the final case
write. There is no remote snapshot or rollback. Cancelling a wrong guess does not erase
a request the peer already observed; unused valid cache entries can remain until expiry.

## Compare

```sh
cargo build --locked --release
target/sdk-venv/bin/python scripts/benchmark_hr.py target/release/waypilot \
  --runs 32 --concurrency 4 --tool-ms 250 --think-ms 200 --wrong-every 5 \
  --rounds 2 --output target/hr-benchmark.json
target/sdk-venv/bin/python scripts/check_hr.py target/debug/waypilot
```

Modes are uncached (`off`), validated caching, initialization/catalog prewarming and
speculative reads. Only the last mode invokes tools ahead of demand. Compare the last
three for identical observation guards. Compiled schemas and resource budgets are equal.
Training employees never appear in measurement, so ordinary cache warming cannot serve
the measured reads. All modes must match structured observations, decisions and exact
case effects. Two rounds reverse mode order; no latency threshold is a correctness gate.

Vary `--think-ms` (including zero), `--wrong-every`, `--slots`, `--concurrency` and
`--training-runs 0`. Reports include actual agent/tool waits, p50/p95/p99, completion time
including client admission/initialization, CPU, sampled gateway memory/descriptors,
tool RPCs and started/selected/reused/unused/skipped predictions. RPC counts cover
`tools/call`, including validators; initialization costs remain in wall time and CPU. `reused` counts selected
nominations followed by a cache hit/join; RPC counts establish the actual upstream work.
After each mode, sessions, receipts, requests, speculative jobs and cleanup must drain.

Gateway OTLP spans include `mcp.predict`, `mcp.speculate` and the usual preparation,
validation, cache and invocation spans. Arguments, employee IDs, results and receipts
stay out of trace fields. Client think/tool times are in the agent JSON report.

Local measurements on 2026-10-17 (macOS ARM64, 32 agents, concurrency 4, 250 ms reads,
20% benefits branches, four disjoint training agents, two reversed rounds):

| Agent pause | Prewarm p95 | Speculative p95 |
| --- | ---: | ---: |
| 0 ms | 807–815 ms | 818–842 ms |
| 200 ms | 1,414–1,418 ms | 1,013–1,014 ms |

At 200 ms, each round had 58 nominations, 52 selected cache hits and six unused guesses;
tool/validator RPCs rose from 482 to 598. Observations and all 32 case effects matched.
Frequency gates predictability; there is no learned cost/benefit gate yet. The zero-pause
control gives no useful speedup. Run the cold and miss-heavy controls before generalizing
these warm-history measurements to a different agent or server.
