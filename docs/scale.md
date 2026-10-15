# Single-node qualification

The first target is **1,000 connected MCP clients and 500 concurrent authoritative calls**. Connected clients retain distinct authenticated MCP sessions and TCP connections. Burst phases launch 16 callers per batch with a 2 ms pause (roughly a 64 ms ramp for 500 calls); the barrier must still observe the entire target concurrently. The test observes 500 calls simultaneously inside the owned upstream before releasing their barrier; launching 500 client tasks alone is insufficient evidence.

Run against a release binary, with other benchmarks/builds stopped:

```sh
cargo build --locked --release
python3 load/qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --seconds 60 --rate 500 \
  --output target/scale-qualification.json

python3 load/qualify.py target/release/waypilot \
  --clients 1000 --concurrency 500 --seconds 60 --rate 500 \
  --trace-ratio 0.1 --output target/scale-qualification-tracing.json
```

Python 3.11+ and Unix process/file-descriptor inspection are required. The runner raises its soft descriptor limit when possible and rejects an insufficient hard limit. It starts owned loopback peers, generates ephemeral credentials, creates isolated policy state, and cleans up child processes. It sends no traffic to public MCP services. Optional tracing uses a separate owned OTLP/HTTP JSON collector, with actual exported node timing fields required to pass. Ambient OTLP variables are removed for the tracing-off case.

`bash scripts/verify.sh` runs a smaller 64-client/32-call sampled-tracing version on both CI platforms. Linux CI also runs the complete 1,000/500 release test for 60 seconds at an offered 500 calls/second. Raw reports are uploaded even when qualification fails.

## What must pass

| Phase | Required evidence |
| --- | --- |
| Abandoned initialization | Cancel while the peer allocates a session but withholds the initialize reply; after release, no tool dispatch and no orphan session |
| Capacity and overload | 1,000 connected clients, 500 observed active upstream calls, excess request rejected |
| Authored DAG | 500 workflows, 1,000 simultaneously observed child branches over separate logical MCP sessions, a dependent effect/join, equivalent outputs, exactly 1,500 child calls |
| Body pressure | 500 incomplete authenticated bodies occupy bounded read slots; excess rejected and admin stats available |
| Rate bucket | One concurrent burst admits four calls and rejects sixteen |
| Byte pressure | 60 KB requests encounter the 16 MiB accounted body budget; accepted outputs match their caller and rejected calls do not dispatch |
| Cancellation | 500 active calls canceled using independent control connections; local work and remote sessions drain without replay |
| Singleflight | 500 same-user identical calls produce one upstream invocation; another user requires a separate invocation |
| Uncertain effects | Up to 64 peers apply an effect then drop the response; no duplicate effect, and subsequent distinct calls have isolated sessions |
| Deadlines | 500 held calls time out under the original deadline, with active work and cleanup returning to zero |
| Sustained traffic | Open-loop offered load, every output checked for nonce/owner, no request errors, generator lag recorded |
| Session churn | Three cycles of closing/recreating all clients, zero remote sessions after close, independent upstream session identities |
| Queued revocation | Server quota 16 with 500 active/queued calls; disable prevents further dispatch, fails all affected calls, persists across restart |
| Persistence | Restart rejects disabled credentials; enabling persists and a fresh session works |
| Idle expiry | Active work survives the idle TTL and remains addressable; subsequently idle local/remote sessions disappear without new client traffic |
| Shutdown | Readiness reports draining, an admitted call completes during grace, a nonreading HTTP client cannot block exit, process exits zero and remote sessions close |
| Stdio error exit | An oversized JSONL line fails the process after a valid call; its HTTP MCP session is still deleted |

Cleanup overflow or cleanup errors fail the run. A 512 MiB sampled gateway RSS ceiling is a default test gate and can be set with `--max-rss-mb`. Reports retain sampled RSS, descriptor counts, admission gauges, cleanup counts and all soak latency trials. The peer keeps only a bounded audit ring. Credentials are excluded from reports. Failure snapshots can contain synthetic fixture outputs; the runner does not accept real user tool payloads.

## Interpreting the numbers

The runner configures 500 root-request slots and 1,000 upstream-call slots for the two-branch workflow case. Ordinary capacity/cancellation/deadline phases observe 500 upstream calls; the workflow phase observes 1,000. Per-server limits and the 16-call revocation quota are tested separately. The service example uses a more conservative 500 global upstream-call slots, which queues additional child work rather than exceeding that budget.

On the macOS host, an instantaneous post-cancellation connection burst intermittently received TCP resets before the peer observed tool dispatch. The default ramp separates sustained concurrency from this upstream connection-acceptance limit; the gateway reports failed invocations without replaying them. `--ramp-ms 0` retains an unpaced connection-burst experiment and may fail on such a host. This mode is not part of the zero-error qualification profile. The open-loop soak is separately paced at its declared offered rate.

Concurrency and throughput are separate dimensions. The barriers prove 500 concurrent calls; the soak offers 500 requests/second while 1,000 clients remain connected. The soak is scheduled from an external clock rather than waiting for each response before sending the next. Its latency starts at the scheduled time, so a slow generator does not hide queueing. Generator p99 lag must stay below 250 ms; exceeding it fails qualification instead of attributing the generator's limit to the service. Request latency and generator lag are reported separately.

The load generator and upstream share the test host with Waypilot. Results include their contention, loopback networking, JSON and policy checks; they are not isolated scheduler costs or remote-service SLOs. The resource sampler is periodic: RSS/FD peaks between samples can be missed. Body accounting excludes JSON objects, allocator overhead, response buffers and runtime stacks; it is not a hard process memory limit. Tracing-off/on runs are separate experiments, and differences include run-to-run noise.

This is a first single-node qualification with owned HTTP peers. It does not establish multi-hour leak freedom, multi-replica consistency, WAN/TLS throughput, arbitrary server behavior, large stdio process fleets, agent quality, or crash recovery of remote effects. The independent SDK and transport regressions cover JSON/SSE and stdio correctness separately. Keep a deployment-level soak, resource monitoring and ingress limits before assigning a production SLO.
