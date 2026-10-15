# Gateway reference

## Configuration and clients

Load TOML with `waypilot serve --config FILE`. Start with [service.toml](../examples/service.toml). Restart to change users, credentials, grants, limits or server contracts. Secrets come from named environment variables. Upstream credentials are separate from gateway user tokens.

HTTP upstreams require HTTPS, loopback HTTP or `allow_insecure_http = true`. Redirects, automatic retries and environment proxy discovery are disabled. Use TLS and connection/header limits at your hosting ingress. A connection's read-idle timeout does not enforce an absolute header deadline. Requests with `Origin` require an exact match in `allowed_origins`. Browser CORS preflight is unsupported.

Stdio endpoints take an absolute `command`, `args` and optional `inherit_env` names. `env:WAYPILOT_PYTHON` resolves a command from that variable. `{config_dir}` resolves paths relative to the configuration file. Child processes receive a cleared environment plus the explicit allowlist. Configuration can launch programs and choose network destinations, so only operators should edit it.

HTTP clients POST to `/mcp` with a bearer token and `Accept: application/json, text/event-stream`. Initialize, retain `MCP-Session-Id`, send `notifications/initialized`, then include the session ID and negotiated `MCP-Protocol-Version` on subsequent messages. DELETE closes the session. The `list` and `call` CLI commands handle this exchange.

For stdio ingress, launch the absolute binary path with `serve-stdio --config FILE --user ID`. The operator selects the configured principal. Stdout contains JSON-RPC; logs go to stderr.

Both sides support stdio and classic Streamable HTTP. Upstream POST responses may be JSON or SSE; ingress returns JSON. Supported versions are `2025-11-25`, `2025-06-18` and `2025-03-26`. Only tools are advertised. Legacy SSE, GET streams, resumption, server callbacks, OAuth, durable jobs and agent adapters are unsupported. Full tool input-schema validation belongs to the upstream. A fresh session rediscovers its catalog; catalog-change notifications do not update it.

## Users and administration

Configure users, tenants, tokens, tool grants and token buckets in TOML. Tokens must be distinct and contain 16 to 4096 visible ASCII bytes. Grants accept an exact tool name, `server__*` or `*`. A workflow requires its own grant and every child tool's grant.

Tool calls consume a token, including reuse hits. Workflows also consume an entry token. Prewarm batches consume one token; metadata requests use request admission without consuming tool tokens.

Admin endpoints require the separate admin token:

| Request | Body | Result |
| --- | --- | --- |
| `GET /admin/stats` | None | Admission, connections, accounted bytes, cache and cleanup counters |
| `GET /admin/users` | None | Users, grants and rate settings, without tokens |
| `POST /admin/users/update` | `{"id":"developer","enabled":false}` | Enable or disable a user |
| `POST /admin/invalidate` | Empty | Clear retained results and advance the generation |

```sh
target/debug/waypilot admin --url http://127.0.0.1:7400 \
  --token-env WAYPILOT_ADMIN_TOKEN --action disable --user developer
```

Other actions are `stats`, `users`, `enable` and `invalidate`. Disabling cancels the user's sessions and prevents queued dispatch. Re-enabling requires a new session. Admin changes advance a global generation, clear retained results and reject older-generation results. This can interrupt other users' active work.

Set `state_path = "../var/policy.json"` to persist enabled-state overrides relative to the configuration file. One process owns the file through an OS lock. Writes use mode 0600 on Unix, atomic replacement and file/directory sync. Startup fails if the file is unwritable or names an unconfigured user. Admin disconnects do not cancel an admitted write. A write error leaves the running override unchanged; an I/O error after rename can leave an uncertain disk outcome.

Keep the state on a local filesystem and preserve it across restarts. Without `state_path`, overrides stay in memory. Sessions, cached results and usage profiles always stay in memory. Replicas do not share policy updates or sessions.

## Workflows, reuse and prewarming

Waypilot owns MCP semantics and user controls. Wayframe supplies graph execution, caching, process supervision and telemetry through the revision pinned in [Cargo.toml](../Cargo.toml).

Tools use `server__tool` names; workflows use `waypilot__name`. [local.toml](../examples/local.toml) shows input bindings and dependencies on earlier steps. Independent steps run concurrently, up to four nodes per workflow. All configured steps execute, including those omitted from selected outputs. Access checks run before lookup, before dispatch and before release.

Each downstream session has separate upstream session IDs and catalogs. Calls within one upstream session serialize; separate sessions and servers can run in parallel. HTTP transport pools share TCP connections without sharing logical MCP state.

Reuse is off by default. Enable it only when the tool has no required effect and its result stays valid for the declared TTL, independently of session state:

```toml
[servers.tools.echo]
reusable = true
cache_ttl_ms = 60000
```

Keys include tenant, user, grants, generation, server revision, catalog digest, tool and exact arguments. Errors are not retained. `readOnlyHint` does not enable reuse. TTL expiry limits staleness but cannot detect external changes. Bump the server revision and restart when its contract changes.

Matching misses share a singleflight guard. A follower may use the leader's retained result after current access checks. If the leader fails, a follower may make its own eligible call. The gateway retains individual tool results. Unchanged workflow branches avoid calls through those results; it does not retain arbitrary whole graphs.

Set `prewarm = true` to allow initialization and catalog discovery without tool invocation. On new HTTP sessions, the predictor can prepare up to two permitted servers based on that user's past server use. Manual preparation uses `POST /v1/prewarm` with `{"servers":["deps","source"]}` and the MCP user/session/protocol headers. Two prewarm batches may run at once, with at most two servers each.

## Remote state validation

Use `state_tool` for a cooperative upstream that can report the exact versions read by a
reusable tool. This is Waypilot's `state-v1` contract, carried in ordinary MCP tool results
through [custom `_meta`](https://modelcontextprotocol.io/specification/2025-11-25/basic#meta).
It is not a standard MCP freshness guarantee.

```toml
[servers.tools.read_document]
reusable = true
cache_ttl_ms = 60000
state_tool = "document_version"
```

The gateway invokes `document_version` on the same server with
`{"tool":"read_document","arguments":{"id":"42"}}`. Both that validator and
`read_document` must return this metadata with their normal MCP result:

```json
{"_meta":{"io.waypilot/state":{"version":1,"resources":{"doc:42":"deployment-7:revision-19"}}}}
```

The upstream must capture the read result and its complete resource-version map atomically.
Versions must change whenever relevant state, tool semantics, or schema changes, and must
never be reused after deletion, recreation, or restart. Include a durable revision or
incarnation identifier. The validator must return current versions independently of session
history. Configure this only for tools with no required effect. Tool annotations do not
establish this contract, and an uncooperative server cannot acquire it through gateway config.

Waypilot compares exact evidence before lookup, checks ancestor observations before dependent
work, and validates all recorded observations again before the release authorization gate.
A changed witness invalidates the retained computation and its transitive descendants.
Unchanged independent branches remain reusable. A recomputed parent gets a new receipt even
if its value is identical. Unretained parents prevent descendant retention. Stale in-flight
parent receipts cannot populate the dependent cache.

Missing, malformed, or changed evidence rejects the request; there is no stale fallback or
automatic replay. A state conflict uses JSON-RPC `-32010`. Transport errors, deadlines, rate
limits, and access denials retain their existing error classes. These are validation-point
checks, not a distributed snapshot, remote rollback, or a guarantee that the resource cannot
change after the final observation. A workflow with external effects may fail after an effect
has occurred; the gateway does not undo it.

Users need grants for both the read tool and its validator. Validator calls consume tokens
and upstream admission, use current authorization, and share the original request deadline.
Responses retain at most 64 distinct validation observations; their arguments and version
maps share the workflow's 1 MiB retained-data budget. Each map permits 1 to 64 resource/version
pairs with nonempty strings up to 256 bytes. Arguments, results, and version values are not
trace fields. `mcp.state.validate`, `waypilot.cache.lookup`, and the graph's `validate_state`
node expose validation, lookup, and release-barrier timing. Admin statistics count validation
calls, conflicts, and cache invalidations.

The [state regression suite](../scripts/check_state.py) exercises HTTP JSON, HTTP SSE and
stdio peers, including external mutation with unchanged downstream arguments. See
[benchmarks](testing.md#remote-state-validation) for comparisons with the TTL-only baseline.

## Limits and shutdown

Top-level defaults are `max_sessions = 128`, `max_in_flight = 32`, `session_ttl_seconds = 900` and `request_timeout_ms = 30000`. Each session permits 32 pending request IDs. Each server's `max_in_flight` defaults to 512.

Defaults under `[limits]`:

| Setting | Default | Controls |
| --- | ---: | --- |
| `max_connections` | 2048 | Accepted TCP connections |
| `connection_idle_ms` | 180000 | Connection read inactivity |
| `body_readers` | 64 | Concurrent authenticated body reads |
| `admin_requests` | 16 | Separate admin admission |
| `request_bytes` | 1048576 | One inbound body |
| `buffered_bytes` | 33554432 | Complete ordinary bodies held by handlers |
| `upstream_calls` | 512 | Tool calls across all servers |
| `upstream_initializations` | 64 | Handshakes and catalog preparation |
| `upstream_idle_per_host` | 64 | Idle HTTP connections per server/host |
| `cleanup_pending` | 4096 | Queued and active cleanup actions |
| `shutdown_grace_ms` | 10000 | Grace for admitted work |

Excess requests receive HTTP 503 or a JSON-RPC capacity error; session capacity returns HTTP 429. Admitted calls wait for server/global quotas under their deadline. Cancellation notifications bypass MCP request admission. Admin slots share the outer TCP connection cap.

The HTTP deadline includes body reading and execution. Body reads also have a five-second cap. Stdio execution deadlines start after a complete line. Upstream responses, combined catalogs and retained workflow tool results have 1 MiB caps. Catalogs allow 16 pages and 512 tools. The cache holds 512 entries and 16 MiB of accounted payload, with 128 singleflight keys.

Byte budgets exclude decoded JSON, responses and allocator overhead. Readers may hold another `body_readers * request_bytes` before complete-body admission. Set OS/container memory and descriptor limits too.

Idle maintenance runs every 250 ms and preserves active work. SIGINT/SIGTERM makes `/healthz` return 503, rejects new work and allows the configured shutdown grace. The service then cancels sessions, allows up to three more seconds for preparation/cleanup and stops network reads and writes. Stdio EOF and input errors also trigger cleanup. Readiness returns 200 otherwise; it does not probe upstream health.

HTTP cleanup queues cancellation and DELETE for known sessions. At most 32 cleanup actions run at once, each with a two-second timeout. `/admin/stats` reports pending work, errors and drops. An abandoned initialization keeps its preparation deadline so it can receive and delete the session ID. If the server never returns that ID, its own expiry policy must reclaim the session. DELETE 405 is tolerated. Stdio cancellation invalidates its process.

Cleanup is best effort. Remote effects can finish after local cancellation. A failed invocation is never replayed or rolled back; a later distinct invocation may open a fresh session.

## Tracing

```sh
OTEL_SERVICE_NAME=waypilot \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/json \
target/debug/waypilot serve --config examples/local.toml
```

Spans cover ingress, MCP requests, preparation, invocation and graph nodes. Node fields separate dependency wait, scheduler wait and execution. Results include elapsed time and `executed`, `reused`, `joined` or `bypassed` under `_meta["io.waypilot/runtime"]`. Application trace fields exclude arguments, credentials and results.

The exporter also supports `http/protobuf`. Incoming W3C context extraction and upstream trace propagation are unsupported. Overlapping span durations do not add up to elapsed request time.
