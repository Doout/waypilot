# Operating the MCP prototype

## Request path

HTTP clients send MCP JSON-RPC to `/mcp` with a user bearer token. Initialize first, retain the returned `MCP-Session-Id`, send `notifications/initialized`, and include that ID and the negotiated `MCP-Protocol-Version` on subsequent messages. The bundled `list` and `call` commands handle this lifecycle and close their session afterward. The official Python MCP SDK is an independent interoperability check in `scripts/check_sdk.py`.

`tools/list` exposes permitted tools as `server__tool` and eligible authored workflows as `waypilot__name`. Discovery fails if a configured, accessible upstream cannot initialize; this prototype does not silently return a partial catalog. `tools/call` executes through a Wayframe graph, rechecks grants, and returns the upstream MCP ToolResult with runtime metadata under `_meta["io.waypilot/runtime"]`.

The HTTP endpoint responds with JSON. It accepts clients advertising both JSON and SSE, as required by the [classic Streamable HTTP transport contract](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports). HTTP GET returns 405; this implementation does not offer a server event stream. DELETE closes the downstream session and its upstream connections. `/healthz` is a process readiness check, not an upstream connectivity or authorization probe.

Stdio ingress uses `serve-stdio --config FILE --user ID`. It binds the process to an operator-selected configured user, accepts bounded JSONL, and applies the same tool controls and graph execution. Notifications can cancel active requests while the input loop continues. The local process owner controls which principal is selected.

## Transport support

| Behavior | Ingress | Upstream |
| --- | --- | --- |
| Stdio JSON-RPC | Yes | Yes, supervised subprocess |
| Streamable HTTP POST, JSON | Yes | Yes |
| Streamable HTTP POST, SSE response | Returns JSON | Yes, including fragmented events and notifications |
| Classic protocol negotiation | 2025-11-25, 2025-06-18, 2025-03-26 | Same supported set |
| Static bearer credential | Per configured user | Separate optional environment credential |
| Per-request cancellation | Local work and response delivery | Drops local exchange/session; no remote rollback guarantee |
| Legacy HTTP+SSE with separate message endpoint | No | No |
| GET stream, resumption, server push | No | No |
| Sampling, elicitation, roots, resources, prompts | No advertised capability | No forwarded client callbacks |
| OAuth discovery/token exchange | No | No |

Only the tool capability is advertised. An upstream callback request fails the exchange; unsolicited notification events are skipped. Dynamic catalog notifications are not applied to an existing cached catalog. Restart the gateway after changing operator contracts/configuration; create a fresh downstream session to rediscover an upstream catalog. A catalog digest participates in reuse keys but does not prove that external resource data is fresh.

## Identities, grants and administration

The TOML configuration is the durable source of users, tenants, credential environment names, tool grants and rate limits. Tokens must be distinct, 16–4096 visible ASCII bytes; use randomly generated tokens. Only hashes of user/admin tokens are retained in the authorization table. Upstream credentials remain in the upstream client and are never copied from a downstream bearer token.

Sessions are random identifiers bound to the current authenticated user. They cannot be used with another user's token. Each downstream session owns its own upstream connections; credentials and mutable session state are not pooled across users. Reusable results may cross sessions only for the same user and explicit contract. Such a contract must declare that output is independent of unkeyed session state.

Administrative endpoints require the separate administrator credential:

| Method and path | Request | Result |
| --- | --- | --- |
| `GET /admin/stats` | None | Aggregate requests, upstream calls, reuse counts, prewarm counts, sessions and cache bounds |
| `GET /admin/users` | None | IDs, tenants, enabled state, grants and rate settings; no tokens |
| `POST /admin/users/update` | `{"id":"developer","enabled":false}` | In-memory enabled override and new generation |
| `POST /admin/invalidate` | Empty body | New generation and removal of previous retained results |

The `admin` CLI exposes these operations through `--action stats|users|disable|enable|invalidate`, with `--user` for enable/disable. Runtime overrides are not written to disk. To add/remove users, rotate tokens, edit grants or change limits durably, update the configuration/environment and restart. Session results, reuse state and server-use profiles are also process-local.

Disabling a user cancels their sessions. Both disabling and enabling increment the **global** generation, invalidating all retained data and rejecting results from an earlier generation. This conservative implementation can interrupt other users' active work during an administrative update. Re-enabling requires a fresh session; it does not revive canceled sessions.

## Bounds and deadlines

Defaults are 128 downstream sessions, a 15-minute idle session lifetime, and 32 admitted active MCP requests. Each session allows at most 32 pending request IDs. Admission rejects overload rather than maintaining an unbounded request queue. Each upstream session serializes its own preparation and invocation; different servers/sessions can execute in parallel. Authored workflows admit at most four simultaneously running nodes.

Bodies, upstream responses, combined upstream catalogs and retained workflow tool results have 1 MiB limits. A final encoded workflow response can exceed the response cap because it contains both text and structured content; it then fails instead of releasing a partial response. Catalog discovery allows at most 16 pages and 512 tools per upstream. The result store has 512 entries, a 16 MiB accounted payload budget, TTL expiry and 128 tracked singleflight keys. Payload accounting excludes allocator/object overhead.

`request_timeout_ms` bounds HTTP body read and execution together. Body reads are additionally capped at five seconds. The original execution deadline passes through Wayframe and the upstream client. Stdio requests start their execution deadline after a complete input line. Notifications such as cancellation do not consume an MCP request admission slot, so they can stop work at capacity.

Tool calls consume a per-user token-bucket token, including reuse hits and requests joining a retained result. A virtual workflow consumes one entry token plus each invoked child token. Prewarm batches consume one token, prepare at most two servers and share a separate two-batch admission pool. Metadata operations still use request admission; they do not consume tool tokens. These are prototype application limits, not a complete network abuse or identity-management service.

## Preparation, reuse and effects

Only servers with `prewarm = true` and current user grants can be prepared. The manual endpoint is `POST /v1/prewarm` with `{"servers":["deps","source"]}` and the same user/session/protocol headers as MCP calls. Success reports `prepared` and `tool_calls: 0`. Preparation performs initialization and `tools/list`; a server may still observe these requests or perform its own startup actions.

After at least two successful tool-use observations, a per-user frequency baseline can select up to two opted-in servers seen in at least half of that user's observations. HTTP initialization starts bounded background preparation for those servers. This is an environment-use predictor, not a learned dependency graph or speculative tool execution. Failed preparation is counted and ordinary authoritative work may initialize a new session later.

Reuse requires a named, operator-approved tool rule with `reusable = true` and a nonzero TTL. The tool must have no required issue-time effect and must be valid for the key's arguments, user/grants, session-independence contract, server revision and catalog digest during the declared freshness interval. Unknown tools bypass reuse. A server's `readOnlyHint` is insufficient; the owned counter fixture intentionally lies about that annotation and still runs twice.

Exact relevant arguments are bound from workflow inputs or previous complete MCP results. If the source-only input changes, unchanged dependency-tool results remain reusable. The gateway's current granularity is individual tool results; the original fixture engine also demonstrates complete two-tool fragment reuse. Neither is an external dependency tracker. For changing resources, leave reuse disabled until a reliable revision witness or an explicitly accepted bounded-staleness policy is available.

A same-key miss takes a bounded singleflight guard and rechecks the store. A matching successful follower can return `joined`; a retained hit returns `reused`; fresh reusable work returns `executed`; ineligible work returns `bypassed`. A leader failure releases the guard without retaining an error. Followers remain authoritative requests, and may issue their own eligible read after a failed leader; this is not a detached task or a shared execution lease.

Cancellation, revocation, disconnection or timeout drops local work. Stdio cancellation invalidates that subprocess; HTTP cancellation discards the local session handle. Already-issued remote effects may continue, and an abandoned remote session relies on the server's expiry policy. A later **distinct invocation** can initialize a fresh connection. Waypilot never automatically replays the failed invocation, including HTTP 404, a dropped response or an uncertain effect. Workflows provide no transaction rollback.

## Hosting and observability

Bind to loopback by default. For a self-hosted deployment, use your normal TLS ingress, secret injection, connection/header limits and access logs. The operator configuration can launch programs and contact configured addresses, so it belongs to the host's trust boundary. It is not user-supplied executable configuration or a sandbox. Upstream redirects and environment proxy discovery are disabled; private CAs and explicit proxy configuration are not exposed as options in this prototype.

Requests with an `Origin` header are denied unless that exact origin is in `allowed_origins`. The service does not implement browser CORS preflight; the intended clients are MCP applications and SDKs. Use HTTPS for remote clients and servers; upstream cleartext requires loopback or explicit opt-in.

Set standard OTLP trace environment variables as shown in the README. `gateway.http`, `mcp.gateway.request`, `mcp.prepare`, `mcp.invoke` and `workflow.node` spans show time spent inside Waypilot. Wayframe node spans record dependency wait, scheduler wait, execution and outcome. These are local trace trees; incoming W3C trace extraction and upstream trace-header propagation are not implemented yet. Trace durations are not billing-grade per-request accounting, and overlapping spans must not be summed as elapsed time.

SIGINT/SIGTERM stop the HTTP server, cancel sessions and attempt bounded cleanup. `/healthz` remains deliberately lightweight. There is no durable recovery journal, multi-replica invalidation bus or persistent session store in this prototype.
