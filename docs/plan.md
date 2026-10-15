# Delivery plan

The product hypothesis is that recurring agent work can become faster through shared dependency tracking, early preparation and valid result reuse. Every optimization must retain an executable baseline, explain its decision and measure both useful work and wasted work. The working name is Waypilot.

## M0: Executable baseline and local evidence — initial implementation

- Pin a committed Wayframe revision and use its graph scheduler and process transport directly.
- Run two multi-tool MCP fragments with independent immutable inputs and a synchronous join.
- Retain exact results under bounded identity-scoped keys; show partial input invalidation.
- Export and validate a simple trace-derived environment-use profile.
- Warm real MCP processes while a simulated model gap runs.
- Test gates, revocation, deadlines, result equivalence and actual OTLP export.
- Compare serial, ordinary parallel and learned prewarming execution; report training work and raw samples.

This proves mechanics. It does not establish prediction accuracy on diverse workloads, generalized workflow inference or real-model task quality.

## M1: Explicit contracts and incremental fragments

Waypilot: versioned tool catalog, pure/read/write/unknown effect declarations, argument bindings, resource read/write sets, privacy/disclosure eligibility, validity witnesses and fragment output schemas. Begin with operator-approved contracts. Keep unknown tools ordered and ineligible for speculation/reuse. Add nested fragments, branch selectors and resource-driven invalidation. Avoid persisting identities or secrets in learned profiles.

Wayframe: a small transport-neutral reuse store with typed outputs, application-supplied scope keys, dependency revisions, expiry/byte budgets and explicit validation outcomes. Retain authorization outside it. Start with a standalone crate; integrate with flow boundaries only after the semantics are exercised.

Acceptance: mutate one nested dependency, tool implementation, selector and permission independently; only valid reusable computations survive, every control still executes, and wrong-scope reuse is zero. Include an equivalent non-agent data-processing fixture.

## M2: Bounded speculation and shared in-flight work

Waypilot: predict candidate fragments from traces, bind only known current arguments, and issue only explicitly permitted reads. Keep speculative outputs private until an authoritative request matches all guards. Discard mismatches. Use separate speculative process/session pools initially. Treat external disclosure as an issue-time effect. Prefer preparing an environment when arguments or disclosure permission are missing.

Wayframe: bounded work ownership, separate committed/speculative capacity, caller leases, individual deadlines and explicit result promotion. A canceled consumer releases its lease without killing work still required by another consumer. Upstream work is canceled only when no valid owner remains and the transport supports it safely.

Acceptance: wrong prediction, changed input, revoked access, stale result, exhausted budgets, upstream failure and one-of-many consumer cancellation. Measure hit rate, unused work, extra requests and authoritative p95 under load. Prediction never bypasses rate limits or mandatory controls. No speculative writes in this milestone.

## M3: Trace-derived candidate workflows and guarded promotion

Waypilot: normalize tool events, record parameter provenance, cluster recurring tasks and propose executable fragments. Frequency and adjacent timestamps do not establish independence. Preserve unknown relationships, required errors, pagination and effects. Mark unresolved semantic choices as model nodes. Persist candidate, evidence, contract revision and evaluation outcome.

Promotion stages: proposed → structural validation → held-out replay → controlled real-tool comparison → canary → active → retired on drift. Mock fidelity is declared per tool; passing a simulation is not production proof. Removing a model decision needs outcome evaluation separate from scheduler correctness.

Wayframe: immutable prepared regions and reusable structural plans. Cost hints and node reports remain generic. Agent clustering and model calls stay in Waypilot.

Acceptance: held-out branches and new argument values, changed tool schemas, model-dependent decisions and misleading trace correlations. Compare against authored macros and static prefetch. Charge offline compilation/evaluation cost to the break-even analysis.

## M4: Dynamic continuations and recoverable execution

Waypilot: an adapter contract for existing agents to submit/observe tool calls, results and continuation boundaries. Network gateway mode covers visible calls; SDK hooks expose in-process work. Begin with one integration and publish its coverage limits. Add HTTP MCP after stdio lifecycle qualification; add custom transport adapters through the same tool contract rather than treating every network protocol as MCP.

Wayframe: append a validated immutable execution region after previous results determine new work. Inherit the original deadline, budgets and mandatory controls. Store stable execution IDs and outcomes for resume. Never silently expand an existing region's permissions or mutate a running graph in place.

Acceptance: linear → fan-out → join → new conditional region → synchronous follow-up, cancellation at every boundary and no duplicate completed effects on recovery. External effects with uncertain outcomes require application idempotency/reconciliation; a local journal cannot create exactly-once remote execution.

## Delivery discipline

Add an execution explorer after the event contract stabilizes: compare the baseline and optimized graph, inspect reused/invalidated fragments, and display model waits, environment preparation, tool work and wasted predictions on one timeline. The viewer must read real execution events rather than animate a fabricated optimization.

Each milestone lands a runnable Waypilot scenario, a narrow Wayframe API change where necessary, framework unit/regression tests, application integration tests, a baseline comparison and trace evidence. Use pinned dependency revisions; bump the pin only after both repositories pass. Keep release benchmarks and behavioral tests separate. Do not extract a broad framework API based on a single MCP example.

First follow-up: implement M1's explicit dependency revisions and nested invalidation while preserving the current immutable-input baseline. Then use M2's speculation workload to qualify lease and cancellation behavior before enabling speculative tool calls.

## Research inputs

- [Agent JIT Compilation](https://agent-jit-compilation.github.io/): checked programs, retained semantic decisions and task-dependent scheduling. The evaluation targets web applications, not this runtime.
- [TraceCompiler](https://arxiv.org/abs/2608.02680): workflow extraction and argument provenance; its paper does not claim net efficiency gains.
- [PASTE](https://arxiv.org/abs/2603.18897): isolated speculative calls and joint serving/scheduling considerations.
- [SpecBox](https://arxiv.org/abs/2607.23933): environment prewarming and preparation/execution separation.
- [Ghost Tool Calls](https://arxiv.org/abs/2606.02483): privacy effects occur when a speculative request is issued.
- [Salsa](https://github.com/salsa-rs/salsa): dependency-aware result reuse for pure computations; mutable tool effects need additional contracts.

These establish relevant prior work. They do not establish novelty or performance for Waypilot.
