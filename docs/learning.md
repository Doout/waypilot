# Automatic next-call learning

Waypilot can discover adjacent MCP tool calls and copy their parameters from earlier
inputs or structured results. No transition list or parameter mapping is required.
Every agent still chooses and sends each actual call. One predicted read may run ahead;
a matching demand can join it after the usual authorization and state checks.

The target needs a configured effect-free, session-independent, state-validated reuse
contract and an explicit user `speculative_tools` grant. Observed behavior cannot grant
permission, establish read-only semantics, or authorize early disclosure. Writes are
never inferred as eligible. See [HR contracts](hr.md#gateway-behavior).

For already eligible tools, replace authored `speculation.rules` with:

```toml
[speculation]
max_in_flight = 8
min_samples = 8
confidence_percent = 80
max_duration_ms = 2000

[speculation.learning]
mode = "approval" # observe, approval, auto
min_savings_ms = 10
max_waste_percent = 25
```

- `observe` collects evidence and estimates without predictive tool calls.
- `approval` lists candidates for an administrator to approve or reject.
- `auto` activates qualifying candidates, continues evaluating them, and retires rules
  with poor recent confidence, wait reduction, or excessive unused predictions.

Approval does not override performance thresholds. Rejection remains effective in auto
mode. Retired rules can recover after fresh successful shadow observations (initially 16).
Repeated failures double this interval up to 64 times the base, capped at 4,096 observations.
Late outcomes from an earlier trial remain in lifetime work counts but cannot retire a
recovered trial.
One randomly placed opportunity in each block of five stays unspeculated to refresh the
baseline without synchronizing with periodic branches. Defaults
retain 32 recent observations and require eight prediction outcomes before applying the
outcome-based retirement gate. Frequency and expected-benefit gates also apply during
that trial period. These limits can be set under `speculation.learning`.

## Review and control

```sh
waypilot admin --url http://127.0.0.1:7400 --token-env WAYPILOT_ADMIN_TOKEN --action predictions
waypilot admin --url http://127.0.0.1:7400 --token-env WAYPILOT_ADMIN_TOKEN --action approve --rule RULE_ID
waypilot admin --url http://127.0.0.1:7400 --token-env WAYPILOT_ADMIN_TOKEN --action reject --rule RULE_ID
waypilot admin --url http://127.0.0.1:7400 --token-env WAYPILOT_ADMIN_TOKEN --action learning-mode --mode auto
```

The same admin-authenticated API provides `GET /admin/predictions`,
`POST /admin/predictions/update` with `{"id":"…","approved":true}`, and
`POST /admin/predictions/mode` with `{"mode":"auto"}`. Rejection or leaving an enabled
mode cancels affected speculative work. Previously issued read requests remain observable
at the peer; cancellation is not rollback.

Each candidate reports its tool sequence, source pointers, recent samples/matches,
confidence, baseline mean, estimated net saving, status, and retirement reason. Once
used, it also reports starts, reused/unused predictions, holdouts, demand wait, and
baseline-relative net saving. Shadow estimates use the observed gap before demand,
capped by normal demand duration. Controls include cache hits when no prediction was
selected, so an already fast cache can suppress further predictions. Selected predictions
never train their own baseline. Both gates charge an unused prediction the full
estimated baseline cost. These are policy estimates, not causal end-to-end savings;
use the benchmark's separate direct and gateway trials to measure those.

Approvals/rejections persist when `state_path` is configured, with the same exclusive
file lock and atomic replacement as user overrides. Evidence is in memory: restart or
policy generation change requires new observations. Approval identities include the
user, configuration, source/target catalogs, and inferred mappings. Changing a mapping
needs its own approval. Runtime mode changes last until restart; edit configuration to
set the startup mode. At most 1,024 explicit decisions are retained.

## Discovery limits

Learning uses successful, non-overlapping actual calls in the same session. It matches
string/number leaves across inputs and `structuredContent`, including nested arguments.
Each binding needs multiple distinct observed values. If several source pointers match,
a prediction runs only while they agree; subsequent actual calls can disambiguate them.
Constants, booleans/nulls, transformations, unstructured text, cross-session sequences,
and arbitrary multi-step graphs are not inferred. Speculative completions do not train
or trigger further predictions. Catalog identity is checked again before learned dispatch.
The ordinary runtime response metadata includes `predicted_tool` when a prediction was
dispatched. Correctness tests use peer events as a completion barrier; benchmarks never do.

Defaults bound discovery to 64 rules globally, 32 argument leaves, eight possible source
pointers per binding, eight levels of nesting, 16 KiB per session context, and 1 MiB of
contexts globally. Exhausted inactive candidates can be evicted; active/approved ones
are retained. Full budgets skip discovery or nomination. Reports and `mcp.learn`/`mcp.learning` spans
contain no argument/result values; the brief source context is released after its next
call or session closure. Each actual call still enforces the original resource budgets.

## Run it

```sh
cargo build --locked
# Inspect candidates, then opt into approval for this synthetic demo.
target/sdk-venv/bin/python demos/hr_demo.py --learning approval --batch-validation
target/sdk-venv/bin/python demos/hr_demo.py --learning approval --approve-learned --batch-validation
target/sdk-venv/bin/python demos/hr_demo.py --learning auto --batch-validation
target/sdk-venv/bin/python scripts/check_learning.py target/debug/waypilot

cargo build --locked --release
target/sdk-venv/bin/python scripts/benchmark_hr.py target/release/waypilot \
  --mode direct --mode speculate_batch --mode learn_observe --mode learn_approval --mode learn_auto \
  --training-runs 6 --training-think-ms 200 --rounds 2 --output target/learning.json
```

The HR peer is owned and uses synthetic data; thinking and tool delays are timers, with
no LLM or paid service. Training uses disjoint employees. `learn_approval` explicitly
approves qualified fixture candidates after training; `learn_auto` starts from no rules.
Compare zero thinking time, cold starts, branch changes, slower validators, and load.
Reports require identical observations and exact case effects and include upstream work,
CPU/memory, and drained resources. `--baseline FILE --mode baseline_speculate_batch`
compares against a preserved pre-change binary; `baseline_prewarm_batch` provides the
same guarded execution without discovery or speculative reads. See [HR measurements](hr.md#compare).
