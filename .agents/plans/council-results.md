# Council Benchmarking — Findings & Index

One-stop overview of the Mjolnir "council" (Thor solver / Loki reviewer / Eitri
delegate) benchmarking effort on DeepSWE. This is the index; deeper material is
referenced, not duplicated.

## TL;DR

- We made the council **mechanically correct** — delegation, compaction,
  advice delivery, and per-seat model/effort/caching all now work as designed,
  validated across ~500 graded DeepSWE attempts.
- We could **not** show the council robustly beats a strong solo solver at the
  sample sizes run. Every configuration lands in one noise band on same-tier
  comparisons.
- The one durable positive is an **existence proof**, not a general effect:
  on `happy-dom-deterministic-intersectionobserver`, opus-4-8 and gpt-5.6-terra
  each score **0.000** solo, yet an opus+terra+terra council reproducibly
  reaches **13/14** f2p on both runs — collaboration manufacturing capability
  neither model has alone. It held on 2 of 22 tasks; the rest showed nothing.

## Final results (null-ish, across model families)

Each block is a **different task set and metric** — do not read across rows as a
direct comparison. Capability is measured as **f2p** (fraction of new-feature
tests passing) for cross-model runs; the DeepSeek block predates that convention
and uses binary solves on the "easy-20" set. Our `partial` field is
p2p-inflated (counts pre-existing tests) — always use f2p_passed/f2p_total for
capability.

| Family | Council (Thor / Loki / Eitri) | Task set | Council result | Solo baseline | Verdict |
|---|---|---|---|---|---|
| **DeepSeek** | pro / flash / flash | easy-20 (ranks 1-20), 20×2 | best 14/40 solved (pff3) | vanilla pro 10/40; flash 4/40 | No significant council edge; all arms one noise band (p≈0.1-1.0) |
| **GPT (cross)** | sol@high / opus@high / terra@med | 11 tasks sol-can't-but-fable-can, 2 runs | f2p **0.169**, 1 solve | solo sol **0.000**; solo fable 0.500 | Modest lift over sol; stays far below fable |
| **Claude (cross)** | opus@high / terra@high / terra@high | 22 tasks opus-can't (fable-or-sol-can), 2 runs | f2p **0.069**, 0 solves | solo opus **0.000** | Aggregate ~null; 1 reproducible near-solve (happy-dom 13/14) |

Supporting facts: zero AGENT_FAILED across the graded corpus; the "council beats
X" deltas never cleared significance at n=20-40 tasks.

## What we built (three repos, cross-dependent)

The results depend on infrastructure fixes spanning three repos. Mjolnir changes
are committed; **anvil and brokkbench changes are validated but left in the
working tree** on the benchmark host (they are not part of any repo's shipping
history yet).

- **mjolnir** (committed `652ff50` + this branch): Eitri slice-and-pause
  (`code_agent` runs ~250s slices, returns PAUSED with partial diff + run_id;
  `code_agent_continue`/`code_agent_cancel`; poll-lease removed); Loki compaction
  reliability (32k threshold, abort-race fix, advice ledger, drain, gate/dedup,
  boundary rendezvous, verification-honesty contract); **per-seat reasoning
  effort** (role selectors accept `+effort`, applied via the ACP reasoning-effort
  config option).
- **anvil** (`~/Projects/anvil`, working tree): MCP timeout split
  (60s startup / 300s tool-call) + `notifications/cancelled` on timeout;
  gpt-5.6 family Bedrock Mantle `/openai/v1` routing; **Responses-API
  server-side chaining** (`store:true` + `previous_response_id`, content-keyed
  continuation cache, expiry fallback) — fixed GPT-reasoning-on-Bedrock cache
  from ~30%-and-falling to ~80%, broadly useful beyond this effort.
- **brokkbench** (`~/Projects/brokkbench`, working tree): `--council-loki` /
  `--council-eitri` (with `+effort` suffixes), `--aws-region`, deepswe engine
  wiring. Deepseek path is byte-identical to before.

Tracking ticket for deferred anvil work: **BrokkAi/anvil#292** (adaptive `_meta`
timeouts, MCP progress support, un-sliced `explore_agents` >300s gap). Add to it:
40-way Bedrock discovery race ("ACP adapter no longer advertises selected
model") — retry discovery / wait for `--default-model` before serving.

## Why the council was broken before we started

The first benchmark round measured a **broken** council. Anvil's flat 60s MCP
timeout made every >60s `code_agent` call fail (100% of sampled calls; 64% of
Eitri workers killed as abandoned; orphaned workers corrupted workspaces by
editing concurrently with Thor). The slice-and-pause redesign + 300s tool-call
budget fixed delegation to 100% completion. Any pre-fix arm is not a valid
council measurement.

## Key findings

1. **Delegation was the dominant infra bug** (fixed). See above.
2. **Advice uptake, not plumbing, is the residual lever.** Trace audits: 73% of
   failures had the correct diagnosis somewhere in Loki's advice; 63% of those
   were delivered and **ignored** by Thor. ~80% of failures are BELIEVED-DONE
   (self-verification laundering: golden-file regen, unexercised new-feature
   tests, post-failure test-scope narrowing). Framing interventions are a NULL
   (80/80 first-turn engagement at baseline); a verification protocol helped
   offline (declare-resolved 91%→55% at loss points) but was invisible live at
   n=40.
3. **Grouped/synchronous review is a closed negative result.** Batching Loki
   reviews collapsed advice supply (his emission is per-prompt, not per-step) and
   lost mid-flight timeliness; pooled sync 38/200 vs async 38/120 (p≈0.01).
   Loki's silence-bias/selectivity is load-bearing. Do not re-run.
4. **Cross-model collaboration can manufacture capability — rarely.** The
   happy-dom existence proof (above) is the clearest support for "a not-stronger
   Loki can still help Thor," but it did not generalize (2/22 tasks).
5. **n=20-40 tasks cannot resolve prompt-level interventions.** Repeated arms
   cluster in one noise band. Real signal needs 3-4× runs, the full 117-task set,
   or structural (not prompt) enforcement.

## Reproduction & artifacts

- Pinned binaries + sha256, launch scripts: `/mnt/optane/council-ab/`
  (`anvil-gpt56-chain`, `mj-652ff50-effort`, `mj-652ff50-protocol`,
  `launch-council.sh` generalized launcher).
- Result trees: `/mnt/optane/{councilpff*,councilppf*,gpt56-council,opus-terra-council,...}/`
  (per-run `results/`, `archive/` with `mjolnir.log`, `mjolnir-events.jsonl`
  council_usage, `anvil-trace.jsonl`).
- **GPT-on-Bedrock requires us-east-2 + `~/.secrets/bedrock_api_key_use2`.**
  Wire ids: `bedrock::openai.gpt-5.6-sol|terra`,
  `bedrock::us.anthropic.claude-opus-4-8`.
- **`claude-fable-5` is BLOCKED on Bedrock** (all regions:
  "data retention mode 'default' not available") — needs AWS-console
  model-access/retention config; anvil has no anthropic-direct backend, so the
  true fable-Thor mirror is blocked until that setting is enabled.
- Task selection: `deep-swe/jbe/easiest_deepswe_tasks.py` +
  `deep-swe/published-results/deepswe-v1.1/per-task-by-model-effort.csv`
  (their `avg_score` ≈ our f2p).

## Related docs

- `AGENTS.md` — canonical repo guidance (Mjolnir + tooling).
- `~/Projects/brokkbench/BPR_AGENT.md` — the benchmark runner (`bpr_agent.py`
  `--engine deepswe`), flags, result layout, scoring.
- BrokkAi/anvil#292 — deferred MCP timeout/progress work.
- Auto-memory `council-benchmark-uncommitted-state.md` — running ledger of
  uncommitted cross-repo state, per-experiment numbers, and gotchas.

## Open threads

- **Fable-Thor mirror**: one AWS-console retention setting away from running;
  everything else is ready.
- **happy-dom mechanism**: read what terra-as-Loki flagged that took opus from
  0/14 → 13/14 — is it a designable mechanism or a lucky interaction? Highest-
  value next step from these results.
- **Statistical power**: any further prompt-level council tuning needs the
  117-task set or 3-4× runs to be resolvable.
