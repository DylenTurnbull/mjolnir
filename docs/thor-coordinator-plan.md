# Thor coordinator plan

Thor is the default `mj` experience: a coordinator persona running inside a
selected ACP host agent. `mj` injects a local MCP server into that ACP session so
Thor can list configured ACP harnesses, delegate work to them, and collect
transcripts, permission outcomes, usage, and errors.

The user-facing path is intentionally simple. The user enters a task, the Thor
host model chooses worker harnesses and models through the MCP bridge, presents
a plan for approval, executes through ACP worker sessions, then recaps what
changed and how much each harness/model used.

## UX contract

- `mj` opens a Thor host ACP session, not an agent/model picker.
- First run is a Thor setup wizard, not an advanced picker. It asks for work
  style, which ready agents Thor may use, and where Thor should run. Model
  preference and reasoning stay saved Thor defaults rather than user-facing
  picker controls.
- Before first-run setup marks a worker usable, `mj` validates configured ACP
  servers by launching each candidate and waiting for initialize plus
  `session/new`.
- The registry/setup layer is the source of available ACP server types.
  Thor's runtime worker inventory is the persisted configured ACP server set,
  not the full registry and not locally installed provider CLIs.
- The onboarding flow can add a known registry-backed agent or an installed
  agent launch command from setup, persist it, and rerun validation before Thor
  uses it. The visible happy path says `known agent` and `installed agent`;
  registry/ACP/custom-command concepts stay implementation details. It is not
  production-grade until the setup process feels like an end-user wizard rather
  than a validation list: concrete install/auth recovery actions for failed
  agents, a clear retry path, and a way for a user to reach a usable Thor host
  without editing TOML by hand.
- The normal prompt flow has no visible model picker or agent picker.
- Thor presents an execution plan before doing work.
- Thor is instructed to keep the transcript alive with short visible progress
  updates while it gathers facts and runs implementation/review/correction
  phases. `mj` also titles local sessions from the submitted user task and
  emits distinct elapsed heartbeat lines during long host turns so the TUI and
  remote/browser transcript do not look frozen when the host has not produced
  text yet. The remote-control server path also receives the Thor MCP worker
  progress side channel, not only the local TUI path. This still needs
  real-provider validation on long turns because live use reported generic Thor
  session names and no visible transcript updates over a multi-minute turn.
- `mj thor-mcp` mirrors visible worker lifecycle, tool, permission, completion,
  timeout, and error events through an out-of-band progress stream consumed by
  the interactive UI and remote tracker, so the transcript continues to update
  while the Thor host waits for a delegated ACP worker call to return.
- The MCP bridge is provided to the Thor host as an ACP `mcpServers` stdio
  entry that launches `mj thor-mcp`.
- The final response includes a concise recap, validation, unresolved risks,
  and usage by harness/model when available.
- `mj acp-smoke` is the repeatable compatibility probe. It is no-token by
  default, stopping after initialize plus `session/new`; `--prompt <text>` is
  the explicit path for exercising one `session/prompt` turn when token spend
  or a deterministic mock agent is acceptable. `--cancel-after-ms <ms>` requests
  `session/cancel` during that prompt and requires a cancelled stop reason.

## Initial routing rules

- Thor supports optimization modes:
  - balanced: legacy/default config value; first-run onboarding presents
    Architect and Accountant only.
  - cost/accountant: use cheaper models when the task is judged sufficiently
    simple.
  - best-solution/architect: for complex work, run two independent versions
    with different model families, then have Thor compare and choose the better
    result.
- Thor's coordinator model should default to a strong model.
- Model strength comes from cached LM Arena leaderboard metadata.
- Non-subscription pricing comes from cached OpenRouter model metadata.
- Claude-family models prefer Claude Code when configured.
- GPT/OpenAI-family models prefer Codex when configured.
- Other model families prefer Anvil when Anvil is configured for that model.
- Claude Code and Codex subscriptions are used evenly and maximally before
  falling back to metered OpenRouter routing, subject to remaining
  quota/rate-limit hints returned by direct Claude Code `/usage` and Codex
  appserver `account/rateLimits/read` queries. Unknown quota remains unknown;
  Thor must not invent availability.
- Simple tasks should prefer cheaper capable models; hard tasks should prefer
  stronger models.
- Every implemented task must include an adversarial review and correction
  cycle before final recap.
- Implementation and review should use separate agents/models when capacity
  allows; review should prefer a different vendor family from the
  implementation model.

## Implementation phases

1. Done: make Thor the startup/default UX and persist Thor preferences in config.
2. Done: launch Thor inside the configured ACP host and inject `mj thor-mcp`
   through ACP `mcpServers`.
3. Done: expose initial MCP tools to list ACP workers and run a prompt through
   a selected worker.
4. Done: first-run onboarding opens a Thor setup path, validates configured
   ACP server candidates, chooses the Thor host, and persists Thor defaults.
5. Done: add a local model catalog cache populated from LM Arena and OpenRouter
   data when Thor requests a refresh.
6. Done: support concurrent ACP worker sessions with aggregated progress and
   usage through the Thor MCP bridge, including live UI mirroring for visible
   worker progress during long-running delegated calls.
7. Done: return structured worker progress/tool/usage views instead of relying
   on raw worker transcript dumps.
8. Done: validate ACP worker candidates during onboarding and expose Thor MCP
   validation through both `thor_validate_acp_agents` and
   `thor_list_acp_agents` with `validate: true`.
9. Done: detect and cache quota/rate-limit hints from direct provider queries,
   then include those hints in worker listings.
10. Done: separate ACP server setup from quota probing. Registry entries and
    configured custom servers produce persisted ACP server instances; quota
    probes only run when a configured server declares a provider quota backend.
11. Partially done: make onboarding end-user quality. Installed-agent commands
    can be added from setup and revalidated through the normal configured-server
    path. Registry entries can also be added as known-agent choices without
    probing the full registry, and their website/repository links are preserved
    on configured servers. Failed rows have provider-specific guidance for
    Anvil, Claude ACP, Codex ACP, `npx`, and `uvx`. All-broken candidate sets
    no longer mark failed agents as available workers. Small and large setup
    render tests cover the recovery rows and no-ready-host summary, and every
    setup step now has automated 50x16 and 40x12 render coverage. The first
    setup screen now summarizes ready/broken agents in plain language, known
    agent rows show the command they will add when known plus setup
    expectations, current-platform binary registry entries are represented as
    installed-command candidates instead of being dropped, and a "Retry checks"
    action reruns ACP validation after install/sign-in fixes. Step summaries
    now include explicit next-action guidance for work style, agent selection,
    known-agent add, installed-agent command, recovery, and confirmation paths.
    Local provider setup profiles fill known auth/install gaps until the
    upstream registry exposes exact metadata, and those inferred hints are also
    used for generic configured-agent validation failures. Registry-provided
    exact setup hints and setup-doc URLs are persisted and preferred when
    present.
    Remaining: continue polishing the guided setup progression, replace
    remaining inferred setup labels with registry-provided exact commands/links
    where possible, manually smoke-test the setup UI across terminal sizes, and
    run real long-turn Thor smoke covering task-derived titles, local
    heartbeats, worker progress mirroring, transcript freshness in the UI the
    user is watching, and final recap.

## Quota reads

Active quota detection is provider-specific and intentionally narrow:

- Claude Code: `mj` runs the installed Claude CLI with
  `claude -p /usage --output-format json` and parses the synthetic,
  zero-token usage result into session/week quota windows.
- Codex: `mj` starts the installed `codex app-server` over stdio and sends the
  direct JSON-RPC request `account/rateLimits/read`, then maps the returned
  primary/secondary limit windows into Thor quota snapshots.

Thor quota does not use ACP metadata, stream rate-limit events, generic command
probes, or placeholder HTTP endpoints. It also does not discover or configure
workers. If a configured ACP server does not declare a direct provider quota
backend, quota remains unknown even if a provider CLI happens to be installed.

## Data sources

- LM Arena leaderboard: `https://huggingface.co/spaces/lmarena-ai/arena-leaderboard`
- OpenRouter model pricing: `https://openrouter.ai/api/v1/models`

Both should be cached locally and refreshed opportunistically. If live data is
unavailable, Thor should use the most recent cache and say when routing
confidence is lower because quota/pricing data is stale or missing.
