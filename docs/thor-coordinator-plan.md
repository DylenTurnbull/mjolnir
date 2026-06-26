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
- First run asks the user to select Thor workers, pick Architect or Accountant,
  and choose Thor's host agent, model preference, and reasoning level.
- The normal prompt flow has no visible model picker or agent picker.
- Thor presents an execution plan before doing work.
- The MCP bridge is provided to the Thor host as an ACP `mcpServers` stdio
  entry that launches `mj thor-mcp`.
- The final response includes a concise recap, validation, unresolved risks,
  and usage by harness/model when available.

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
  quota/rate-limit hints.
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
4. Done: first-run onboarding selects available Thor workers, persona, Thor
   host, model preference, and reasoning level.
5. Next: add a local model catalog cache populated from LM Arena and OpenRouter
   data.
6. Next: support concurrent ACP worker sessions with aggregated progress and
   usage.
7. Next: replace raw worker transcripts with richer Thor progress views while
   preserving the single coordinated chat.

## Data sources

- LM Arena leaderboard: `https://huggingface.co/spaces/lmarena-ai/arena-leaderboard`
- OpenRouter model pricing: `https://openrouter.ai/api/v1/models`

Both should be cached locally and refreshed opportunistically. If live data is
unavailable, Thor should use the most recent cache and say when routing
confidence is lower because quota/pricing data is stale or missing.
