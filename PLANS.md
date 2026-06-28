# mjolnir plan

This document is a working plan for `mjolnir`, extracted from the earlier
Brokk-side ACP client work and adjusted for this standalone repository.

## Background

`mjolnir` started as `brokk-tui-rust` inside `BrokkAi/brokk`.

Relevant Brokk history:

- Brokk PR #3562 scaffolded `brokk-tui-rust` as "Phase 0 of Rust ACP TUI".
- Brokk PR #3592 replaced the stub with a working interactive ACP chat client.
- Brokk PR #3600 added slash-command autocomplete.
- Brokk PR #3666 removed `brokk-tui-rust` from `BrokkAi/brokk` after extracting
  it to a standalone repo with history preserved.
- The repo was briefly named `hammer`; it is now `mjolnir`, with binary `mj`.

The original intent was broader than "a Brokk TUI": build a native Rust terminal
client whose only prompt interface is Thor, backed by Agent Client Protocol
(ACP) worker harnesses. The default worker backend is `anvil`, but Thor should
stay harness-agnostic.

## Current status

The current crate is already a usable MVP:

- Spawns the configured ACP backend as the Thor host, defaulting to `anvil` on
  `PATH`, and injects a local MCP bridge into the host session.
- Talks ACP JSON-RPC over the child process stdio.
- Opens a new ACP session for `--cwd` or the current directory.
- Sends text prompts and receives prompt responses.
- Streams ACP `SessionUpdate` values into a ratatui transcript.
- Renders user messages, agent messages, reasoning/thought chunks, plans, and
  tool calls.
- Handles `session/request_permission` with a keyboard-driven modal.
- Supports slash-command autocomplete from `AvailableCommandsUpdate`.
- Supports prompt cancellation with `Ctrl-C`.
- Keeps TUI logging out of stderr via `--log-file`.
- Captures or discards agent stderr via `--agent-stderr`.
- Has focused unit tests around ACP lifecycle and UI state transitions.

Current command-line surface:

- `mj --cwd /path/to/repo` to choose the ACP session root.
- `mj --log-file /path/to/mj.log` for TUI logs.
- `mj --agent-stderr /path/to/agent.err` for child-process stderr.
- `mj thor-mcp` is the internal stdio MCP bridge injected into the Thor host
  ACP session.
- `mj acp-smoke ...` validates configured or ad-hoc ACP launch commands. By
  default it stops after initialize plus `session/new`; `--prompt <text>` is an
  explicit opt-in for exercising `session/prompt`.

There is no `--command` / `--agent` flag. Startup creates an `anvil` backend
default automatically when the platform config file has no `agent` block
(`~/.config/mj/config.toml` on Linux,
`~/Library/Application Support/mj/config.toml` on macOS, or
`%APPDATA%\mj\config.toml` on Windows).
First run is a Thor setup flow: the user chooses work style
(`Architect`/`Accountant`), chooses which validated agents Thor may use, chooses
which ready agent hosts Thor, and can add a known agent or paste the launch
command for an installed agent without editing TOML. Model preference and
reasoning level stay automatic during onboarding. The previous agent/model
picker is no longer part of the normal user path.

The remaining startup gap is not the old picker; it is production-grade
recovery and validation. The setup flow is now simpler, but failed provider rows
still rely on partly inferred install/auth guidance when the registry lacks
exact metadata. Before v1, the flow still needs real-provider recovery testing
and broader terminal-size smoke so users are not left guessing how to install,
sign in, or retry.

M1 hardening landed (PR #34): an explicit `ConnectionState` lifecycle drives
the header label, a `LaunchError` enum surfaces spawn / initialize /
`session/new` failures with one-line action hints, permission prompts queue
FIFO instead of silently overwriting, and an unexpected agent exit raises a
single Fatal instead of an unbounded "prompt failed" stream.

## Product goal

Make `mjolnir` the best small terminal client for Thor, an omni-agent
coordinator that routes work across ACP harnesses:

1. Thor-first: users type one prompt, approve one plan, and receive one recap.
2. Agent-agnostic underneath: works with Brokk, Codex-style agents, Claude ACP
   agents, Gemini agents, Goose, and any other ACP-compatible server.
3. Terminal-native: fast startup, reliable keyboard UX, readable output, and no
   GUI assumptions.
4. Protocol-faithful: use ACP primitives directly instead of inventing
   Brokk-only control paths.
5. Safe by default: permission prompts are clear, never auto-accepted, and do
   not block the JSON-RPC dispatch loop.
6. Easy to install and run: one binary, simple launch flags, sane defaults.

## Non-goals

These were present in earlier Brokk plans, but should not be v1 scope for this
repository:

- Recreating the Python/Textual `brokk-code` feature set one-for-one.
- Owning Brokk's Java/Python launcher migration.
- Implementing Brokk subcommands such as `issue`, `pr`, `provider`, `install`,
  `github`, or `commit`.
- Building a Tauri/Svelte desktop app like the removed `brokk-foreman` plan.
- Building an ACP registry browser with install/uninstall flows in v1.
- Multi-repo project management.
- Multiple raw ACP chat sessions in one UI. Thor may supervise multiple worker
  sessions, but the user-facing interface should remain one coordinated flow.
- Agent-side credential management beyond surfacing ACP auth failures clearly.

## Thor coordinator direction

Thor is not a subagent framework. It is a coordinator persona backed by a strong
model and a small set of tools. Thor always runs inside a selected ACP host
agent. At session startup, `mj` passes a stdio MCP bridge (`mj thor-mcp`)
through ACP `mcpServers`; that bridge gives Thor tools to list configured ACP
workers and run delegated prompts through them. The host model receives model
metadata, pricing, quota hints, user preferences, and the user's prompt, then
decides how to split the work and monitor worker sessions.

The durable plan lives in
[docs/thor-coordinator-plan.md](docs/thor-coordinator-plan.md).

The Thor MCP bridge now exposes configured workers, real ACP worker validation,
a cached model catalog backed by LM Arena/OpenRouter refreshes, direct quota
reads through Claude Code `/usage` and Codex appserver
`account/rateLimits/read`, optional validation on worker inventory,
single-worker delegation, and
concurrent multi-worker delegation with structured progress and aggregate
usage.

The interactive Thor runtime is still the active hardening area. Current code
sets the visible local title from the submitted user task, keeps that
task-derived title sticky after submit, rejects generic Thor/coordinator/persona
host titles locally and in the remote transcript, and strips those generic
titles from session listings. The Thor host prompt now starts with an explicit
`Task title:` line before any Thor persona instructions, so ACP hosts that
auto-title from prompt text have a user-task title source instead of seeing the
coordinator instructions first. `mj` also persists a local session-title
override keyed by ACP session id after interactive and headless runs, then
applies that override in `resume --list` and the in-app session picker. This
covers ACP hosts that omit or mangle saved titles. `mj` no longer synthesizes a
`SessionInfoUpdate` just to seed a Thor title, and the fullscreen header hides
the redundant `Thor` agent label once a task title exists.

Progress plumbing is present but still needs real user-path proof. Prompt
submission records an immediate `Thor is preparing a plan...` status in the
local transcript, and the remote tracker records the same status when it
observes the command. The UI state machine can append distinct elapsed
heartbeat lines during long host turns, and `mj` consumes the Thor MCP bridge's
out-of-band worker progress stream so delegated ACP
tool/permission/completion events can appear while the host waits for worker
calls. The remote-control server path receives the same Thor MCP progress side
channel and heartbeat stream as the local TUI path. A 2026-06-28 live report
still found generic Thor session naming and a transcript that appeared frozen
for several minutes. The current branch addresses the likely UI causes by
rejecting broader Thor persona placeholder titles (`Thor Architect`, `Thor
orchestrator ...`, etc.) and keeping the inline full-transcript reader pinned to
new live updates until the user intentionally scrolls or filters. Treat
title/progress as open until a real interactive or remote long-turn smoke proves
the exact transcript the user is watching updates correctly.

The headless `--print --output-format stream-json` path runs the same Thor MCP
bridge with a progress side channel and emits `info` stream records for worker
progress and elapsed heartbeats. A deterministic mock-host/mock-worker smoke has
now proven newline-delimited MCP bridge calls, plan submission, delegated
implementation/review/correction worker runs, mirrored worker progress, final
recap text, and non-empty final `result.text` through the real configured Thor
host path. A real Codex-host/Anvil-worker smoke has now proven real-provider
Thor MCP bridge use, worker validation, model catalog loading, structured plan
submission, implementation and adversarial-review delegation, mirrored worker
progress, elapsed heartbeats, final recap text, and usage reporting. The
correction worker timed out, but that timeout was mirrored and recapped instead
of freezing the transcript. A real configured Anvil-backed smoke has proven
heartbeat emission and bounded timeout reporting, but it did not produce a Thor
plan, worker progress, final recap, or usage before timing out. Headless
`--print` supports `--print-timeout-seconds` so real-provider smoke runs fail
cleanly instead of hanging indefinitely.

Session listing also strips generic Thor/coordinator titles reported by ACP
hosts, applies persisted local task-title overrides by session id, and uses the
locally known task title for the active session row, so the session picker no
longer reinforces host-saved "Thor session" placeholders when the user opens
`/load` during or after a task.

Initial routing policy:

- Thor supports balanced, cost/accountant, and best-solution/architect
  optimization modes.
- LM Arena leaderboard metadata ranks model strength.
- OpenRouter model metadata supplies non-subscription pricing.
- Claude-family models prefer Claude Code when configured.
- GPT/OpenAI-family models prefer Codex when configured.
- Other models prefer Anvil when configured for the target model.
- Claude Code and Codex subscription quota is used evenly and maximally before
  metered OpenRouter fallback when direct Claude Code or Codex appserver quota
  reads succeed; unknown quota remains explicit.
- Cost/accountant mode prefers cheaper capable models when Thor judges the task
  simple enough.
- Best-solution/architect mode runs two independent versions with different
  model families for complex work, then Thor compares and chooses the better
  result.
- Every implemented task includes an adversarial review and correction cycle
  before final recap; review uses different vendor models when capacity allows.

## Architecture

Runtime shape:

```text
+------------+     UiCommand      +-------------+      stdio      +-----------+
| ratatui UI | -----------------> | ACP runtime | --------------> | ACP agent |
|            | <----------------- |             | <-------------- | process   |
+------------+      UiEvent       +-------------+    JSON-RPC     +-----------+
```

Key constraints:

- The UI task owns the terminal alternate screen and crossterm input stream.
- The ACP runtime owns child-process stdio and JSON-RPC dispatch.
- UI and ACP runtime communicate over channels.
- Permission requests cross into the UI through a oneshot responder so the ACP
  dispatch loop is not blocked by terminal input.
- The client advertises ACP filesystem and terminal capabilities backed by the
  local runtime. Filesystem access is scoped to the configured session root.

## Milestones

### M0: Preserve the extracted client

Status: done.

- Rename crate and binary to `mjolnir` / `mj`.
- Keep the Brokk TUI history useful while removing Brokk monorepo assumptions.
- Add README, license, and contributor guidance.
- Keep CI checks simple: fmt, clippy, tests, release build.

### M1: Make the MVP dependable

Status: done (PR #34, 2026-05-20). Issue #35 follow-up retired
`TurnState`; turn-in-flight is now derived from `ConnectionState` via
`AppState::is_streaming`.

Goal: the current feature set should feel stable enough for daily local use.

Deliverables:

- ✅ Tighten error messages when agent launch, initialize, or `session/new`
  fails — `LaunchError` enum classifies five distinct failures
  (`CommandNotFound`, `SpawnFailed`, `StderrFileOpen`, `InitializeFailed`,
  `AuthRequired`, `SessionCreateFailed`) and each renders a headline plus a
  `hint:` line.
- ✅ Add visible connection states for launching, initializing, ready,
  streaming, cancelled, closed, and fatal — `ConnectionState` enum drives
  the header label.
- ✅ Improve shutdown so child processes are reliably reaped on normal exit
  and cancellation paths — `run()` races `drive_client` against
  `child.wait()` and surfaces an unexpected agent exit as a single Fatal.
- ✅ Make transcript scrolling predictable during active streaming and after
  resize — integration test `streaming_chunks_and_resize_preserve_user_scroll_anchor`
  locks in the reconciler composition.
- ✅ Keep permission modal behavior deterministic under streaming, resize,
  and autocomplete interactions — permission prompts queue FIFO; the modal
  header shows `(1 of N)` when more are queued; runtime close fans out
  `Cancelled` to every queued responder.
- ✅ Add regression tests for the state transitions above — test count went
  from 40 → 88, including portable integration tests on Linux / macOS /
  Windows for the agent-exit and stderr-blame paths.

Exit criteria:

- ✅ `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test`, and `cargo build --release` pass on all three CI targets.
- ✅ Manual smoke test against `anvil` can launch, send a prompt, handle a
  tool permission, cancel a prompt, and exit without a leftover child
  process.

### M2: Improve protocol coverage

Goal: render and respond to common ACP features well enough that `mjolnir` is
useful with multiple agents, not only the default one.

Deliverables:

- Render more `ContentBlock` variants with clear fallbacks:
  `resource_link`, embedded `resource`, `image`, `audio`, and unknown variants.
  ✅ Text extraction now includes useful metadata for image/audio/resource links
  and embedded resources, including MIME type, URI/name/title, known sizes, and
  embedded text resource contents. True inline media previews remain a later UI
  enhancement.
- ✅ Improve tool-call rendering for diff, terminal, and structured content —
  tool-call content is split into text, diff, and terminal outputs, terminal
  snapshots update live, and the transcript reader expands collapsed tool
  output in full.
- ✅ Hide host-agent session config options from the Thor UX; model, mode, and
  reasoning selection are Thor routing decisions, not user-facing pickers.
- ✅ Handle ACP auth-required responses with actionable UI text — shipped
  early in M1 as `LaunchError::AuthRequired`, classified at both
  `initialize` and `session/new`.
- ✅ Track compatibility quirks discovered with at least two non-Brokk ACP
  agents — `@agentclientprotocol/claude-agent-acp` 0.36.1 and OpenCode
  1.17.11 are recorded in the Compatibility section below.
- ✅ Add a repeatable compatibility smoke path — `mj acp-smoke
  --command "<agent acp command>"` or `mj acp-smoke --configured-source-id
  <id>` runs initialize plus `session/new`, prints text/json capability
  evidence, and exits non-zero if the agent is not usable. `mj acp-smoke
  --list-configured` shows the persisted Thor source IDs, and
  `mj acp-smoke --all-configured` validates the whole persisted Thor worker set
  after onboarding or config edits. By default this is no-token; passing
  `--prompt <text>` explicitly sends one prompt turn, records completion and
  stop reason, and fails if `session/prompt` does not complete. Adding
  `--cancel-after-ms <ms>` requests `session/cancel` during that prompt and
  fails unless the turn completes as cancelled.
- Explore session rewind as an ACP extension paired with Anvil. The current
  proposal is documented in [docs/session-rewind-extension.md](docs/session-rewind-extension.md):
  model rewind as fork-from-checkpoint using `session/fork` `_meta`, not as
  in-place mutation of the active session.

Exit criteria:

- Manual compatibility matrix documents which ACP agents were tested and which
  features worked.
- Unsupported ACP features degrade visibly and politely instead of becoming
  silent no-ops.

### M3: Make command entry and session workflow pleasant

Goal: reduce friction in everyday terminal use.

Deliverables:

- ✅ Prompt history across the current process.
- ✅ Optional persisted prompt history under a user config directory.
- ✅ Better multiline editing, including newline insertion and submit semantics.
- ✅ Search or filter over the transcript through the inline full-transcript
  reader.
- ✅ Copy-friendly transcript output mode or an export command.
- ✅ Session title display and clearer session metadata.
- ✅ Startup now opens Thor and defaults to `anvil` without exposing an agent or
  model picker.

Exit criteria:

- A user can start `mj`, get the Thor default backend, carry out a few turns,
  recover recent prompts, and copy or export useful output without leaving the
  terminal.

### M4: Installation and distribution

Goal: make `mj` easy to install without cloning the repo.

Deliverables:

- ✅ GitHub release workflow for Linux x86_64, macOS aarch64, and Windows
  x86_64. The current workflow also publishes Linux aarch64, macOS universal,
  and Android/Termux artifacts.
- ✅ Release artifacts named consistently, with `.sha256` checksums.
- ✅ Document `cargo install --git`, release binary install, shell installer,
  and local build paths in README.
- ✅ Use the shell installer as the first install path. Do not add Homebrew for
  v1 unless user demand justifies maintaining a formula.
- ✅ Keep `mj` independent of Brokk's installer for v1. Revisit only if Brokk
  product packaging explicitly owns Thor distribution.
- ✅ The installer has a no-network self-test for release metadata parsing,
  checksum lookup, and Linux/macOS `mj` plus `bifrost` asset selection.
- ✅ Re-verified the local installer guardrail on 2026-06-28:
  `bash -n install.sh` exited cleanly and `./install.sh --self-test` printed
  `mjolnir-installer: self-test passed`.

Exit criteria:

- Fresh machine install path works for at least macOS aarch64 and Linux x86_64.
  The release workflow and installer support this, and the installer now has a
  no-network `mj` plus `bifrost` asset-selection self-test; a fresh-machine
  smoke pass still needs to be recorded before calling distribution
  production-grade. Use
  [docs/install-smoke.md](docs/install-smoke.md) for the exact evidence to
  collect; tracked in
  [#249](https://github.com/BrokkAi/mjolnir/issues/249).
- `mj --version` and `mj --cwd .` work after install.

### M5: Optional client capabilities

Goal: decide whether `mjolnir` should become more than a prompt/permission UI.

Candidate capabilities:

- ACP filesystem operations (`fs/read_text_file`, `fs/write_text_file`) backed
  by local disk. Design note:
  [docs/trust-folder-support.md](docs/trust-folder-support.md).
- ACP terminal operations backed by a managed subprocess view.
  Design note: [docs/trust-folder-support.md](docs/trust-folder-support.md).
- ACP registry lookup and agent launch presets. Design note:
  [docs/acp-registry-presets.md](docs/acp-registry-presets.md).
- Deeper session persistence UX beyond the current session history, `/load`,
  and `/fork` support, including checkpoint/rewind flows. Design note:
  [docs/session-rewind-extension.md](docs/session-rewind-extension.md).

These are intentionally later because they can expand the blast radius quickly.
Each should start with a design note before implementation; the current
candidate notes are linked above.

## Feature backlog

Near-term:

- ✅ Multiline input.
- ✅ Prompt history.
- ✅ More complete `SessionUpdate` rendering (image/audio/resource metadata and
  resource text fallbacks; structured tool-call output for diff/terminal).
- Compatibility smoke tests against more non-Brokk ACP agents (one done in
  M1; see the Compatibility section).
- Production-grade Thor first-run onboarding: the old advanced picker is gone,
  and setup now starts with architect/accountant mode, lets users choose which
  ready agents Thor may use, and supports custom/known-agent setup from the
  flow. The remaining blocker is not the basic path; it is proving recovery
  quality with real providers, exact registry/auth guidance, and more
  real-terminal smoke. Tracked in
  [#252](https://github.com/BrokkAi/mjolnir/issues/252).

(M1 closed: fatal/error rendering, child-process cleanup, transcript
scrolling.)

Medium-term:

- Real-provider Thor runtime smoke: verify task-derived titles, visible
  planning, elapsed heartbeats, worker progress mirroring, final recap, and
  usage reporting with Claude ACP, Codex ACP, Anvil, and at least one non-Claude
  non-OpenAI host.
- Registry/setup metadata coverage: replace remaining inferred install/auth
  hints with exact registry-provided setup commands and docs.
- Compatibility smoke expansion for Gemini CLI and Goose once installed or once
  an approved safe test environment exists.
- Session checkpoint/rewind support if agents expose it through the experimental
  `_meta` extension.

Later:

- Filesystem capability support.
- Terminal capability support.
- Multiple sessions.

## Onboarding (Thor first-run) issues

Tracked from review of PR #243 (`codex/thor-orchestrator`, "thor onboarding").
The first-run Thor setup screen (`src/thor_setup.rs`) must be end-user-quality
before it ships. The flow is the first product impression, so implementation
concepts must not leak into the setup path.

Current assessment: the flow is improved but still not production-grade. It is
no longer the old advanced picker: it starts with work style, exposes which
ready agents Thor may use, and keeps model routing defaults out of the user
path. A new user can add a known agent or paste the launch command for an
installed agent from onboarding, and `mj` validates it before Thor uses it. The
remaining product problem is recovery quality: failed rows can still land on
broad install/auth messages when the registry does not provide exact setup
instructions, so users may still have to infer whether they need to install
Node/uv, sign in to Claude or Codex, pick a known agent, or retry validation
after fixing their environment.

Required end-user setup behavior before production:

- First screen must clearly answer: what Thor is, what is already usable, what
  needs setup, and the next best action.
- A user with no working agent must have an obvious path to install or
  configure one without editing TOML.
- Known-agent choices must show plain-language setup expectations before they
  are added, including install/auth hints and the command that will be run when
  known.
- Failed validation must produce a concrete recovery action and a retry path,
  not just a disabled row.
- The UI must not require understanding ACP, quota backends, source IDs, or raw
  package names unless the user opens the installed-agent command path.
- The success path must feel like: choose work style, choose which ready agents
  Thor may use, choose where Thor runs, optionally add/fix an agent, start Thor.
  It must not expose raw routing internals, quotas, source IDs, or model picker
  controls.
- Manual smoke must verify the setup flow with no configured agents, one broken
  default, at least one known-agent add, and one successful configured agent.

Fixed in this PR:

- [x] Replaced the old "Thor is the only prompt path." headline with first-run
  copy that explains Thor as the coordinator and says choices can be changed
  later.
- [x] Replaced the old advanced worker/model/reasoning picker with a simple
  ready-agent selection step. The user chooses which validated agents Thor may
  use and which ready agent hosts Thor, without seeing source IDs, quota
  backends, model picker controls, or reasoning controls.
- [x] Stopped seeding first-run candidates from the full ACP registry.
  Onboarding now validates configured/custom/default agents only, so first
  launch no longer probes uninstalled registry packages.
- [x] Made the setup window responsive instead of a fixed 80x24 box.
- [x] Added cursor-following row windowing so long setup lists remain reachable.
- [x] Reworded setup steps and summary labels away from worker/quota-backend
  jargon.
- [x] Replaced "persona" step copy with "work style" copy that explains the
  architect/accountant tradeoff in user-facing terms.
- [x] Collapsed first run from host/work-style/model/reasoning/confirm to
  work-style/agents/confirm; model preference and reasoning now use saved Thor
  defaults and stay out of the onboarding path.
- [x] Replaced the dead-end "needs setup" validation label with inferred user
  actions such as `install <program>` or `sign in or add key`.
- [x] Added an onboarding recovery path to add a custom ACP command, persist it
  as a named custom agent, rerun ACP validation, and only expose it to Thor after
  the normal configured-server path sees it.
- [x] Added an onboarding path to add ACP registry entries without probing the
  whole registry. Selecting a registry entry persists it as a configured Thor
  ACP server, then the normal validation loop decides whether it is usable.
- [x] Preserved registry website/repository links on configured servers and
  surfaced those links in failed-row setup guidance when available.
- [x] Kept failed candidates visible while making the add-command row reachable
  in long or mostly broken setup lists.
- [x] Added provider-specific failed-row guidance for Anvil, Claude ACP, Codex
  ACP, `npx`, and `uvx` failures, including clearer install/sign-in next steps.
- [x] Fixed the all-broken state so failed candidates remain visible but are not
  internally treated as available Thor workers; the summary now says no Thor
  host is ready instead of naming a failed host.
- [x] Added automated render coverage for Thor setup at small and large terminal
  sizes, including the registry/custom recovery rows and no-ready-host summary.
- [x] Added an explicit "Retry checks" setup action. After a user installs a
  missing command or signs in, onboarding reruns ACP validation without making
  them quit and restart `mj`.
- [x] Made the first setup screen state the current readiness summary and next
  action in plain language instead of only listing validation results.
- [x] Show the command that registry-backed setup will run when the registry
  entry is added, so users are not asked to choose from names alone.
- [x] Show inferred install/auth expectations for registry rows before adding
  them, such as Node.js/npm, uv, Claude Code sign-in, Codex sign-in, Gemini CLI
  auth, OpenCode config, Cursor auth, or GitHub Copilot auth when known.
- [x] Registry-backed onboarding now includes current-platform binary
  distributions as installed-command candidates instead of dropping binary-only
  agents such as OpenCode, Goose, and Cursor. `mj` does not download or execute
  registry binaries during setup; validation still proves whether the command
  is actually installed and usable.
- [x] Added local provider setup profiles for known registry entries so setup
  rows can say which companion CLI or provider configuration is required when
  the upstream registry does not expose exact auth metadata.
- [x] Replaced first-run summary labels that exposed internal source IDs,
  model defaults, and reasoning defaults with friendly agent names and a simple
  work-style summary. Model selection remains automatic during onboarding.
- [x] Replaced primary setup action labels like `Add from ACP registry` and
  `Add ACP command` with end-user wording: `Add known agent` and
  `Add installed agent`.
- [x] Made the setup summary step-aware. While selecting a registry entry it
  shows `Will add`, `Runs`, and `Setup` as separate lines before anything is
  persisted; while adding an installed agent it states that `mj` checks the
  command before Thor uses it.
- [x] Corrected config-path docs to describe the actual platform config
  directory. The macOS path is `~/Library/Application Support/mj/config.toml`,
  not `~/.config/mj/config.toml`.
- [x] Added provider-specific recovery labels for known binary registry agents
  when validation exits or times out without an auth-shaped error. OpenCode now
  shows `set OpenCode provider` / `Set provider, retry` instead of generic
  `agent exited` guidance.
- [x] Manually smoke-tested the 80-column no-working-agent first-run path with a
  temporary home and stripped `PATH`:
  `HOME=/tmp/mj-thor-smoke-home-4 XDG_CONFIG_HOME=/tmp/mj-thor-smoke-home-4/config XDG_CACHE_HOME=/tmp/mj-thor-smoke-home-4/cache PATH=/usr/bin:/bin target/debug/mj --cwd .`.
  Verified the rebuilt binary opens the new `Set up Thor` flow, not the old
  worker/model picker; shows no-ready guidance; defaults to `Add installed agent`;
  keeps `Retry checks` visible; and exits cleanly with Esc.
- [x] Manually smoke-tested an 80-column configured-but-broken path with a
  temporary macOS config under `/tmp/mj-thor-success-smoke/Library/Application Support/mj/config.toml`
  pointing at a local OpenCode ACP wrapper. OpenCode could not validate in that
  isolated setup, but the failure rows now render compactly as `agent exited /
  Check auth/config, then retry` and `timeout / Retry after install/auth is
  ready`, with `Add installed agent` and `Retry checks` still reachable.
- [x] Re-ran the 80-column configured OpenCode path with an isolated macOS home
  under `/tmp/mj-thor-opencode-success`, symlinking the real OpenCode config
  directories. OpenCode still exits during validation in that isolated setup, so
  this does not close the successful configured-agent smoke requirement. It did
  verify the improved failed row: `OpenCode / set OpenCode provider / Set
  provider, retry; docs: opencode`.
- [x] Manually smoke-tested the registry-add path at 80 columns with a copied
  registry cache under `/tmp/mj-thor-registry-smoke/Library/Caches/mj/registry.json`.
  The flow opened `Add known agent`, showed the step-aware `Will add /
  Runs / Setup` summary, selected the binary OpenCode entry without fetching
  `npx` code, persisted it to
  `/tmp/mj-thor-registry-smoke/Library/Application Support/mj/config.toml`, and
  reran validation. The registry count dropped from 37 to 36 and OpenCode
  returned as a configured-but-not-ready row with provider-specific recovery
  guidance.
- [x] Manually smoke-tested the successful configured-agent path at 80 columns
  with a deterministic local mock ACP command under `/tmp/mj-mock-acp.py` and
  isolated macOS config under `/tmp/mj-thor-success-mock/Library/Application Support/mj/config.toml`.
  The setup screen showed `Mock ACP / ready`, `Ready to use: Mock ACP`, and
  `Run Thor in: Mock ACP`; confirming `Start Thor` saved the configured host,
  selected worker, and `onboarding_complete = true` before handing off to the
  later theme picker.
- [x] Fixed a Thor onboarding completion bug found during that smoke: the Thor
  completion marker was previously written only after the later spinner picker,
  so cancelling theme/spinner could make completed Thor setup repeat.
- [x] Reworked the first-run setup sequence so it starts with an end-user work
  style choice (`Architect` or `Accountant`), then shows ready agents with
  explicit include toggles for the agents Thor may use, and lets Enter choose
  the ACP agent that hosts Thor. The summary now states the work style, selected
  worker agents, and Thor host instead of implying hidden defaults.
- [x] Manually smoke-tested the new work-style-first onboarding screen with an
  isolated fresh home under `/tmp/mj-thor-persona-smoke-2` and stripped `PATH`.
  The first screen now shows readable `Architect` and `Accountant` choices at
  the 80-column terminal size used by the PTY smoke, and pressing Enter advances
  to the agent step where the no-ready-agent path defaults to `Add custom
  command` while keeping Anvil install guidance visible.
- [x] Fixed the interactive Thor runtime title path in the UI state machine:
  `record_user_prompt` now assigns the visible session title from the submitted
  user task immediately, before any host-supplied title can arrive, and later
  host-supplied titles no longer overwrite the user-task title once it exists.
- [x] Moved the raw user task to the top of the Thor host prompt and instructed
  host agents to use that task, not the Thor persona preamble, when setting a
  saved session title.
- [x] Fixed the remote/browser transcript title path: the remote tracker now
  names a session from the first real task when the current name is blank, a
  raw session id, or a generic Thor title, and ignores later generic Thor host
  titles and host renames after a task name exists.
- [x] Fixed session-list title display for generic Thor host names: ACP session
  listings now drop generic Thor/coordinator titles, and the in-app session
  picker overrides the active session row with the locally known user-task
  title.
- [x] Removed the synthetic first-turn `SessionInfoUpdate` that was used only
  to seed a Thor title. The runtime now relies on the real submitted prompt for
  local/remote task naming and ignores generic Thor host titles.
- [x] Hid the redundant `Thor` header agent label after a task title exists, so
  users do not see a title-like Thor placeholder competing with the actual
  session title in the main header.
- [x] Added an immediate user-visible Thor planning status when a task is
  submitted or a queued task drains. The local transcript and remote tracker
  both record `Thor is preparing a plan...` before the host model has to stream
  anything, and the Thor host prompt still instructs providers to emit concise
  progress updates around long-running fact gathering and
  implementation/review/correction phases.
- [x] Added a UI-state fallback heartbeat for active Thor turns, so the local
  transcript can append distinct `Thor is still working... Ns elapsed` entries
  even if the host agent streams no text and no worker side-channel event has
  arrived yet.
- [x] Brought headless `--print --output-format stream-json` onto the same Thor
  progress bridge: it now injects `mj thor-mcp` with a progress file and emits
  stream `info` records for worker progress and elapsed heartbeats during the
  active Thor host turn.
- [x] Added bounded headless Thor smoke support with
  `--print-timeout-seconds`, so a provider that never produces a plan, worker
  progress, or final recap returns a structured timeout error/result instead of
  requiring manual interruption.
- [x] Added a live Thor worker progress side channel from `mj thor-mcp` back to
  the interactive UI. Visible worker lifecycle, tool, permission, completion,
  timeout, and error events are mirrored into the transcript, so a delegated ACP
  run should no longer look frozen while the host Thor agent is blocked waiting
  for a worker result.
- [x] Made local Thor status events remote-visible. Planning, long-running
  heartbeats, and worker side-channel progress are now recorded as `system`
  entries in the remote/browser transcript instead of only appearing in the TUI.
- [x] Wired the remote-control server path to the same Thor MCP progress file
  and elapsed heartbeat used by the local TUI path, so browser-only Thor
  sessions can receive worker progress and distinct long-turn status entries.
- [x] Added regression coverage for distinct Thor heartbeat messages so long
  host turns do not lose progress lines to transcript dedupe; inactive periods
  reset the elapsed timer before the next turn.
- [x] Fixed headless and Thor delegated-worker text collection so an ACP agent
  that streams `agent_message_chunk` immediately after a prompt, without first
  echoing a user chunk or tool event, still contributes to the final answer.
- [x] Added a final headless progress-file drain before emitting the final
  stream `result`, so worker progress written just before `PromptDone` is not
  lost when the progress polling task is stopped.
- [x] Made visible delegated-worker progress include the planned job id and
  worker source, so implementation/review/correction phases no longer collapse
  into duplicate-looking `worker session ready` / `end_turn` messages.
- [x] Removed the remaining registry/custom-command/ACP jargon from the main
  first-run setup screen. The visible path now says `known agent` for registry
  choices and `installed agent` for pasted commands, while implementation terms
  stay internal.
- [x] Added automated small-terminal render coverage for every first-run setup
  step at 50x16 and 40x12, extending the previous 72x24 and 120x36 recovery
  path coverage.
- [x] Added step-specific `Next` guidance to the setup summary so the work
  style, agent selection, known-agent add, installed-agent command, recovery,
  and confirmation steps each tell the user the next action without relying
  only on footer shortcuts.
- [x] Threaded inferred setup hints into configured-agent validation rows, so
  unclassified exits, timeouts, and no-detail failures can still show the
  concrete install/auth expectation before `Retry checks` instead of falling
  straight back to generic `Check auth/config` copy.
- [x] Added a forward-compatible exact setup metadata path for registry-backed
  agents. Registry entries can now carry setup hints and setup-doc URLs into
  persisted Thor server config, and onboarding prefers those exact hints over
  local inferred provider profiles when present.
- [x] Split registry-backed setup metadata into structured install and auth
  labels (`setup_install` / `setup_auth`) while keeping the legacy combined
  `setup_hint`. Exact registry `setup.install`, `setup.auth`, and
  `setup.docsUrl` now persist into Thor server config, `mj acp-smoke
  --list-configured` JSON, known-agent setup summaries, and validation
  recovery labels. Known-provider and distribution fallbacks also populate the
  structured fields so recovery rows no longer have to parse a combined hint to
  decide whether the user needs install or auth/setup work.
- [x] Persisted known-provider setup fallback hints from registry resolution
  when upstream registry entries omit exact setup metadata. Claude, Codex,
  Gemini, OpenCode, Goose, Cursor, GitHub Copilot, and Anvil now carry install
  and auth expectations into saved Thor server config instead of relying only on
  transient onboarding inference; exact registry setup metadata still wins.
- [x] Added conservative distribution-based fallback setup hints for registry
  entries outside the known-provider list. Registry-backed `npx`, `uvx`, and
  current-platform binary agents now persist package-manager/install guidance
  plus "configure or sign in if prompted" text instead of blank setup metadata.
- [x] Added automated provider recovery matrix coverage for Anvil, Claude,
  Codex, Gemini, OpenCode, Goose, Cursor, and GitHub Copilot rows, and made
  Gemini generic exits/timeouts resolve to Gemini sign-in guidance instead of
  generic `agent exited` / `timeout` copy.
- [x] Manually smoke-tested the structured setup metadata path at 80x24 with a
  deterministic mock ACP command under `/tmp/mj-mock-acp.py` and isolated macOS
  home `/tmp/mj-thor-structured-smoke`. The setup flow showed `1 ready, 1 need
  setup`, listed `Mock ACP / ready` as both an available worker and Thor host,
  showed the broken Anvil row with install/setup recovery text, confirmed
  `Start Thor`, saved `onboarding_complete = true`, selected
  `custom:Mock ACP`, preserved `setup_install = "install Python 3"` and
  `setup_auth = "no sign-in required"`, then handed off to the theme picker.
  Follow-up checks showed `mj acp-smoke --list-configured --format json`
  returning `setupInstall`/`setupAuth`, and
  `mj acp-smoke --all-configured --format json` validating the mock ACP server
  as usable in 19ms.

Still not production-grade:

1. **Thor runtime progress and titles need real long-turn validation.**
   Live use on 2026-06-28 found generic Thor session naming and a transcript
   that appeared frozen for several minutes. Current branch code keeps
   user-task titles sticky, rejects broader Thor/coordinator/persona host
   titles locally and in the remote/browser transcript and session lists, no
   longer emits a synthetic Thor title update at first prompt, adds a `Task
   title:` line before the Thor persona in the host prompt, persists local
   task-title overrides for interactive/headless sessions and applies them to
   future session listings, hides the redundant `Thor` header label once a task
   title exists, records immediate local and remote planning status, records a
   UI-state fallback heartbeat during active local turns, keeps the
   remote-control heartbeat, mirrors Thor MCP worker progress, keeps the inline
   full-transcript reader pinned to live updates until manual scroll/filter,
   and exposes the same progress stream through headless
   `--print --output-format stream-json` for repeatable smoke capture.
   Deterministic tests cover those local/remote/headless/listing plumbing paths,
   including broader Thor persona-title rejection and transcript-reader
   tail-follow behavior.
   A deterministic mock-host/mock-worker headless smoke now proves the real Thor
   MCP bridge can list workers, load a cached model catalog, accept a structured
   plan, run implementation/review/correction worker phases, mirror all three
   phases into stream `info` records, and return a non-empty final recap in
   `result.text`. A real-provider Codex-host/Anvil-worker headless smoke now
   proves bridge use, worker validation, model catalog loading, plan
   submission, implementation delegation, adversarial-review delegation,
   mirrored worker progress, elapsed heartbeats, final recap, and usage
   reporting in the watched stream; a follow-up title-persistence smoke proved
   `resume --list` displays the locally persisted task title for a Codex host
   session whose provider title would otherwise be absent. A bounded
   real-provider Anvil-backed headless smoke proves heartbeat emission and
   structured timeout handling, but it timed out before Thor produced a plan,
   worker progress, or recap. What remains is real interactive and remote
   browser validation of the same behavior, plus a real-provider correction
   phase that completes instead of timing out. This item stays open until those
   paths are recorded.
2. **Registry-backed agent setup still needs broader upstream metadata coverage.**
   Registry entries can now be added from onboarding, and website/repository
   links, launch commands, binary installed-command candidates, local provider
   setup profiles, distribution-based fallback hints, exact setup metadata
   fields, and structured install/auth labels are shown when available.
   Known-provider and package-manager fallback labels are now persisted into
   saved Thor server config when upstream registry entries omit setup metadata,
   and validation recovery rows prefer exact structured install/auth text over
   hardcoded generic copy. A live registry check on 2026-06-28 found 37 entries
   and no `setup` or `setupHint` metadata, so the remaining exact-metadata gap
   is upstream registry content rather than an unparsed current field. What
   remains is broader upstream registry coverage for agents outside the
   known-provider and distribution fallback set. Tracked in
   [#250](https://github.com/BrokkAi/mjolnir/issues/250).
3. **Thor setup still needs a real end-user recovery pass.** The main path is
   now the intended Thor setup path: choose work style, choose agents Thor may
   use, choose where Thor runs, optionally add/fix an agent, then start. It is
   not done just because the old picker is gone. What still needs production
   validation is the unhappy path: exact copy, action ordering, failure
   recovery, terminal sizes, and real provider success/failure combinations.
   Tracked in
   [#252](https://github.com/BrokkAi/mjolnir/issues/252).
4. **The setup UI has only been manually smoked for a few terminal scenarios.**
   Unit tests cover state transitions, list windowing, small/large recovery
   rendering, and every setup step at 50x16 and 40x12; manual smoke now covers
   the no-working-agent 80-column path, a configured-but-broken 80-column path,
   a known-agent add path, a successful configured-agent path, structured
   setup metadata persistence with a deterministic mock ACP server, and the
   work-style-first fresh-home path. Broader real-terminal smoke is still
   useful before calling onboarding
   production-grade. Tracked in
   [#252](https://github.com/BrokkAi/mjolnir/issues/252).

## Risks and open questions

1. **How agent-agnostic should the UI stay?** Brokk-specific affordances can make
   Brokk better, but they should not turn the core into a Brokk-only client.
2. **Do we want a config file?** Launch presets and history need persistence, but
   a config format adds compatibility burden.
3. **Should `mj` implement filesystem capabilities?** Local-disk reads are easy;
   doing it safely and predictably with permissions is harder.
4. **Should `mj` implement terminal capabilities?** It fits the app domain, but
   a nested terminal view inside a TUI is a substantial feature.
5. **How much of the old Brokk launcher plan still matters?** The standalone
   repo should not inherit the entire Java/Python migration unless that becomes
   an explicit product decision.
6. **What are the first-class target agents?** We need a small compatibility
   matrix to avoid designing only against one agent.

## Compatibility

Smoke-tested against non-Brokk ACP agents to validate the
"agent-agnostic terminal client" goal (PLANS.md goal #1). Each entry
records the date, agent version, and what worked at the protocol layer.
Update this table when re-running against newer versions or new agents.

Use `mj acp-smoke --command "<agent acp command>" --source-id <name>` for new
matrix entries when a full model turn is not needed. To verify the exact
command/env Thor will use after onboarding, run `mj acp-smoke
--list-configured` to discover source IDs, then `mj acp-smoke
--configured-source-id <id>` against a persisted Thor configured ACP server, or
`mj acp-smoke --all-configured` to validate every persisted Thor worker in one
pass. The smoke starts the ACP server, validates initialize plus
`session/new`, records advertised capabilities, and shuts down without sending
`session/prompt` unless `--prompt <text>` is supplied. Add `--format json` when
preserving machine-readable evidence. Use `--prompt` or `--cancel-after-ms`
only when token spend is acceptable or when testing a deterministic mock ACP
agent. Use the top-level `--agent-stderr <path>` before `acp-smoke` when a
server exits before initialize and the JSON result needs subprocess stderr for
diagnosis.

### Thor deterministic MCP delegation smoke — 2026-06-28

Source: isolated configured Thor host path with two local deterministic ACP
scripts: `/tmp/mj-thor-mcp-host.py` as the Thor host and
`/tmp/mj-thor-mcp-worker.py` as the configured worker. The host mock reads the
`mcpServers` entry passed through ACP `session/new`, starts `mj thor-mcp`, calls
Thor MCP tools over newline-delimited JSON, submits a structured plan, delegates
implementation/review/correction prompts, and returns a final recap. The worker
mock returns deterministic text without model or network use.

Launch:

```text
HOME=/tmp/mj-thor-mcp-smoke XDG_CONFIG_HOME=/tmp/mj-thor-mcp-smoke/config XDG_CACHE_HOME=/tmp/mj-thor-mcp-smoke/cache target/debug/mj --output-format stream-json --permission-mode acceptEdits --print-timeout-seconds 20 --print "deterministic Thor MCP delegation smoke"
```

Observed stream evidence:

| Feature | Result |
| --- | --- |
| ACP host connection | `connected` stream record emitted for `Mock Thor Host 1.0.0` |
| session start | `session_started` stream record emitted with session `mock-thor-host-session` |
| MCP bridge framing | host mock successfully called `tools/list` and Thor tools over newline-delimited JSON |
| model catalog | loaded from an isolated seeded cache, avoiding network/model spend |
| plan and workflow | host submitted an implementation/review/correction plan accepted by `thor_submit_plan` |
| worker progress | stream emitted distinct `info` records for `impl`, `review`, and `correction` jobs: started, prompt sent, and done |
| final recap | `agent_message` and final `result.text` contained `Thor deterministic recap` plus implementation/review/correction worker text |
| usage reporting | not expected; deterministic mocks do not emit usage |

This proves the no-token Thor bridge/runtime path and fixed two bugs found by
the smoke: headless/delegated output collection no longer drops immediate
`agent_message_chunk` text, and headless drains final progress-file lines before
emitting the final stream result. It does not replace the required successful
real-provider long-turn smoke.

### Thor real-provider Codex-host smoke — 2026-06-28

Source: isolated Thor config with Codex ACP as the Thor host
(`custom:codex alt`, `@agentclientprotocol/codex-acp 0.0.46`) and the
configured Anvil/Codex workers available through `mj thor-mcp`. The run used
the real installed providers and auth, not deterministic mocks. The task was
read-only and bounded by `--print-timeout-seconds 240`.

Launch:

```text
MJ_CONFIG=/tmp/mj-thor-codex-host-smoke/config.toml target/debug/mj --agent-stderr /tmp/mj-thor-codex-host-smoke.err --output-format stream-json --permission-mode acceptEdits --print-timeout-seconds 240 --print "Task title smoke: identify repo name. Do not modify files. Use Thor MCP tools. First call thor_list_acp_agents with refreshQuota=true and validate=true, then call thor_get_model_catalog. Submit a concise structured plan with exactly three jobs: implementation by custom:codex alt, review by anvil, correction by custom:codex alt. Reuse each job prompt exactly when calling thor_run_acp_agent. Set timeoutSeconds to 30 and permissionMode to reject for every worker run. Implementation prompt: identify the repository name from the current working directory path only and answer repo: <name-or-unknown>. Review prompt: adversarially check whether repo: mjolnir is consistent with the current working directory path only; avoid commands. Correction prompt: reconcile implementation and review; if no issue, answer no correction needed; repo: mjolnir. After all three phases, finish with a short recap including workers used, visible worker progress, and any usage reported."
```

Observed stream evidence:

| Feature | Result |
| --- | --- |
| ACP host connection | `connected` stream record emitted for `@agentclientprotocol/codex-acp 0.0.46` |
| session start | `session_started` stream record emitted with session `019f0bba-9bde-78f0-ab8b-5f33c9075776` |
| worker inventory | host called `thor_list_acp_agents` with validation/quota refresh and reported Codex/Anvil usable |
| model catalog | host called `thor_get_model_catalog` before planning |
| plan and workflow | host submitted a three-job implementation/review/correction plan accepted by `thor_submit_plan` |
| heartbeat | stream emitted elapsed `info` records through 120s |
| implementation progress | stream emitted started, prompt-sent, and done records for `implementation-repo-name` on `custom:codex alt` |
| review progress | stream emitted started, prompt-sent, and done records for `review-repo-name` on `anvil` |
| correction progress | stream emitted started and prompt-sent records plus visible tool-attempt records for `correction-repo-name` on `custom:codex alt` |
| correction outcome | delegated worker timed out after 30s; timeout was mirrored into stream `info` and included in the final recap |
| final recap | final `result.text` contained a recap with workers used, visible worker progress, timeout note, and usage |
| usage reporting | final recap reported 14,694 total tokens for implementation and 8,850 total tokens for review; final stream usage reported 62,734 total host tokens |

Follow-up title persistence smoke:

```text
MJ_CONFIG=/tmp/mj-thor-codex-host-smoke/config.toml target/debug/mj --agent-stderr /tmp/mj-thor-title-store-smoke.err --output-format stream-json --permission-mode acceptEdits --print-timeout-seconds 20 --print "Title persistence smoke unique 1782608290. Answer with ok and do not modify files."
MJ_CONFIG=/tmp/mj-thor-codex-host-smoke/config.toml target/debug/mj resume --list --format json
```

Observed result: the new Codex host session
`019f0bc0-03b3-7ff3-8c42-9c99014fbb8c` listed with title
`Title persistence smoke unique 1782608290. Answer with ok and do not modify file...`,
proving the local session-title override is applied when listing sessions.

Known gaps:

- This proves the headless stream path, not the fullscreen TUI transcript or
  browser remote transcript.
- The correction phase timed out instead of completing normally, though the
  timeout was visible and recapped.
- Anvil-hosted Thor still needs a successful bridge-use smoke; the earlier
  Anvil-hosted run only proved heartbeat and bounded timeout behavior.

### Thor headless runtime smoke — 2026-06-28

Source: configured Thor host path using the local configured Anvil-backed
agent. This was a real-provider headless Thor runtime run, not a no-token
protocol-only ACP smoke.

Launch:

```text
mj --output-format stream-json --permission-mode acceptEdits --print-timeout-seconds 45 --print "Thor runtime smoke. Do not modify files. Use the Thor tools to list configured ACP agents, submit a concise plan, delegate one read-only worker task if a usable worker is available, include an adversarial review step if practical, then recap what happened with any usage reported. Keep the final answer short."
```

Observed stream evidence:

| Feature | Result |
| --- | --- |
| ACP host connection | `connected` stream record emitted |
| session start | `session_started` stream record emitted with session `caed4053-f9e9-4eba-aa0b-63fca3264d72` |
| host text | Anvil readiness message emitted |
| elapsed heartbeat | `info` records emitted at 15s and 30s |
| bounded failure | structured `error` and final `result` emitted: `headless prompt timed out after 45s` |
| final recap | not reached |
| worker progress | not observed before timeout |
| usage reporting | not observed before timeout |

Diagnostic rerun:

```text
mj --agent-stderr /tmp/mj-thor-headless-smoke.err --output-format stream-json --permission-mode acceptEdits --print-timeout-seconds 20 --print "Thor runtime smoke. Do not modify files. List available Thor agents, submit a concise plan, and answer with a short recap."
```

The stderr log showed Anvil initialized, discovered ChatGPT subscription models
(`gpt-5.3-codex-spark`, `gpt-5.4-mini`, `gpt-5.4`, `gpt-5.5`), received
`session/prompt`, and started its MCP server. No plan, worker event, or model
response was emitted before the 20s timeout.

Known gap: this proves the progress heartbeat and timeout surface for a real
provider run, but it does not close the production-grade Thor runtime smoke
requirement because Thor did not produce a plan, delegate work, mirror worker
progress, or recap before the 45s timeout.

### `@agentclientprotocol/claude-agent-acp` 0.36.1 — 2026-05-20

Source: npm package in the official `@agentclientprotocol` scope, OIDC-
published from GitHub Actions by Conrad Irwin et al. (Apache-2.0).
Wraps the Claude Agent SDK; uses Claude Code's local credentials, so no
`ANTHROPIC_API_KEY` in the env is needed if Claude Code is already
authenticated on the machine.

Launch:

```text
mj --command "npx -y -p @agentclientprotocol/claude-agent-acp@0.36.1 claude-agent-acp"
```

Verified at the protocol layer (driven by a hand-rolled JSON-RPC probe,
not a full interactive prompt round-trip, to avoid burning model tokens
in a smoke test):

| Feature | Result |
| --- | --- |
| `initialize` handshake (ACP v1) | works; `protocolVersion: 1` returned, matches our advertised version |
| `agentInfo` (name + version) | populated; our `Connected` event renders `Claude Agent 0.36.1` |
| `authMethods` | `[]`; no auth-required path triggered for this configuration |
| `session/new` with `cwd` | works; returns `sessionId`, `models`, `modes`, `configOptions` |
| `configOptions` categories | advertised by some host agents, but hidden from the Thor UX because model, mode, and reasoning are Thor routing policy |
| `available_commands_update` notification | streams immediately after `session/new`; populates the slash autocomplete |
| `loadSession`, `sessionCapabilities` (resume/fork/list/close/delete) | advertised by the agent; mjolnir now drives load/fork where implemented, with broader list/resume/delete UX still M5 territory |
| `promptCapabilities.image`, `embeddedContext` | accepted by the agent; mjolnir still renders these `ContentBlock` variants as `[image]` / `[resource]` placeholders pending M2 |
| `mcpCapabilities.http`, `sse` | advertised; mjolnir now sends the Thor stdio MCP bridge at `session/new` for Thor host sessions |

Known gaps to file as follow-ups when the matrix expands:

- Session capability surfacing is partial: fork gates `/fork`, load powers the
  session picker, while resume/list/delete still need broader UX.
- User-configured arbitrary MCP servers still need a CLI flag or config-file
  entry; the current MCP injection is the built-in Thor ACP bridge.
- Effort levels (`low/medium/high/xhigh/max`) come through the
  `thought_level` config category and render with the auto-titlecased
  name (`Xhigh`). Cosmetic and agent-side, not blocking.

Not yet exercised (would consume model tokens or require interactive
testing): `session/prompt` round-trip, tool-call permission flow,
prompt cancellation against a live agent, agent-initiated errors mid-
turn.

### Anvil dev ACP — 2026-06-28

Source: configured Thor ACP server `custom:anvil dev`, which resolves to the
installed local command `/Users/ryansvihla/.cargo/bin/anvil`. The smoke used the
persisted Thor configured server, so it validated the exact command/env Thor can
delegate to without fetching code or sending a model prompt.

Launch:

```text
mj acp-smoke --configured-source-id "custom:anvil dev" --format json
```

Verified at the protocol layer through `mj acp-smoke`, not a full interactive
prompt round-trip:

| Feature | Result |
| --- | --- |
| `initialize` handshake (ACP v1) | works; `Connected` event received |
| `session/new` with repo cwd | works; validation reached `SessionStarted` |
| `promptCapabilities.image` | advertised as supported |
| session fork capability | advertised as supported |
| config options | none observed during this smoke |
| validation runtime | completed in about 1.5s on this machine |

Known gaps:

- This was a configured local binary smoke, not registry install/setup
  validation.
- The smoke did not send a model prompt or exercise auth/rate-limit failure
  recovery.

Not yet exercised: `session/prompt`, tool-call permission flow, cancellation,
live model/auth failures, and transcript rendering from a real Anvil turn.

### OpenCode 1.17.11 — 2026-06-27

Source: ACP registry entry `opencode`, version `1.17.11`, from
`https://github.com/anomalyco/opencode` / `https://opencode.ai`. The local
machine already had `/Users/ryansvihla/.opencode/bin/opencode` installed, so
the smoke used the installed binary instead of fetching third-party code.

Launch:

```text
/Users/ryansvihla/.opencode/bin/opencode acp
```

Verified at the protocol layer through `thor_probe::validate_agent`, not a full
interactive prompt round-trip, to avoid burning model tokens:

Re-verified through the public smoke command:

```text
mj acp-smoke --command "/Users/ryansvihla/.opencode/bin/opencode acp" --source-id opencode --format json
```

| Feature | Result |
| --- | --- |
| `initialize` handshake (ACP v1) | works; `Connected` event received |
| `agentInfo` (name + version) | populated as `OpenCode 1.17.11` |
| `session/new` with repo cwd | works; validation reached `SessionStarted` |
| `promptCapabilities.image` | advertised as supported |
| session fork capability | advertised as supported |
| config options | none observed during this smoke |
| validation runtime | completed in under 1s on this machine |

Known gaps:

- The smoke did not inspect OpenCode-specific config options because none were
  advertised before validation completed.
- The smoke used an already-installed local binary. Registry installation and
  first-run auth/setup behavior still need separate UX validation.

Not yet exercised: `session/prompt`, tool-call permission flow, cancellation,
live model/auth failures, and transcript rendering from a real OpenCode turn.

### `@agentclientprotocol/codex-acp` 0.0.46 — historical, re-check failed 2026-06-28

Source: configured Thor ACP server `custom:codex alt`, which resolves to the
installed `@agentclientprotocol/codex-acp` package in this local environment.
Earlier on 2026-06-28 this configured-server smoke completed initialize plus
`session/new`. A later re-check on the same date failed during `initialize`, so
this local configured Codex server should not be treated as currently validated
until its underlying Codex process starts cleanly again.

Launch:

```text
mj acp-smoke --configured-source-id "custom:codex alt" --format json
```

Historical successful result, kept for comparison:

| Feature | Result |
| --- | --- |
| `initialize` handshake (ACP v1) | works; `Connected` event received |
| `agentInfo` (name + version) | populated as `@agentclientprotocol/codex-acp 0.0.46` |
| `session/new` with repo cwd | works; validation reached `SessionStarted` |
| `promptCapabilities.image` | advertised as supported |
| session fork capability | not advertised in this smoke |
| config options | none observed during this smoke |
| validation runtime | completed in about 1.1s on this machine |

Current re-check:

```text
mj --agent-stderr /tmp/mj-codex-acp-smoke.err acp-smoke --configured-source-id "custom:codex alt" --format json
```

Current result:

| Feature | Result |
| --- | --- |
| `initialize` handshake (ACP v1) | failed |
| stderr detail | wrapper reported `Codex process has exited with code 1` |
| diagnostic path | top-level `--agent-stderr` now captures subprocess stderr for `acp-smoke` |

Known gaps:

- This was a configured-server smoke, not a registry install/setup test.
- The smoke did not send a model prompt or exercise Codex auth/rate-limit
  failure recovery.
- The currently configured local Codex server needs its underlying Codex
  process fixed before it can count as live compatibility evidence again.

Not yet exercised: `session/prompt`, tool-call permission flow, cancellation,
live model/auth failures, and transcript rendering from a real Codex ACP turn.

### Next targets

- Gemini CLI (auth-required path test).
- Goose (self-hosted, no auth dance).
- Gemini CLI registry command is `npx -y @google/gemini-cli@0.49.0 --acp`,
  but the live smoke was not run because executing freshly fetched `npx` code
  needs explicit user approval. Tracked in
  [#251](https://github.com/BrokkAi/mjolnir/issues/251).

Each future entry should follow the same shape: source / launch
command / verified table / known gaps / not-yet-exercised.

## Discussion checklist

Before turning this into an implementation roadmap, decide:

- Is `mjolnir` primarily a Brokk companion, or a general ACP terminal client?
- Should v1 include named launch presets?
- Should v1 include persisted prompt history?
- How far should v1 go beyond current session load/fork support?
- Which agents must be in the compatibility matrix?
- What install channel should be first: GitHub releases, Homebrew, shell
  installer, or `cargo install` only?
