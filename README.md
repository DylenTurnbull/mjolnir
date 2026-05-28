# mjolnir

`mjolnir` is a Rust terminal UI for Agent Client Protocol (ACP) agents. The
binary is named `mj`.

One fast TUI. Every ACP harness. No UI context switching.

Every serious coding agent comes with a harness. That harness is often the part
you actually need: the auth flow your company approved, the model picker your
team standardized on, the local runtime you trust, the tool policy that keeps a
repo safe, or the wrapper that makes a model useful for code. The problem is
that every harness also brings its own UI, keyboard habits, permission prompts,
session history, and rough edges.

`mj` keeps the harness and replaces the UI.

It is a native, small-footprint Rust terminal app: no Electron shell, no browser
runtime, and no agent-specific frontend. Claude for the corporate repo, Codex
for the OpenAI workflow, Qwen, Kimi, or Pi when you want a different model
family, OpenCode with an Ollama-backed setup for local work, Amp or Cursor when
that is what a project already uses: same transcript, same tool cards, same
permissions, same session resume.

Each session chooses its own harness from the official ACP registry, the bundled
`anvil` default, or a custom command. One terminal can run the agent your
workplace requires, another can run the local or open-source harness you prefer,
and a third can review the same change from a separate Git worktree. The agent
changes. The working rhythm does not.

The default inline chat view keeps the transcript, tool activity, session
metadata, and prompt visible in one terminal window.

![Mjolnir inline chat showing streaming agent output and tool activity](docs/readme-images/default-ui.png)

## Pick The Right Harness

`mj` is not trying to flatten every agent into one generic model picker. The
point is the opposite: keep each agent in the harness where it is strongest, but
stop paying the UI switching cost.

That is the normal shape of modern AI coding work:

- The company repo expects Claude Code because enterprise auth and team policy
  are already wired there.
- The library migration feels better in Codex because you want the OpenAI coding
  workflow for edit/test/fix loops.
- A gnarly design question gets a second read from Qwen, Kimi, Pi, or Copilot
  because a different model family catches different mistakes.
- Local experimentation runs through OpenCode, Goose, or a custom ACP command,
  possibly backed by Ollama, because you want control over data and runtime.
- A project already lives in Amp, Cursor, Cline, Copilot, Junie, or another
  registry harness, and you do not want to fight the local convention.

Without `mj`, those choices usually mean five different interfaces. With `mj`,
they are just different sessions in the same terminal workflow.

The upstream [ACP registry](https://github.com/agentclientprotocol/registry)
keeps adding agents. `mj` reads that registry instead of hard-coding a small
club of supported tools.

## Features

- Rust-native TUI with fast startup, low overhead, and a single `mj` binary.
- One consistent terminal workflow for any ACP-speaking tool harness: same keys,
  same permission flow, same transcript shape, same resume path.
- Interactive per-session agent picker backed by the
  [agentclientprotocol/registry](https://github.com/agentclientprotocol/registry)
  with 24h on-disk caching.
- Support for all agents published through the upstream ACP registry, using the
  launch distribution each entry advertises for the current platform (`binary`,
  `npx`, or `uvx`).
- Bundled `anvil` default and a custom-command path for local or experimental
  ACP servers.
- Parallel sessions with `--worktree`: create or reuse linked Git worktrees
  under `.mjolnir/worktrees/` so multiple agents can edit the same project at
  the same time without sharing a checkout.
- Independent harness selection per session. Run the company-approved agent in
  one terminal, your preferred local harness in another, and a reviewer agent in
  a third.
- Streaming agent messages, reasoning blocks, plans, and tool-call cards.
- Permission prompts with keyboard selection.
- Session listing and resume with `mj resume`.
- Non-interactive one-shot prompts with `mj --print`.
- Searchable session configuration for model, mode, permission, and other
  options exposed by the active agent.
- Slash-command autocomplete from commands advertised by the agent, plus a
  client-side `/mj:` namespace (`/mj:agents` to re-open the picker).
- Optional file logging for the TUI and separate stderr capture for the agent.

## Requirements

- Rust stable with Cargo.
- Network access on first registry use unless you only intend to use `anvil`, a
  custom command, or an already-cached registry copy.
- For registry agents distributed via `npx` / `uvx`, the corresponding runtime
  (`npx` from Node.js, `uvx` from `uv`) must be on `PATH`.

## Build and Run

Install the latest `mj`, `anvil`, and `bifrost` release binaries:

```bash
curl -fsSL https://raw.githubusercontent.com/BrokkAi/mjolnir/master/install.sh | bash
```

By default, the installer writes to `~/.local/bin`. If that directory is not on
`PATH`, it offers to add it to your shell profile. Set `INSTALL_DIR` or
`MJOLNIR_INSTALL_DIR` to install somewhere else.

```bash
cargo build --release
./target/release/mj
```

Install locally from this checkout:

```bash
cargo install --path .
mj --cwd .
```

Each new interactive session opens the agent picker:

```
 mj | choose an agent
+--- agents -------------------------------------------------+
| > anvil [current]    -- default mj agent                   |
|   Claude              -- npx v0.36.1                       |
|   Codex               -- binary v0.14.0                    |
|   ...                                                      |
|   Custom command...   -- type your own command             |
+------------------------------------------------------------+
```

`Up` / `Down` to navigate, type to filter, `Enter` to confirm, `Esc` to
cancel. Registry binary distributions are downloaded to
`~/.cache/mj/agents/<id>/<version>/` and reused on subsequent launches. Picker
preferences are stored in `~/.config/mj/config.toml`; use `/mj:agents` inside
the TUI to switch the current session to a different harness.

## Session Resume

`mj resume` opens a searchable session picker for the selected agent, so you can
jump back into previous ACP sessions without remembering IDs.

![Mjolnir resume picker listing prior ACP sessions](docs/readme-images/session-picker.png)

## Parallel Worktree Sessions

Use `--worktree` when you want isolated checkouts for concurrent agent runs:

```bash
mj --worktree
```

With no value, `mj` creates a linked Git worktree below
`<project>/.mjolnir/worktrees/` and runs the ACP session from the matching
directory inside it. On exit, `mj` prints the worktree path and the resume
command if the worktree is kept, and asks whether to remove a freshly-created
worktree.

Keep the worktree and start more sessions in parallel:

```bash
mj --worktree swift-dawn
mj --worktree quiet-forge
```

Each terminal can choose a different registry agent or custom command. The TUI
stays the same; the harness behind it can change per session. Use one agent to
implement, another to review, and a local harness to experiment without forcing
them through the same working tree.

Resume a session in the same worktree:

```bash
mj resume <session-id> --worktree swift-dawn
```

## Permissions

Permission prompts stay in the same terminal flow and keep the requested command
visible while you choose whether to allow, always allow, or reject it.

![Mjolnir permission prompt for a shell command](docs/readme-images/permission-request.png)

## Session Configuration

Agents can expose session-specific configuration through ACP. `mj` renders those
options as searchable terminal pickers, so model and mode changes do not require
leaving the chat.

![Mjolnir searchable model configuration picker](docs/readme-images/searchable-config-options.png)

## CLI Options

- `--cwd`: workspace directory used for the ACP session. Defaults to the current
  directory.
- `-p, --print [PROMPT]`: run one prompt non-interactively and print the result.
  Omit the value or pass `-` to read stdin.
- `--output-format`: output format for `--print`. Values: `text`, `json`,
  `stream-json`.
- `--debug-file` (alias: `--log-file`): write TUI logs to a file. Equivalent
  env var:
  `BROKK_TUI_LOG`.
- `-w, --worktree`: create a linked Git worktree under
  `<project>/.mjolnir/worktrees/`, or reuse an existing worktree by short name
  or path when a value is provided. When the path is not already ignored, `mj`
  prompts before startup and adds `.mjolnir/worktrees/` to the project
  `.gitignore` if you answer yes.
- `--agent-stderr`: capture the agent subprocess stderr to a file. Equivalent
  env var: `BROKK_TUI_AGENT_STDERR`.
- `--fullscreen-tui`: use the legacy alternate-screen full-screen chat UI. The
  default is inline chat.
- `--permission-mode`: controls headless `--print` permission handling. Canonical
  values: `default`, `acceptEdits`, `bypassPermissions`.

There is no `--command` / `--agent` flag: pick the agent interactively the
first time, re-pick per new session, or switch later with `/mj:agents`. To reset
picker preferences, delete `~/.config/mj/config.toml`.

### Resume commands

- `mj resume`: choose an agent, list its sessions, and resume one
  interactively.
- `mj resume <session-id>`: choose an agent, then resume that session ID.
- `mj resume --list`: list sessions from the configured default agent.
- `mj resume --list --format json`: print the session list as JSON.

Logging is disabled by default because the TUI owns the terminal. Set
`BROKK_TUI_LOG_LEVEL` to override the default `info` filter when `--debug-file`
is enabled.

## Keyboard Controls

- `Enter`: send the current prompt, or accept the selected slash command.
- `Tab`: accept the selected slash command.
- `Up` / `Down`: move within slash-command autocomplete or permission prompts.
- `PageUp` / `PageDown`: scroll the transcript.
- `?` / `F10`: show or hide the help overlay.
- `F1`..`F9`: edit visible session config options.
- `Esc`: dismiss autocomplete, clear input, or cancel a permission prompt.
- `Ctrl-C`: cancel an in-flight prompt; when idle with an empty input, quit.
- `Ctrl-D`: quit when the input is empty.

## Multiline Input and Paste Chips

Paste text with more than 3 lines into the prompt and it appears as a
compact chip (e.g. "📎 45 lines · 1,234 chars") instead of raw text.
Chips keep the input box small and the transcript readable. When you
press `Enter`, chip contents are concatenated with your typed text before
sending to the agent. Use `Backspace` on an empty input to remove the
last chip, or `Esc` to clear everything.

### Built-in `/mj:` commands

- `/mj:agents`: re-open the agent picker so you can switch the current session
  to a different ACP server.

## On-disk Files

- `~/.config/mj/config.toml` — picker preferences and the default selected
  agent (program + args + env).
- `~/.cache/mj/registry.json` — cached registry index, refreshed every 24h.
- `~/.cache/mj/agents/<id>/<version>/` — extracted binary distributions.
- `<project>/.mjolnir/worktrees/` — linked Git worktrees created by
  `mj --worktree`.

## Development

Use the same checks as CI before submitting changes:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

The crate uses inline unit tests under `src/`. Keep runtime, UI state, event,
rendering, registry, install, and picker concerns separated across the existing
modules.

## License

`mjolnir` is licensed under GPL-3.0. See [LICENSE](LICENSE).
