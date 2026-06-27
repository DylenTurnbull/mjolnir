# ACP Registry and Presets

This note scopes registry lookup and named agent presets for `mjolnir`.

## Goal

Make Thor setup easier without turning first run into an agent marketplace or a
raw command editor. The registry should help users add known ACP servers, while
presets should make already-configured servers reusable and readable.

## Current State

`mj` can load the ACP registry, show entries during Thor onboarding, persist a
selected entry into `[thor.configured_acp_servers]`, and validate it before Thor
can use it. `npx`, `uvx`, and current-platform binary distributions are shown;
binary distributions resolve to the expected installed command name and are not
downloaded or executed automatically during setup. Custom commands can also be
saved as named local agents.

The runtime worker inventory is intentionally the configured server set, not the
full registry. Thor must never assume a registry entry is usable until `mj`
validates the configured command.

## User Contract

- First run shows configured and default candidates first.
- Registry entries are an add path, not the default worker inventory.
- A registry row should explain the command that will be run and the likely
  install/auth requirements before it is added.
- A selected registry entry is persisted, then validation is rerun through the
  normal configured-server path.
- Failed validation must leave the entry visible with a concrete next action
  and a retry path.
- Custom command entry is the escape hatch for agents not represented well by
  the registry.

## Preset Model

Presets should be named configured ACP servers:

- `source_id`: stable registry id or `custom:<name>`.
- `name`: user-facing label.
- `program`, `args`, `env`: exact launch command.
- `description`: short explanation.
- `setup_url`: provider docs, website, or repository.
- `quota_backend`: direct quota probe backend, if known.

The preset is usable only after validation reaches `session/new`.

## Registry Metadata Gaps

The current registry is enough to derive package manager requirements and launch
commands for common distributions, but not enough for exact auth flows. `mj`
therefore uses conservative hints for known providers and keeps those hints
visibly inferred.

Useful future registry metadata:

- Install prerequisite label, such as Node.js/npm, uv, or standalone binary.
- Auth setup label and docs URL.
- Whether auth can be checked without starting a model session.
- Whether the server requires an already-installed provider CLI.
- Supported quota/cost introspection method, if any.

## Non-Goals

- Installing arbitrary registry packages automatically on first run.
- Probing every registry entry.
- Exposing the full registry to Thor as available workers.
- Building uninstall/update management before the basic setup path is solid.

## Implementation Order

1. Keep registry entries out of worker inventory until selected and persisted.
2. Show launch command plus inferred setup hints in onboarding.
3. Use local provider setup profiles for known agents until the registry
   exposes exact install/auth metadata.
4. Add exact metadata when the registry provides it.
5. Add named preset editing only after first-run setup is production-grade.
6. Add update/uninstall flows only if users actually need them.
