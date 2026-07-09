# Mjolnir Code-Agent ACP Extension

Interactive Mjolnir sessions advertise a client extension under
`clientCapabilities._meta.mj.codeAgent`:

```json
{
  "version": 1,
  "method": "_mj/codeAgent",
  "agent": "codex-acp"
}
```

An ACP agent invokes the client request with instructions for a temporary coding
agent:

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "_mj/codeAgent",
  "params": { "instructions": "Inspect the failure and implement a fix." }
}
```

Mjolnir starts `npx -y @agentclientprotocol/codex-acp`, opens a fresh ACP
session in the primary session's workspace, streams the nested turn in the TUI,
and keeps the primary request pending. On success the response result is:

```json
{ "message": "The final Codex message" }
```

Only one nested run is allowed. Invalid parameters, concurrent calls, nested
runtime failures, cancellation, and completion without an agent message return
JSON-RPC errors. While the nested turn is active, Ctrl-C cancels it rather than
the primary turn. The nested runtime does not advertise this extension, so it
cannot recursively delegate.

The first version is interactive-only and hard-codes Codex as the nested ACP
agent. Headless, MCP, remote-server, Ragnarok, and other auxiliary runtimes do
not advertise the capability.

## End-to-end checks

After building `mj`, run the deterministic two-process PTY harness:

```sh
tests/e2e/deterministic.sh
```

The opt-in live smoke uses the installed Codex credentials and makes one real
model request in a temporary repository:

```sh
tests/e2e/live-codex.sh
```
