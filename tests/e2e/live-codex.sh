#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bin=${MJ_E2E_BIN:-"$repo/target/debug/mj"}
node=$(command -v node)
real_home=$HOME
root=$(mktemp -d "${TMPDIR:-/tmp}/mj-code-agent-live.XXXXXX")
cleanup() {
  status=$?
  if [ "$status" -eq 0 ]; then
    rm -rf "$root"
  else
    echo "live code-agent artifacts preserved at $root" >&2
  fi
}
trap cleanup EXIT INT TERM
workspace="$root/workspace"
mkdir -p "$workspace" "$root/home/.config/mj" "$root/home/Library/Application Support/mj"
git -C "$workspace" init -q
nonce=$(date +%s)-$$
target="$workspace/codeagent-live-$nonce.txt"
token="CODEAGENT_LIVE_OK_$nonce"

config="[agent]\nsource_id = \"custom:e2e-primary\"\nprogram = \"$node\"\nargs = [\"$repo/tests/e2e/primary-agent.mjs\"]\n"
printf '%b' "$config" >"$root/home/.config/mj/config.toml"
printf '%b' "$config" >"$root/home/Library/Application Support/mj/config.toml"

before="$root/processes-before"
after="$root/processes-after"
pgrep -f '@agentclientprotocol/codex-acp' >"$before" 2>/dev/null || true

HOME="$root/home" \
XDG_CONFIG_HOME="$root/home/.config" \
CODEX_HOME="${CODEX_HOME:-$real_home/.codex}" \
MJ_E2E_BIN="$bin" \
MJ_E2E_MODE=live \
MJ_E2E_WORKSPACE="$workspace" \
MJ_E2E_PRIMARY_RESULT="$root/primary-result.json" \
MJ_E2E_PRIMARY_LOG="$root/primary.log" \
MJ_E2E_TRANSCRIPT="$root/transcript.log" \
MJ_E2E_DEBUG_LOG="$root/mj.log" \
MJ_E2E_AGENT_STDERR="$root/agent.stderr" \
MJ_E2E_LIVE_TOKEN="$token" \
MJ_E2E_CODE_AGENT_INSTRUCTIONS="Create the file $target with exactly this text and no trailing newline: live-code-agent-ok. Then finish your response with exactly $token." \
MJ_E2E_EXIT_ON_RUNTIME_CLOSE=1 \
  expect "$repo/tests/e2e/drive-live.exp"

node -e 'const fs=require("fs"); if(!fs.readFileSync(process.argv[1]).equals(Buffer.from("live-code-agent-ok"))) process.exit(1)' "$target"
node -e 'const r=JSON.parse(require("fs").readFileSync(process.argv[1])); if(!r.response.result.message.includes(process.argv[2])) process.exit(1)' "$root/primary-result.json" "$token"
grep -a "code agent" "$root/transcript.log" >/dev/null
grep -a "codex tool" "$root/transcript.log" >/dev/null
grep -a "$token" "$root/transcript.log" >/dev/null

sleep 1
pgrep -f '@agentclientprotocol/codex-acp' >"$after" 2>/dev/null || true
if comm -13 "$before" "$after" | grep . >/dev/null; then
  echo "live smoke left a codex-acp process behind" >&2
  exit 1
fi

echo "live Codex code-agent smoke passed: $token"
