#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
bin=${MJ_E2E_BIN:-"$repo/target/debug/mj"}
node=$(command -v node)

if [ ! -x "$bin" ]; then
  echo "build mj first: cargo build" >&2
  exit 2
fi
if ! command -v expect >/dev/null 2>&1; then
  echo "expect is required for the PTY smoke test" >&2
  exit 2
fi

run_case() {
  mode=$1
  root=$(mktemp -d "${TMPDIR:-/tmp}/mj-code-agent-e2e.XXXXXX")
  cleanup_case() {
    status=$?
    if [ "$status" -eq 0 ]; then
      rm -rf "$root"
    else
      echo "code-agent E2E artifacts preserved at $root" >&2
    fi
  }
  trap cleanup_case EXIT INT TERM
  workspace="$root/workspace"
  mkdir -p "$workspace" "$root/home/.config/mj" "$root/home/Library/Application Support/mj"
  config="[agent]\nsource_id = \"custom:e2e-primary\"\nprogram = \"$node\"\nargs = [\"$repo/tests/e2e/primary-agent.mjs\"]\n"
  printf '%b' "$config" >"$root/home/.config/mj/config.toml"
  printf '%b' "$config" >"$root/home/Library/Application Support/mj/config.toml"

  HOME="$root/home" \
  XDG_CONFIG_HOME="$root/home/.config" \
  PATH="$repo/tests/e2e/fake-bin:$PATH" \
  MJ_E2E_BIN="$bin" \
  MJ_E2E_MODE="$mode" \
  MJ_E2E_WORKSPACE="$workspace" \
  MJ_E2E_PRIMARY_RESULT="$root/primary-result.json" \
  MJ_E2E_PRIMARY_LOG="$root/primary.log" \
  MJ_E2E_NESTED_LOG="$root/nested.log" \
  MJ_E2E_TRANSCRIPT="$root/transcript.log" \
  MJ_E2E_DEBUG_LOG="$root/mj.log" \
  MJ_E2E_AGENT_STDERR="$root/agent.stderr" \
  MJ_E2E_CODE_AGENT_INSTRUCTIONS="Run the deterministic fixture" \
  MJ_E2E_EXIT_ON_RUNTIME_CLOSE=1 \
    expect "$repo/tests/e2e/drive-mj.exp"

  test -s "$root/primary-result.json"
  grep -a "code agent" "$root/transcript.log" >/dev/null
  if [ "$mode" = complete ]; then
    grep -a "CODEAGENT_E2E_OK" "$root/transcript.log" >/dev/null
    grep -a "nested-terminal-output" "$root/transcript.log" >/dev/null
    grep -a 'permission:' "$root/nested.log" >/dev/null
    node -e 'const fs=require("fs"); const r=JSON.parse(fs.readFileSync(process.argv[1])); const done=Number(fs.readFileSync(process.argv[2],"utf8").match(/completion:(\d+)/)?.[1]); if(r.response.result.message!=="CODEAGENT_E2E_OK" || !done || r.extensionReceivedAt<done) process.exit(1)' "$root/primary-result.json" "$root/nested.log"
  else
    grep -a "cancel-received" "$root/nested.log" >/dev/null
    node -e 'const r=JSON.parse(require("fs").readFileSync(process.argv[1])); if(!r.response.error) process.exit(1)' "$root/primary-result.json"
  fi
  rm -rf "$root"
  trap - EXIT INT TERM
}

case ${MJ_E2E_CASE:-both} in
  complete) run_case complete ;;
  cancel) run_case cancel ;;
  both) run_case complete; run_case cancel ;;
  *) echo "MJ_E2E_CASE must be complete, cancel, or both" >&2; exit 2 ;;
esac
echo "deterministic code-agent PTY E2E passed"
