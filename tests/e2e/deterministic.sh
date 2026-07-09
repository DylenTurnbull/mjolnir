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
  remove_root() {
    attempts=0
    while [ -e "$root" ] && [ "$attempts" -lt 5 ]; do
      rm -rf "$root" 2>/dev/null || true
      attempts=$((attempts + 1))
      [ -e "$root" ] && sleep 0.1
    done
    test ! -e "$root"
  }
  cleanup_case() {
    status=$?
    if [ "$status" -eq 0 ]; then
      remove_root
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

  wait_reaped() {
    pid_file=$1
    label=$2
    test -f "$pid_file" || return 0
    pid=$(cat "$pid_file")
    attempts=0
    while kill -0 "$pid" 2>/dev/null; do
      attempts=$((attempts + 1))
      if [ "$attempts" -ge 30 ]; then
        echo "$label process $pid was not reaped" >&2
        return 1
      fi
      sleep 0.1
    done
  }

  HOME="$root/home" \
  XDG_CONFIG_HOME="$root/home/.config" \
  PATH="$repo/tests/e2e/fake-bin:$PATH" \
  MJ_E2E_BIN="$bin" \
  MJ_E2E_MODE="$mode" \
  MJ_E2E_WORKSPACE="$workspace" \
  MJ_E2E_PRIMARY_RESULT="$root/primary-result.json" \
  MJ_E2E_PRIMARY_LOG="$root/primary.log" \
  MJ_E2E_PRIMARY_PID="$root/primary.pid" \
  MJ_E2E_NESTED_LOG="$root/nested.log" \
  MJ_E2E_NESTED_PID="$root/nested.pid" \
  MJ_E2E_TRANSCRIPT="$root/transcript.log" \
  MJ_E2E_DEBUG_LOG="$root/mj.log" \
  MJ_E2E_AGENT_STDERR="$root/agent.stderr" \
  MJ_E2E_CODE_AGENT_INSTRUCTIONS="Run the deterministic fixture" \
  MJ_E2E_HTTP_UNSUPPORTED="$([ "$mode" = unsupported ] && printf 1 || printf 0)" \
  MJ_E2E_EXIT_ON_RUNTIME_CLOSE=1 \
    expect "$repo/tests/e2e/drive-mj.exp"

  wait_reaped "$root/primary.pid" primary
  wait_reaped "$root/nested.pid" nested
  if grep -a 'MJ_CODE_AGENT_POLICY_READY' "$root/transcript.log" >/dev/null; then
    echo "hidden code-agent session directive leaked into the transcript" >&2
    exit 1
  fi

  if [ "$mode" = unsupported ]; then
    test ! -e "$root/primary-result.json"
    grep -a "does not support HTTP MCP servers required for code-agent delegation" "$root/transcript.log" >/dev/null
    if grep -a '"method":"session/new"' "$root/primary.log" >/dev/null; then
      echo "unsupported primary received session/new" >&2
      exit 1
    fi
  elif [ "$mode" = complete ]; then
    test "$(grep -ac '^session-directive:' "$root/primary.log")" -eq 2
    test -s "$root/primary-result.json"
    grep -a "code agent" "$root/transcript.log" >/dev/null
    grep -a "CODEAGENT_E2E_OK" "$root/transcript.log" >/dev/null
    grep -a "nested-terminal-output" "$root/transcript.log" >/dev/null
    grep -a 'permission:' "$root/nested.log" >/dev/null
    node -e 'const fs=require("fs"); const r=JSON.parse(fs.readFileSync(process.argv[1])); const done=Number(fs.readFileSync(process.argv[2],"utf8").match(/completion:(\d+)/)?.[1]); const text=r.response.content?.map(x=>x.text||"").join(""); if(r.error || r.unauthorizedStatus!==401 || r.response.isError || text!=="CODEAGENT_E2E_OK" || !done || r.toolReceivedAt<done) process.exit(1)' "$root/primary-result.json" "$root/nested.log"
  else
    test "$(grep -ac '^session-directive:' "$root/primary.log")" -eq 2
    test -s "$root/primary-result.json"
    grep -a "code agent" "$root/transcript.log" >/dev/null
    grep -a "cancel-received" "$root/nested.log" >/dev/null
    node -e 'const r=JSON.parse(require("fs").readFileSync(process.argv[1])); if(r.error || r.unauthorizedStatus!==401 || !r.response.isError) process.exit(1)' "$root/primary-result.json"
  fi
  remove_root
  trap - EXIT INT TERM
}

case ${MJ_E2E_CASE:-both} in
  complete) run_case complete ;;
  cancel) run_case cancel ;;
  unsupported) run_case unsupported ;;
  both) run_case complete; run_case cancel; run_case unsupported ;;
  *) echo "MJ_E2E_CASE must be complete, cancel, unsupported, or both" >&2; exit 2 ;;
esac
echo "deterministic code-agent PTY E2E passed"
