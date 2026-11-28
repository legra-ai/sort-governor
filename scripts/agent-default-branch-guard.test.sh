#!/usr/bin/env bash
# Exercises agent-default-branch-guard.sh: agent vs owner environment, default
# branch vs feature branch, commit and push modes. No `set -e`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
GUARD="${REPO_ROOT}/scripts/agent-default-branch-guard.sh"
TEST_TMP="$(mktemp -d)"
trap 'rm -rf "${TEST_TMP}"' EXIT

function fail() {
  echo "FAIL: $*" >&2
  exit 1
}

function git_quiet() {
  git -C "${TEST_TMP}/work" "$@" >/dev/null 2>&1 || fail "git $* failed"
}

# Build an origin plus a clone whose origin/HEAD names the default branch.
git init -q --bare -b trunk "${TEST_TMP}/origin.git" || fail "init bare"
git clone -q "${TEST_TMP}/origin.git" "${TEST_TMP}/work" 2>/dev/null || fail "clone"
git_quiet config user.email t@example.invalid
git_quiet config user.name tester
git_quiet checkout -q -b trunk
git_quiet commit -q --allow-empty -m init
git_quiet push -q origin trunk
git_quiet remote set-head origin trunk

# run_guard <expected-rc> <label> <mode> [VAR=value ...]  (stdin forwarded)
function run_guard() {
  local expected=$1 label=$2 mode=$3
  shift 3
  local out rc
  out=$(cd "${TEST_TMP}/work" && env -i PATH="${PATH}" HOME="${TEST_TMP}" "$@" \
    bash "${GUARD}" "${mode}" 2>&1)
  rc=$?
  if (( rc != expected )); then
    fail "${label}: expected rc ${expected}, got ${rc}: ${out}"
  fi
  if (( expected != 0 )) && [[ "${out}" != *"commit on a branch in a worktree and open a PR"* ]]; then
    fail "${label}: refusal lacks the remedy: ${out}"
  fi
}

AGENT_VARS=(CLAUDECODE=1 CLAUDE_CODE_ENTRYPOINT=cli CLAUDE_CODE_SESSION_ID=abc \
  CODEX_THREAD_ID=abc CODEX_SANDBOX=seatbelt CODEX_SANDBOX_NETWORK_DISABLED=1)

# On the default branch.
run_guard 0 "owner commit on default" commit
for var in "${AGENT_VARS[@]}"; do
  run_guard 1 "agent ${var%%=*} commit on default" commit "${var}"
done
run_guard 0 "empty marker is not an agent" commit CLAUDECODE=

# On a feature branch.
git_quiet checkout -q -b feature/x
run_guard 0 "agent commit on feature branch" commit CLAUDECODE=1
run_guard 0 "owner commit on feature branch" commit
git_quiet checkout -q trunk

# Pre-push.
push_default='refs/heads/feature/x aaa refs/heads/trunk bbb'
push_feature='refs/heads/feature/x aaa refs/heads/feature/x bbb'
function run_push() {
  local expected=$1 label=$2 line=$3
  shift 3
  local out rc
  out=$(cd "${TEST_TMP}/work" && printf '%s\n' "${line}" | env -i PATH="${PATH}" \
    HOME="${TEST_TMP}" "$@" bash "${GUARD}" push 2>&1)
  rc=$?
  (( rc == expected )) || fail "${label}: expected rc ${expected}, got ${rc}: ${out}"
}
run_push 1 "agent push to default" "${push_default}" CODEX_THREAD_ID=abc
run_push 1 "claude push to default" "${push_default}" CLAUDECODE=1
run_push 0 "agent push to feature" "${push_feature}" CLAUDECODE=1
run_push 0 "owner push to default" "${push_default}" FOO=bar

# The hooks must actually call the guard.
grep -q 'agent-default-branch-guard.sh" commit' "${REPO_ROOT}/.githooks/pre-commit" \
  || fail "pre-commit does not call the guard"
grep -q 'agent-default-branch-guard.sh" push' "${REPO_ROOT}/.githooks/pre-push" \
  || fail "pre-push does not call the guard"

echo "ok: agent-default-branch-guard"
