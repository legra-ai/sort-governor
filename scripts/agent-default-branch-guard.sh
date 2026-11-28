#!/usr/bin/env bash
# agent-default-branch-guard.sh — AI agents never commit or push the default branch.
#
# Usage: agent-default-branch-guard.sh commit
#        agent-default-branch-guard.sh push     (reads pre-push stdin lines)
#
# The owner's own commits and pushes are untouched: the guard only fires when
# the process environment shows an AI agent. Agent markers (set by the agent
# harness for every child process):
#   Claude Code: CLAUDECODE=1, CLAUDE_CODE_ENTRYPOINT, CLAUDE_CODE_SESSION_ID
#   Codex:       CODEX_THREAD_ID, CODEX_SANDBOX, CODEX_SANDBOX_NETWORK_DISABLED
# No `set -e`: every exit status is handled explicitly.

AGENT_MARKERS=(
  CLAUDECODE
  CLAUDE_CODE_ENTRYPOINT
  CLAUDE_CODE_SESSION_ID
  CODEX_THREAD_ID
  CODEX_SANDBOX
  CODEX_SANDBOX_NETWORK_DISABLED
)

# Prints the first agent marker that is set and non-empty; returns 1 if none.
function detect_agent_marker() {
  local marker
  for marker in "${AGENT_MARKERS[@]}"; do
    if [[ -n "${!marker:-}" ]]; then
      printf '%s\n' "${marker}"
      return 0
    fi
  done
  return 1
}

# Prints the default branch name (from origin/HEAD, else init.defaultBranch).
function resolve_default_branch() {
  local ref branch
  if ref=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null) && [[ -n "${ref}" ]]; then
    printf '%s\n' "${ref#origin/}"
    return 0
  fi
  if branch=$(git config --get init.defaultBranch 2>/dev/null) && [[ -n "${branch}" ]]; then
    printf '%s\n' "${branch}"
    return 0
  fi
  return 1
}

function refuse() {
  local marker=$1 branch=$2 action=$3
  printf 'Refused: AI agents never %s the default branch (%s); detected agent marker %s.\n' \
    "${action}" "${branch}" "${marker}" >&2
  printf 'Remedy: commit on a branch in a worktree and open a PR.\n' >&2
}

function run_guard() {
  local mode=${1:-}
  local marker branch
  if ! marker=$(detect_agent_marker); then
    return 0
  fi
  if ! branch=$(resolve_default_branch); then
    printf 'Refused: cannot resolve the default branch (no origin/HEAD, no init.defaultBranch) for agent marker %s.\n' \
      "${marker}" >&2
    return 1
  fi
  case "${mode}" in
    commit)
      local current
      if ! current=$(git symbolic-ref --short -q HEAD); then
        return 0 # detached HEAD is not the default branch
      fi
      if [[ "${current}" == "${branch}" ]]; then
        refuse "${marker}" "${branch}" "commit to"
        return 1
      fi
      ;;
    push)
      local local_ref local_sha remote_ref remote_sha
      while read -r local_ref local_sha remote_ref remote_sha; do
        if [[ "${remote_ref}" == "refs/heads/${branch}" ]]; then
          refuse "${marker}" "${branch}" "push to"
          return 1
        fi
      done
      ;;
    *)
      printf 'usage: %s commit|push\n' "${0##*/}" >&2
      return 2
      ;;
  esac
  return 0
}

run_guard "$@"
exit $?
