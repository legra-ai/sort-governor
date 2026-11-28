#!/usr/bin/env python3
"""Mark commits that went through the local hooks, and verify the mark in CI.

`add <message-file>` (run by the commit-msg hook) appends the trailer
`Hooks-Checked: <sha256>`, where the digest covers the normalized message
without the trailer. `verify <base> <head>` (run by CI) fails when any commit
in `base..head` lacks the trailer or carries a digest that does not match its
message. `git commit --no-verify` skips the commit-msg hook, so the commit has
no trailer. The mark is deterministic, not secret: it proves the hook ran or
that someone deliberately forged it, and CI re-runs formatting, clippy and the
tests on every PR regardless.
"""

import hashlib
import re
import subprocess
import sys

TRAILER = "Hooks-Checked"
TRAILER_BLOCK_LINE = re.compile(r"^[A-Za-z][A-Za-z-]*: \S")
TRAILER_LINE = re.compile(rf"^{TRAILER}: ([0-9a-f]{{64}})$")
# Merge commits and dependabot commits are made by GitHub, not by a local hook.
EXEMPT_AUTHORS = ("dependabot[bot]",)


def split_message(message):
    """Return (body lines without trailer and trailing blanks, recorded digest)."""
    lines = [line.rstrip() for line in message.splitlines()]
    recorded = None
    kept = []
    for line in lines:
        match = TRAILER_LINE.match(line)
        if match:
            recorded = match.group(1)
        else:
            kept.append(line)
    while kept and not kept[-1]:
        kept.pop()
    return kept, recorded


def digest(body):
    return hashlib.sha256(("\n".join(body) + "\n").encode()).hexdigest()


def stamped(message):
    """Append the trailer, joining an existing trailer block when there is one."""
    body, _ = split_message(message)
    in_trailer_block = len(body) > 1 and bool(TRAILER_BLOCK_LINE.match(body[-1]))
    separator = "\n" if in_trailer_block else "\n\n"
    return "\n".join(body) + f"{separator}{TRAILER}: {digest(body)}\n"


def check(message):
    """Return None when the message carries a matching trailer, else the reason."""
    body, recorded = split_message(message)
    if recorded is None:
        return f"no {TRAILER} trailer (hooks skipped?)"
    if recorded != digest(body):
        return f"{TRAILER} trailer does not match the message (edited without hooks?)"
    return None


def git(*args):
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def verify(base, head):
    failures = []
    for sha in git("rev-list", "--no-merges", f"{base}..{head}").split():
        author = git("log", "-1", "--format=%an", sha).strip()
        if author in EXEMPT_AUTHORS:
            continue
        reason = check(git("log", "-1", "--format=%B", sha))
        if reason:
            failures.append(f"{sha[:12]}: {reason}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        print("Every PR commit must be made with the repository hooks installed "
              "(never --no-verify); amend the commits listed above.", file=sys.stderr)
        return 1
    return 0


def main(argv):
    if len(argv) == 3 and argv[1] == "add":
        with open(argv[2], encoding="utf-8") as handle:
            message = handle.read()
        with open(argv[2], "w", encoding="utf-8") as handle:
            handle.write(stamped(message))
        return 0
    if len(argv) == 4 and argv[1] == "verify":
        return verify(argv[2], argv[3])
    print("usage: hook-trailer.py add <message-file> | verify <base> <head>", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
