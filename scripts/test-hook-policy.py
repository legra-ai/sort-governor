"""Hooks must refuse agents on the default branch and mark every commit they
check; CI must verify that mark on every pull-request commit."""

from pathlib import Path
import importlib.util
import os
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hook_trailer", ROOT / "scripts/hook-trailer.py")
hook_trailer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook_trailer)


class TrailerTests(unittest.TestCase):
    def test_stamped_message_verifies_and_is_stable(self):
        message = hook_trailer.stamped("fix(sort): repair\n\nbody  \n\n")
        self.assertIsNone(hook_trailer.check(message))
        self.assertEqual(hook_trailer.stamped(message), message)

    def test_trailer_joins_an_existing_trailer_block(self):
        message = hook_trailer.stamped("fix(sort): repair\n\nbody\n\nCo-Authored-By: A <a@b.c>\n")
        self.assertIn("Co-Authored-By: A <a@b.c>\nHooks-Checked: ", message)
        self.assertIsNone(hook_trailer.check(message))

    def test_unstamped_or_edited_message_is_refused(self):
        self.assertIn("no Hooks-Checked", hook_trailer.check("fix(sort): repair\n"))
        edited = hook_trailer.stamped("fix(sort): repair\n").replace("repair", "other")
        self.assertIn("does not match", hook_trailer.check(edited))

    def test_commit_msg_hook_stamps_and_verify_catches_no_verify(self):
        with tempfile.TemporaryDirectory() as tmp:
            def git(*args, env=None):
                return subprocess.run(
                    ["git", "-C", tmp, *args], capture_output=True, text=True,
                    env={**os.environ, **(env or {})},
                )
            git("init", "-q", "-b", "trunk")
            git("config", "user.email", "t@example.invalid")
            git("config", "user.name", "tester")
            git("config", "commit.gpgsign", "false")
            git("commit", "-q", "--allow-empty", "-m", "chore(init): base")
            base = git("rev-parse", "HEAD").stdout.strip()
            hook = Path(tmp, "hook-commit-msg")
            hook.write_text(
                f'#!/bin/sh\npython3 "{ROOT}/scripts/hook-trailer.py" add "$1"\n')
            hook.chmod(0o755)
            hooks = Path(tmp, ".git/hooks")
            (hooks / "commit-msg").write_text(hook.read_text())
            (hooks / "commit-msg").chmod(0o755)
            git("commit", "-q", "--allow-empty", "-m", "fix(sort): hooked")
            hooked = git("rev-parse", "HEAD").stdout.strip()
            ok = subprocess.run(
                ["python3", str(ROOT / "scripts/hook-trailer.py"), "verify", base, hooked],
                cwd=tmp, capture_output=True, text=True)
            self.assertEqual(ok.returncode, 0, ok.stderr)
            git("commit", "-q", "--allow-empty", "--no-verify", "-m", "fix(sort): skipped")
            skipped = git("rev-parse", "HEAD").stdout.strip()
            bad = subprocess.run(
                ["python3", str(ROOT / "scripts/hook-trailer.py"), "verify", base, skipped],
                cwd=tmp, capture_output=True, text=True)
            self.assertEqual(bad.returncode, 1)
            self.assertIn("hooks skipped", bad.stderr)


class WiringTests(unittest.TestCase):
    def test_hooks_call_the_guard_and_the_trailer(self):
        self.assertIn('agent-default-branch-guard.sh" commit',
                      (ROOT / ".githooks/pre-commit").read_text())
        pre_commit = (ROOT / ".githooks/pre-commit").read_text()
        self.assertLess(pre_commit.index("agent-default-branch-guard.sh"),
                        pre_commit.index("RUST_FILES"),
                        "the guard must run before the Rust-only early exit")
        self.assertIn('agent-default-branch-guard.sh" push',
                      (ROOT / ".githooks/pre-push").read_text())
        self.assertIn("hook-trailer.py", (ROOT / ".githooks/commit-msg").read_text())

    def test_ci_runs_the_guard_test_and_verifies_pull_request_commits(self):
        ci = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("bash scripts/agent-default-branch-guard.test.sh", ci)
        self.assertIn("python3 scripts/test-hook-policy.py", ci)
        workflow = (ROOT / ".github/workflows/hook-trailer.yml").read_text()
        self.assertIn("scripts/hook-trailer.py verify", workflow)
        self.assertIn("fetch-depth: 0", workflow)

    def test_build_script_installs_the_hooks_directory_through_rhusky(self):
        build = (ROOT / "build.rs").read_text()
        self.assertIn('hooks_dir(".githooks")', build)
        self.assertIn("install_from_build_script", build)


if __name__ == "__main__":
    unittest.main()
