"""Local hooks must reject the same invalid headers as release automation, and
the toolchains pinned for hooks and CI must be the ones the manifest declares."""

from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest


class CommitPolicyTests(unittest.TestCase):
    def test_ci_and_hooks_share_pinned_toolchains(self):
        root = Path(__file__).resolve().parents[1]
        self.assertRegex((root / "rustfmt-toolchain").read_text().strip(), r"^nightly-\d{4}-\d{2}-\d{2}$")
        manifest = tomllib.loads((root / "Cargo.toml").read_text())
        msrv = manifest["package"]["rust-version"].split(".")
        while len(msrv) < 3:
            msrv.append("0")
        toolchain = tomllib.loads((root / "rust-toolchain.toml").read_text())
        self.assertEqual(toolchain["toolchain"]["channel"], ".".join(msrv),
                         "rust-toolchain.toml must pin the manifest rust-version")
        workflow = (root / ".github/workflows/ci.yml").read_text()
        self.assertIn("./scripts/fmt.sh --check", workflow)
        self.assertNotIn("cargo +nightly", workflow)
        self.assertIn("./scripts/fmt.sh --check", (root / ".githooks/pre-commit").read_text())

    def test_commit_header_contract(self):
        hook = Path(__file__).resolve().parents[1] / ".githooks/commit-msg"
        cases = [
            ("build(deps): update serde", True),
            ("ci(deps-dev): update action", True),
            ("feat(api)!: change contract", True),
            ("fix(api): repair\n\nBREAKING CHANGE: new wire shape", True),
            ("fix: missing scope", False),
            ("oops(api): invalid type", False),
            ("bad headline\n\nfix(api): not the headline", False),
            ("fix(api): ", False),
        ]
        for message, valid in cases:
            with self.subTest(message=message), tempfile.NamedTemporaryFile(mode="w") as source:
                source.write(message + "\n")
                source.flush()
                result = subprocess.run([str(hook), source.name], capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, valid, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
