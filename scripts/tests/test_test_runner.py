#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise the shell entrypoint without compiling Rust or starting services."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class TestRunner(unittest.TestCase):
    def run_tier(self, *args, fail_package=""):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cargo = root / "cargo"
            cargo.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "with open(os.environ['TEST_COMMAND_LOG'], 'a') as log:\n"
                "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "sys.exit(19 if os.environ['TEST_FAIL_PACKAGE'] in sys.argv[1:] else 0)\n"
            )
            cargo.chmod(0o755)
            log = root / "commands"
            env = dict(os.environ, PATH=tmp + os.pathsep + os.environ["PATH"],
                       TEST_COMMAND_LOG=str(log), TEST_FAIL_PACKAGE=fail_package)
            result = subprocess.run([os.path.relpath(ROOT / "scripts/test.sh", ROOT.parent), *args],
                                    cwd=ROOT.parent, env=env, capture_output=True, text=True)
            commands = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result.returncode, commands

    def test_fast_scope(self):
        self.assertEqual(self.run_tier("fast", "analysis"),
                         (0, [["test", "-p", "analyzer-analysis", "--locked"]]))

    def test_focused_integration(self):
        self.assertEqual(self.run_tier("engine", "pipeline", "keyed_equivalence"),
                         (0, [["test", "-p", "analyzer-pipeline", "--locked",
                               "--test", "keyed_equivalence", "--", "--ignored"]]))

    def test_full_preserves_all_pr_suites(self):
        status, commands = self.run_tier("full")
        self.assertEqual(status, 0)
        self.assertEqual(commands, [
            ["test", "--workspace", "--locked"],
            ["test", "-p", "analyzer-pipeline", "--locked", "--", "--ignored"],
            ["test", "-p", "analyzer-module-example", "--locked", "--", "--ignored", "--test-threads=1"],
            ["test", "-p", "analyzer-api", "--locked", "--", "--ignored", "--test-threads=1"],
            ["test", "-p", "analyzer-daemon", "--locked", "--test", "loop",
             "--", "--ignored", "--test-threads=1"],
            ["test", "-p", "analyzer-serving-clickhouse", "--locked", "--", "--ignored",
             "--test-threads=1"],
            ["test", "-p", "infertap-analyzer", "--locked", "--test", "reader_corpus",
             "--", "--ignored"],
        ])

    def test_release_retains_workspace_transport_tests(self):
        self.assertEqual(self.run_tier("release"), (0, [["test", "--workspace", "--locked",
                                                       "--no-fail-fast", "--", "--ignored",
                                                       "--test-threads=1"]]))

    def test_failure_propagates_and_stops_full_run(self):
        status, commands = self.run_tier("full", fail_package="analyzer-pipeline")
        self.assertEqual(status, 19)
        self.assertEqual(len(commands), 2)

    def test_invalid_scope_never_runs_cargo(self):
        for args in [(), ("fast", "typo"), ("engine", "storage"),
                     ("full", "api"), ("fast", "api", "goldens"),
                     ("engine", "api", "--all-targets")]:
            with self.subTest(args=args):
                self.assertEqual(self.run_tier(*args), (2, []))


if __name__ == "__main__":
    unittest.main()
