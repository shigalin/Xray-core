"""Execute the release build step with a child process recording its environment."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


class ReleaseBuildEnvironmentTest(unittest.TestCase):
    def test_cpu_levels_reach_build_process(self):
        workflow = Path(__file__).parents[1] / "workflows" / "release.yml"
        # Only the `build` job sets CPU levels; `build-release-only` has its own Build Xray step.
        job = re.search(r"^  build:\n(.*?)(?=^  \S|\Z)", workflow.read_text(encoding="utf-8"), re.M | re.S)
        self.assertIsNotNone(job, "build job not found")
        step = re.search(r"^      - name: Build Xray\n(.*?)(?=^      - |\Z)", job[1], re.M | re.S)
        self.assertIsNotNone(step, "Build Xray step not found in build job")
        run = re.search(r"^        run: \|\n(.*)", step[1], re.M | re.S)
        self.assertIsNotNone(run, "Build Xray step has no `run: |` block")
        script = textwrap.dedent(run[1])
        cases = [
            ("windows", "amd64", "v2:unset"),
            ("darwin", "amd64", "v2:unset"),
            ("windows", "arm64", "unset:v8.2"),
            ("darwin", "arm64", "unset:v8.4"),
            ("linux", "amd64", "unset:unset"),
            ("linux", "arm64", "unset:unset"),
            ("android", "arm64", "unset:unset"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, body in {
                "git": "echo test-commit",
                "go": 'printf "%s:%s\\n" "${GOAMD64-unset}" "${GOARM64-unset}" > "$BUILD_ENV_RESULT"',
            }.items():
                executable = root / name
                executable.write_text("#!/bin/sh\n" + body + "\n", encoding="utf-8")
                executable.chmod(0o755)
            for goos, goarch, expected in cases:
                with self.subTest(goos=goos, goarch=goarch):
                    env = os.environ.copy()
                    for key in ("GOAMD64", "GOARM64", "BASH_ENV", "SHELLOPTS"):
                        env.pop(key, None)
                    result_file = root / "result"
                    result_file.unlink(missing_ok=True)
                    env.update(GOOS=goos, GOARCH=goarch,
                               PATH=directory + os.pathsep + env["PATH"],
                               BUILD_ENV_RESULT=str(result_file))
                    subprocess.run(["bash", "-e", "-c", script], cwd=root, env=env,
                                   check=True, capture_output=True, text=True)
                    self.assertEqual(result_file.read_text(encoding="utf-8").strip(), expected)


if __name__ == "__main__":
    unittest.main()
