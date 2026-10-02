#!/usr/bin/env python3
"""Check downloadable archives after GitHub artifact permissions are lost."""

from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


class ReleaseArtifactsTest(unittest.TestCase):
    def test_downloaded_binaries_are_executable_after_packaging(self):
        payload = b"#!/bin/sh\nprintf 'dd runtime help\\n'\n"
        with tempfile.TemporaryDirectory(prefix="dd-release-artifacts-") as root_name:
            root = Path(root_name)
            artifacts, output = root / "artifacts", root / "release"
            for suffix in ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64", "win32-x64"):
                directory = artifacts / f"dd-runtime-{suffix}"
                (directory / "bin").mkdir(parents=True)
                name = "dd_dev_runtime.exe" if "win32" in suffix else "dd_dev_runtime"
                binary = directory / "bin" / name
                binary.write_bytes(payload)
                binary.chmod(0o644)
            platform = artifacts / "dd-linux-platform"
            (platform / "target" / "dist").mkdir(parents=True)
            for name in ("dd_server", "cli"):
                binary = platform / "target" / "dist" / name
                binary.write_bytes(payload)
                binary.chmod(0o644)
            (platform / "dd-linux-x64.spdx.json").write_text("{}")
            subprocess.run([
                "python3", str(Path(__file__).with_name("package-release-artifacts.py")),
                "--artifacts", str(artifacts), "--out", str(output), "--verify-linux",
            ], check=True)
            for archive_path in output.glob("*.tar.gz"):
                with tarfile.open(archive_path) as archive:
                    binary = next(member for member in archive if member.name.startswith("./bin/"))
                    self.assertEqual(binary.mode & 0o777, 0o755)
                    self.assertEqual(archive.extractfile(binary).read(), payload)
                    if "linux-x64" in archive_path.name:
                        extracted = root / "extracted"
                        archive.extractall(extracted, filter="data")
                        self.assertEqual(subprocess.check_output([str(extracted / binary.name), "--help"]), b"dd runtime help\n")
            for name in ("dd-server-linux-x64", "dd-cli-linux-x64"):
                self.assertEqual(subprocess.check_output([str(output / name), "--help"]), b"dd runtime help\n")


if __name__ == "__main__":
    unittest.main()
