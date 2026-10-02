#!/usr/bin/env python3
"""Package downloaded GitHub artifacts, restoring executable permissions."""

import argparse
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

RUNTIME_PACKAGES = (
    "dd-runtime-darwin-arm64",
    "dd-runtime-darwin-x64",
    "dd-runtime-linux-arm64",
    "dd-runtime-linux-x64",
    "dd-runtime-win32-x64",
)


def package_artifacts(artifacts: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    for package in RUNTIME_PACKAGES:
        directory = artifacts / package
        binary = "dd_dev_runtime.exe" if "win32" in package else "dd_dev_runtime"
        binary_path = directory / "bin" / binary
        if not binary_path.is_file():
            raise ValueError(f"missing runtime binary: {binary_path}")
        binary_path.chmod(0o755)
        with tarfile.open(output / f"{package}.tar.gz", "w:gz") as archive:
            archive.add(directory, arcname=".")

    platform = artifacts / "dd-linux-platform"
    shutil.copyfile(platform / "dd-linux-x64.spdx.json", output / "dd-linux-x64.spdx.json")
    for source, name in (("dd_server", "dd-server-linux-x64"), ("cli", "dd-cli-linux-x64")):
        destination = output / name
        shutil.copyfile(platform / "target" / "dist" / source, destination)
        destination.chmod(0o755)


def verify_linux_artifacts(output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="dd-release-download-") as temporary:
        with tarfile.open(output / "dd-runtime-linux-x64.tar.gz") as archive:
            archive.extractall(temporary, filter="data")
        subprocess.run([str(Path(temporary) / "bin" / "dd_dev_runtime"), "--help"], check=True)
    for name in ("dd-server-linux-x64", "dd-cli-linux-x64"):
        subprocess.run([str((output / name).resolve()), "--help"], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts"))
    parser.add_argument("--out", type=Path, default=Path("release"))
    parser.add_argument("--verify-linux", action="store_true", help="extract and smoke the Linux x64 download")
    args = parser.parse_args()
    package_artifacts(args.artifacts, args.out)
    if args.verify_linux:
        verify_linux_artifacts(args.out)
