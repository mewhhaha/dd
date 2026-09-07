#!/usr/bin/env python3
"""Build the three dist benchmark binaries and record reproducible source/build provenance."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib


BINARIES = ["bench_fetch_fast", "bench_memory_storage", "bench_kv_store"]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_snapshot(source):
    patch = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=source)
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=source,
    ).decode().split("\0")
    return {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip(),
        "source_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "untracked_sha256": {name: sha256(source / name) for name in untracked if name and (source / name).is_file()},
    }, patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--target-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="new artifact directory outside the source tree")
    parser.add_argument("--jobs", type=int, default=4)
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("jobs must be positive")
    source = arguments.source.resolve(strict=True)
    target = arguments.target_dir.resolve()
    output = arguments.output.resolve()
    if output.is_relative_to(source):
        parser.error("build provenance must be written outside the source tree")
    output.mkdir(parents=True, exist_ok=False)
    before, patch = source_snapshot(source)
    (output / "source.patch").write_bytes(patch)
    command = ["cargo", "build", "--locked", "--profile", "dist", "-p", "runtime"]
    for binary in BINARIES:
        command += ["--bin", binary]
    command += ["--target-dir", str(target), "--jobs", str(arguments.jobs)]
    configuration = tomllib.loads((source / "Cargo.toml").read_text())
    configuration_paths = [directory / ".cargo" / name for directory in [source, *source.parents]
                           for name in ["config", "config.toml"]]
    cargo_config_root = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    configuration_paths += [cargo_config_root / "config", cargo_config_root / "config.toml"]
    record = {
        **before, "source": str(source), "build_cwd": str(source), "build_command": command,
        "build_environment": {key: value for key, value in os.environ.items()
                              if re.match(r"^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTC_WRAPPER|CARGO_BUILD_TARGET|CARGO_PROFILE_.*)$", key)},
        "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=source, text=True),
        "cargo": subprocess.check_output(["cargo", "-V"], cwd=source, text=True),
        "profile": "dist", "profile_definitions": configuration.get("profile", {}),
        "cargo_lock_sha256": sha256(source / "Cargo.lock"),
        "cargo_configuration_sha256": {str(path): sha256(path) for path in configuration_paths if path.is_file()},
        "build_log": str(output / "build.log"), "started_at_unix": time.time(),
    }
    record_path = output / "build-record.json"
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    print(f"Building from {source}; log: {record['build_log']}", flush=True)
    started = time.monotonic()
    with (output / "build.log").open("w") as log:
        completed = subprocess.run(command, cwd=source, stdout=log, stderr=log)
    after, _ = source_snapshot(source)
    record.update(returncode=completed.returncode, wall_seconds=time.monotonic() - started,
                  source_unchanged=before == after, source_after=after)
    if completed.returncode == 0:
        record["binary_sha256"] = {binary: sha256(target / "dist" / binary) for binary in BINARIES}
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    if completed.returncode:
        raise SystemExit(completed.returncode)
    if before != after:
        raise RuntimeError(f"source changed during build; rebuild a stable tree; inspect {record_path}")
    print(f"Built verified-source binaries; provenance: {record_path}")


if __name__ == "__main__":
    main()
