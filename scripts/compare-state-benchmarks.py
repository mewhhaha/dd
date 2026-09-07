#!/usr/bin/env python3
"""Compare state and runtime workloads using five alternating pairs on available 8, 16, and 32 CPU sets."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import time


WORKLOADS = {
    "kv-store-put": {
        "binary": "bench_kv_store", "baseline": "set-utf8", "candidate": "set-utf8",
        "contract": "Native KV put returns after FULL commit on both revisions; one shared key.",
    },
    "memory-store-put": {
        "binary": "bench_memory_storage", "baseline": "storage-write-memory-wide",
        "candidate": "storage-write-memory-wide",
        "contract": "Native apply_batch returns after FULL commit; one assignment per request across 256 entities.",
    },
    "memory-atomic-read": {
        "binary": "bench_memory_storage", "baseline": "atomic-read-memory",
        "candidate": "atomic-read-memory",
        "contract": "An atomic callback reads one seeded entity; no durable mutation in the timed region.",
    },
    "memory-atomic-readwrite": {
        "binary": "bench_memory_storage", "baseline": "atomic-readwrite-memory-wide",
        "candidate": "atomic-readwrite-memory-wide",
        "contract": "Atomic read and assignment across 256 entities; response follows FULL commit on both revisions.",
    },
    "memory-atomic-effect": {
        "binary": "bench_memory_storage", "baseline": "atomic-write-memory-wide",
        "candidate": "atomic-write-effect-memory-wide",
        "contract": "Atomic assignment and persisted outbox effect across 256 entities; response follows FULL commit.",
    },
    "runtime-instant-text": {
        "binary": "bench_fetch_fast", "baseline": "instant-text", "candidate": "instant-text",
        "contract": "The same static worker returns an immediate text response; no bindings or state writes. Measures runtime dispatch and scheduling overhead.",
    },
    "runtime-instant-json": {
        "binary": "bench_fetch_fast", "baseline": "instant-json", "candidate": "instant-json",
        "contract": "The same static worker returns an immediate JSON response; no bindings or state writes. Measures runtime dispatch and scheduling overhead.",
    },
    "memory-write-api-migration": {
        "binary": "bench_memory_storage", "baseline": "direct-write-memory-wide",
        "candidate": "atomic-write-only-memory-wide",
        "contract": "One FULL-committed assignment across 256 entities; candidate adds the required callback and lease. Measures API migration cost, not an identical API.",
    },
}
DEFAULT_WORKLOADS = list(WORKLOADS)[:-1]
DEFAULT_CPU_COUNTS = [8, 16, 32]
SAMPLE = re.compile(
    r"^(\S+)\s+requests=(\d+) concurrency=(\d+) (?:total=([\d.]+)ms )?"
    r"throughput=([\d.]+) req/s mean=([\d.]+)ms p50=([\d.]+)ms "
    r"p95=([\d.]+)ms p99=([\d.]+)ms$", re.MULTILINE
)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_sample(directory, binary, workload, side, cpus, arguments):
    directory.mkdir()
    temporary_root = directory / "temporary-store"
    temporary_root.mkdir()
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("DD_", "TOKIO_", "TURSO_"))
    }
    settings = {
        "TMPDIR": str(temporary_root),
        "DD_BENCH_REQUESTS": str(arguments.requests),
        "DD_BENCH_CONCURRENCY": str(len(cpus)),
        "DD_BENCH_KEY_SPACE": "256", "DD_BENCH_WIDE_KEY_SPACE": "256",
        "DD_BENCH_MEMORY_KEY_MODE": "pool",
        "DD_BENCH_MIN_ISOLATES": str(arguments.isolates or len(cpus)),
        "DD_BENCH_MAX_ISOLATES": str(arguments.isolates or len(cpus)),
        "DD_BENCH_MAX_INFLIGHT": str(arguments.inflight),
        "DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES": "4096",
        "DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_BYTES": str(64 * 1024 * 1024),
        "DD_BENCH_REQUEST_TIMEOUT_MS": "30000",
        "DD_BENCH_VERIFY_TIMEOUT_MS": "30000",
        "DD_OTEL_ENABLED": "false",
    }
    selector = "DD_BENCH_MODE" if workload["binary"] == "bench_memory_storage" else "DD_BENCH_INTERNAL_SCENARIO"
    settings[selector] = workload[side]
    if workload["binary"] == "bench_fetch_fast":
        settings["DD_BENCH_CUSTOM_NAME"] = "fixed-cpu"
        settings["DD_BENCH_INTERNAL_CONFIG"] = "fixed-cpu"
    environment.update(settings)
    command = [shutil.which("taskset"), "--cpu-list", ",".join(map(str, cpus))]
    command.append(str(binary))
    metadata = {"side": side, "mode": workload[side], "contract": workload["contract"],
                "command": command, "cpu_affinity": cpus, "environment": settings,
                "started_at_unix": time.time(),
                "host_start": {"load_average": os.getloadavg(),
                               "proc_stat": Path("/proc/stat").read_text()}}
    write_json(directory / "run.json", metadata)
    started = time.monotonic()
    with (directory / "stdout.log").open("w") as stdout, (directory / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(command, env=environment, stdout=stdout, stderr=stderr)
        try:
            while True:
                pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.monotonic() - started > arguments.timeout:
                    process.kill()
                    _, status, usage = os.wait4(process.pid, 0)
                    process.returncode = os.waitstatus_to_exitcode(status)
                    metadata["timed_out"] = True
                    break
                time.sleep(0.01)
        except BaseException:
            process.kill()
            process.wait()
            raise
    metadata.update(returncode=process.returncode, wall_seconds=time.monotonic() - started)
    metadata["host_end"] = {"load_average": os.getloadavg(),
                            "proc_stat": Path("/proc/stat").read_text()}
    metadata["resources"] = {"user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime,
                             "max_rss_kib": usage.ru_maxrss}
    write_json(directory / "run.json", metadata)
    if process.returncode:
        raise RuntimeError(f"benchmark exited {process.returncode}; inspect {directory}")
    samples = SAMPLE.findall((directory / "stdout.log").read_text())
    if len(samples) != 1:
        raise RuntimeError(f"expected one benchmark result, found {len(samples)} in {directory}")
    label, requests, concurrency, *measurements = samples[0]
    if int(requests) != arguments.requests or int(concurrency) != len(cpus):
        raise RuntimeError(f"benchmark ignored requested workload settings in {directory}")
    metadata["sample"] = {metric: float(value) for metric, value in zip(
        ["total_ms", "throughput_rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms"],
        measurements, strict=True,
    ) if value}
    for metric, value in metadata["sample"].items():
        if not math.isfinite(value) or value <= 0:
            raise RuntimeError(f"nonpositive or nonfinite {metric}={value} in {directory}; increase requests if timing rounded to zero")
    metadata["label"] = label
    write_json(directory / "run.json", metadata)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True, type=Path,
                        help="directory containing 0922cce dist binaries with the reviewed temp-directory patch")
    parser.add_argument("--candidate-dir", required=True, type=Path,
                        help="directory containing the verified candidate dist binaries")
    parser.add_argument("--output", required=True, type=Path, help="new artifact directory")
    parser.add_argument("--baseline-source", required=True, type=Path)
    parser.add_argument("--candidate-source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--baseline-build-record", type=Path)
    parser.add_argument("--candidate-build-record", type=Path)
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--cpu-count", action="append", type=int, choices=DEFAULT_CPU_COUNTS,
                        help="repeat to select CPU counts; defaults to every supported count")
    parser.add_argument("--requests", type=int, default=20000)
    parser.add_argument("--isolates", type=int, help="fixed isolates on both revisions; defaults to CPU count")
    parser.add_argument("--inflight", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=600, help="seconds allowed for each process")
    parser.add_argument("--workload", action="append", choices=WORKLOADS)
    parser.add_argument("--plan-only", action="store_true", help="validate binaries and write the manifest without running benchmarks")
    arguments = parser.parse_args()
    if not arguments.plan_only and (not arguments.baseline_build_record or not arguments.candidate_build_record):
        parser.error("both build records are required for measured comparisons; include exact build command, flags, source patch and toolchain")
    if min(arguments.pairs, arguments.requests, arguments.inflight, arguments.timeout) < 1 or arguments.isolates == 0:
        parser.error("counts and timeouts must be positive")
    if arguments.isolates is not None and arguments.isolates < 1:
        parser.error("isolates must be positive")
    if not shutil.which("taskset") or not hasattr(os, "sched_getaffinity"):
        parser.error("Linux CPU affinity and taskset are required")
    available_cpus = sorted(os.sched_getaffinity(0))
    if len(available_cpus) < 8:
        parser.error(f"at least 8 allowed CPUs required, found {available_cpus}")
    if arguments.cpu_count:
        unavailable = sorted({count for count in arguments.cpu_count if count > len(available_cpus)})
        if unavailable:
            parser.error(f"requested CPU counts {unavailable} exceed the {len(available_cpus)} allowed CPUs: {available_cpus}")
        cpu_counts = sorted(set(arguments.cpu_count))
    else:
        cpu_counts = [count for count in DEFAULT_CPU_COUNTS if count <= len(available_cpus)]
    cpu_sets = {count: available_cpus[:count] for count in cpu_counts}
    skipped_counts = [{"cpu_count": count,
                       "reason": f"host allows {len(available_cpus)} CPUs" if count > len(available_cpus) else "not explicitly selected"}
                      for count in DEFAULT_CPU_COUNTS if count not in cpu_sets]
    workloads = arguments.workload or DEFAULT_WORKLOADS
    binaries = {}
    for side in ["baseline", "candidate"]:
        root = getattr(arguments, f"{side}_dir").resolve(strict=True)
        binaries[side] = {}
        for name in sorted({WORKLOADS[key]["binary"] for key in workloads}):
            binary = (root / name).resolve(strict=True)
            if not os.access(binary, os.X_OK):
                parser.error(f"binary is not executable: {binary}")
            binaries[side][name] = {"path": str(binary), "sha256": sha256(binary)}
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    filesystem = subprocess.check_output(["stat", "-f", "-c", "%T", str(output)], text=True).strip()
    if filesystem in {"tmpfs", "ramfs"}:
        parser.error(f"durable storage comparisons require a disk-backed output directory; {output} uses {filesystem}")
    provenance = {}
    metadata_module = Path(__file__).resolve().parents[1] / "benchmarks/lib/metadata.mjs"
    for side in ["baseline", "candidate"]:
        source = getattr(arguments, f"{side}_source").resolve(strict=True)
        source_patch = output / f"{side}-source.patch"
        source_patch.write_bytes(subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=source))
        command = f"import {{collectMetadata}} from {json.dumps(metadata_module.as_uri())}; console.log(JSON.stringify(collectMetadata(process.argv[1])));"
        machine = json.loads(subprocess.check_output(
            ["node", "--input-type=module", "-e", command, str(source)],
            env={**os.environ, "TMPDIR": str(output)}, text=True,
        ))
        untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=source).decode().split("\0")
        provenance[side] = {"source": str(source), "machine": machine,
                            "source_patch_sha256": sha256(source_patch),
                            "untracked_sha256": {name: sha256(source / name) for name in untracked if name and (source / name).is_file()}}
        build_record = getattr(arguments, f"{side}_build_record")
        if build_record:
            record = json.loads(build_record.read_text())
            for name, binary in binaries[side].items():
                if record.get("binary_sha256", {}).get(name) != binary["sha256"]:
                    parser.error(f"{side} build record does not match binary {name}")
            if record.get("source_patch_sha256") != provenance[side]["source_patch_sha256"]:
                parser.error(f"{side} source patch changed since its build record")
            if record.get("commit") != machine["git_commit"]:
                parser.error(f"{side} source revision changed since its build record")
            if "untracked_sha256" in record and record["untracked_sha256"] != provenance[side]["untracked_sha256"]:
                parser.error(f"{side} untracked source files changed since its build record")
            provenance[side]["build_record"] = record
        if side == "baseline":
            if machine["git_commit"] != "0922cce01e319a587a23eb5cf1dbe2d362caeb66":
                parser.error(f"workload mappings require baseline 0922cce, found {machine['git_commit']}")
            allowed = {"crates/runtime/src/bin/bench_kv_store.rs", "crates/runtime/src/bin/bench_fetch_fast.rs",
                       "crates/runtime/src/bin/bench_memory_storage/support.rs"}
            changed = set(subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=source, text=True).splitlines())
            if changed != allowed or any(untracked):
                parser.error(f"baseline must contain only the three benchmark harness edits; changed={sorted(changed)}, untracked={untracked}")
            for name in allowed:
                original = subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=source, text=True)
                expected = original.replace('PathBuf::from(format!("/tmp/dd-bench-{tag}-{}", Uuid::new_v4()))', 'std::env::temp_dir().join(format!("dd-bench-{tag}-{}", Uuid::new_v4()))')
                expected = expected.replace('PathBuf::from(format!("/tmp/dd-bench-kv-store-{}", Uuid::new_v4()))', 'std::env::temp_dir().join(format!("dd-bench-kv-store-{}", Uuid::new_v4()))')
                expected = expected.replace('PathBuf::from(format!("/tmp/dd-fast-fetch-{tag}-{}", Uuid::new_v4()))', 'std::env::temp_dir().join(format!("dd-fast-fetch-{tag}-{}", Uuid::new_v4()))')
                if name.endswith("bench_fetch_fast.rs"):
                    expected = expected.replace('throughput={:.0} req/s mean={:.2}ms p50={:.2}ms p95={:.2}ms p99={:.2}ms', 'throughput={:.0} req/s mean={:.6}ms p50={:.6}ms p95={:.6}ms p99={:.6}ms')
                if name.endswith("bench_kv_store.rs"):
                    expected = expected.replace("use std::path::PathBuf;\n", "")
                if (source / name).read_text() != expected:
                    parser.error(f"baseline contains edits beyond the reviewed temp-directory and timing-precision patch in {name}")
            provenance[side]["description"] = "0922cce plus benchmark-only temp_dir and timing-precision patch; business code unchanged"
    manifest = {
        "binaries": binaries, "provenance": provenance, "output_filesystem": filesystem,
        "pairs": arguments.pairs, "requests": arguments.requests,
        "cpu_sets": cpu_sets, "skipped_default_cpu_counts": skipped_counts,
        "workloads": {key: WORKLOADS[key] for key in workloads},
        "isolates": arguments.isolates or "CPU count", "max_inflight_per_isolate": arguments.inflight,
        "notes": [
            "Every process starts a fresh disposable store; deployment and seed precede the benchmark's timed region.",
            "Both revisions have the identical benchmark-only temp_dir patch; TMPDIR points inside the disk-backed artifact directory.",
            "Both fast-fetch harnesses print latency to six decimal places; instant text/JSON use only the fixed-cpu configuration.",
            "Process CPU/RSS/wall measurements include setup and verification; latency/throughput measure the timed region only.",
            "This is a shared host. Each run records starting/ending load averages and /proc/stat counters to expose background contention.",
            "Baseline has 16 namespace memory shards; candidate has 32 fixed shared shards. This topology change is part of the comparison.",
            "Both sides use pool keys; same-shard selectors are excluded because routing and ownership changed.",
            "JavaScript KV is excluded: baseline acknowledges admission, candidate acknowledges FULL commit.",
            "Atomic callbacks execute through the old coordinator at baseline and in the caller isolate at candidate.",
            "No build or integration checks run here; compare verified binaries built with the same dist profile and toolchain.",
        ],
    }
    write_json(output / "manifest.json", manifest)
    if arguments.plan_only:
        print(f"Wrote plan: {output / 'manifest.json'}")
        return
    summary = []
    for cpu_count, cpus in cpu_sets.items():
        for key in workloads:
            ratios = []
            for pair in range(arguments.pairs):
                pair_directory = output / f"cpu-{cpu_count}-{key}-pair-{pair + 1}"
                pair_directory.mkdir()
                order = ["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]
                runs = {}
                for side in order:
                    print(f"{cpu_count} CPUs {key} pair {pair + 1}/{arguments.pairs}: {side}", flush=True)
                    binary = Path(binaries[side][WORKLOADS[key]["binary"]]["path"])
                    runs[side] = run_sample(pair_directory / side, binary, WORKLOADS[key], side, cpus, arguments)
                ratio = {
                    "throughput_candidate_over_baseline": runs["candidate"]["sample"]["throughput_rps"] / runs["baseline"]["sample"]["throughput_rps"],
                    "p95_candidate_over_baseline": runs["candidate"]["sample"]["p95_ms"] / runs["baseline"]["sample"]["p95_ms"],
                    "p99_candidate_over_baseline": runs["candidate"]["sample"]["p99_ms"] / runs["baseline"]["sample"]["p99_ms"],
                }
                pair_gates = {"throughput": ratio["throughput_candidate_over_baseline"] >= 0.95,
                              "p99": ratio["p99_candidate_over_baseline"] <= 1.10}
                write_json(pair_directory / "pair.json", {"order": order, "ratios": ratio, "gates": pair_gates})
                ratios.append(ratio)
            median_throughput = statistics.median(row["throughput_candidate_over_baseline"] for row in ratios)
            median_p99 = statistics.median(row["p99_candidate_over_baseline"] for row in ratios)
            summary.append({"cpus": cpu_count, "workload": key, "pairs": ratios,
                            "median_throughput_ratio": median_throughput, "median_p99_ratio": median_p99,
                            "gates": {"throughput_at_least_0_95": median_throughput >= 0.95,
                                      "p99_at_most_1_10": median_p99 <= 1.10}})
            write_json(output / "summary.json", summary)
    print(f"Completed {len(summary) * arguments.pairs} pairs: {output / 'summary.json'}")
    if any(not all(result["gates"].values()) for result in summary):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
