#!/usr/bin/env python3
"""Measure concurrent memory transactions with matched workloads and physical CPU affinity."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def parse_case(value):
    parts = value.split(":")
    if len(parts) not in {4, 6} or parts[0] not in {"read", "write", "mixed"}:
        raise argparse.ArgumentTypeError(f"case {value!r} must be MODE:WIDTH:POPULATION:PAYLOAD_BYTES[:KEYS_PER_ENTITY:PAYLOAD_KIND]")
    try:
        width, population, payload = map(int, parts[1:4])
        keys = int(parts[4]) if len(parts) == 6 else 1
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"case {value!r} requires integer dimensions") from error
    if width not in {1, 4, 16} or population < width or payload < 1:
        raise argparse.ArgumentTypeError(f"case {value!r} requires width 1/4/16, population >= width, and positive payload")
    kind = parts[5] if len(parts) == 6 else "repeated"
    if not 1 <= keys <= 256 or kind not in {"repeated", "varied"}:
        raise argparse.ArgumentTypeError("expected 1..=256 keys and repeated/varied payload kind")
    return {"name": value.replace(":", "-"), "mode": parts[0], "width": width,
            "population": population, "payload_bytes": payload,
            "keys_per_entity": keys, "payload_kind": kind}


def run_sample(folder, binary, cpus, case, arguments, read_api):
    folder.mkdir()
    temporary = folder / "store-root"
    temporary.mkdir()
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("DD_", "TOKIO_", "TURSO_"))}
    settings = {
        "TMPDIR": str(temporary), "DD_OTEL_ENABLED": "false",
        "DD_FANOUT_MODE": case["mode"], "DD_FANOUT_WIDTH": str(case["width"]),
        "DD_FANOUT_READ_API": read_api,
        "DD_FANOUT_POPULATION": str(case["population"]),
        "DD_FANOUT_PAYLOAD_BYTES": str(case["payload_bytes"]),
        "DD_FANOUT_KEYS_PER_ENTITY": str(case["keys_per_entity"]),
        "DD_FANOUT_PAYLOAD_KIND": case["payload_kind"],
        "DD_FANOUT_CONCURRENCY": str(arguments.concurrency or len(cpus) * 4),
        "DD_FANOUT_DURATION_MS": str(arguments.duration_ms),
        "DD_FANOUT_WARMUP_MS": str(arguments.warmup_ms),
        "DD_FANOUT_PROFILE": "1" if arguments.profile or arguments.v8_profile else "0",
    }
    if arguments.requests_per_second:
        settings["DD_FANOUT_REQUESTS_PER_SECOND"] = str(arguments.requests_per_second)
    if arguments.v8_profile:
        settings["DD_FANOUT_V8_LOG"] = str(folder / 'v8.log')
    environment.update(settings)
    command = [shutil.which("taskset"), "--cpu-list", ",".join(map(str, cpus)), str(binary)]
    record = {"command": command, "environment": settings, "started_at_unix": time.time(),
              "host_start": {"load_average": os.getloadavg(), "proc_stat": Path('/proc/stat').read_text()}}
    write_json(folder / "run.json", record)
    started = time.monotonic()
    threads = {}
    with (folder / "stdout.log").open("w") as stdout, (folder / "stderr.log").open("w") as stderr:
        child = subprocess.Popen(command, env=environment, stdout=stdout, stderr=stderr)
        try:
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                if pid:
                    child.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.monotonic() - started > arguments.timeout:
                    child.kill()
                    _, status, usage = os.wait4(child.pid, 0)
                    child.returncode = os.waitstatus_to_exitcode(status)
                    record["timed_out"] = True
                    break
                for stat in Path(f"/proc/{child.pid}/task").glob("*/stat"):
                    try:
                        raw = stat.read_text()
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    name = raw[raw.index("(") + 1:raw.rindex(")")]
                    fields = raw[raw.rindex(")") + 2:].split()
                    thread = threads.setdefault(stat.parent.name, {})
                    thread.update(name=name, cpu_ticks=int(fields[11]) + int(fields[12]))
                    if arguments.profile or arguments.v8_profile:
                        try:
                            thread['schedstat'] = list(map(int, (stat.parent / 'schedstat').read_text().split()))
                            wait = (stat.parent / 'wchan').read_text().strip()
                        except (FileNotFoundError, ProcessLookupError):
                            continue
                        waits = thread.setdefault('wait_samples', {})
                        waits[wait] = waits.get(wait, 0) + 1
                time.sleep(0.1)
        except BaseException:
            child.kill()
            child.wait()
            raise
    record.update(returncode=child.returncode, wall_seconds=time.monotonic() - started,
                  host_end={"load_average": os.getloadavg(), "proc_stat": Path('/proc/stat').read_text()},
                  resources={"user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime,
                             "max_rss_kib": usage.ru_maxrss},
                  sampled_threads=threads, clock_ticks_per_second=os.sysconf('SC_CLK_TCK'))
    write_json(folder / "run.json", record)
    if child.returncode:
        raise RuntimeError(f"benchmark exited {child.returncode}; inspect {folder}")
    result = json.loads((folder / "stdout.log").read_text())
    expected_config = {key: case[key] for key in ['mode', 'width', 'population', 'payload_bytes', 'keys_per_entity', 'payload_kind']}
    expected_config.update(read_api=read_api, concurrency=arguments.concurrency or len(cpus) * 4,
                           duration_ms=arguments.duration_ms, warmup_ms=arguments.warmup_ms,
                           available_cpus=len(cpus), profile=arguments.profile or arguments.v8_profile,
                           v8_log=settings.get('DD_FANOUT_V8_LOG'))
    if 'requests_per_second' in result.get('config', {}) or arguments.requests_per_second:
        expected_config['requests_per_second'] = arguments.requests_per_second
    verification = result.get('measurements', {}).get('verification', {})
    profile = result.get('measurements', {}).get('memory_profile')
    if expected_config['profile'] and (not isinstance(profile, dict) or profile.get('enabled') is not True):
        raise RuntimeError(f"benchmark did not enable memory profiling; inspect {folder}")
    if (result.get('ok') is not True or result.get('config') != expected_config
            or verification.get('before_shutdown') is not True
            or verification.get('after_reopen') is not True):
        raise RuntimeError(f"benchmark ignored configuration or did not verify live/reopened state; inspect {folder}")
    for key in ["request_throughput_rps", "transaction_throughput_rps", "p99_ms"]:
        if not isinstance(result.get(key), (int, float)) or not math.isfinite(result[key]) or result[key] <= 0:
            raise RuntimeError(f"invalid {key}={result.get(key)!r}; inspect {folder}")
    if not math.isclose(result['transaction_throughput_rps'], result['request_throughput_rps'] * case['width']):
        raise RuntimeError(f"transaction throughput does not match requested fanout; inspect {folder}")
    for phase in ['warmup', 'timed']:
        measured = result['measurements'][phase]
        if arguments.requests_per_second:
            duration_ms = arguments.warmup_ms if phase == 'warmup' else arguments.duration_ms
            expected_requests = (arguments.requests_per_second * duration_ms + 999) // 1000
            if measured['requests'] != expected_requests:
                raise RuntimeError(f"fixed-rate phase skipped scheduled requests; inspect {folder}")
        by_operation = measured.get('by_operation')
        if by_operation is not None:
            if (by_operation['write']['requests'] != measured['write_requests']
                    or sum(operation['requests'] for operation in by_operation.values()) != measured['requests']):
                raise RuntimeError(f"read/write latency counts do not match completed requests; inspect {folder}")
    record["sample"] = result
    write_json(folder / "run.json", record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--baseline-record", required=True, type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--candidate-record", type=Path)
    parser.add_argument("--baseline-read-api", choices=["atomic", "snapshot"], default="atomic")
    parser.add_argument("--candidate-read-api", choices=["atomic", "snapshot"], default="atomic")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cpu-count", action="append", type=int, choices=[1, 2, 4, 8, 16, 32])
    parser.add_argument("--case", action="append", type=parse_case)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--duration-ms", type=int, default=8000)
    parser.add_argument("--warmup-ms", type=int, default=2000)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--requests-per-second", type=int, default=0,
                        help="scheduled request rate; 0 saturates the caller pool; late requests drain and retain scheduled latency")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--profile", action="store_true", help="collect native phase timings and thread wait samples; diagnostic runs only")
    parser.add_argument("--v8-profile", action="store_true", help="also write per-isolate V8 sampling logs; diagnostic runs only")
    arguments = parser.parse_args()
    if min(arguments.pairs, arguments.duration_ms, arguments.warmup_ms, arguments.timeout) < 1:
        parser.error("pair counts, durations and timeouts must be positive")
    if arguments.concurrency is not None and arguments.concurrency < 1:
        parser.error("concurrency must be positive")
    if arguments.requests_per_second < 0:
        parser.error("requests per second must be nonnegative")
    if bool(arguments.candidate) != bool(arguments.candidate_record):
        parser.error("candidate binary and build record must be supplied together")
    if not shutil.which("taskset") or not hasattr(os, "sched_getaffinity"):
        parser.error("Linux CPU affinity and taskset are required")
    available = sorted(os.sched_getaffinity(0))
    counts = sorted(set(arguments.cpu_count or [count for count in [1, 2, 4, 8, 16] if count <= len(available)]))
    if max(counts) > len(available):
        parser.error(f"requested {max(counts)} CPUs, host allows {available}")
    topology = {}
    for cpu in available:
        topology[cpu] = {name: int(Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/{name}").read_text())
                         for name in ['physical_package_id', 'core_id']}
    physical = []
    siblings = []
    seen = set()
    for cpu in available:
        core = tuple(topology[cpu].values())
        (siblings if core in seen else physical).append(cpu)
        seen.add(core)
    cpu_order = physical + siblings
    cases = arguments.case or [parse_case(f"{mode}:{width}:1024:128")
                              for mode in ["read", "mixed", "write"] for width in [4, 16]]
    if len({case['name'] for case in cases}) != len(cases):
        parser.error("case definitions must be unique")
    binaries = {}
    for side in ['baseline', 'candidate'] if arguments.candidate else ['baseline']:
        binary = getattr(arguments, side).resolve(strict=True)
        record = json.loads(getattr(arguments, f'{side}_record').read_text())
        digest = sha256(binary)
        if record.get('returncode') != 0 or not record.get('source_unchanged'):
            parser.error(f"{side} build did not verify a stable successful source build")
        if digest != record.get('binary_sha256', {}).get('bench_memory_fanout'):
            parser.error(f"{side} binary differs from its build record")
        binaries[side] = {'path': str(binary), 'sha256': digest, 'build_record': record,
                          'read_api': getattr(arguments, f'{side}_read_api')}
    if arguments.candidate:
        baseline = binaries['baseline']['build_record']
        candidate = binaries['candidate']['build_record']
        for key in ['benchmark_sources_sha256', 'rustc', 'cargo', 'profile', 'profile_definitions', 'build_environment', 'cargo_lock_sha256']:
            if not baseline.get(key) or baseline[key] != candidate.get(key):
                if key == 'build_environment' and baseline.get(key) == candidate.get(key) == {}:
                    continue
                parser.error(f"comparison requires matching {key}")
        configurations = []
        for record in [baseline, candidate]:
            source = Path(record['source'])
            configurations.append({
                str(Path(path).relative_to(source)) if Path(path).is_relative_to(source) else path: digest
                for path, digest in record['cargo_configuration_sha256'].items()
            })
        if configurations[0] != configurations[1]:
            parser.error('comparison requires matching Cargo configuration hashes')
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    filesystem = subprocess.check_output(['stat', '-f', '-c', '%T', str(output)], text=True).strip()
    if filesystem in {'tmpfs', 'ramfs'}:
        parser.error(f"durable comparisons require physical-disk storage; {output} is {filesystem}")
    manifest = {'binaries': binaries, 'cpu_sets': {count: cpu_order[:count] for count in counts},
                'profile': arguments.profile or arguments.v8_profile, 'v8_profile': arguments.v8_profile,
                'cpu_topology': topology, 'physical_cores': len(physical), 'cases': cases,
                'pairs': arguments.pairs, 'duration_ms': arguments.duration_ms, 'warmup_ms': arguments.warmup_ms,
                'requests_per_second': arguments.requests_per_second,
                'run_order': 'paired rounds across CPU/workload groups; reverse group and binary order each round',
                'concurrency': arguments.concurrency or '4 per allowed CPU', 'filesystem': filesystem,
                'storage_mount': json.loads(subprocess.check_output(
                    ['findmnt', '--json', '--target', str(output), '--output', 'TARGET,SOURCE,FSTYPE,OPTIONS'], text=True)),
                'uname': list(os.uname()), 'harness_sha256': sha256(Path(__file__)),
                'notes': ['In-process RuntimeService invocation; excludes network transport.',
                          'Separate fresh durable stores; every response and exact state before/after restart verified.',
                          'CPU sets use distinct physical cores first, then SMT siblings.',
                          'Whole-process CPU/RSS includes startup, seed, warmup, verification and shutdown.',
                          'Shared host: load and CPU counters retained per process. No unrelated jobs stopped.']}
    write_json(output / 'manifest.json', manifest)
    groups = [{'cpus': count, 'case': case, 'pairs': []} for count in counts for case in cases]
    for pair in range(arguments.pairs):
        for group in groups if pair % 2 == 0 else reversed(groups):
            count, case = group['cpus'], group['case']
            folder = output / f"cpu-{count}-{case['name']}-pair-{pair + 1}"
            folder.mkdir()
            order = list(binaries) if pair % 2 == 0 else list(reversed(binaries))
            runs = {}
            for side in order:
                print(f"{count} CPUs {case['name']} pair {pair + 1}/{arguments.pairs}: {side}", flush=True)
                runs[side] = run_sample(folder / side, Path(binaries[side]['path']), cpu_order[:count], case, arguments,
                                        binaries[side]['read_api'])
            sample = {'order': order, 'runs': runs}
            if 'candidate' in runs:
                b, c = [runs[side]['sample'] for side in ['baseline', 'candidate']]
                sample['throughput_ratio'] = c['request_throughput_rps'] / b['request_throughput_rps']
                sample['p99_ratio'] = c['p99_ms'] / b['p99_ms']
            write_json(folder / 'pair.json', sample)
            group['pairs'].append(sample)
            if arguments.candidate:
                group['median_throughput_ratio'] = statistics.median(sample['throughput_ratio'] for sample in group['pairs'])
                group['median_p99_ratio'] = statistics.median(sample['p99_ratio'] for sample in group['pairs'])
            write_json(output / 'summary.json', [observed for observed in groups if observed['pairs']])
    for side, binary in binaries.items():
        if sha256(Path(binary['path'])) != binary['sha256']:
            raise RuntimeError(f"{side} executable changed during measurement")
    if sha256(Path(__file__)) != manifest['harness_sha256']:
        raise RuntimeError("comparison harness changed during measurement")
    write_json(output / 'completion.json', {'groups': len(groups), 'pairs_per_group': arguments.pairs,
                                          'binaries_and_harness_unchanged': True})
    print(f"Completed {len(groups) * arguments.pairs} paired groups: {output}")


if __name__ == '__main__':
    main()
