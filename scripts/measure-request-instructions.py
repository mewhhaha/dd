#!/usr/bin/env python3
"""Compare marginal request instructions using two prebuilt Linux binaries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--counter", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--cpu", type=int, required=True)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.cycles < 1 or args.cpu not in os.sched_getaffinity(0):
        parser.error("cycles must be positive and CPU must be in the process affinity")
    binaries = {
        "baseline": args.baseline.resolve(),
        "candidate": args.candidate.resolve(),
    }
    counter = args.counter.resolve()
    runs = []
    samples = []
    for kind in ("hello", "mem"):
        for cycle in range(args.cycles):
            for build in ("baseline", "candidate", "candidate", "baseline"):
                counts = []
                for requests in (2000, 12000):
                    env = os.environ.copy()
                    env.update(REQ_KIND=kind, REQ_N=str(requests), REQ_CONC="8")
                    result = subprocess.run(
                        ["taskset", "-c", str(args.cpu), str(counter), str(binaries[build])],
                        env=env, text=True, capture_output=True, timeout=300, check=True,
                    )
                    count = json.loads(result.stderr.splitlines()[-1])
                    if count["time_running"] <= 0:
                        raise RuntimeError("instruction counter did not run")
                    if count["time_enabled"] != count["time_running"]:
                        raise RuntimeError("multiplexed counters would invalidate this sample")
                    runs.append(dict(kind=kind, cycle=cycle, build=build,
                                     requests=requests, **count))
                    counts.append(count["instructions"])
                marginal = (counts[1] - counts[0]) / 10000
                if marginal <= 0:
                    raise RuntimeError("nonpositive marginal instruction count")
                samples.append(dict(kind=kind, build=build, instructions=marginal))
                print(f"{kind} {build}: {marginal:,.0f} instructions/request", flush=True)
    summary = {}
    for kind in ("hello", "mem"):
        medians = {}
        for build in binaries:
            values = [sample["instructions"] for sample in samples
                      if sample["kind"] == kind and sample["build"] == build]
            medians[build] = statistics.median(values)
        summary[kind] = dict(**medians, change_percent=(
            medians["candidate"] / medians["baseline"] - 1) * 100)
    output = dict(
        baseline_revision=args.baseline_revision,
        candidate_revision=args.candidate_revision,
        binary_sha256={name: hashlib.sha256(path.read_bytes()).hexdigest()
                       for name, path in binaries.items()},
        kernel=platform.release(), cpu=args.cpu, cycles=args.cycles,
        method="user instructions; inherited threads; ABBA; (N12000-N2000)/10000",
        samples=samples, runs=runs, summary=summary,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
