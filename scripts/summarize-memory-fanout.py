#!/usr/bin/env python3
"""Summarize completed paired memory fanout runs; optionally plot one run's core scaling."""

import argparse
import json
from pathlib import Path
import statistics


def median(group, side, measure):
    return statistics.median(measure(pair['runs'][side]) for pair in group['pairs'])


def cache_miss_percent(run):
    measurements = run['sample']['measurements']
    before, after = [measurements[key] for key in ['admin_before_timed', 'admin_after_timed']]
    hits = after['memory_snapshot_cache_hits'] - before['memory_snapshot_cache_hits']
    misses = after['memory_snapshot_cache_misses'] - before['memory_snapshot_cache_misses']
    return 100 * misses / (hits + misses) if hits + misses else 0


def commit_density(run):
    measurements = run['sample']['measurements']
    before, after = [measurements[key]['state_storage'] for key in ['admin_before_timed', 'admin_after_timed']]
    groups = after['committed_groups'] - before['committed_groups']
    commands = after['committed_commands'] - before['committed_commands']
    return commands / groups if groups else 0


def print_run(folder, manifest, groups):
    print(f"## {folder.name}\n")
    print(f"Raw artifacts: `{folder}`. Each row has {manifest['pairs']} alternating paired runs; "
          f"{manifest['warmup_ms'] / 1000:g}s warmup and {manifest['duration_ms'] / 1000:g}s timed work. "
          f"Caller concurrency: {manifest['concurrency']}.\n")
    print("Values are medians. Gain is the median candidate/baseline ratio of matched pairs; "
          "the range shows every pair, not a confidence interval. Request latency includes all concurrent "
          "memory transactions and response validation. CPU time and RSS cover the whole process, "
          "including setup, verification, and reopening.\n")
    for side in ['baseline', 'candidate']:
        binary = manifest['binaries'][side]
        print(f"- {side.capitalize()} binary SHA-256: `{binary['sha256']}`; "
              f"source patch SHA-256: `{binary['build_record']['source_patch_sha256']}`.")
    print("\n| CPUs | Mode / fanout / population / bytes | Requests/s before → after | Transactions/s before → after | Gain (pair range) | p99 ms before → after |")
    print("|---:|---|---:|---:|---:|---:|")
    for group in groups:
        rates = [median(group, side, lambda run: run['sample']['request_throughput_rps'])
                 for side in ['baseline', 'candidate']]
        transactions = [median(group, side, lambda run: run['sample']['transaction_throughput_rps'])
                        for side in ['baseline', 'candidate']]
        latencies = [median(group, side, lambda run: run['sample']['p99_ms'])
                     for side in ['baseline', 'candidate']]
        ratios = [pair['throughput_ratio'] for pair in group['pairs']]
        case = group['case']
        label = f"{case['mode']} / {case['width']} / {case['population']} / {case['payload_bytes']}"
        print(f"| {group['cpus']} | {label} | {rates[0]:,.0f} → {rates[1]:,.0f} | "
              f"{transactions[0]:,.0f} → {transactions[1]:,.0f} | "
              f"{statistics.median(ratios):.2f}× ({min(ratios):.2f}–{max(ratios):.2f}) | "
              f"{latencies[0]:.2f} → {latencies[1]:.2f} |")
    print("\n| CPUs | Case | Cache misses % before → after | Commands/commit before → after | CPU seconds before → after | Peak RSS MiB before → after |")
    print("|---:|---|---:|---:|---:|---:|")
    for group in groups:
        measures = [cache_miss_percent, commit_density,
                    lambda run: run['resources']['user_seconds'] + run['resources']['system_seconds'],
                    lambda run: run['resources']['max_rss_kib'] / 1024]
        values = [[median(group, side, measure) for side in ['baseline', 'candidate']]
                  for measure in measures]
        columns = ' | '.join(f'{before:.2f} → {after:.2f}' for before, after in values)
        print(f"| {group['cpus']} | {group['case']['name']} | {columns} |")
    runs = [pair['runs'][side] for group in groups for pair in group['pairs']
            for side in ['baseline', 'candidate']]
    loads = [run[phase]['load_average'][0] for run in runs for phase in ['host_start', 'host_end']]
    writes = sum(run['sample']['measurements']['verification']['completed_writes'] for run in runs)
    print(f"\nAll {len(runs)} processes validated responses and exact final state before shutdown and "
          f"after reopening, covering {writes:,} completed writes. "
          f"Host one-minute load ranged from {min(loads):.2f} to {max(loads):.2f}.\n")


def plot_scaling(path, manifest, groups):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    modes = [mode for mode in ['read', 'mixed', 'write'] if sum(
        group['case']['name'] == f'{mode}-16-1024-128' for group in groups) >= 2]
    if not modes:
        raise ValueError('need at least two CPU counts for a MODE:16:1024:128 case')
    figure, panels = plt.subplots(1, len(modes), figsize=(4.2 * len(modes), 4.2),
                                 constrained_layout=True, squeeze=False)
    axes = panels[0]
    for axis, mode in zip(axes, modes):
        selected = sorted([group for group in groups if group['case'] == {
            'name': f'{mode}-16-1024-128', 'mode': mode, 'width': 16,
            'population': 1024, 'payload_bytes': 128}], key=lambda group: group['cpus'])
        counts = [group['cpus'] for group in selected]
        for side, label, color in [('baseline', 'Before', '#64748b'), ('candidate', 'After', '#087f8c')]:
            samples = [[pair['runs'][side]['sample']['transaction_throughput_rps'] / 1000
                        for pair in group['pairs']] for group in selected]
            axis.plot(counts, [statistics.median(values) for values in samples],
                      marker='o', label=label, color=color)
            axis.fill_between(counts, [min(values) for values in samples],
                              [max(values) for values in samples], color=color, alpha=0.12)
        axis.set_xscale('log', base=2)
        axis.set_xticks(counts, [str(count) if count <= manifest['physical_cores']
                               else f'{manifest["physical_cores"]}+SMT' for count in counts])
        axis.set_ylim(bottom=0)
        axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:g}k'))
        axis.set_title({'read': 'Read', 'mixed': '90% read / 10% write', 'write': 'Durable write'}[mode])
        axis.set_xlabel('Physical cores, then SMT threads')
        axis.grid(alpha=0.2)
    axes[0].set_ylabel('Memory transactions / second')
    axes[-1].legend(frameon=False)
    title = '16 memories / request · 1,024 entities · 128-byte payload'
    if len(modes) == 1:
        title = '16 memories / request · 1,024 entities\n128-byte payload'
    figure.suptitle(f"{title}\nCallers: {manifest['concurrency']}", fontsize=11)
    figure.savefig(path, metadata={'Date': None})
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', nargs='+', type=Path)
    parser.add_argument('--plot', type=Path, help='SVG/PNG scaling plot from the first run; requires matplotlib')
    arguments = parser.parse_args()
    print('# Concurrent memory measurements\n')
    for index, folder in enumerate(arguments.runs):
        manifest = json.loads((folder / 'manifest.json').read_text())
        groups = json.loads((folder / 'summary.json').read_text())
        completion = json.loads((folder / 'completion.json').read_text())
        expected = {'groups': len(groups), 'pairs_per_group': manifest['pairs'],
                    'binaries_and_harness_unchanged': True}
        if completion != expected or 'candidate' not in manifest['binaries']:
            raise ValueError(f'{folder} is not a complete verified paired comparison')
        print_run(folder, manifest, groups)
        if index == 0 and arguments.plot:
            plot_scaling(arguments.plot, manifest, groups)


if __name__ == '__main__':
    main()
