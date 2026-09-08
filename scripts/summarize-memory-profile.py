#!/usr/bin/env python3
"""Summarize opt-in memory fanout diagnostics; overlapping waits are not CPU time."""
import argparse
import json
from pathlib import Path


def summarize(path):
    result = json.loads((path / 'stdout.log').read_text())
    measurement = result['measurements']
    profile = measurement.get('memory_profile')
    if not result['ok'] or not isinstance(profile, dict) or not profile.get('enabled'):
        raise ValueError(f'{path}: expected a successful profiling run')
    before = measurement['admin_before_timed']['state_storage']
    after = measurement['admin_after_timed']['state_storage']
    timings = {key: value - before['timings'][key] for key, value in after['timings'].items()}
    groups = after['committed_groups'] - before['committed_groups']
    commands = after['committed_commands'] - before['committed_commands']
    def mean(total, count):
        return total / count if count else None
    return {
        'run': str(path), 'config': result['config'],
        'transactions_per_second': result['transaction_throughput_rps'],
        'request_p99_ms': result['p99_ms'],
        'snapshot_reload_fraction': (profile['js_hydrate_full']['calls'] / profile['op_snapshot']['calls']
                                     if 'js_hydrate_full' in profile else None),
        'mean_native_us': {name: mean(profile[name]['total_us'], profile[name]['calls'])
                           for name in ['store_lease', 'op_snapshot', 'op_apply_batch']},
        'writer': {'commands': commands, 'groups': groups,
                   'commands_per_group': mean(commands, groups), 'totals': timings,
                   'mean_queue_wait_us': mean(timings['queue_wait_us'], timings['queued_commands']),
                   'mean_sql_us': mean(timings['sql_us'], timings['sql_attempts']),
                   'mean_commit_us': mean(timings['commit_us'], timings['sql_attempts']),
                   'mean_publication_us': mean(timings['snapshot_publish_us'], groups)},
        'verification': measurement['verification'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaigns', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    runs = []
    for campaign in args.campaigns:
        completion = json.loads((campaign / 'completion.json').read_text())
        if not completion['binaries_and_harness_unchanged']:
            raise ValueError(f'{campaign}: benchmark provenance was not verified')
        runs.extend(summarize(path.parent) for path in sorted(campaign.glob('*/*/stdout.log')))
    args.output.write_text(json.dumps({'notes': [
        'Profiling instrumentation and sampling affect throughput; use unprofiled pairs for speed comparisons.',
        'Native durations are elapsed waits; operations and shard writers overlap. Totals are not CPU time.',
        'SQL includes BEGIN and statements; COMMIT includes durable I/O and automatic checkpoints.',
        'Lease timing includes catalog lookup and entity ownership wait.',
        'JavaScript durations use the runtime frozen clock and are excluded from phase means.'
    ], 'runs': runs}, indent=2) + '\n')
    for run in runs:
        config = run['config']; writer = run['writer']
        print(f"{config['available_cpus']} CPUs {config['mode']} x{config['width']} "
              f"({config['population']} entities, {config['keys_per_entity']} x {config['payload_bytes']} B): "
              f"{run['transactions_per_second']:.0f} tx/s; "
              f"lease {run['mean_native_us']['store_lease']:.1f} us; "
              f"commit {writer['mean_commit_us'] or 0:.1f} us/group")


if __name__ == '__main__':
    main()
