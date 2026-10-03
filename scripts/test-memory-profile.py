#!/usr/bin/env python3
"""Exercise profiling summaries for snapshot reads and committed writes."""
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


class ProfileSummaryTests(unittest.TestCase):
    def test_read_and_write_campaigns_use_current_metrics(self):
        with TemporaryDirectory(prefix='dd-profile-summary-') as root:
            root = Path(root)
            (root / 'completion.json').write_text(json.dumps({'binaries_and_harness_unchanged': True}))
            for mode, commits in [('read', 0), ('write', 2)]:
                run = root / mode / 'run'
                run.mkdir(parents=True)
                profile = {'enabled': True}
                for name, calls, total in [
                    ('store_lease', commits, commits * 10),
                    ('op_snapshot', 4, 80),
                    ('op_apply_batch', commits, commits * 20),
                    ('store_snapshot_cache_hit', 3, 0),
                    ('store_snapshot_cache_miss', 1, 0),
                    ('store_snapshot_cache_eviction', 1, 0),
                ]:
                    profile[name] = {'calls': calls, 'total_us': total, 'total_items': 0, 'max_us': 0}
                before = {'committed_groups': 10, 'committed_commands': 20, 'timings': {
                    'queued_commands': 20, 'sql_attempts': 10, 'queue_wait_us': 100,
                    'sql_us': 200, 'commit_us': 300, 'snapshot_publish_us': 400,
                }}
                after = {'committed_groups': 10 + commits, 'committed_commands': 20 + commits,
                         'timings': {key: value + commits for key, value in before['timings'].items()}}
                (run / 'stdout.log').write_text(json.dumps({
                    'ok': True, 'config': {'available_cpus': 1, 'mode': mode, 'width': 1,
                                          'population': 4, 'keys_per_entity': 1, 'payload_bytes': 64},
                    'transaction_throughput_rps': 50, 'p99_ms': 2,
                    'measurements': {'memory_profile': profile, 'admin_before_timed': {'state_storage': before},
                                     'admin_after_timed': {'state_storage': after}, 'timed': {}, 'verification': {'ok': True}},
                }))
            output = root / 'summary.json'
            completed = subprocess.run([
                sys.executable, str(Path(__file__).with_name('summarize-memory-profile.py')),
                str(root), '--output', str(output),
            ], capture_output=True, text=True, check=True)
            runs = json.loads(output.read_text())['runs']
            self.assertEqual(len(runs), 2)
            read, write = runs
            self.assertIsNone(read['mean_native_us']['store_lease'])
            self.assertIsNone(read['writer']['mean_commit_us'])
            self.assertIn('lease n/a', completed.stdout)
            self.assertEqual(write['mean_native_us']['store_lease'], 10)
            self.assertEqual(write['writer']['commands'], 2)
            self.assertEqual(write['writer']['mean_commit_us'], 1)
            for run in runs:
                self.assertEqual(run['snapshot_cache'], {'hits': 3, 'misses': 1, 'hit_fraction': .75, 'evictions': 1})


if __name__ == '__main__':
    unittest.main()
