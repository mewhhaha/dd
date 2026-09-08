#!/usr/bin/env python3
"""Exercise the API benchmark's admission and abort boundaries without network load."""
from collections import Counter
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest

sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name('bench-fly-api.py')
spec = importlib.util.spec_from_file_location('bench_fly_api', SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class SlowApi:
    def __init__(self, status=200):
        self.status = status
        self.active = self.peak = 0
        self.lock = threading.Lock()

    def request(self, *args, **kwargs):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(.2)
        with self.lock:
            self.active -= 1
        return self.status, json.dumps({'allowed': True, 'count': 1}).encode(), 200


class AdmissionTests(unittest.TestCase):
    def test_control_calls_close_connections_and_can_reconnect(self):
        received = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                received.append((self.headers.get('Connection'), self.client_address))
                self.send_response(200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'{}')

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        api = bench.Api(f'http://127.0.0.1:{server.server_port}')
        try:
            for _ in range(2):
                status, content, _ = api.request('/', connection_policy='close')
                self.assertEqual((status, content), (200, b'{}'))
            self.assertEqual([header for header, _ in received], ['close', 'close'])
            self.assertNotEqual(received[0][1], received[1][1])
        finally:
            api.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_busy_callers_skip_slots_without_growing_concurrency(self):
        api = SlowApi()
        result = bench.phase(api, 'unused', 'test', 'rate-limiter', 20, 1, Counter())
        self.assertLessEqual(api.peak, 2)
        self.assertGreater(result['skipped_slots'], 0)
        self.assertEqual(result['completed'] + result['skipped_slots'], 20)
        self.assertEqual(result['errors'], 0)

    def test_first_error_stops_new_requests(self):
        result = bench.phase(SlowApi(status=503), 'unused', 'test', 'rate-limiter', 5, 3, Counter())
        self.assertIsNotNone(result['stopped'])
        self.assertLessEqual(result['completed'], 2)
        self.assertGreater(result['errors'], 0)

    def test_inventory_response_requires_all_eight_correct_snapshots(self):
        with self.assertRaises(ValueError):
            bench.validate('inventory-dashboard', 0, b'{"products":[]}')

    def test_excessive_request_budget_fails_before_credentials_or_network(self):
        result = subprocess.run([sys.executable, str(SCRIPT), '--public-origin', 'https://unused.invalid',
            '--worker-domain', 'unused.invalid', '--private-token-file', '/does-not-exist',
            '--output', '/does-not-exist', '--seconds', '15', '--rate', '20', '--rate', '20'],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('budget exceeds', result.stderr)


if __name__ == '__main__':
    unittest.main()
