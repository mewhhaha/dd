#!/usr/bin/env python3
"""Small, capped public HTTPS case studies; never a saturation/capacity test."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
from pathlib import Path
import re
import ssl
import statistics
import threading
import time
from urllib.parse import urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[1]
RUST_WORKERS = ROOT / 'crates/runtime/src/bin/bench_memory_storage/workers.rs'


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


class Api:
    def __init__(self, origin, token=None):
        self.origin = urlsplit(origin)
        self.token = token
        self.context = ssl.create_default_context()
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()

    def request(self, path, *, worker=None, run_id=None, payload=None, connection_policy=None):
        if not hasattr(self.local, 'connection'):
            cls = http.client.HTTPSConnection if self.origin.scheme == 'https' else http.client.HTTPConnection
            options = {'context': self.context} if self.origin.scheme == 'https' else {}
            self.local.connection = cls(self.origin.hostname, self.origin.port, timeout=3, **options)
            with self.lock:
                self.connections.append(self.local.connection)
        connection = self.local.connection
        headers = {'Accept': 'application/json'}
        if connection_policy is not None:
            headers['Connection'] = connection_policy
        if worker:
            headers['Host'] = worker
        if run_id:
            headers['x-dd-case-study'] = run_id
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        body = None
        if payload is not None:
            body = json.dumps(payload).encode()
            headers['Content-Type'] = 'application/json'
        started = time.perf_counter()
        try:
            connection.request('POST' if payload is not None else 'GET', path, body, headers)
            response = connection.getresponse()
            content = response.read(65537)
            if len(content) > 65536:
                raise RuntimeError('response exceeded 64 KiB budget')
            return response.status, content, (time.perf_counter() - started) * 1000
        finally:
            if connection_policy == 'close':
                connection.close()

    def close(self):
        for connection in self.connections:
            connection.close()


def fixture(name):
    match = re.search(rf'const {name}: &str = r#"(.*?)"#;', RUST_WORKERS.read_text(), re.S)
    if not match:
        raise ValueError(f'missing existing case-study fixture: {name}')
    return match.group(1)


def public_source(source, run_id):
    if source.count('export default {') != 1:
        raise ValueError('case study must export exactly one default object')
    return source.replace('export default {', 'const caseStudy = {', 1) + f'''
export default {{
  async fetch(request, env) {{
    if (request.headers.get("x-dd-case-study") !== {json.dumps(run_id)}) {{
      return new Response("not found", {{ status: 404 }});
    }}
    return caseStudy.fetch(request, env);
  }},
}};
'''


def validate(case, key, content):
    response = json.loads(content)
    if case == 'rate-limiter':
        if response.get('allowed') is not True or response.get('count', 0) < 1:
            raise ValueError('rate-limiter response did not allow and count request')
    elif case == 'auth-dashboard':
        if (response.get('ok') is not True or response.get('route') != 'dashboard'
                or response.get('username') != f'user_{key}' or response.get('touches', 0) < 1):
            raise ValueError('auth dashboard returned incorrect user/session')
    else:
        expected = [{'sku': sku, 'available': sku + 1, 'priceCents': 1000 + sku}
                    for sku in [(key + offset) % 32 for offset in range(8)]]
        if response != {'products': expected}:
            raise ValueError('inventory dashboard returned incorrect concurrent snapshots')


def phase(api, worker, run_id, case, rate, seconds, counts):
    samples, pending = [], []
    skipped = 0
    started = time.perf_counter()
    stopped = None

    def invoke(index, scheduled):
        key = index % (32 if case == 'inventory-dashboard' else 16)
        path = '/check' if case == 'rate-limiter' else '/dashboard'
        dispatched = time.perf_counter()
        sample = {'key': key, 'dispatch_delay_ms': (dispatched - scheduled) * 1000}
        try:
            status, content, latency = api.request(f'{path}?key={key}', worker=worker, run_id=run_id)
            sample.update(status=status, http_ms=latency)
            if status != 200:
                raise ValueError(f'HTTP {status}')
            validate(case, key, content)
            sample['ok'] = True
        except Exception as error:
            sample.update(ok=False, error=str(error))
        sample['scheduled_ms'] = (time.perf_counter() - scheduled) * 1000
        return sample

    def collect():
        nonlocal pending
        finished, remaining = [], []
        for future in pending:
            (finished if future.done() else remaining).append(future)
        pending = remaining
        for future in finished:
            sample = future.result()
            samples.append(sample)
            if sample['ok']:
                counts[sample['key']] += 1

    with ThreadPoolExecutor(max_workers=2) as pool:
        for index in range(rate * seconds):
            scheduled = started + index / rate
            time.sleep(max(0, scheduled - time.perf_counter()))
            collect()
            if any(not sample['ok'] for sample in samples):
                stopped = 'stopped on first request/validation error'
                break
            if len(samples) >= 10 and percentile([s['scheduled_ms'] for s in samples[-20:]], .95) > 1000:
                stopped = 'stopped because recent p95 exceeded 1 second'
                break
            if len(pending) == 2 or time.perf_counter() - scheduled > 1 / rate:
                skipped += 1
                continue
            pending.append(pool.submit(invoke, index, scheduled))
        for future in pending:
            future.result()
        collect()
        if not stopped:
            time.sleep(max(0, started + seconds - time.perf_counter()))
    elapsed = time.perf_counter() - started
    successful = [s for s in samples if s['ok']]
    latencies = [s['scheduled_ms'] for s in successful]
    return {'offered_rps': rate, 'seconds': seconds, 'elapsed_seconds': elapsed,
            'completed': len(samples), 'successful': len(successful), 'skipped_slots': skipped,
            'successful_rps': len(successful) / elapsed, 'errors': len(samples) - len(successful),
            'p50_ms': percentile(latencies, .5), 'p95_ms': percentile(latencies, .95),
            'p99_ms': percentile(latencies, .99), 'mean_ms': statistics.mean(latencies) if latencies else None,
            'stopped': stopped, 'samples': samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--public-origin', required=True)
    parser.add_argument('--worker-domain', required=True)
    parser.add_argument('--private-origin', default='http://127.0.0.1:18089')
    parser.add_argument('--private-token-file', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--rate', type=int, action='append')
    parser.add_argument('--case', action='append', choices=['rate-limiter', 'auth-dashboard', 'inventory-dashboard'])
    args = parser.parse_args()
    rates = args.rate or [5, 15]
    cases = args.case or ['rate-limiter', 'auth-dashboard', 'inventory-dashboard']
    if len(cases) != len(set(cases)):
        parser.error('case selections must be unique')
    if not 1 <= args.seconds <= 15 or not rates or any(not 1 <= rate <= 20 for rate in rates):
        parser.error('duration must be 1–15 seconds; each rate must be 1–20 requests/s')
    if len(cases) * args.seconds * sum(rates) > 1000:
        parser.error('timed request budget exceeds 1,000')
    for origin in [args.public_origin, args.private_origin]:
        parsed = urlsplit(origin)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.path not in {'', '/'}:
            parser.error('origins must be plain HTTP(S) origins')
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    admin = Api(args.private_origin, args.private_token_file.read_text().strip())
    public = Api(args.public_origin)
    deployed = []
    report = {'run_id': run_id, 'public_origin': args.public_origin, 'worker_domain': args.worker_domain,
              'max_concurrency': 2, 'timed_request_budget': len(cases) * args.seconds * sum(rates),
              'started_at_unix': time.time(), 'cases': [], 'cleanup': [], 'complete': False}

    def save():
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')

    def control(api, path, **options):
        time.sleep(.2)
        status, content, elapsed = api.request(path, connection_policy='close', **options)
        if status != 200:
            raise RuntimeError(f'control request {path} failed: HTTP {status}; {content[:256]!r}')
        return content

    def deploy(name, source, bindings, exposed):
        deployed.append(name)
        body = {'name': name, 'source': public_source(source, run_id) if exposed else source,
                'temporary': True, 'config': {'public': exposed, 'bindings': bindings}}
        response = json.loads(control(admin, '/v1/deploy', payload=body))
        report.setdefault('deployments', []).append({'worker': name, 'deployment_id': response['deployment_id'],
            'source_sha256': hashlib.sha256(source.encode()).hexdigest()})
        save()

    def cleanup():
        while deployed:
            name = deployed[-1]
            control(admin, '/v1/admin/undeploy', payload={'worker': name})
            report['cleanup'].append(name)
            deployed.pop()
            save()

    try:
        control(public, '/readyz')
        report['admin_before'] = json.loads(control(admin, '/v1/admin/status'))
        for case in cases:
            name = f'case-{run_id[:12]}-{case}'
            host = f'{name}.{args.worker_domain}'
            if case == 'rate-limiter':
                deploy(name, fixture('REALWORLD_RATE_LIMITER_WORKER_SOURCE'),
                       [{'type': 'memory', 'binding': 'BENCH_MEMORY'}], True)
            elif case == 'auth-dashboard':
                backend = f'case-{run_id[:12]}-auth'
                deploy(backend, fixture('REALWORLD_AUTH_WORKER_SOURCE'),
                       [{'type': 'kv', 'binding': 'AUTH_DB'}, {'type': 'memory', 'binding': 'AUTH_STATE'}], False)
                deploy(name, fixture('REALWORLD_AUTH_FRONTEND_WORKER_SOURCE'),
                       [{'type': 'service', 'binding': 'AUTH', 'service': backend}], True)
            else:
                deploy(name, (ROOT / 'scripts/fly-api-case-studies/inventory.js').read_text(),
                       [{'type': 'memory', 'binding': 'INVENTORY'}], True)
            population = 32 if case == 'inventory-dashboard' else 16
            for key in range(population):
                control(public, f'/seed?key={key}', worker=host, run_id=run_id)
            counts = Counter()
            entry = {'case': case, 'population': population, 'phases': []}
            report['cases'].append(entry)
            for rate in rates:
                control(public, '/readyz')
                result = phase(public, host, run_id, case, rate, args.seconds, counts)
                entry['phases'].append(result)
                save()
                print(json.dumps({'case': case, **{k: v for k, v in result.items() if k != 'samples'}}), flush=True)
                if result['stopped'] or result['errors']:
                    raise RuntimeError('case study stopped to protect the service; inspect partial results')
            if case != 'inventory-dashboard':
                observed = {}
                for key in range(population):
                    observed[key] = int(control(public, f'/get?key={key}', worker=host, run_id=run_id))
                if observed != {key: counts[key] for key in range(population)}:
                    raise RuntimeError(f'{case}: exact durable counters do not match successful requests')
                entry['verified_writes'] = sum(observed.values())
            else:
                entry['verified_snapshot_reads'] = sum(counts.values()) * 8
            cleanup()
        control(public, '/readyz')
        report['admin_after'] = json.loads(control(admin, '/v1/admin/status'))
        report['complete'] = True
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        try:
            cleanup()
        except Exception as error:
            report['cleanup_error'] = str(error)
            raise
        finally:
            report['finished_at_unix'] = time.time()
            save()
            public.close()
            admin.close()


if __name__ == '__main__':
    main()
