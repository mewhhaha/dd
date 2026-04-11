# Security Review 2026-04-11

Scope:
- public/private HTTP routing
- dynamic worker deploy/invoke
- dynamic egress guardrails
- request URL construction

## Confirmed Findings

### 1. Public request URL trusts raw client `x-forwarded-host` and `x-forwarded-proto`

Severity: High

Files:
- `crates/api/src/handlers.rs:1050-1072`
- `crates/api/src/handlers.rs:1700-1711`

What happens:
- Public worker selection uses `Host`.
- But worker-visible `request.url` prefers raw client `x-forwarded-host` and `x-forwarded-proto`.
- Any direct client can therefore lie about host/scheme seen by worker code.

Impact:
- Host-based auth/origin checks in worker code can be bypassed.
- Workers can mint wrong absolute URLs, redirects, callback URLs, or cookie domains.
- Multi-tenant workers that trust `request.url.host` can be confused into acting on behalf of another host.

Minimal exploit:

1. Deploy any public worker that reflects `new URL(request.url).host` or gates logic on host.
2. Send:

```bash
curl 'https://echo.example.com/' \
  -H 'Host: echo.example.com' \
  -H 'x-forwarded-host: admin.example.com' \
  -H 'x-forwarded-proto: https'
```

3. Worker sees `https://admin.example.com/...` in `request.url`, even though request was routed as `echo.example.com`.

Why confirmed:
- Code path explicitly prefers forwarded headers.
- Existing unit test asserts this behavior.

Remediation:
- Fixed by building worker-visible public URL from actual `Host` / URI authority only.
- Spoofed client `x-forwarded-host` / `x-forwarded-proto` no longer affect `request.url`.
- Coverage:
  - `crates/api/src/handlers.rs` test `build_public_request_url_ignores_spoofed_forwarded_host_and_proto`
  - `crates/api/src/handlers.rs` test `build_public_request_url_ignores_spoofed_forwarded_proto`
  - `crates/api/src/handlers.rs` test `public_host_invoke_ignores_spoofed_forwarded_request_url`

### 2. Dynamic worker egress allowlist is host-only, not host+port

Severity: Medium

Files:
- `crates/runtime/src/service.rs:6063-6118`
- `crates/runtime/src/ops.rs:2288-2297`
- `crates/runtime/src/ops.rs:4881-4894`

What happens:
- Dynamic deploy accepts `egress_allow_hosts`.
- Enforcement extracts only `parsed_url.host_str()`.
- Port never participates in policy.

Impact:
- If host `api.example.com` is allowed, dynamic code can hit any port on that host.
- This widens blast radius to admin/debug sidecars, alternate control ports, Docker daemons, metadata proxies, or internal service variants on same hostname.

Repro:

1. Deploy dynamic worker with:

```json
{
  "egress_allow_hosts": ["127.0.0.1"]
}
```

2. Worker source:

```js
export default {
  async fetch() {
    await fetch("http://127.0.0.1:1/port-probe");
    return new Response("unexpected");
  },
};
```

3. Invoke worker.
4. Result is network/connect failure, not `egress host is not allowed`.

Why confirmed:
- I verified this locally with a temporary runtime test before removing it from tree.
- This behavior follows directly from current host-only matcher.

Remediation:
- Fixed by changing allowlist semantics to origin policy:
  - bare host => default port only
  - `host:port` => exact port only
  - `*.suffix` and `*.suffix:port` supported
- Coverage:
  - `crates/runtime/src/ops.rs` test `egress_url_rules_require_matching_port`
  - `crates/runtime/src/ops.rs` test `egress_url_rules_support_explicit_ports_and_wildcards`
  - `crates/runtime/src/service.rs` test `dynamic_worker_config_accepts_host_port_and_wildcard_rules`

## Probable High-Risk Issue

### 3. Dynamic egress allowlist likely bypassable through redirects

Severity: High

Files:
- `crates/runtime/src/ops.rs:2288-2297`
- `crates/runtime/js/execute_worker.js:891-911`

What happens:
- `op_http_prepare` validates only initial URL host.
- JS then hands request to runtime `fetch(...)`.
- No second allowlist check happens after redirect resolution.
- `fetch` defaults to normal redirect-following behavior.

Likely exploit:
- Allowed host returns `302 Location: http://disallowed-host/...`.
- Runtime follows redirect.
- Second hop escapes original allowlist.

Status:
- Code path strongly suggests bypass.
- I attempted live socket repro in this environment, but local listener constraints made that run inconclusive.
- This should be treated as real until disproven, because guard is clearly pre-redirect only.

Remediation:
- Fixed by switching host fetch wrapper to manual redirect handling with bounded hop count.
- Every redirect hop now resolves next URL, re-runs runtime egress policy, then continues only if allowed.
- Redirects to disallowed host or disallowed port now fail before outbound hop.

## Operational Risk

### 4. Private control plane has no authentication

Severity: Critical if private listener is reachable by untrusted network paths

Files:
- `crates/api/src/handlers.rs:77-100`
- `deploy/fly/README.md:59`

What happens:
- `POST /v1/deploy`
- `POST /v1/dynamic/deploy`
- `ANY /v1/invoke/...`
- No auth layer.

Impact:
- Any client that can reach private listener gets arbitrary worker deploy/invoke.
- For dynamic deploy, that is effectively arbitrary code execution with configured egress policy.

Notes:
- This is documented design, not accidental behavior.
- Still main operational sharp edge in current system.

Remediation:
- Fixed by requiring `Authorization: Bearer <token>` on private deploy and invoke routes.
- Server startup now fails without token unless `ALLOW_INSECURE_PRIVATE_LOOPBACK=1` (or `DD_ALLOW_INSECURE_PRIVATE_LOOPBACK=1`) and private bind is loopback-only.
- CLI and deploy helper now auto-send bearer token from `DD_PRIVATE_TOKEN` / `PRIVATE_BEARER_TOKEN`.
- Coverage:
  - `crates/api/src/handlers.rs` test `private_routes_reject_missing_bearer_token`
  - `crates/api/src/lib.rs` tests `private_auth_config_requires_token_by_default`, `private_auth_config_allows_loopback_escape_hatch`, `private_auth_config_rejects_non_loopback_escape_hatch`

## Suggested Fix Order

1. Stop trusting raw client `x-forwarded-host` / `x-forwarded-proto` unless request came from trusted proxy boundary.
2. Make dynamic egress policy include scheme + host + optional port rules, or default-deny non-default ports.
3. Re-check egress policy after each redirect hop, or force `redirect: "manual"` and enforce in host runtime.
4. Add auth or mutually-authenticated network boundary on private control plane.
