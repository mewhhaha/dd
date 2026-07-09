# Security policy

## Supported versions

This project is greenfield and supports only the current `main` branch and the latest published release. Security fixes may include breaking changes.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting for this repository and include reproduction steps, affected versions, impact, and any proposed mitigation. Maintainers should acknowledge a report within seven days.

## Threat model

Worker code is semi-trusted: the runtime is designed to contain buggy or abusive workers with quotas, bounded queues, storage namespaces, and egress policy. V8 isolates are not an operating-system security boundary. Running mutually hostile tenants requires separate processes or containers and is outside the supported deployment model.

The private listener is an authenticated control plane and must not be exposed directly to the public Internet. Public deployment tokens must be scoped, short-lived, and use-limited. TLS is expected to terminate at the deployment edge for HTTP/1.1 and HTTP/2; HTTP/3 uses the configured certificate directly.

## Dependency policy

RustSec advisories and moderate-or-higher production npm advisories fail CI. Dependency exceptions must document reachability, an owner, and an expiry date. The Deno crate family is upgraded as one compatible set, and the local `deno_crypto` patch must remain reproducible from `patches/deno_crypto.patch`.

Current upstream exceptions, owned by the runtime maintainers and expiring on 2026-10-01, are `RUSTSEC-2026-0118`, `RUSTSEC-2026-0119`, and `RUSTSEC-2023-0071`. Latest Deno still pins Hickory 0.25 and RustCrypto RSA 0.9. DNSSEC is not enabled, URL/hostname sizes are bounded, and worker code is semi-trusted; these exceptions must be removed as soon as Deno adopts fixed dependencies or the integrations are replaced.
