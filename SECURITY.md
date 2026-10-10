# Security policy

## Supported versions

This project is greenfield and supports only the current `main` branch and the latest published release. Security fixes may include breaking changes.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting for this repository and include reproduction steps, affected versions, impact, and any proposed mitigation. Maintainers should acknowledge a report within seven days.

## Threat model

Worker code is semi-trusted: the runtime is designed to contain buggy or abusive workers with quotas, bounded queues, storage namespaces, and egress policy. Worker JavaScript reaches the host only through the web platform and its bindings; the runtime's ops and internal state are private to the runtime's own scripts. V8 isolates are not an operating-system security boundary. Running mutually hostile tenants requires separate processes or containers and is outside the supported deployment model.

The private listener is an authenticated control plane and must not be exposed directly to the public Internet. Public deployment tokens must be scoped, short-lived, and use-limited. TLS is expected to terminate at the deployment edge for HTTP/1.1 and HTTP/2.

## Dependency policy

RustSec vulnerabilities and moderate-or-higher production npm advisories fail CI. Informational RustSec warnings remain visible and are reviewed separately. Dependency exceptions must document reachability, an owner, a review date, and an expiry date. The runtime embeds V8 through `rusty_v8` (the `v8` crate) alone; its web platform layer (`crates/runtime/src/web` and `crates/runtime/js/vendor`) is ported from Deno under the MIT license and maintained here.

The following exceptions were reviewed on 2026-10-02. CI reads this table, rejects expired exceptions and changed package versions, and refuses review periods longer than 90 days. Renewing an exception requires reassessing its reachability and available fixes.

| Advisory | Package | Reviewed | Expires | Owner |
| --- | --- | --- | --- | --- |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | `rsa 0.9.10` | 2026-10-02 | 2026-11-01 | Runtime maintainers |


**RSA private keys:** Web Crypto exposes RustCrypto RSA private-key operations, including RSA-OAEP decryption in `crates/runtime/src/web/crypto/decrypt.rs`. A worker that exposes these operations to network callers can expose private-key timing information. Worker tenancy rules and request quotas do not remove this risk. Do not expose worker RSA private-key operations as an attacker-accessible service; use an implementation or external service with appropriate timing protections. Platform deployment tokens do not use RSA. As of this review, the [upstream advisory](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) still reports no fixed release, including the latest stable 0.9.10 and 0.10 release candidate. This exception preserves Web Crypto compatibility while accepting that documented limitation; it must be removed when a fix or replacement is available.

The audit also reports unmaintained `paste` (a build-time macro of `v8`), plus [RUSTSEC-2026-0097](https://rustsec.org/advisories/RUSTSEC-2026-0097.html) for `rand 0.8.5`, which Web Crypto and RustCrypto's `rsa 0.9` use. The Rand issue requires a custom logger that calls the thread-local RNG while it reseeds; this project does not implement such a logger. Do not introduce that logging pattern. Moving off `rand 0.8` waits on the RustCrypto 0.14 releases (`rsa 0.10` and the matching elliptic-curve crates). Review these visible warnings when updating them.
