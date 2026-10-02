# Security policy

## Supported versions

This project is greenfield and supports only the current `main` branch and the latest published release. Security fixes may include breaking changes.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting for this repository and include reproduction steps, affected versions, impact, and any proposed mitigation. Maintainers should acknowledge a report within seven days.

## Threat model

Worker code is semi-trusted: the runtime is designed to contain buggy or abusive workers with quotas, bounded queues, storage namespaces, and egress policy. V8 isolates are not an operating-system security boundary. Running mutually hostile tenants requires separate processes or containers and is outside the supported deployment model.

The private listener is an authenticated control plane and must not be exposed directly to the public Internet. Public deployment tokens must be scoped, short-lived, and use-limited. TLS is expected to terminate at the deployment edge for HTTP/1.1 and HTTP/2.

## Dependency policy

RustSec vulnerabilities and moderate-or-higher production npm advisories fail CI. Informational RustSec warnings remain visible and are reviewed separately. Dependency exceptions must document reachability, an owner, a review date, and an expiry date. The Deno crate family is upgraded as one compatible set. The local `deno_crypto` and `deno_tls` patches must remain reproducible from `patches/deno_crypto.patch` and `patches/deno_tls.patch`.

The following exceptions were reviewed on 2026-10-02. CI reads this table, rejects expired exceptions and changed package versions, and refuses review periods longer than 90 days. Renewing an exception requires reassessing its reachability and available fixes.

| Advisory | Package | Reviewed | Expires | Owner |
| --- | --- | --- | --- | --- |
| [RUSTSEC-2026-0118](https://rustsec.org/advisories/RUSTSEC-2026-0118.html) | `hickory-proto 0.25.2` | 2026-10-02 | 2026-11-01 | Runtime maintainers |
| [RUSTSEC-2026-0119](https://rustsec.org/advisories/RUSTSEC-2026-0119.html) | `hickory-proto 0.25.2` | 2026-10-02 | 2026-11-01 | Runtime maintainers |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | `rsa 0.9.10` | 2026-10-02 | 2026-11-01 | Runtime maintainers |

**Hickory DNS:** Deno Fetch 0.283.0 depends on Hickory 0.25.2, but this runtime initializes Fetch with its default operating-system resolver. Egress requests use the custom resolver that pins addresses after policy checks. The project does not construct Hickory resolvers or encode DNS messages through Hickory. DNSSEC features are disabled, which CI verifies. On this configuration, neither the NSEC3 validation loop nor the many-record message encoder is reached. These are configuration-dependent exceptions, not fixes in Hickory. Reassess them before changing DNS resolution or Deno Fetch. The [upstream fixes](https://github.com/hickory-dns/hickory-dns/releases/tag/v0.26.1) require the 0.26 crate/API migration; there is no fixed 0.25 release to select with a compatible dependency update.

**RSA private keys:** Web Crypto exposes RustCrypto RSA private-key operations, including RSA-OAEP decryption in `patched-crates/deno_crypto/decrypt.rs`. A worker that exposes these operations to network callers can expose private-key timing information. Worker tenancy rules and request quotas do not remove this risk. Do not expose worker RSA private-key operations as an attacker-accessible service; use an implementation or external service with appropriate timing protections. Platform deployment tokens do not use RSA. As of this review, the [upstream advisory](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) still reports no fixed release, including the latest stable 0.9.10 and 0.10 release candidate. This exception preserves Web Crypto compatibility while accepting that documented limitation; it must be removed when a fix or replacement is available.

The audit also reports unmaintained `paste` and `rustls-pemfile`, plus [RUSTSEC-2026-0097](https://rustsec.org/advisories/RUSTSEC-2026-0097.html) for Deno's exact `rand 0.8.5` dependency. The Rand issue requires a custom logger that calls the thread-local RNG while it reseeds; this project does not implement such a logger. Do not introduce that logging pattern. Several Deno crates share the exact Rand pin, so changing only the crypto patch cannot resolve it. Review these visible warnings with each Deno update.
