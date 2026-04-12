# Secret Audit 2026-04-11

Purpose: public-repo readiness check focused on deploy/auth history, especially Fly-related material.

## Scope

- working tree: `README.md`, `deploy/fly/**`, `docs/**`, `examples/**`, `crates/api/**`, `crates/cli/**`, `justfile`
- git history for same paths across `git rev-list --all`

## Method

Scanner availability check:

- `gitleaks`: not installed
- `trufflehog`: not installed
- `detect-secrets`: not installed

Fallback audit used:

- working-tree `rg` for `DD_PRIVATE_TOKEN`, `FLY_API_TOKEN`, bearer headers, OpenAI-style keys, AWS-style keys, Slack token shapes, and private-key markers
- `git grep` across all revisions for same secret-shaped patterns
- focused `git log -G` over Fly/deploy/auth files for suspicious additions

## Result

No real secret or likely real secret found in reviewed working tree or reviewed git history.

Observed placeholders and fake tokens only:

- [deploy/fly/README.md](/home/mewhhaha/src/grugd/deploy/fly/README.md): `replace-me-with-long-random-secret`
- API tests: `test-private-token`
- public docs/examples now use explicit placeholders such as `'<set-via---env>'`

Most relevant history hit:

- repeated Fly README placeholder `DD_PRIVATE_TOKEN=replace-me-with-long-random-secret`

## Follow-up rule

If a real secret is ever found later:

1. rotate or revoke credential first
2. rewrite git history with `git filter-repo` or equivalent
3. force-push rewritten refs
4. notify downstream clones to re-clone or hard-reset to rewritten history
