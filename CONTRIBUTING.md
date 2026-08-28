# Contributing to HT

Thanks for helping make agent sessions easier to understand and safer to
manage. Small, focused pull requests with a clear test plan are the easiest to
review. By participating, you agree to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Before you start

- Search existing issues and pull requests.
- Open an issue before a large feature, storage migration, provider adapter, or
  user-visible breaking change.
- Do not include real transcripts, proprietary code, credentials, private
  paths, customer data, or personal information. Use synthetic fixtures.
- Report vulnerabilities through [SECURITY.md](SECURITY.md), not a public issue.

## Development

HT requires [Bun 1.3+](https://bun.sh/). A separate Node.js installation is not
required.

```sh
git clone https://github.com/use-habitat/ht.git
cd ht
bun install --frozen-lockfile
bun run check
bun run build
./dist/ht --help
```

Use a temporary state directory during manual testing so your installed HT
configuration and captured sessions stay untouched:

```sh
HT_HOME="$(mktemp -d)" bun run dev -- preflight
```

Before opening a pull request, run:

```sh
bun run check
bun run build
git diff --check
```

## Design invariants

1. Provider-owned transcripts are read-only.
2. Canonical snapshots are deterministic and versioned.
3. Local capture, inspection, and search work without a Habitat account.
4. Upload destinations remain adapters around the collector core.
5. Fixtures contain synthetic data only.
6. Configuration changes, hooks, services, and uploads require clear user intent.

Provider format changes require a sanitized fixture and an idempotence test.
Changes to the ingestion wire format must follow the
[protocol compatibility policy](protocol/README.md) and update its fixtures.

## Pull requests and licensing

Explain the user problem, approach, risks, privacy impact, and verification.
Call out configuration or protocol changes explicitly. Unless stated otherwise,
contributions intentionally submitted for inclusion are licensed under the MIT
License described in [LICENSE](LICENSE).
