# Maintainer launch checklist

Repository files cannot enforce organization ownership, GitHub security
settings, DNS, or incident access. A repository administrator must complete and
record this checklist before the public launch announcement.

## Ownership and continuity

- [ ] At least two active maintainers can administer `use-habitat/ht` and ship
  an emergency release.
- [ ] Departed maintainers' organization, repository, Actions, DNS, package,
  and production access has been reviewed.
- [ ] Release credentials, domain ownership, and recovery methods are stored in
  the company password manager—not a founder's personal account.
- [ ] Company counsel has confirmed the inbound ownership of the initial code
  and the intended MIT outbound license.

## GitHub settings

- [ ] Give current maintainers admin access and make the `CODEOWNERS` entries
  valid repository owners.
- [ ] Protect `main` with a ruleset requiring pull requests, one approval,
  code-owner review for owned paths, conversation resolution, and current CI,
  CodeQL, Trivy, and dependency-review checks.
- [ ] Prevent force pushes and branch deletion on `main`.
- [ ] Protect `v*` tags from modification or deletion outside the release role.
- [ ] Enable private vulnerability reporting, Dependabot alerts and security
  updates, secret scanning, and push protection.
- [ ] Keep default workflow permissions read-only and allow write permissions
  only where a checked-in workflow declares them.
- [ ] Add a second release environment reviewer if GitHub plan support allows it.

## Repository presentation

- [ ] Set the homepage to `https://use-habitat.com/`.
- [ ] Add topics such as `cli`, `observability`, `codex`, `claude-code`,
  `local-first`, `sqlite`, and `developer-tools`.
- [ ] Add a social preview using approved Habitat brand assets.
- [ ] Decide whether Discussions has an active owner before enabling it.
- [ ] Seed a small set of scoped `good first issue` and `help wanted` items.

## Product boundary and operations

- [ ] Merge and deploy the private API compatibility check before publishing a
  public producer change.
- [ ] Configure a branded API hostname, validate every CLI route, then change
  the default only in a tested release. Do not point users at an unresolved DNS
  name.
- [ ] Verify the stable installer proxy, all four platform downloads,
  `SHA256SUMS`, CycloneDX SBOM, and GitHub provenance attestations.
- [ ] Test install, setup, local capture, local search, cloud delivery, update,
  logout, and uninstall on a clean macOS and Linux machine.
- [ ] Confirm someone owns vulnerability response and issue triage during the
  launch week.

Verify a downloaded release after the hardened workflow ships:

```sh
gh attestation verify ./ht-darwin-arm64 --repo use-habitat/ht
shasum -a 256 -c SHA256SUMS
```
