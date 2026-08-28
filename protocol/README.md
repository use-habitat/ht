# Habitat ingestion protocol

This directory is the public compatibility boundary between `ht` and any
Habitat-compatible ingestion API.

- [`ingest-v2.schema.json`](ingest-v2.schema.json) describes the structural
  JSON wire format.
- [`manifest.json`](manifest.json) identifies the current contract and lists
  canonical positive and negative fixtures.
- [`fixtures`](fixtures) contains synthetic data only. Hosted and self-hosted
  servers should validate every fixture against their runtime parser.

The runtime validator in [`src/ingest-batch.ts`](../src/ingest-batch.ts) also
enforces cross-field invariants that JSON Schema cannot express portably. Run
`bun run protocol:check` to ensure the generated schema is current.

## Compatibility policy

Before 1.0, HT may add optional fields in a minor release. Removing a field,
changing its meaning, or tightening accepted values requires a new
`schemaVersion`. Habitat Cloud supports the current public schema and at least
the previous released schema during upgrades.

Changes to this directory require:

1. updated valid and invalid fixtures;
2. a passing public runtime validation test;
3. a passing private API compatibility test; and
4. a changelog entry describing producer and consumer impact.
