# Privacy and data flow

Agent sessions can contain source code, credentials, personal information,
internal URLs, prompts, and file paths. Treat the HT state directory and any
exported data as sensitive.

## Local-only flow

1. HT reads Codex and Claude Code transcript files without modifying them.
2. Provider-specific events are normalized and redacted.
3. Canonical sessions, ingestion state, and local search data are stored in
   `~/.ht/sessions.sqlite` by default.
4. `ht sessions list`, `ht sessions show`, and `ht sessions search` operate on
   this local database without requiring a Habitat account.

Run `ht install` to enable local hooks and the background collector without
connecting a cloud workspace. Run `ht uninstall` to remove hooks and the
service; local data is preserved for recovery unless the user removes it.

## Optional Habitat Cloud flow

`ht setup` connects a Habitat workspace, asks which projects to monitor and how
much history to backfill, then enables delivery. Each workspace has independent
credentials and delivery state. Project routes and upload policy remain in the
user-controlled configuration.

HT sends normalized events and session metadata—not the original transcript
file—to the configured API. Redaction runs before local storage and delivery,
but cannot guarantee that every sensitive value is removed.

## Operator checklist

- Review selected projects and backfill range before enabling upload.
- Use `ht preflight`, `ht config check`, and `ht status --verbose` to inspect
  behavior and destinations.
- Keep credentials out of shell history by using browser login or
  `--api-key-stdin`.
- Sanitize diagnostics before sharing them publicly.
- Use a dedicated `HT_HOME` when testing changes or untrusted fixtures.
