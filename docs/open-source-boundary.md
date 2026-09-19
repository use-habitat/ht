# Open-source boundary

The public HT repository contains the complete local CLI experience:

- command-line UX and automation-friendly JSON output;
- Codex and Claude Code discovery and normalization;
- local SQLite storage, search, diagnostics, and recovery;
- redaction, configuration, hooks, and background service lifecycle;
- installer, updater, release build, and checksum verification;
- the public ingestion wire contract and Habitat exporter.

The hosted Habitat product remains separate and private:

- workspace authentication and authorization;
- hosted ingestion, PostgreSQL persistence, and migrations;
- the web application, team collaboration, billing, and operations;
- hosted summaries, search infrastructure, and production deployment.

The boundary is the versioned contract in [`protocol`](../protocol). HT must
remain useful locally without a hosted account. Habitat Cloud is an optional
compatible destination rather than a runtime dependency of capture, storage,
inspection, or search.
