# Habitat CLI (`ht`)

`ht` installs Habitat's Claude Code and Codex ingestion pipeline. It discovers local
projects, backfills selected history, and keeps future sessions synchronized with
[Habitat](https://app.use-habitat.com/).

## Install and set up

Install the latest checksum-verified release:

```sh
curl -fsSL https://app.use-habitat.com/install.sh | sh
ht setup
```

When no writable directory is already on `PATH`, the installer adds Habitat to the active shell
profile and tells the user to open a new terminal (or prints the one command that works now).

The installer resolves the latest version from Habitat's public download endpoint. To pin a
specific published version instead, place `HT_VERSION` on the shell side of the pipeline:

```sh
curl -fsSL https://app.use-habitat.com/install.sh | HT_VERSION=v0.4.0 sh
```

If another program already owns the `ht` command, the installer names it and prints the
full path to Habitat's launcher. Put `~/.ht/bin` first in `PATH`, or run
`~/.ht/bin/ht setup` directly.

`ht setup` is the normal entry point. It:

1. logs in to a Habitat workspace;
2. scans local Codex and Claude history, then fuzzy-searches saved project folders
   with a historical session count beside each one; zero-history folders remain
   selectable after confirming that setup should watch for future sessions;
3. lets the user choose a 30-day, 90-day, or all-time backfill;
4. watches each selected folder for both Codex and Claude sessions, including
   sessions started in subdirectories;
5. installs both provider hooks and schedules the selected history for background
   scanning and upload;
6. exits without launching Codex or Claude, printing the manual Codex hook trust
   and verification instructions in the final summary.

Setup is idempotent. It is safe to run again after an interruption.
Its durable output is one user-facing configuration file at
`~/.config/ht/ht.config.json` (or `HT_CONFIG`). The background daemon reads that
file on startup and reconciles changes whenever it is rewritten. Credentials
and ingestion checkpoints remain in Keychain/`~/.ht` as secret runtime state.

```sh
ht setup
ht configure
ht logout
ht update
ht status
ht uninstall
```

`ht configure` changes project routing for an existing workspace. Running `ht setup`
again can add another workspace. One project routes to one workspace, while delivery and
retry state remain isolated per workspace. A selected Git project automatically includes
all of its active worktrees, including worktrees created after configuration.

`ht logout` removes the active workspace's local credential, configuration, and project
routes. Use `ht logout --workspace ID` for another saved workspace or `ht logout --all`
to remove every saved workspace login.

`ht update` checks Habitat's release endpoint, verifies the platform binary against the
published checksum, replaces the managed binary, and restarts the background service when
it is active. It preserves configuration, credentials, hooks, and local session data:

```sh
ht update
ht update --to v0.4.0
ht update --force
```

## Automation and assistant-driven setup

Claude Cowork, Codex, an IT installer, or another permissioned harness can use the same
flow without terminal prompts:

```sh
printf '%s' "$HABITAT_API_KEY" | ht setup \
  --api-key-stdin \
  --project /path/to/project \
  --backfill 30d \
  --non-interactive \
  --json
```

An assistant should ask before changing provider hooks, installing a background service,
opening browser login, or uploading session content. `ht preflight --json` is a read-only
way to inspect the machine first.

JSON reports `ready: false` plus an `actionsRequired` entry until HT observes a
real Codex hook. The assistant should leave the trust decision to the user; it
must not bypass or answer it. Setup installs the hook but never launches Codex or
Claude. The final summary tells the user to review the installed hook with
`/hooks`, trust it, and complete one turn.

Self-hosted and local Habitat deployments can set the endpoints explicitly:

```sh
ht setup \
  --api-url http://127.0.0.1:4319 \
  --app-url http://localhost:3000
```

## Ingestion model

Provider transcripts are read-only. Canonical local session data and the durable ingestion
ledger live in `~/.ht/sessions.sqlite`.

- A stop hook writes the exact provider and transcript path to SQLite before it wakes the
  daemon. If the daemon is unavailable, the request remains queued.
- The daemon processes that transcript only. It does not re-scan every session and has no
  periodic source scan.
- The daemon watches the unified `ht.config.json`; changing project routes,
  workspaces, backfill settings, or upload filters triggers reconciliation.
- The first upload for a logical source is a baseline. Later uploads contain only new
  events and an ordered byte cursor.
- A transcript moved to the Codex archive retains its logical source identity. Its final
  state is captured once and later archive scans skip it.
- Immutable batches have stable IDs. Per-workspace delivery rows record acknowledgement,
  bounded exponential backoff, and quarantine state.
- Upload retries may run on a timer, but retry work never triggers a provider-wide source
  scan.

Manual reconciliation remains available:

```sh
ht sync
ht backfill
```

`ht sync` explicitly scans provider histories. Normal live ingestion is hook-targeted.
Setup schedules one full reconciliation in the daemon and returns immediately;
historical uploads continue in the background.

## Inspect and recover

```sh
ht status
ht doctor
ht backfill --retry-quarantined
```

`ht status` is a concise human-readable health view. It reports each selected harness's
hook state, daemon reachability, the last cycle, selected projects, local sessions, queue
health, and per-workspace delivery.
Use `ht status --verbose` for local paths, destination IDs, and recent delivery errors, or
`ht status --json` for automation. `ht doctor` remains the full diagnostic report.

Background upload failures and quarantined payloads are reported by `ht status`
and `ht doctor`.

Uninstall removes Habitat's provider hooks, background service, launcher, and installed
binary. Local SQLite data and credentials are preserved so reinstalling is recoverable:

```sh
ht uninstall
```

## Build from source

HT requires Bun 1.3 or newer.

```sh
git clone https://github.com/use-habitat/ht.git
cd ht
bun install
bun run check
bun run build
./dist/ht --help
```

The release workflow builds standalone macOS and Linux binaries for Arm and x64. The install
script verifies each binary against the published `SHA256SUMS` file before activation.

## Provider locations

- Codex: `CODEX_HOME`, then `~/.codex`
- Claude Code: `CLAUDE_CONFIG_DIR`, then `~/.claude`

Overrides:

- `HT_HOME`: Habitat state directory (default `~/.ht`)
- `HT_CODEX_HOME`: Codex data directory
- `HT_CLAUDE_HOME`: Claude Code data directory
- `HABITAT_API_URL`: Habitat API
- `HABITAT_APP_URL`: Habitat web app
- `HABITAT_API_KEY`: non-interactive workspace credential
