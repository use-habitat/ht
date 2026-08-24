import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import {
  appendFile,
  cp,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  writeFile
} from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { Writable } from "node:stream"

import { SessionCollector } from "../src/collector.ts"
import { runCli } from "../src/cli.ts"
import {
  codexReviewCommand,
  resolveCodexExecutable,
  runCodexReview
} from "../src/codex-review.ts"
import {
  expandGitWorktreeRoutes,
  htPaths,
  projectWorkspaceId,
  readConfig,
  resolveHabitatExports,
  saveHabitatLogin,
  saveSetupSelection,
  writeConfig,
  type HTConfig
} from "../src/config.ts"
import {
  daemonVersionMatches,
  drainBackfillRequests,
  drainPipelineRequests,
  requestDaemonSync,
  watchConfig,
  wakeDaemon
} from "../src/daemon.ts"
import { SnapshotExportError } from "../src/exporter.ts"
import { HabitatExporter } from "../src/exporters/habitat.ts"
import { loginWithBrowser } from "../src/habitat-login.ts"
import { installHT, uninstallHT } from "../src/install.ts"
import {
  baselineBatch,
  chunkIngestBatch,
  type IngestBatch
} from "../src/ingest-batch.ts"
import { parseJsonlSource } from "../src/jsonl.ts"
import { normalizeSnapshot } from "../src/normalize.ts"
import { IncrementalPipelineStore } from "../src/pipeline-store.ts"
import { discoverCodexSources } from "../src/providers/codex.ts"
import {
  discoverGitWorktrees,
  discoverProjectSuggestions,
  rankProjectSuggestions
} from "../src/project-suggestions.ts"
import { redactValue } from "../src/redaction.ts"
import { LocalSessionStore } from "../src/store.ts"
import {
  emptyProjectSelectionWarning,
  projectCandidates,
  projectPickerOptions,
  projectSessionCount,
  runSetup
} from "../src/setup.ts"
import {
  readProjectUploadPolicy,
  userConfigPath
} from "../src/user-config.ts"
import { cliVersion } from "../src/version.ts"

const fixtures = resolve(import.meta.dir, "fixtures")
const codexFixtures = join(fixtures, "codex")
const claudeFixtures = join(fixtures, "claude")
const temporary: string[] = []

const captureCliOutput = () => {
  let stdout = ""
  let stderr = ""
  const stream = (append: (value: string) => void): Writable => new Writable({
    write(chunk, _encoding, callback) {
      append(String(chunk))
      callback()
    }
  })
  return {
    stdout: stream((value) => { stdout += value }),
    stderr: stream((value) => { stderr += value }),
    readStdout: (): string => stdout,
    readStderr: (): string => stderr,
    takeStdout: (): string => {
      const value = stdout
      stdout = ""
      return value
    },
    clear: (): void => {
      stdout = ""
      stderr = ""
    }
  }
}

afterEach(async () => {
  await Promise.all(temporary.splice(0).map((path) => rm(path, { recursive: true, force: true })))
})

describe("ht collector", () => {
  test("adds delivery priority before creating its index on an existing outbox", () => {
    const database = new Database(":memory:")
    database.exec(`
      CREATE TABLE pipeline_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        destination_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        delivered_at TEXT,
        quarantined_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(destination_id, batch_id)
      );
      INSERT INTO pipeline_deliveries (
        destination_id, source_id, batch_id, created_at, updated_at
      ) VALUES ('habitat:test', 'source:test', 'batch:test', 'now', 'now');
    `)

    try {
      new IncrementalPipelineStore(database)
      const columns = database.query<{ name: string }, []>(
        "PRAGMA table_info(pipeline_deliveries)"
      ).all()
      const delivery = database.query<{ priority: number }, []>(`
        SELECT priority FROM pipeline_deliveries LIMIT 1
      `).get()
      const liveIndex = database.query<{ name: string }, []>(`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND name = 'pipeline_deliveries_live_idx'
      `).get()

      expect(columns.some((column) => column.name === "priority")).toBe(true)
      expect(delivery?.priority).toBe(0)
      expect(liveIndex?.name).toBe("pipeline_deliveries_live_idx")
    } finally {
      database.close()
    }
  })

  test("waits through brief SQLite writer contention", () => {
    const store = new LocalSessionStore(":memory:")
    try {
      const timeout = store.database.query<{ timeout: number }, []>(
        "PRAGMA busy_timeout"
      ).get()
      expect(timeout?.timeout).toBe(30_000)
    } finally {
      store.close()
    }
  })

  test("splits a large archived baseline into ordered cursor chunks", async () => {
    const source = (await discoverCodexSources(codexFixtures))[0]!
    const parsed = await parseJsonlSource(source)
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-chunked-backfill",
      generation: 1,
      source,
      records: parsed.records
    })!
    const template = snapshot.events[0]!
    const large = {
      ...snapshot,
      source: { ...snapshot.source, sourceSize: 1_000_000 },
      session: { ...snapshot.session, status: "archived" as const },
      events: Array.from({ length: 451 }, (_, sequence) => ({
        ...template,
        id: `${template.id}-large-${sequence}`,
        sequence,
        text: `event ${sequence}: ${"x".repeat(2_000)}`
      }))
    }

    const batches = chunkIngestBatch(baselineBatch(large), { maxEvents: 100 })

    expect(batches).toHaveLength(5)
    expect(batches[0]).toMatchObject({ mode: "baseline", cursor: { from: 0 } })
    expect(batches.at(-1)).toMatchObject({
      mode: "final",
      cursor: { to: large.source.sourceSize }
    })
    expect(batches.every((batch) => batch.events.length <= 100)).toBe(true)
    expect(batches.flatMap((batch) => batch.events).map((event) => event.id))
      .toEqual(large.events.map((event) => event.id))
    for (let index = 1; index < batches.length; index += 1) {
      expect(batches[index]!.cursor.from).toBe(batches[index - 1]!.cursor.to)
    }
  })

  test("prioritizes hook-triggered delivery and reports a jittered retry", async () => {
    const [historicalSource, liveSource] = await discoverCodexSources(codexFixtures)
    expect(historicalSource).toBeDefined()
    expect(liveSource).toBeDefined()
    const historicalParsed = await parseJsonlSource(historicalSource!)
    const liveParsed = await parseJsonlSource(liveSource!)
    const historical = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-live-priority",
      generation: 1,
      source: historicalSource!,
      records: historicalParsed.records
    })!
    const live = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-live-priority",
      generation: 1,
      source: liveSource!,
      records: liveParsed.records
    })!
    const store = new LocalSessionStore(":memory:")
    const pipeline = new IncrementalPipelineStore(store.database)
    try {
      pipeline.capture({
        provider: "codex",
        source: historicalSource!,
        snapshot: historical,
        projectOrigin: "/work/historical",
        cursorTo: historicalParsed.nextOffset,
        destinationIds: ["habitat:test"],
        priority: "backfill"
      })
      pipeline.capture({
        provider: "codex",
        source: liveSource!,
        snapshot: live,
        projectOrigin: "/work/live",
        cursorTo: liveParsed.nextOffset,
        destinationIds: ["habitat:test"],
        priority: "live"
      })

      const due = pipeline.due("habitat:test", 2)
      expect(due.map((delivery) => delivery.sourceId)).toEqual([
        live.source.id,
        historical.source.id
      ])

      pipeline.fail(due[0]!, "HTTP 503", true)
      const stats = pipeline.stats("habitat:test")
      expect(stats).toMatchObject({ pending: 2, retrying: 1 })
      expect(stats.nextRetryAt).not.toBeNull()
      expect(new Date(stats.nextRetryAt!).getTime()).toBeGreaterThan(Date.now())
    } finally {
      store.close()
    }
  })

  test("upgrades an interrupted monolithic backfill before it is retried", async () => {
    const source = (await discoverCodexSources(codexFixtures))[0]!
    const parsed = await parseJsonlSource(source)
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-rechunk-backfill",
      generation: 1,
      source,
      records: parsed.records
    })!
    const template = snapshot.events[0]!
    const large = {
      ...snapshot,
      source: { ...snapshot.source, sourceSize: 1_000_000 },
      session: { ...snapshot.session, status: "archived" as const },
      events: Array.from({ length: 401 }, (_, sequence) => ({
        ...template,
        id: `${template.id}-retry-${sequence}`,
        sequence,
        text: "x".repeat(2_000)
      }))
    }
    const store = new LocalSessionStore(":memory:")
    const { database } = store
    const pipeline = new IncrementalPipelineStore(database)
    try {
      pipeline.capture({
        provider: "codex",
        source: { ...source, classification: "archived", size: 1_000_000 },
        snapshot: large,
        projectOrigin: "/work/habitat",
        cursorTo: large.source.sourceSize
      })
      const oldBatch = baselineBatch(large)
      const now = new Date().toISOString()
      database.query(`
        INSERT INTO pipeline_batches (
          batch_id, source_id, epoch, mode, cursor_from, cursor_to, payload, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        oldBatch.batchId,
        oldBatch.source.id,
        oldBatch.source.epoch,
        oldBatch.mode,
        oldBatch.cursor.from,
        oldBatch.cursor.to,
        JSON.stringify(oldBatch),
        now
      )
      database.query(`
        INSERT INTO pipeline_deliveries (
          destination_id, source_id, batch_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
      `).run("habitat:test", oldBatch.source.id, oldBatch.batchId, now, now)

      expect(pipeline.prepareDestination("habitat:test")).toBeGreaterThan(1)
      expect(pipeline.stats("habitat:test").pending).toBeGreaterThan(1)
      expect(pipeline.source(large.source.id)?.epoch).toBe(2)
      expect(pipeline.due("habitat:test", 1)[0]?.batch).toMatchObject({
        mode: "baseline",
        cursor: { from: 0 },
        source: { epoch: 2 }
      })
    } finally {
      store.close()
    }
  })

  test("runs Codex hook review in the current terminal until verified", async () => {
    let command: readonly string[] = []
    const result = await runCodexReview({
      projectOrigin: "/work/project with spaces",
      platform: "darwin",
      isVerified: () => true,
      resolveExecutable: () => "/Applications/ChatGPT.app/Contents/Resources/codex",
      runInteractive: async (input) => {
        command = input.command
        return { opened: true, verified: input.isVerified() }
      }
    })

    expect(result).toEqual({
      opened: true,
      verified: true,
      reason: "verified",
      executable: "/Applications/ChatGPT.app/Contents/Resources/codex"
    })
    expect(command).toEqual([
      "/Applications/ChatGPT.app/Contents/Resources/codex",
      "-C",
      "/work/project with spaces",
      "Reply exactly with this sentence: Habitat hook verification complete. When /exit appears, press Enter again to return to Habitat."
    ])
  })

  test("passes Codex review arguments without shell interpolation", () => {
    expect(codexReviewCommand(
      "/Applications/Codex's App/codex",
      "/work/it's here"
    )).toEqual([
      "/Applications/Codex's App/codex",
      "-C",
      "/work/it's here",
      "Reply exactly with this sentence: Habitat hook verification complete. When /exit appears, press Enter again to return to Habitat."
    ])
  })

  test("runs the review command through a real pseudo-terminal", async () => {
    const result = await runCodexReview({
      projectOrigin: "/work/project",
      platform: "darwin",
      isVerified: () => true,
      resolveExecutable: () => "/usr/bin/true"
    })
    expect(result).toMatchObject({
      opened: true,
      verified: true,
      reason: "verified",
      executable: "/usr/bin/true"
    })
  })

  test("finds bundled Codex and falls back cleanly without a PTY", async () => {
    expect(resolveCodexExecutable({
      which: () => null,
      exists: (path) =>
        path === "/Applications/ChatGPT.app/Contents/Resources/codex",
      platform: "darwin",
      home: "/Users/test",
      environmentCandidate: ""
    })).toBe("/Applications/ChatGPT.app/Contents/Resources/codex")

    expect(await runCodexReview({
      projectOrigin: "/work/project",
      platform: "win32",
      isVerified: () => false
    })).toEqual({
      opened: false,
      verified: false,
      reason: "unsupported-platform",
      executable: null
    })
  })

  test("links a browser-approved Habitat account to a device API key", async () => {
    const requests: Array<{ url: string; body: unknown }> = []
    const responses = [
      Response.json({
        data: {
          deviceCode: "device-code-that-is-at-least-thirty-two-characters",
          userCode: "H7KM-9QPX",
          expiresIn: 600,
          interval: 1
        }
      }, { status: 201 }),
      Response.json({ data: { status: "pending" } }, { status: 202 }),
      Response.json({
        data: {
          status: "authorized",
          apiKey: "hab_test_device_api_key",
          workspace: { id: "workspace-test", slug: "test", name: "Test Workspace" },
          principal: { id: "principal-device", kind: "device", name: "HT on Test Mac" }
        }
      })
    ]
    let verification: { url: string; userCode: string; browserOpened: boolean } | undefined

    const result = await loginWithBrowser({
      apiUrl: "https://api.example.test",
      appUrl: "https://app.example.test",
      deviceName: "Test Mac",
      fetch: (async (input, init) => {
        requests.push({
          url: String(input),
          body: init?.body ? JSON.parse(String(init.body)) as unknown : null
        })
        return responses.shift()!
      }) as typeof fetch,
      delay: async () => {},
      openBrowser: async () => false,
      onVerification: (value) => {
        verification = value
      }
    })

    expect(requests).toHaveLength(3)
    expect(requests[0]).toEqual({
      url: "https://api.example.test/v1/cli/login",
      body: { deviceName: "Test Mac" }
    })
    expect(requests[1]?.body).toEqual({
      deviceCode: "device-code-that-is-at-least-thirty-two-characters"
    })
    expect(verification).toEqual({
      url: "https://app.example.test/cli/authorize?code=H7KM-9QPX",
      userCode: "H7KM-9QPX",
      browserOpened: false
    })
    expect(result).toEqual({
      apiKey: "hab_test_device_api_key",
      workspace: { id: "workspace-test", slug: "test", name: "Test Workspace" },
      principal: { id: "principal-device", kind: "device", name: "HT on Test Mac" }
    })
  })

  test("captures canonical sessions locally without an exporter", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-local-"))
    temporary.push(directory)
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: codexFixtures,
      claudeHome: claudeFixtures
    })
    try {
      const result = await collector.runOnce()
      expect(result.scan).toMatchObject({ scanned: 6, captured: 4, ignored: 2 })
      expect(result.flush).toEqual({
        exportEnabled: false,
        exporter: null,
        destinationId: null,
        attempted: 0,
        exported: 0,
        withheld: 0,
        failed: 0,
        quarantined: 0,
        remaining: 4
      })
      expect(collector.store.snapshots()).toHaveLength(4)
      expect(collector.store.stats()).toEqual({
        pending: 0,
        sources: 4,
        sessions: 4,
        failed: 0,
        quarantined: 0,
        delivered: 0
      })
      expect(collector.pipeline.projectOriginCounts()).toEqual([{
        origin: "/work/habitat",
        sessions: 4
      }])
    } finally {
      collector.close()
    }
  })

  test("uploads one baseline and then only new events for an appended transcript", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-incremental-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    await cp(codexFixtures, codexHome, { recursive: true })
    const batches: IngestBatch[] = []
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome,
      claudeHome: join(directory, "missing-claude"),
      exporter: {
        kind: "test",
        destinationId: "test:incremental",
        exportSnapshot: async () => {
          throw new Error("Incremental exporter should receive a batch.")
        },
        exportBatch: async (batch) => {
          batches.push(batch)
        }
      }
    })
    try {
      const first = await collector.runOnce("codex")
      expect(first.scan).toMatchObject({ captured: 2, ignored: 1 })
      expect(batches).toHaveLength(2)
      expect(batches.every((batch) => batch.mode === "baseline")).toBe(true)

      const path = join(
        codexHome,
        "2026-07-14-session-11111111-1111-1111-1111-111111111111.jsonl"
      )
      await appendFile(path, `${JSON.stringify({
        timestamp: "2026-07-14T10:00:07.000Z",
        type: "response_item",
        payload: {
          type: "message",
          id: "assistant-incremental",
          role: "assistant",
          content: [{ type: "output_text", text: "Only upload this new event." }]
        }
      })}\n`)
      const beforeDelta = batches.length
      const second = await collector.runOnce("codex")
      expect(second.scan).toMatchObject({ captured: 1 })
      expect(batches).toHaveLength(beforeDelta + 1)
      const delta = batches.at(-1)!
      expect(delta.mode).toBe("delta")
      expect(delta.cursor.from).toBeGreaterThan(0)
      expect(delta.cursor.to).toBeGreaterThan(delta.cursor.from)
      expect(delta.events).toHaveLength(1)
      expect(delta.events[0]?.text).toBe("Only upload this new event.")

      const third = await collector.runOnce("codex")
      expect(third.scan.captured).toBe(0)
      expect(batches).toHaveLength(beforeDelta + 1)
    } finally {
      collector.close()
    }
  })

  test("finalizes an archived transcript once and skips future archive scans", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-archived-once-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const active = join(codexHome, "sessions")
    const archived = join(codexHome, "archived_sessions")
    await Promise.all([
      mkdir(active, { recursive: true }),
      mkdir(archived, { recursive: true })
    ])
    const filename = "2026-07-14-session-11111111-1111-1111-1111-111111111111.jsonl"
    await cp(
      join(codexFixtures, filename),
      join(active, filename)
    )
    const batches: IngestBatch[] = []
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome,
      claudeHome: join(directory, "missing-claude"),
      exporter: {
        kind: "test",
        destinationId: "test:archive",
        exportSnapshot: async () => {},
        exportBatch: async (batch) => {
          batches.push(batch)
        }
      }
    })
    try {
      const first = await collector.runOnce("codex")
      expect(first.scan).toMatchObject({ scanned: 1, captured: 1 })
      expect(batches[0]?.mode).toBe("baseline")
      const sourceId = batches[0]?.source.id

      await rename(join(active, filename), join(archived, filename))
      const archivedCycle = await collector.runOnce("codex")
      expect(archivedCycle.scan).toMatchObject({ scanned: 1, captured: 1 })
      expect(batches.at(-1)).toMatchObject({
        mode: "final",
        source: { id: sourceId, classification: "archived" },
        session: { status: "archived" }
      })
      expect(collector.pipeline.stats().finalizedSources).toBe(1)
      const second = await collector.scan("codex")
      expect(second).toMatchObject({ scanned: 1, unchanged: 1, captured: 0 })
    } finally {
      collector.close()
    }
  })

  test("follows a Codex hook transcript when Codex moves it to the archive", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-archived-hook-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const active = join(codexHome, "sessions")
    const archived = join(codexHome, "archived_sessions")
    await Promise.all([
      mkdir(active, { recursive: true }),
      mkdir(archived, { recursive: true })
    ])
    const filename = "2026-07-14-session-11111111-1111-1111-1111-111111111111.jsonl"
    const activePath = join(active, filename)
    await cp(join(codexFixtures, filename), activePath)
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome
    })
    try {
      expect(await collector.processPath("codex", activePath)).toMatchObject({
        captured: true
      })
      await rename(activePath, join(archived, filename))
      expect(await collector.processPath("codex", activePath)).toMatchObject({
        captured: true
      })
      expect(collector.pipeline.stats().finalizedSources).toBe(1)
    } finally {
      collector.close()
    }
  })

  test("finalizes an active Codex transcript on SessionEnd", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-codex-session-end-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const active = join(codexHome, "sessions")
    await mkdir(active, { recursive: true })
    const filename = "2026-07-14-session-11111111-1111-1111-1111-111111111111.jsonl"
    const transcriptPath = join(active, filename)
    await cp(join(codexFixtures, filename), transcriptPath)
    const batches: IngestBatch[] = []
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome,
      destinations: [{
        exporter: {
          kind: "test",
          destinationId: "test:session-end",
          exportSnapshot: async () => {},
          exportBatch: async (batch) => { batches.push(batch) }
        }
      }]
    })
    try {
      collector.pipeline.enqueueRequest({
        provider: "codex",
        transcriptPath,
        reason: "Stop"
      })
      await drainPipelineRequests(collector)
      await collector.flush()
      expect(batches.at(-1)).toMatchObject({
        mode: "baseline",
        session: { status: "active" }
      })

      collector.pipeline.enqueueRequest({
        provider: "codex",
        transcriptPath,
        reason: "SessionEnd"
      })
      await drainPipelineRequests(collector)
      await collector.flush()
      expect(batches.at(-1)).toMatchObject({
        mode: "final",
        source: { classification: "active" },
        session: { status: "completed" }
      })
      expect(collector.pipeline.stats().finalizedSources).toBe(1)
    } finally {
      collector.close()
    }
  })

  test("discards a hook request after its transcript is no longer available", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-missing-hook-"))
    temporary.push(directory)
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: join(directory, "codex")
    })
    try {
      collector.pipeline.enqueueRequest({
        provider: "codex",
        transcriptPath: join(directory, "missing.jsonl"),
        projectOrigin: "/work/habitat"
      })
      expect(await drainPipelineRequests(collector)).toEqual({
        attempted: 1,
        processed: 1,
        failed: 0
      })
      expect(collector.pipeline.dueRequests()).toEqual([])
    } finally {
      collector.close()
    }
  })

  test("persists a targeted hook request even when the daemon is unavailable", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-hook-durable-"))
    temporary.push(directory)
    const transcriptPath = join(
      codexFixtures,
      "2026-07-14-session-11111111-1111-1111-1111-111111111111.jsonl"
    )
    const child = Bun.spawn([
      process.execPath,
      resolve(import.meta.dir, "..", "src", "entry.ts"),
      "hook",
      "codex"
    ], {
      env: { ...process.env, HT_HOME: directory, HT_DAEMON_PORT: "65530" },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe"
    })
    child.stdin.write(JSON.stringify({
      session_id: "11111111-1111-1111-1111-111111111111",
      transcript_path: transcriptPath,
      cwd: "/work/habitat",
      hook_event_name: "Stop"
    }))
    child.stdin.end()
    expect(await child.exited).toBe(0)

    const store = new LocalSessionStore(htPaths(directory).database)
    try {
      const pipeline = new IncrementalPipelineStore(store.database)
      const requests = pipeline.dueRequests()
      expect(requests).toEqual([expect.objectContaining({
        provider: "codex",
        transcriptPath,
        nativeSessionId: "11111111-1111-1111-1111-111111111111",
        projectOrigin: "/work/habitat",
        reason: "Stop"
      })])
      expect(pipeline.harnesses()).toEqual([expect.objectContaining({
        provider: "codex",
        selected: false,
        reviewedAt: expect.any(String),
        lastHookReceivedAt: expect.any(String),
        lastEvent: "Stop"
      })])
    } finally {
      store.close()
    }
  })

  test("persists and completes a backfill after a delayed daemon start", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-durable-backfill-"))
    temporary.push(directory)
    const destinationId = "test:durable-backfill"
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      destinations: [{
        exporter: {
          kind: "test",
          destinationId,
          exportSnapshot: async () => {}
        }
      }],
      autoPrepareDestinations: false
    })
    try {
      collector.pipeline.enqueueBackfill({ destinationId, reason: "setup" })
      expect(collector.pipeline.stats()).toMatchObject({ backfills: 1, pending: 0 })
      expect(collector.pipeline.backfillStatus(destinationId)).toMatchObject({
        state: "pending",
        pending: 1,
        completedAt: null
      })

      const result = await drainBackfillRequests(
        collector,
        collector.options.destinations ?? []
      )

      expect(result.backfills).toEqual({ attempted: 1, processed: 1, failed: 0 })
      expect(result.scan).toMatchObject({ captured: 4 })
      expect(collector.pipeline.stats(destinationId)).toMatchObject({
        backfills: 0,
        pending: 4
      })
      expect(collector.pipeline.backfillStatus(destinationId)).toMatchObject({
        state: "complete",
        pending: 0
      })
    } finally {
      collector.close()
    }
  })

  test("keeps a durable backfill queued when its destination is unavailable", async () => {
    const collector = new SessionCollector({
      statePath: ":memory:",
      codexHome: join(fixtures, "missing-codex"),
      claudeHome: join(fixtures, "missing-claude"),
      autoPrepareDestinations: false
    })
    try {
      collector.pipeline.enqueueBackfill({
        destinationId: "test:temporarily-unavailable",
        reason: "setup"
      })
      const result = await drainBackfillRequests(collector, [])
      expect(result.backfills).toEqual({ attempted: 1, processed: 0, failed: 1 })
      expect(collector.pipeline.stats()).toMatchObject({ backfills: 1 })
      expect(
        collector.pipeline.backfillStatus("test:temporarily-unavailable")
      ).toMatchObject({
        state: "retrying",
        pending: 1,
        retrying: 1,
        lastError: expect.stringContaining("is unavailable")
      })
    } finally {
      collector.close()
    }
  })

  test("suggests existing project folders saved by Codex Desktop and Claude", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-project-suggestions-"))
    temporary.push(directory)
    const codexHome = join(directory, ".codex")
    const claudeHome = join(directory, ".claude")
    const codexProject = join(directory, "projects", "codex-project")
    const claudeProject = join(directory, "projects", "claude-project")
    const sharedProject = join(directory, "projects", "shared-project")
    const missingProject = join(directory, "projects", "missing-project")
    const codexWorktree = join(codexHome, "worktrees", "abcd", "codex-project")
    await Promise.all([
      mkdir(codexHome, { recursive: true }),
      mkdir(claudeHome, { recursive: true }),
      mkdir(codexProject, { recursive: true }),
      mkdir(claudeProject, { recursive: true }),
      mkdir(sharedProject, { recursive: true }),
      mkdir(codexWorktree, { recursive: true })
    ])
    await Promise.all([
      writeFile(join(codexHome, ".codex-global-state.json"), JSON.stringify({
        "local-projects": {
          codex: { name: "Codex Project", rootPaths: [codexProject] },
          worktree: { name: "Codex Project", rootPaths: [codexWorktree] },
          shared: { name: "Shared Project", rootPaths: [sharedProject] },
          missing: { name: "Missing Project", rootPaths: [missingProject] }
        }
      })),
      writeFile(join(directory, ".claude.json"), JSON.stringify({
        projects: {
          [claudeProject]: {},
          [codexWorktree]: {},
          [sharedProject]: {},
          [missingProject]: {}
        }
      }))
    ])

    expect(await discoverProjectSuggestions({ codexHome, claudeHome })).toEqual([
      {
        origin: claudeProject,
        name: "claude-project",
        sources: ["claude"]
      },
      {
        origin: codexProject,
        name: "Codex Project",
        sources: ["codex-desktop"]
      },
      {
        origin: sharedProject,
        name: "Shared Project",
        sources: ["claude", "codex-desktop"]
      }
    ])
  })

  test("shows history counts and warns before monitoring an empty folder", () => {
    const emptyCheckout = "/Users/test/Documents/GitHub/project-alpha"
    const activeCheckout = "/Users/test/dev/project-alpha"
    const worktreeRoot = "/Users/test/projects/worktree-project"
    const worktree = "/Users/test/.codex/worktrees/abcd/worktree-project"
    const origins = [
      { origin: activeCheckout, sessions: 89 },
      { origin: worktree, sessions: 3 }
    ]
    const projects = projectCandidates([
      {
        origin: emptyCheckout,
        name: "project-alpha",
        sources: ["claude"]
      },
      {
        origin: activeCheckout,
        name: "project-alpha",
        sources: ["claude"]
      },
      {
        origin: worktreeRoot,
        name: "worktree-project",
        sources: ["codex-desktop"]
      }
    ], [], undefined, origins, [{ origin: worktree, projectOrigin: worktreeRoot }])

    expect(projectSessionCount(emptyCheckout, origins)).toBe(0)
    expect(projectSessionCount(activeCheckout, origins)).toBe(89)
    expect(projectSessionCount("/Users/test/dev", origins)).toBe(89)
    expect(projects.slice(0, 2).map((project) => project.origin)).toEqual([
      activeCheckout,
      emptyCheckout
    ])

    const options = projectPickerOptions(projects)
    expect(options.find((option) => option.value === emptyCheckout)).toEqual({
      value: emptyCheckout,
      label: "project-alpha · 0 historical sessions",
      hint: `${emptyCheckout} · Claude`,
      disabled: false
    })
    expect(options.find((option) => option.value === activeCheckout)).toEqual({
      value: activeCheckout,
      label: "project-alpha · 89 historical sessions",
      hint: `${activeCheckout} · Claude`,
      disabled: false
    })
    expect(options.find((option) => option.value === worktreeRoot)).toEqual({
      value: worktreeRoot,
      label: "worktree-project · 3 historical sessions",
      hint: `${worktreeRoot} · Codex Desktop`,
      disabled: false
    })
    expect(emptyProjectSelectionWarning(
      [emptyCheckout],
      projects,
      origins
    )).toBe(
      `No historical sessions were found in ${emptyCheckout}. ` +
      `A same-named folder has 89 historical sessions: ${activeCheckout}. ` +
      "Habitat can still monitor new sessions there. " +
      "The initial backfill will upload nothing. Continue?"
    )
    expect(emptyProjectSelectionWarning(
      [activeCheckout],
      projects,
      origins
    )).toBeNull()
  })

  test("discovers Git worktrees associated with a selected core project", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-project-worktrees-"))
    temporary.push(directory)
    const codexHome = join(directory, ".codex")
    const project = join(directory, "projects", "habitat")
    const worktree = join(codexHome, "worktrees", "abcd", "habitat")
    await createGitWorktree(project, worktree)

    expect(await discoverGitWorktrees([project])).toEqual([{
      origin: await realpath(worktree),
      projectOrigin: project
    }])
  })

  test("ranks fuzzy project-folder matches by relevance", () => {
    const projects = [
      { origin: "/work/archive/habitat", name: "Old Habitat" },
      { origin: "/work/habitat-sessions", name: "Habitat Sessions" },
      { origin: "/work/habits", name: "Habits" },
      { origin: "/work/unrelated", name: "Unrelated" }
    ]

    expect(rankProjectSuggestions("habitat", projects).slice(0, 2)).toEqual([
      projects[1]!,
      projects[0]!
    ])
  })

  test("routes sessions in project subdirectories to the most specific folder", () => {
    const config: HTConfig = {
      schemaVersion: 2,
      daemonPort: 4322,
      activeWorkspaceId: "workspace-root",
      workspaces: [],
      backfillWindow: "30d",
      upload: { projectOrigins: {} },
      projects: [
        {
          origin: "/work/habitat",
          name: "Habitat",
          workspaceId: "workspace-root",
          providers: ["codex", "claude"],
          selectedAt: "2026-07-30T00:00:00.000Z"
        },
        {
          origin: "/work/habitat/apps/web",
          name: "Habitat Web",
          workspaceId: "workspace-web",
          providers: ["claude"],
          selectedAt: "2026-07-30T00:00:00.000Z"
        }
      ]
    }

    expect(projectWorkspaceId(config, "/work/habitat/packages/api", "codex"))
      .toBe("workspace-root")
    expect(projectWorkspaceId(config, "/work/habitat/apps/web/src", "claude"))
      .toBe("workspace-web")
    expect(projectWorkspaceId(config, "/work/habitat/apps/web/src", "codex"))
      .toBe("workspace-root")
    expect(projectWorkspaceId(config, "/work/habitat-old", "claude")).toBeNull()
  })

  test("routes Git worktrees created after project configuration", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-dynamic-worktrees-"))
    temporary.push(directory)
    const codexHome = join(directory, ".codex")
    const project = join(directory, "projects", "habitat")
    const firstWorktree = join(codexHome, "worktrees", "aaaa", "habitat")
    const laterWorktree = join(directory, "worktrees", "bbbb", "habitat")
    await createGitWorktree(project, firstWorktree)
    const config: HTConfig = {
      schemaVersion: 2,
      daemonPort: 4322,
      activeWorkspaceId: "workspace-habitat",
      workspaces: [],
      backfillWindow: "30d",
      upload: { projectOrigins: {} },
      projects: [{
        origin: project,
        name: "Habitat",
        workspaceId: "workspace-habitat",
        providers: ["codex", "claude"],
        selectedAt: "2026-08-11T00:00:00.000Z"
      }]
    }

    const before = await expandGitWorktreeRoutes(config, { codexHome })
    const firstWorktreePath = await realpath(firstWorktree)
    expect(projectWorkspaceId(before, join(firstWorktreePath, "apps", "web"), "codex"))
      .toBe("workspace-habitat")
    expect(projectWorkspaceId(before, laterWorktree, "codex")).toBeNull()

    await mkdir(dirname(laterWorktree), { recursive: true })
    await runGit([
      "-C", project, "worktree", "add", "-q", "--detach", laterWorktree
    ])
    const after = await expandGitWorktreeRoutes(config, { codexHome })
    const laterWorktreePath = await realpath(laterWorktree)
    expect(projectWorkspaceId(after, join(laterWorktreePath, "apps", "web"), "codex"))
      .toBe("workspace-habitat")
    expect(projectWorkspaceId(after, laterWorktreePath, "claude"))
      .toBe("workspace-habitat")
    expect(projectWorkspaceId(
      after,
      join(directory, "unrelated", "habitat"),
      "codex"
    )).toBeNull()
    expect(config.projects).toHaveLength(1)
  })

  test("runs setup non-interactively and is safe to repeat", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-setup-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const claudeHome = join(directory, "claude")
    await Promise.all([
      cp(codexFixtures, codexHome, { recursive: true }),
      cp(claudeFixtures, claudeHome, { recursive: true })
    ])
    const projectOrigin = join(directory, "projects", "habitat")
    const worktreeOrigin = join(codexHome, "worktrees", "abcd", "habitat")
    await createGitWorktree(projectOrigin, worktreeOrigin)
    const previousHome = process.env.HT_HOME
    const previousKeychain = process.env.HT_DISABLE_KEYCHAIN
    process.env.HT_HOME = join(directory, "state")
    process.env.HT_DISABLE_KEYCHAIN = "1"
    const originalFetch = globalThis.fetch
    let backgroundSyncs = 0
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(String(input))
      if (url.pathname === "/v1/me") {
        return Response.json({
          data: {
            workspace: { id: "workspace-setup", slug: "setup", name: "Setup Workspace" },
            principal: { id: "principal-setup", kind: "device", name: "Setup Device" }
          }
        })
      }
      if (url.pathname === "/v1/ingest/batches") {
        throw new Error("Setup must not upload historical sessions in the foreground.")
      }
      throw new Error(`Unexpected setup request: ${url}`)
    }) as typeof fetch
    try {
      const options = {
        entrypoint: resolve(import.meta.dir, "..", "src", "entry.ts"),
        apiUrl: "https://api.example.test",
        apiKey: "hab_setup_test_key",
        codexHome,
        claudeHome,
        projectOrigins: [projectOrigin],
        backfillWindow: "all" as const,
        noDaemon: true,
        nonInteractive: true,
        requestBackgroundSync: async () => {
          backgroundSyncs += 1
          return true
        }
      }
      const first = await runSetup(options)
      expect(first).toMatchObject({
        configured: true,
        repeatedSafe: true,
        workspace: { id: "workspace-setup" },
        workspaceUrl: "https://app.use-habitat.com/w/workspace-setup/sessions",
        backfill: {
          scheduled: true,
          notified: true,
          exported: 0,
          failed: 0,
          remaining: 0,
          retrying: 0,
          quarantined: 0
        }
      })
      expect(backgroundSyncs).toBe(1)
      const [setupConnection] = await resolveHabitatExports(htPaths())
      expect(setupConnection).toBeDefined()
      const setupStore = new LocalSessionStore(htPaths().database)
      try {
        expect(
          new IncrementalPipelineStore(setupStore.database)
            .backfillStatus(setupConnection!.destinationId)
        ).toMatchObject({ state: "not-scheduled", pending: 0 })
      } finally {
        setupStore.close()
      }
      const config = await readConfig(htPaths())
      expect(config).toMatchObject({
        schemaVersion: 2,
        activeWorkspaceId: "workspace-setup",
        backfillWindow: "all"
      })
      expect(config.projects).toEqual([
        expect.objectContaining({
          origin: projectOrigin,
          providers: ["codex", "claude"]
        })
      ])
      expect(config.upload).toEqual({ projectOrigins: {} })
      expect(htPaths().config).toBe(join(process.env.HT_HOME!, "ht.config.json"))

      const verifiedStore = new LocalSessionStore(htPaths().database)
      let verifiedAt: string | null
      try {
        const pipeline = new IncrementalPipelineStore(verifiedStore.database)
        pipeline.recordHookObservation("codex", "Stop", "verified-session")
        verifiedAt = pipeline.harnesses().find((state) =>
          state.provider === "codex"
        )?.lastHookReceivedAt ?? null
      } finally {
        verifiedStore.close()
      }

      const second = await runSetup(options)
      expect(second.backfill).toMatchObject({ exported: 0, failed: 0, remaining: 0 })
      expect(second.harnesses).toContainEqual(expect.objectContaining({
        provider: "codex",
        state: "active",
        lastHookReceivedAt: verifiedAt
      }))
      expect(second.installation?.notices).not.toContain(
        "Codex requires one-time hook review before automatic ingestion can begin."
      )
      expect(backgroundSyncs).toBe(2)
      expect(second).toMatchObject({
        ready: true,
        actionsRequired: []
      })
      expect(second.harnesses.map((harness) => harness.provider).sort())
        .toEqual(["claude", "codex"])
      expect((await readConfig(htPaths())).projects.find((project) =>
        project.origin === projectOrigin
      )?.providers).toEqual(["codex", "claude"])
      expect(await readFile(join(codexHome, "hooks.json"), "utf8"))
        .toContain("hook codex")
      expect(await readFile(join(claudeHome, "settings.json"), "utf8"))
        .toContain("hook claude")
    } finally {
      globalThis.fetch = originalFetch
      if (previousHome === undefined) delete process.env.HT_HOME
      else process.env.HT_HOME = previousHome
      if (previousKeychain === undefined) delete process.env.HT_DISABLE_KEYCHAIN
      else process.env.HT_DISABLE_KEYCHAIN = previousKeychain
    }
  })

  test("stores credentials per workspace and moves a project route explicitly", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-workspaces-"))
    temporary.push(directory)
    const paths = htPaths(directory)
    const previousKeychain = process.env.HT_DISABLE_KEYCHAIN
    const previousUrl = process.env.HABITAT_API_URL
    const previousKey = process.env.HABITAT_API_KEY
    process.env.HT_DISABLE_KEYCHAIN = "1"
    delete process.env.HABITAT_API_URL
    delete process.env.HABITAT_API_KEY
    try {
      for (const workspace of ["a", "b"]) {
        await saveHabitatLogin({
          apiUrl: "https://api.example.test",
          workspace: {
            id: `workspace-${workspace}`,
            slug: workspace,
            name: `Workspace ${workspace.toUpperCase()}`
          },
          principal: {
            id: `principal-${workspace}`,
            kind: "device",
            name: `Device ${workspace.toUpperCase()}`
          }
        }, `key-${workspace}`, paths)
      }
      await saveSetupSelection({
        activeWorkspaceId: "workspace-a",
        projects: [{
          origin: "/work/habitat",
          name: "habitat",
          workspaceId: "workspace-a",
          providers: ["codex", "claude"]
        }],
        backfillWindow: "30d"
      }, paths)
      await saveSetupSelection({
        activeWorkspaceId: "workspace-b",
        projects: [{
          origin: "/work/habitat",
          name: "habitat",
          workspaceId: "workspace-b",
          providers: ["codex", "claude"]
        }],
        backfillWindow: "90d"
      }, paths)

      const config = await readConfig(paths)
      expect(config.workspaces).toHaveLength(2)
      expect(config.projects).toEqual([
        expect.objectContaining({
          origin: "/work/habitat",
          workspaceId: "workspace-b"
        })
      ])
      const connections = await resolveHabitatExports(paths)
      expect(connections.map((connection) => ({
        workspace: connection.config?.workspace.id,
        apiKey: connection.apiKey
      }))).toEqual([
        { workspace: "workspace-a", apiKey: "key-a" },
        { workspace: "workspace-b", apiKey: "key-b" }
      ])
    } finally {
      if (previousKeychain === undefined) delete process.env.HT_DISABLE_KEYCHAIN
      else process.env.HT_DISABLE_KEYCHAIN = previousKeychain
      if (previousUrl === undefined) delete process.env.HABITAT_API_URL
      else process.env.HABITAT_API_URL = previousUrl
      if (previousKey === undefined) delete process.env.HABITAT_API_KEY
      else process.env.HABITAT_API_KEY = previousKey
    }
  })

  test("logs out explicit, active, and all saved workspaces", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-logout-"))
    temporary.push(directory)
    const previousHome = process.env.HT_HOME
    const previousKeychain = process.env.HT_DISABLE_KEYCHAIN
    const previousUrl = process.env.HABITAT_API_URL
    const previousKey = process.env.HABITAT_API_KEY
    process.env.HT_HOME = directory
    process.env.HT_DISABLE_KEYCHAIN = "1"
    delete process.env.HABITAT_API_URL
    delete process.env.HABITAT_API_KEY
    const output = captureCliOutput()
    try {
      for (const workspace of ["a", "b", "c"]) {
        await saveHabitatLogin({
          apiUrl: "https://api.example.test",
          workspace: {
            id: `workspace-${workspace}`,
            slug: workspace,
            name: `Workspace ${workspace.toUpperCase()}`
          },
          principal: {
            id: `principal-${workspace}`,
            kind: "device",
            name: `Device ${workspace.toUpperCase()}`
          }
        }, `key-${workspace}`)
        await saveSetupSelection({
          activeWorkspaceId: `workspace-${workspace}`,
          projects: [{
            origin: `/work/${workspace}`,
            name: workspace,
            workspaceId: `workspace-${workspace}`,
            providers: ["claude"]
          }],
          backfillWindow: "30d"
        })
      }

      expect(await runCli([
        "logout", "--workspace", "workspace-a", "--json"
      ], output)).toBe(0)
      expect(JSON.parse(output.takeStdout())).toMatchObject({
        loggedOut: true,
        workspaces: [{ id: "workspace-a" }],
        activeWorkspaceId: "workspace-c",
        remainingWorkspaces: 2
      })
      let config = await readConfig()
      expect(config.workspaces.map((workspace) => workspace.workspace.id))
        .toEqual(["workspace-b", "workspace-c"])
      expect(config.projects.map((project) => project.workspaceId))
        .toEqual(["workspace-b", "workspace-c"])
      let credentials = JSON.parse(
        await readFile(htPaths().credentials, "utf8")
      ) as { habitatApiKeys?: Record<string, string> }
      expect(credentials.habitatApiKeys).toEqual({
        "workspace-b": "key-b",
        "workspace-c": "key-c"
      })

      expect(await runCli(["logout", "--json"], output)).toBe(0)
      expect(JSON.parse(output.takeStdout())).toMatchObject({
        workspaces: [{ id: "workspace-c" }],
        activeWorkspaceId: "workspace-b",
        remainingWorkspaces: 1
      })
      config = await readConfig()
      expect(config.workspaces.map((workspace) => workspace.workspace.id))
        .toEqual(["workspace-b"])

      expect(await runCli(["logout", "--all", "--json"], output)).toBe(0)
      expect(JSON.parse(output.takeStdout())).toMatchObject({
        workspaces: [{ id: "workspace-b" }],
        activeWorkspaceId: null,
        remainingWorkspaces: 0
      })
      config = await readConfig()
      expect(config.workspaces).toEqual([])
      expect(config.projects).toEqual([])
      credentials = JSON.parse(
        await readFile(htPaths().credentials, "utf8")
      ) as { habitatApiKeys?: Record<string, string> }
      expect(credentials.habitatApiKeys).toBeUndefined()

      expect(await runCli([
        "logout", "--workspace", "workspace-a", "--all"
      ], output)).toBe(1)
    } finally {
      restoreEnvironment("HT_HOME", previousHome)
      restoreEnvironment("HT_DISABLE_KEYCHAIN", previousKeychain)
      restoreEnvironment("HABITAT_API_URL", previousUrl)
      restoreEnvironment("HABITAT_API_KEY", previousKey)
    }
  })

  test("syncs provider logs before listing sessions", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-list-"))
    temporary.push(directory)
    const previousHome = process.env.HT_HOME
    process.env.HT_HOME = directory
    const output = captureCliOutput()
    try {
      expect(await runCli([
        "sessions",
        "list",
        "--provider",
        "codex",
        "--codex-home",
        codexFixtures,
        "--claude-home",
        claudeFixtures
      ], output)).toBe(0)
      const store = new LocalSessionStore(htPaths(directory).database)
      try {
        expect(store.snapshots("codex")).not.toHaveLength(0)
        expect(store.stats().sources).toBeGreaterThan(0)
      } finally {
        store.close()
      }
    } finally {
      if (previousHome === undefined) delete process.env.HT_HOME
      else process.env.HT_HOME = previousHome
    }
  })

  test("normalizes provider snapshots and redacts secrets before local storage", async () => {
    const source = (await discoverCodexSources(codexFixtures))[0]!
    const parsed = await parseJsonlSource(source)
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-test",
      generation: 1,
      source,
      records: parsed.records
    })
    expect(snapshot).not.toBeNull()
    expect(snapshot?.session.title).toBe("Build local session storage")
    expect(snapshot?.session.project).toBe("habitat")
    expect(JSON.stringify(snapshot)).not.toContain("/work/habitat")
    expect(snapshot?.events.some((event) => event.kind === "tool_call")).toBe(true)

    const movedSnapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-other",
      generation: 1,
      source: {
        ...source,
        path: join("/tmp/archived_sessions", source.path.split("/").at(-1)!)
      },
      records: parsed.records
    })
    expect(movedSnapshot?.source.id).not.toBe(snapshot?.source.id)
    expect(movedSnapshot?.session.id).toBe(snapshot?.session.id)

    const redacted = redactValue({
      text: "Authorization: Bearer fixture-token password=fixture-password",
      nested: { token: "api_key=fixture-api-key" }
    })
    expect(JSON.stringify(redacted.value)).not.toContain("fixture-password")
    expect(JSON.stringify(redacted.value)).not.toContain("fixture-api-key")
    expect(redacted.replacements).toBe(3)
  })

  test("preserves freeform Code Mode input for exec post-processing", () => {
    const startedAt = new Date("2026-07-28T12:00:00.000Z")
    const source = {
      path: "/tmp/rollout-11111111-1111-1111-1111-111111111111.jsonl",
      classification: "fixture" as const,
      size: 1,
      modifiedAt: startedAt
    }
    const rawRecords = [
      {
        timestamp: startedAt.toISOString(),
        type: "session_meta",
        payload: { id: "11111111-1111-1111-1111-111111111111", cwd: "/work/habitat" }
      },
      {
        timestamp: "2026-07-28T12:00:01.000Z",
        type: "event_msg",
        payload: { type: "user_message", message: "Run the tests" }
      },
      {
        timestamp: "2026-07-28T12:00:02.000Z",
        type: "response_item",
        payload: {
          type: "reasoning",
          id: "reasoning-1",
          summary: [{ type: "summary_text", text: "Checking the focused test command" }],
          encrypted_content: "must-not-be-persisted"
        }
      },
      {
        timestamp: "2026-07-28T12:00:03.000Z",
        type: "response_item",
        payload: {
          type: "custom_tool_call",
          name: "exec",
          call_id: "call-exec",
          input: "await tools.exec_command({ cmd: 'bun test' })"
        }
      },
      {
        timestamp: "2026-07-28T12:00:04.000Z",
        type: "response_item",
        payload: {
          type: "custom_tool_call_output",
          call_id: "call-exec",
          output: [
            { type: "input_text", text: "Script completed\nOutput:\n" },
            { type: "input_text", text: "2 tests passed" }
          ]
        }
      }
    ]
    const records = rawRecords.map((raw, index) => ({
      line: index + 1,
      offset: index,
      raw,
      rawText: JSON.stringify(raw),
      occurredAt: new Date(raw.timestamp)
    }))

    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-code-mode",
      generation: 1,
      source,
      records
    })

    expect(snapshot?.events.find((event) => event.kind === "tool_call")?.attributes.arguments)
      .toBe("await tools.exec_command({ cmd: 'bun test' })")
    expect(snapshot?.events.find((event) => event.kind === "tool_result")).toMatchObject({
      content: [
        { type: "input_text", text: "Script completed\nOutput:\n" },
        { type: "input_text", text: "2 tests passed" }
      ],
      text: "Script completed\nOutput:\n\n2 tests passed",
      attributes: { callId: "call-exec" }
    })
    expect(snapshot?.events.find((event) => event.kind === "reasoning")).toMatchObject({
      text: "Checking the focused test command",
      attributes: {
        providerReasoningId: "reasoning-1",
        contentOmitted: false,
        encryptedContentAvailable: true
      }
    })
    expect(JSON.stringify(snapshot)).not.toContain("must-not-be-persisted")
  })

  test("captures current Codex subagent lineage, spawn links, and usage identity", () => {
    const startedAt = new Date("2026-08-19T12:00:00.000Z")
    const childId = "22222222-2222-2222-2222-222222222222"
    const parentId = "11111111-1111-1111-1111-111111111111"
    const rawRecords = [
      {
        timestamp: startedAt.toISOString(),
        type: "session_meta",
        payload: {
          id: childId,
          cwd: "/work/habitat",
          source: {
            subagent: {
              thread_spawn: { parent_thread_id: parentId }
            }
          }
        }
      },
      {
        timestamp: "2026-08-19T12:00:01.000Z",
        type: "turn_context",
        payload: { turn_id: "turn-child", model: "gpt-5.6-codex" }
      },
      {
        timestamp: "2026-08-19T12:00:02.000Z",
        type: "response_item",
        payload: {
          type: "function_call",
          name: "spawn_agent",
          call_id: "call-spawn",
          arguments: "{}"
        }
      },
      {
        timestamp: "2026-08-19T12:00:03.000Z",
        type: "response_item",
        payload: {
          type: "function_call_output",
          call_id: "call-spawn",
          output: JSON.stringify({ agent_id: "33333333-3333-3333-3333-333333333333" })
        }
      },
      {
        timestamp: "2026-08-19T12:00:04.000Z",
        type: "event_msg",
        payload: {
          type: "token_count",
          info: {
            total_token_usage: {
              input_tokens: 40,
              output_tokens: 8,
              cached_input_tokens: 10,
              reasoning_output_tokens: 2
            }
          }
        }
      }
    ]
    const records = rawRecords.map((raw, index) => ({
      line: index + 1,
      offset: index,
      raw,
      rawText: JSON.stringify(raw),
      occurredAt: new Date(raw.timestamp)
    }))
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-codex-subagent",
      generation: 1,
      source: {
        path: `/tmp/rollout-${childId}.jsonl`,
        classification: "fixture",
        size: 1,
        modifiedAt: startedAt
      },
      records
    })

    expect(snapshot?.source.nativeParentSessionId).toBe(parentId)
    expect(snapshot?.session.kind).toBe("agent")
    expect(snapshot?.events.find((entry) => entry.kind === "tool_call")?.attributes)
      .toMatchObject({
        callId: "call-spawn",
        childNativeSessionId: "33333333-3333-3333-3333-333333333333"
      })
    expect(snapshot?.events.find((entry) => entry.kind === "token_usage")?.attributes)
      .toMatchObject({ providerUsageId: "codex:turn-child:0" })
  })

  test("links Claude Agent calls to child sessions and assigns stable usage identity", () => {
    const startedAt = new Date("2026-08-19T13:00:00.000Z")
    const rawRecords = [
      {
        type: "user",
        uuid: "user-1",
        timestamp: startedAt.toISOString(),
        message: { role: "user", content: "Delegate the investigation" }
      },
      {
        type: "assistant",
        uuid: "assistant-1",
        requestId: "request-1",
        timestamp: "2026-08-19T13:00:01.000Z",
        message: {
          id: "message-1",
          role: "assistant",
          model: "claude-opus-4-1",
          content: [{
            type: "tool_use",
            id: "toolu-agent",
            name: "Agent",
            input: { description: "Inspect lineage" }
          }],
          usage: { input_tokens: 10, output_tokens: 4 }
        }
      },
      {
        type: "user",
        uuid: "result-1",
        timestamp: "2026-08-19T13:00:02.000Z",
        message: {
          role: "user",
          content: [{ type: "tool_result", tool_use_id: "toolu-agent", content: "done" }]
        },
        toolUseResult: { status: "completed", agentId: "child-1" }
      }
    ]
    const records = rawRecords.map((raw, index) => ({
      line: index + 1,
      offset: index,
      raw,
      rawText: JSON.stringify(raw),
      occurredAt: new Date(raw.timestamp)
    }))
    const snapshot = normalizeSnapshot({
      provider: "claude",
      deviceId: "device-claude-subagent",
      generation: 1,
      source: {
        path: "/tmp/project/root/subagents/agent-parent.jsonl",
        classification: "fixture",
        size: 1,
        modifiedAt: startedAt
      },
      records
    })

    expect(snapshot?.source.nativeParentSessionId).toBe("root")
    expect(snapshot?.events.find((entry) => entry.kind === "tool_call")?.attributes)
      .toMatchObject({
        callId: "toolu-agent",
        childNativeSessionId: "agent-child-1"
      })
    expect(snapshot?.events.find((entry) => entry.kind === "token_usage")?.attributes)
      .toMatchObject({ providerUsageId: "claude:message-1" })
  })

  test("preserves visible Claude thinking separately from assistant text", () => {
    const startedAt = new Date("2026-07-28T13:00:00.000Z")
    const source = {
      path: "/tmp/claude-reasoning.jsonl",
      classification: "fixture" as const,
      size: 1,
      modifiedAt: startedAt
    }
    const rawRecords = [
      {
        type: "user",
        uuid: "user-1",
        timestamp: startedAt.toISOString(),
        message: { role: "user", content: "Inspect the parser" }
      },
      {
        type: "assistant",
        uuid: "assistant-1",
        timestamp: "2026-07-28T13:00:01.000Z",
        message: {
          id: "message-1",
          role: "assistant",
          model: "claude-opus-4-1",
          content: [
            { type: "thinking", thinking: "I should inspect the normalizer first." },
            { type: "text", text: "I found the normalization path." }
          ]
        }
      }
    ]
    const records = rawRecords.map((raw, index) => ({
      line: index + 1,
      offset: index,
      raw,
      rawText: JSON.stringify(raw),
      occurredAt: new Date(raw.timestamp)
    }))

    const snapshot = normalizeSnapshot({
      provider: "claude",
      deviceId: "device-claude-reasoning",
      generation: 1,
      source,
      records
    })

    expect(snapshot?.events.find((event) => event.kind === "reasoning")?.text)
      .toBe("I should inspect the normalizer first.")
    expect(snapshot?.events.find((event) => event.kind === "assistant_message")?.text)
      .toBe("I found the normalization path.")
  })

  test("uses the latest Codex state title and refreshes it without a JSONL change", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-codex-title-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const sessionDirectory = join(codexHome, "sessions", "2026", "07", "14")
    const nativeSessionId = "44444444-4444-4444-4444-444444444444"
    await mkdir(sessionDirectory, { recursive: true })
    await Bun.write(join(sessionDirectory, `rollout-${nativeSessionId}.jsonl`), [
      JSON.stringify({
        timestamp: "2026-07-14T10:00:00.000Z",
        type: "session_meta",
        payload: { id: nativeSessionId, cwd: "/work/habitat" }
      }),
      JSON.stringify({
        timestamp: "2026-07-14T10:00:01.000Z",
        type: "event_msg",
        payload: { type: "user_message", message: "First prompt fallback" }
      }),
      JSON.stringify({
        timestamp: "2026-07-14T10:00:02.000Z",
        type: "event_msg",
        payload: {
          type: "thread_name_updated",
          thread_id: nativeSessionId,
          thread_name: "Older JSONL title"
        }
      })
    ].join("\n") + "\n")
    const statePath = join(codexHome, "state_12.sqlite")
    const state = new Database(statePath, { create: true })
    state.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
    state.query("INSERT INTO threads (id, title) VALUES (?, ?)").run(
      nativeSessionId,
      "Native Codex title"
    )
    state.close()

    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome,
      claudeHome: join(directory, "missing-claude")
    })
    try {
      const initial = await collector.runOnce("codex")
      expect(initial.scan.captured).toBe(1)
      expect(collector.store.snapshots("codex")[0]?.session.title).toBe("Native Codex title")

      const updatedState = new Database(statePath)
      updatedState.query("UPDATE threads SET title = ? WHERE id = ?").run(
        "Renamed Codex session",
        nativeSessionId
      )
      updatedState.close()

      const refreshed = await collector.runOnce("codex")
      expect(refreshed.scan).toMatchObject({ captured: 1, unchanged: 0 })
      expect(collector.store.snapshots("codex")[0]?.session.title).toBe("Renamed Codex session")
    } finally {
      collector.close()
    }
  })

  test("prefers the Codex session index over a default state title", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-codex-index-title-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const sessionDirectory = join(codexHome, "sessions")
    const nativeSessionId = "55555555-5555-5555-5555-555555555555"
    await mkdir(sessionDirectory, { recursive: true })
    await Bun.write(join(sessionDirectory, `rollout-${nativeSessionId}.jsonl`), [
      JSON.stringify({
        timestamp: "2026-07-14T10:00:00.000Z",
        type: "session_meta",
        payload: { id: nativeSessionId, cwd: "/work/habitat" }
      }),
      JSON.stringify({
        timestamp: "2026-07-14T10:00:01.000Z",
        type: "event_msg",
        payload: { type: "user_message", message: "First prompt fallback" }
      })
    ].join("\n") + "\n")
    await Bun.write(join(codexHome, "session_index.jsonl"), [
      "{malformed",
      JSON.stringify({
        id: nativeSessionId,
        thread_name: "Indexed Codex title",
        updated_at: "2026-07-14T10:01:00.000Z"
      })
    ].join("\n") + "\n")
    const state = new Database(join(codexHome, "state_12.sqlite"), { create: true })
    state.exec("CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
    state.query("INSERT INTO threads (id, title) VALUES (?, ?)").run(
      nativeSessionId,
      "First prompt fallback"
    )
    state.close()

    const [source] = await discoverCodexSources(codexHome)
    expect(source?.nativeTitle).toBe("Indexed Codex title")
  })

  test("keeps failed Habitat exports for a later retry", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-export-"))
    temporary.push(directory)
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: new HabitatExporter({
        apiUrl: "http://127.0.0.1:1",
        apiKey: "unreachable-test-key"
      })
    })
    try {
      const result = await collector.runOnce()
      expect(result.scan.captured).toBe(4)
      expect(result.flush).toMatchObject({
        exportEnabled: true,
        exporter: "habitat",
        failed: 4,
        remaining: 4
      })
      expect(collector.store.stats().failed).toBe(4)
    } finally {
      collector.close()
    }
  })

  test("rebases automatically when the server reports a cursor divergence", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-cursor-rebase-"))
    temporary.push(directory)
    const accepted: IngestBatch[] = []
    let diverged = false
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: {
        kind: "test",
        destinationId: "test:cursor-rebase",
        exportSnapshot: async () => {},
        exportBatch: async (batch) => {
          if (!diverged) {
            diverged = true
            throw new SnapshotExportError(
              "Expected source cursor 20, received 10.",
              true,
              409,
              "cursor-gap",
              { expectedCursor: 20, expectedEpoch: 2 }
            )
          }
          accepted.push(batch)
        }
      }
    })
    try {
      const first = await collector.runOnce()
      expect(first.flush).toMatchObject({ failed: 1, exported: 3, remaining: 1 })
      const second = await collector.flush()
      expect(second).toMatchObject({ failed: 0, exported: 1, remaining: 0 })
      expect(accepted.at(-1)).toMatchObject({
        mode: "baseline",
        source: { epoch: 2 },
        cursor: { from: 0 }
      })
    } finally {
      collector.close()
    }
  })

  test("tracks delivery independently for each destination", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-destinations-"))
    temporary.push(directory)
    const statePath = join(directory, "sessions.sqlite")
    const exportedA: string[] = []
    const exportedB: string[] = []
    const exporter = (destinationId: string, output: string[]) => ({
      kind: "test",
      destinationId,
      exportSnapshot: async (snapshot: { snapshotId: string }) => {
        output.push(snapshot.snapshotId)
      }
    })

    const first = new SessionCollector({
      statePath,
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: exporter("test:workspace-a", exportedA)
    })
    try {
      const result = await first.runOnce()
      expect(result.flush.exported).toBe(4)
      expect(first.store.stats("test:workspace-a")).toMatchObject({
        delivered: 4,
        pending: 0
      })
    } finally {
      first.close()
    }

    const repeated = new SessionCollector({
      statePath,
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: exporter("test:workspace-a", exportedA)
    })
    try {
      const result = await repeated.runOnce()
      expect(result.scan).toMatchObject({ unchanged: 4, ignored: 2 })
      expect(result.flush.exported).toBe(0)
      expect(exportedA).toHaveLength(4)
    } finally {
      repeated.close()
    }

    const secondDestination = new SessionCollector({
      statePath,
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: exporter("test:workspace-b", exportedB)
    })
    try {
      const result = await secondDestination.runOnce()
      expect(result.scan).toMatchObject({ unchanged: 4, ignored: 2 })
      expect(result.flush.exported).toBe(4)
      expect(exportedB).toHaveLength(4)
      expect(secondDestination.store.stats("test:workspace-a").delivered).toBe(4)
      expect(secondDestination.store.stats("test:workspace-b").delivered).toBe(4)
    } finally {
      secondDestination.close()
    }
  })

  test("migrates existing local snapshots into the destination-aware ledger", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-ledger-migration-"))
    temporary.push(directory)
    const databasePath = join(directory, "sessions.sqlite")
    const source = (await discoverCodexSources(codexFixtures))[0]!
    const parsed = await parseJsonlSource(source)
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-migration",
      generation: 1,
      source,
      records: parsed.records
    })!
    const legacy = new Database(databasePath, { create: true })
    legacy.exec(`
      CREATE TABLE local_snapshots (
        source_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        session_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE export_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id TEXT NOT NULL UNIQUE,
        source_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        project_origin TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `)
    const now = new Date().toISOString()
    legacy.query(`
      INSERT INTO local_snapshots (
        source_id, provider, session_id, payload, updated_at
      ) VALUES (?, ?, ?, ?, ?)
    `).run(snapshot.source.id, "codex", snapshot.session.id, JSON.stringify(snapshot), now)
    legacy.query(`
      INSERT INTO export_outbox (
        snapshot_id, source_id, payload, project_origin,
        attempts, next_attempt_at, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 0, 0, ?, ?)
    `).run(
      snapshot.snapshotId,
      snapshot.source.id,
      JSON.stringify(snapshot),
      "/work/habitat",
      now,
      now
    )
    legacy.close()

    const migrated = new LocalSessionStore(databasePath)
    try {
      expect(migrated.deliveryCandidates("test:new-destination")).toEqual([
        expect.objectContaining({
          snapshotId: snapshot.snapshotId,
          projectOrigin: "/work/habitat",
          state: "new"
        })
      ])
      expect(migrated.prepareDestination("test:new-destination")).toBe(1)
      expect(migrated.stats("test:new-destination").pending).toBe(1)
      expect(migrated.database.query<{ value: string }, []>(`
        SELECT value FROM collector_meta
        WHERE key = 'migration:local-snapshot-metadata:v1'
      `).get()?.value).toBe("complete")
    } finally {
      migrated.close()
    }

    const changed = new Database(databasePath)
    changed.query(`
      UPDATE local_snapshots SET project_origin = NULL
    `).run()
    changed.close()

    const reopened = new LocalSessionStore(databasePath)
    try {
      expect(reopened.deliveryCandidates("test:new-destination")[0]?.projectOrigin)
        .toBeNull()
    } finally {
      reopened.close()
    }
  })

  test("does not acknowledge a newer generation after a concurrent export", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-stale-ack-"))
    temporary.push(directory)
    const source = (await discoverCodexSources(codexFixtures))[0]!
    const parsed = await parseJsonlSource(source)
    const snapshot = normalizeSnapshot({
      provider: "codex",
      deviceId: "device-stale-ack",
      generation: 1,
      source,
      records: parsed.records
    })!
    const store = new LocalSessionStore(join(directory, "sessions.sqlite"))
    try {
      store.enqueue("codex", source, snapshot, "/work/habitat")
      store.prepareDestination("test:workspace")
      const inFlight = store.due("test:workspace")[0]!
      store.enqueue("codex", source, {
        ...snapshot,
        snapshotId: `${snapshot.snapshotId}-newer`,
        source: { ...snapshot.source, generation: 2 }
      }, "/work/habitat")

      store.acknowledge(inFlight.id, inFlight.snapshotId)

      expect(store.stats("test:workspace")).toMatchObject({
        pending: 1,
        delivered: 0
      })
      expect(store.due("test:workspace")[0]?.snapshotId).toBe(
        `${snapshot.snapshotId}-newer`
      )
    } finally {
      store.close()
    }
  })

  test("does not wake a daemon belonging to another HT home", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-daemon-identity-"))
    temporary.push(directory)
    const paths = htPaths(directory)
    const store = new LocalSessionStore(paths.database)
    const deviceId = store.deviceId()
    store.close()
    let reportedDeviceId = "another-device"
    let reportedVersion: string | undefined
    let wakeRequests = 0
    let backgroundSyncRequests = 0
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (request) => {
        const url = new URL(request.url)
        if (url.pathname === "/healthz") {
          return Response.json({
            status: "ok",
            deviceId: reportedDeviceId,
            version: reportedVersion
          })
        }
        if (url.pathname === "/wake") {
          wakeRequests += 1
          return Response.json({ accepted: true }, { status: 202 })
        }
        if (
          url.pathname === "/sync" &&
          url.searchParams.get("background") === "true"
        ) {
          backgroundSyncRequests += 1
          return Response.json({ accepted: true }, { status: 202 })
        }
        return Response.json({ error: "not-found" }, { status: 404 })
      }
    })
    const previousPort = process.env.HT_DAEMON_PORT
    process.env.HT_DAEMON_PORT = String(server.port)
    try {
      expect(await daemonVersionMatches(paths)).toBe(false)
      expect(await wakeDaemon(undefined, paths)).toBe(false)
      expect(wakeRequests).toBe(0)

      reportedDeviceId = deviceId
      expect(await daemonVersionMatches(paths)).toBe(false)
      reportedVersion = cliVersion
      expect(await daemonVersionMatches(paths)).toBe(true)
      expect(await wakeDaemon(undefined, paths)).toBe(true)
      expect(wakeRequests).toBe(1)
      expect(await requestDaemonSync(undefined, paths)).toBe(true)
      expect(backgroundSyncRequests).toBe(1)
    } finally {
      server.stop(true)
      restoreEnvironment("HT_DAEMON_PORT", previousPort)
    }
  })

  test("quarantines permanent payload failures without blocking later cycles", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-quarantine-"))
    temporary.push(directory)
    const collector = new SessionCollector({
      statePath: join(directory, "sessions.sqlite"),
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter: new HabitatExporter({
        apiUrl: "https://api.example.test",
        apiKey: "test-key",
        destinationId: "habitat:test-workspace",
        fetch: (async () =>
          Response.json(
            { error: "invalid snapshot" },
            { status: 422 }
          )) as unknown as typeof fetch
      })
    })
    try {
      const initial = await collector.runOnce()
      expect(initial.flush).toMatchObject({
        attempted: 4,
        exported: 0,
        failed: 4,
        quarantined: 4,
        remaining: 0
      })
      expect(collector.store.stats("habitat:test-workspace")).toMatchObject({
        quarantined: 4,
        pending: 0
      })
      expect(collector.store.deliveryIssues("habitat:test-workspace")).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            state: "quarantined",
            attempts: 1,
            lastError: expect.stringContaining("HTTP 422")
          })
        ])
      )

      const repeated = await collector.runOnce()
      expect(repeated.flush.attempted).toBe(0)
      expect(collector.store.retryDestination(
        "habitat:test-workspace",
        undefined,
        true
      )).toBe(4)
      expect(collector.store.stats("habitat:test-workspace")).toMatchObject({
        quarantined: 0,
        pending: 4
      })
    } finally {
      collector.close()
    }
  })

  test("returns a structured auth error for agent-driven backfill", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-backfill-error-"))
    temporary.push(directory)
    const previousHome = process.env.HT_HOME
    const previousUrl = process.env.HABITAT_API_URL
    const previousKey = process.env.HABITAT_API_KEY
    process.env.HT_HOME = directory
    delete process.env.HABITAT_API_URL
    delete process.env.HABITAT_API_KEY
    const output = captureCliOutput()
    try {
      expect(await runCli(["backfill", "--json"], output)).toBe(1)
      expect(JSON.parse(output.readStdout())).toMatchObject({
        ok: false,
        error: { code: "AUTH_REQUIRED" }
      })
    } finally {
      if (previousHome === undefined) delete process.env.HT_HOME
      else process.env.HT_HOME = previousHome
      if (previousUrl === undefined) delete process.env.HABITAT_API_URL
      else process.env.HABITAT_API_URL = previousUrl
      if (previousKey === undefined) delete process.env.HABITAT_API_KEY
      else process.env.HABITAT_API_KEY = previousKey
    }
  })

  test("prints a concise status by default and preserves JSON output", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-status-"))
    temporary.push(directory)
    const previousHome = process.env.HT_HOME
    const previousConfig = process.env.HT_CONFIG
    const previousPort = process.env.HT_DAEMON_PORT
    process.env.HT_HOME = directory
    process.env.HT_CONFIG = join(directory, "ht.config.json")
    process.env.HT_DAEMON_PORT = "65534"
    const store = new LocalSessionStore(join(directory, "sessions.sqlite"))
    store.database.query(`
      INSERT INTO local_snapshots (
        source_id, provider, session_id, snapshot_id, payload, updated_at
      ) VALUES ('source-status', 'codex', 'session-status', 'snapshot-status',
        'not-json', '2026-08-11T12:00:00.000Z')
    `).run()
    store.close()
    const output = captureCliOutput()
    try {
      expect(await runCli(["status"], output)).toBe(1)
      expect(output.readStdout()).toContain("Habitat status")
      expect(output.readStdout()).toContain("Ingestion needs attention")
      expect(output.readStdout()).toContain("Run `ht setup`")
      expect(output.readStdout()).not.toStartWith("{")

      output.clear()
      expect(await runCli(["status", "--json"], output)).toBe(1)
      expect(JSON.parse(output.readStdout())).toMatchObject({
        overall: "needs-attention",
        configured: false,
        local: { sessions: 1 },
        daemon: { reachable: false }
      })
    } finally {
      restoreEnvironment("HT_HOME", previousHome)
      restoreEnvironment("HT_CONFIG", previousConfig)
      restoreEnvironment("HT_DAEMON_PORT", previousPort)
    }
  })

  test("generates command help and version output through Commander", async () => {
    const output = captureCliOutput()
    expect(await runCli([], output)).toBe(0)
    expect(output.readStdout()).toContain("Usage: ht [options] [command]")
    expect(output.readStdout()).toContain("Lifecycle:")
    expect(output.readStdout()).toContain("Workspace:")
    expect(output.readStdout()).toContain("Diagnostics:")
    expect(output.readStdout()).toContain("help [command]")

    output.clear()
    expect(await runCli(["setup", "--help"], output)).toBe(0)
    expect(output.readStdout()).toContain("--project <origin>")
    expect(output.readStdout()).toContain("select a project folder (repeatable)")
    expect(output.readStdout()).not.toContain("--all-projects")

    output.clear()
    expect(await runCli(["help", "status"], output)).toBe(0)
    expect(output.readStdout()).toContain("Usage: ht status [options]")
    expect(output.readStdout()).toContain("show ingestion and delivery health")

    output.clear()
    expect(await runCli(["version", "--json"], output)).toBe(0)
    expect(JSON.parse(output.readStdout())).toEqual({
      version: "0.4.7",
      command: "ht"
    })
  })

  test("rejects malformed and unknown setup options before changing state", async () => {
    const output = captureCliOutput()
    expect(await runCli([
      "setup", " --api-url", "https://api.example.test"
    ], output)).toBe(1)
    expect(output.readStderr()).toContain("INVALID_ARGUMENT")
    expect(output.readStderr()).toContain("Unexpected whitespace")

    output.clear()
    expect(await runCli(["setup", "--wat"], output)).toBe(1)
    expect(output.readStderr()).toContain("unknown option '--wat'")
  })

  test("does not upload historical sessions in the setup process", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-setup-failure-"))
    temporary.push(directory)
    const codexHome = join(directory, "codex")
    const claudeHome = join(directory, "claude")
    await Promise.all([
      cp(codexFixtures, codexHome, { recursive: true }),
      cp(claudeFixtures, claudeHome, { recursive: true })
    ])
    const previousHome = process.env.HT_HOME
    const previousKeychain = process.env.HT_DISABLE_KEYCHAIN
    process.env.HT_HOME = join(directory, "state")
    process.env.HT_DISABLE_KEYCHAIN = "1"
    const originalFetch = globalThis.fetch
    let ingestRequests = 0
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(String(input))
      if (url.pathname === "/v1/me") {
        return Response.json({
          data: {
            workspace: { id: "workspace-failure", slug: "failure", name: "Failure Workspace" },
            principal: { id: "principal-failure", kind: "device", name: "Failure Device" }
          }
        })
      }
      if (url.pathname === "/v1/ingest/batches") {
        ingestRequests += 1
        throw new Error("Setup attempted a foreground upload.")
      }
      throw new Error(`Unexpected setup request: ${url}`)
    }) as typeof fetch
    const output = captureCliOutput()
    try {
      expect(await runCli([
        "setup",
        "--api-url",
        "https://api.example.test",
        "--api-key",
        "hab_setup_failure_key",
        "--codex-home",
        codexHome,
        "--claude-home",
        claudeHome,
        "--project",
        "/work/habitat",
        "--backfill",
        "all",
        "--no-daemon",
        "--non-interactive"
      ], {
        entrypoint: resolve(import.meta.dir, "..", "src", "entry.ts"),
        stdout: output.stdout,
        stderr: output.stderr
      })).toBe(1)
      expect(ingestRequests).toBe(0)
      expect(output.readStdout()).toContain("Habitat setup needs one final action.")
      expect(output.readStdout()).toContain(
        "Historical ingestion is saved in the config and will reconcile when the background service starts."
      )
      const [connection] = await resolveHabitatExports(htPaths())
      expect(connection).toBeDefined()
      const durableStore = new LocalSessionStore(htPaths().database)
      try {
        expect(
          new IncrementalPipelineStore(durableStore.database)
            .backfillStatus(connection!.destinationId)
        ).toMatchObject({ state: "not-scheduled", pending: 0 })
      } finally {
        durableStore.close()
      }
    } finally {
      globalThis.fetch = originalFetch
      restoreEnvironment("HT_HOME", previousHome)
      restoreEnvironment("HT_DISABLE_KEYCHAIN", previousKeychain)
    }
  })

  test("previews and completes a resumable backfill through the CLI", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-backfill-"))
    temporary.push(directory)
    const environment = {
      home: process.env.HT_HOME,
      url: process.env.HABITAT_API_URL,
      key: process.env.HABITAT_API_KEY,
      config: process.env.HT_CONFIG
    }
    process.env.HT_HOME = directory
    process.env.HT_CONFIG = join(directory, "ht.config.json")
    process.env.HABITAT_API_URL = "https://api.example.test"
    process.env.HABITAT_API_KEY = "test-backfill-key"
    await writeConfig({
      schemaVersion: 2,
      daemonPort: 4322,
      activeWorkspaceId: null,
      workspaces: [],
      projects: [],
      backfillWindow: "all",
      upload: { projectOrigins: {} }
    })
    const originalFetch = globalThis.fetch
    const requests: string[] = []
    globalThis.fetch = (async (input: string | URL | Request) => {
      requests.push(String(input))
      return Response.json({ data: { accepted: true } }, { status: 201 })
    }) as unknown as typeof fetch
    const output = captureCliOutput()
    try {
      expect(await runCli([
        "backfill",
        "--json",
        "--codex-home",
        codexFixtures,
        "--claude-home",
        claudeFixtures
      ], output)).toBe(0)
      const completed = JSON.parse(output.takeStdout()) as any
      expect(completed).toMatchObject({
        dryRun: false,
        resumable: true,
        complete: true,
        flush: { exported: 4 },
        delivery: { delivered: 4, pending: 0 }
      })
      expect(requests).toHaveLength(4)

      expect(await runCli([
        "backfill",
        "--dry-run",
        "--json",
        "--codex-home",
        codexFixtures,
        "--claude-home",
        claudeFixtures
      ], output)).toBe(0)
      const preview = JSON.parse(output.takeStdout()) as any
      expect(preview).toMatchObject({
        dryRun: true,
        preview: { eligible: 0, alreadyDelivered: 4 }
      })
      expect(requests).toHaveLength(4)
    } finally {
      globalThis.fetch = originalFetch
      restoreEnvironment("HT_HOME", environment.home)
      restoreEnvironment("HABITAT_API_URL", environment.url)
      restoreEnvironment("HABITAT_API_KEY", environment.key)
      restoreEnvironment("HT_CONFIG", environment.config)
    }
  })

  test("discovers ht.config.json using standard config paths", () => {
    expect(userConfigPath({
      env: { XDG_CONFIG_HOME: "/tmp/custom-config" },
      home: "/Users/test",
      cwd: "/work/project"
    })).toBe("/tmp/custom-config/ht/ht.config.json")
    expect(userConfigPath({
      env: {},
      home: "/Users/test",
      cwd: "/work/project"
    })).toBe("/Users/test/.config/ht/ht.config.json")
    expect(userConfigPath({
      env: { HT_CONFIG: "./private/ht.config.json" },
      home: "/Users/test",
      cwd: "/work/project"
    })).toBe("/work/project/private/ht.config.json")
  })

  test("migrates legacy setup and upload policy into one config", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-config-migrate-"))
    temporary.push(directory)
    const paths = htPaths(directory)
    await Promise.all([
      writeFile(paths.legacyConfig, JSON.stringify({
        schemaVersion: 2,
        daemonPort: 4322,
        activeWorkspaceId: null,
        workspaces: [],
        projects: [{
          origin: "/work/habitat",
          name: "habitat",
          workspaceId: "workspace-habitat",
          providers: ["codex", "claude"],
          selectedAt: "2026-08-12T00:00:00.000Z"
        }],
        backfillWindow: "90d"
      })),
      writeFile(paths.config, JSON.stringify({
        schemaVersion: 1,
        upload: { projectOrigins: { exclude: ["*-private"] } }
      }))
    ])

    const config = await readConfig(paths)

    expect(config).toMatchObject({
      schemaVersion: 2,
      projects: [{ origin: "/work/habitat" }],
      backfillWindow: "90d",
      upload: { projectOrigins: { exclude: ["*-private"] } }
    })
    expect(JSON.parse(await readFile(paths.config, "utf8"))).toMatchObject({
      schemaVersion: 2,
      projects: [{ origin: "/work/habitat" }],
      upload: { projectOrigins: { exclude: ["*-private"] } }
    })
    await expect(readFile(paths.legacyConfig, "utf8")).rejects.toMatchObject({
      code: "ENOENT"
    })
  })

  test("watches atomic config rewrites", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-config-watch-"))
    temporary.push(directory)
    const path = join(directory, "ht.config.json")
    await writeFile(path, "{}\n")
    let changes = 0
    const watcher = watchConfig(path, () => {
      changes += 1
    }, 5)
    try {
      await writeFile(join(directory, "next.json"), "{}\n")
      await rename(join(directory, "next.json"), path)
      for (let attempt = 0; attempt < 40 && changes === 0; attempt += 1) {
        await Bun.sleep(10)
      }
      expect(changes).toBe(1)
    } finally {
      watcher.close()
    }
  })

  test("withholds project origins locally and exports them after the policy changes", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-policy-"))
    temporary.push(directory)
    const configPath = join(directory, "ht.config.json")
    const statePath = join(directory, "sessions.sqlite")
    const environment = { HT_CONFIG: configPath }
    await Bun.write(configPath, JSON.stringify({
      schemaVersion: 1,
      upload: {
        projectOrigins: {
          include: ["another-project"]
        }
      }
    }))
    const blockedPolicy = await readProjectUploadPolicy({
      env: environment,
      home: "/",
      cwd: directory
    })
    const exported: string[] = []
    const exporter = {
      kind: "test",
      destinationId: "test:workspace",
      exportSnapshot: async (snapshot: { session: { project: string | null } }) => {
        exported.push(snapshot.session.project ?? "unknown")
      }
    }
    const blockedCollector = new SessionCollector({
      statePath,
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter,
      uploadPolicy: blockedPolicy
    })
    try {
      const result = await blockedCollector.runOnce()
      expect(result.scan.captured).toBe(4)
      expect(result.flush).toMatchObject({
        attempted: 0,
        exported: 0,
        withheld: 4,
        remaining: 0
      })
      expect(exported).toEqual([])
    } finally {
      blockedCollector.close()
    }

    await Bun.write(configPath, JSON.stringify({
      schemaVersion: 1,
      upload: {
        projectOrigins: {
          include: ["~/work/**"],
          exclude: ["**/private/**"]
        }
      }
    }))
    const allowedPolicy = await readProjectUploadPolicy({
      env: environment,
      home: "/",
      cwd: directory
    })
    const allowedCollector = new SessionCollector({
      statePath,
      codexHome: codexFixtures,
      claudeHome: claudeFixtures,
      exporter,
      uploadPolicy: allowedPolicy
    })
    try {
      const result = await allowedCollector.runOnce()
      expect(result.scan).toMatchObject({ unchanged: 4, ignored: 2 })
      expect(result.flush).toMatchObject({
        attempted: 4,
        exported: 4,
        withheld: 0,
        remaining: 0
      })
      expect(exported).toEqual(["habitat", "habitat", "habitat", "habitat"])
    } finally {
      allowedCollector.close()
    }
  })

  test("installs Codex and Claude hooks without replacing existing hooks", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ht-install-"))
    temporary.push(directory)
    const paths = htPaths(join(directory, "ht"))
    const codexHome = join(directory, "codex")
    const claudeHome = join(directory, "claude")
    await mkdir(codexHome, { recursive: true })
    await mkdir(claudeHome, { recursive: true })
    await Bun.write(join(codexHome, "hooks.json"), JSON.stringify({
      hooks: { Stop: [{ hooks: [{ type: "command", command: "existing-codex-hook" }] }] }
    }))
    await Bun.write(join(claudeHome, "settings.json"), JSON.stringify({
      permissions: { allow: ["Read"] },
      hooks: { Stop: [{ hooks: [{ type: "command", command: "existing-claude-hook" }] }] }
    }))
    const options = {
      entrypoint: join(import.meta.dir, "..", "src", "entry.ts"),
      paths,
      codexHome,
      claudeHome,
      activateDaemon: false
    }
    const result = await installHT(options)
    await installHT(options)
    expect(result.notices).toContain(
      "Background service installation was skipped; run `ht daemon` manually when needed."
    )
    const codex = JSON.parse(await readFile(join(codexHome, "hooks.json"), "utf8")) as any
    const claude = JSON.parse(await readFile(join(claudeHome, "settings.json"), "utf8")) as any
    expect(JSON.stringify(codex)).toContain("existing-codex-hook")
    expect(JSON.stringify(claude)).toContain("existing-claude-hook")
    expect(codex.hooks.Stop).toBeDefined()
    expect(codex.hooks.SessionEnd).toBeDefined()
    expect(JSON.stringify(codex).match(/hook codex/g)).toHaveLength(2)
    expect(JSON.stringify(claude).match(/hook claude/g)).toHaveLength(2)

    await installHT({ ...options, providers: ["claude"] })
    const claudeOnlyCodex = await readFile(join(codexHome, "hooks.json"), "utf8")
    const claudeOnlyClaude = await readFile(join(claudeHome, "settings.json"), "utf8")
    expect(claudeOnlyCodex).toContain("existing-codex-hook")
    expect(claudeOnlyCodex).not.toContain("hook codex")
    expect(claudeOnlyClaude).toContain("hook claude")

    await installHT({ ...options, providers: ["codex"] })
    const codexOnlyCodex = await readFile(join(codexHome, "hooks.json"), "utf8")
    const codexOnlyClaude = await readFile(join(claudeHome, "settings.json"), "utf8")
    expect(codexOnlyCodex).toContain("hook codex")
    expect(codexOnlyClaude).toContain("existing-claude-hook")
    expect(codexOnlyClaude).not.toContain("hook claude")

    await Bun.write(join(paths.bin, "codex-hook-review"), "temporary helper")
    await Bun.write(
      join(paths.home, "codex-hook-review.terminal"),
      "temporary profile"
    )
    await uninstallHT({ paths, codexHome, claudeHome })
    expect(await readFile(join(codexHome, "hooks.json"), "utf8")).toContain("existing-codex-hook")
    expect(await readFile(join(codexHome, "hooks.json"), "utf8")).not.toContain("hook codex")
    expect(await Bun.file(join(paths.bin, "codex-hook-review")).exists()).toBe(false)
    expect(await Bun.file(join(paths.home, "codex-hook-review.terminal")).exists()).toBe(false)
  })
})

const restoreEnvironment = (name: string, value: string | undefined): void => {
  if (value === undefined) delete process.env[name]
  else process.env[name] = value
}

const runGit = async (args: readonly string[]): Promise<void> => {
  const child = Bun.spawn(["git", ...args], { stdout: "ignore", stderr: "pipe" })
  const [error, exitCode] = await Promise.all([
    new Response(child.stderr).text(),
    child.exited
  ])
  if (exitCode !== 0) throw new Error(error.trim() || `git exited with ${exitCode}`)
}

const createGitWorktree = async (project: string, worktree: string): Promise<void> => {
  await Promise.all([
    mkdir(project, { recursive: true }),
    mkdir(dirname(worktree), { recursive: true })
  ])
  await runGit(["init", "-q", project])
  await writeFile(join(project, "README.md"), "# Habitat\n")
  await runGit(["-C", project, "add", "README.md"])
  await runGit([
    "-C", project,
    "-c", "user.name=HT Tests",
    "-c", "user.email=ht@example.test",
    "commit", "-qm", "Initial commit"
  ])
  await runGit(["-C", project, "worktree", "add", "-q", "--detach", worktree])
}
