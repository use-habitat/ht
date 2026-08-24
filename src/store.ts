import { mkdirSync } from "node:fs"
import { dirname } from "node:path"
import { randomUUID } from "node:crypto"
import { Database } from "bun:sqlite"

import type { Provider, SourceFile } from "./domain.ts"
import { IncrementalPipelineStore } from "./pipeline-store.ts"
import type { SessionSnapshot } from "./snapshot.ts"

const localSnapshotMetadataMigration = "migration:local-snapshot-metadata:v1"

export interface SourceState {
  readonly sourceId: string
  readonly provider: Provider
  readonly localPath: string
  readonly pathHash: string
  readonly size: number
  readonly modifiedMs: number
  readonly metadataRevision: string | null
  readonly normalizerVersion: string | null
  readonly revision: string | null
  readonly generation: number
}

export interface PendingExport {
  readonly id: number
  readonly destinationId: string
  readonly snapshotId: string
  readonly sourceId: string
  readonly payload: SessionSnapshot
  readonly projectOrigin: string | null
  readonly attempts: number
}

export interface DeliveryStats {
  readonly pending: number
  readonly failed: number
  readonly quarantined: number
  readonly delivered: number
}

export interface StoreStats extends DeliveryStats {
  readonly sources: number
  readonly sessions: number
}

export interface DeliveryCandidate {
  readonly snapshotId: string
  readonly sourceId: string
  readonly payload: SessionSnapshot
  readonly projectOrigin: string | null
  readonly state: "new" | "pending" | "delivered" | "quarantined"
}

export interface DeliveryIssue {
  readonly sourceId: string
  readonly snapshotId: string
  readonly state: "retrying" | "quarantined"
  readonly attempts: number
  readonly lastError: string
  readonly updatedAt: string
  readonly nextRetryAt: string | null
}

export class LocalSessionStore {
  readonly database: Database

  constructor(readonly path: string) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true })
    this.database = new Database(path, { create: true })
    this.database.exec("PRAGMA busy_timeout = 30000")
    this.database.exec("PRAGMA journal_mode = WAL")
    this.database.exec("PRAGMA foreign_keys = ON")
    this.database.exec(`
      CREATE TABLE IF NOT EXISTS collector_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS source_state (
        source_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        local_path TEXT NOT NULL,
        path_hash TEXT NOT NULL,
        size INTEGER NOT NULL,
        modified_ms INTEGER NOT NULL,
        metadata_revision TEXT,
        normalizer_version TEXT,
        revision TEXT,
        generation INTEGER NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS source_state_provider_path_idx
        ON source_state(provider, path_hash);
      CREATE TABLE IF NOT EXISTS export_outbox (
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
      CREATE INDEX IF NOT EXISTS export_outbox_due_idx
        ON export_outbox(next_attempt_at, id);
      CREATE INDEX IF NOT EXISTS export_outbox_source_idx
        ON export_outbox(source_id, id DESC);
      CREATE TABLE IF NOT EXISTS local_snapshots (
        source_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        session_id TEXT NOT NULL,
        snapshot_id TEXT,
        payload TEXT NOT NULL,
        project_origin TEXT,
        updated_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS local_snapshots_provider_idx
        ON local_snapshots(provider, updated_at DESC);
      CREATE TABLE IF NOT EXISTS export_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        destination_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        snapshot_id TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        delivered_at TEXT,
        quarantined_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(destination_id, source_id),
        FOREIGN KEY(source_id) REFERENCES local_snapshots(source_id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS export_deliveries_due_idx
        ON export_deliveries(destination_id, delivered_at, quarantined_at, next_attempt_at, id);
    `)
    this.ensureColumn("export_outbox", "project_origin", "TEXT")
    this.ensureColumn("source_state", "metadata_revision", "TEXT")
    this.ensureColumn("source_state", "normalizer_version", "TEXT")
    this.ensureColumn("local_snapshots", "snapshot_id", "TEXT")
    this.ensureColumn("local_snapshots", "project_origin", "TEXT")
    this.migrateLocalSnapshotMetadata()
  }

  deviceId(): string {
    const row = this.database.query<{ value: string }, []>(
      "SELECT value FROM collector_meta WHERE key = 'device_id'"
    ).get()
    if (row) return row.value
    const id = `device-${randomUUID()}`
    this.database.query(
      "INSERT INTO collector_meta (key, value) VALUES ('device_id', ?)"
    ).run(id)
    return id
  }

  source(provider: Provider, pathHash: string): SourceState | null {
    const row = this.database.query<{
      source_id: string
      provider: Provider
      local_path: string
      path_hash: string
      size: number
      modified_ms: number
      metadata_revision: string | null
      normalizer_version: string | null
      revision: string | null
      generation: number
    }, [Provider, string]>(`
      SELECT source_id, provider, local_path, path_hash, size, modified_ms,
        metadata_revision, normalizer_version, revision, generation
      FROM source_state WHERE provider = ? AND path_hash = ?
    `).get(provider, pathHash)
    return row ? fromSourceRow(row) : null
  }

  unchanged(provider: Provider, pathHash: string, source: SourceFile): boolean {
    const state = this.source(provider, pathHash)
    return state !== null && state.size === source.size &&
      state.modifiedMs === Math.floor(source.modifiedAt.getTime()) &&
      state.normalizerVersion === (source.normalizerVersion ?? null) &&
      (source.metadataRevision === undefined ||
        state.metadataRevision === source.metadataRevision)
  }

  recordIgnored(
    sourceId: string,
    provider: Provider,
    pathHash: string,
    source: SourceFile,
    previousGeneration: number
  ): void {
    this.upsertSource(sourceId, provider, pathHash, source, null, previousGeneration)
  }

  enqueue(
    provider: Provider,
    source: SourceFile,
    batch: SessionSnapshot,
    projectOrigin: string | null = null
  ): boolean {
    const now = new Date().toISOString()
    const operation = this.database.transaction(() => {
      const previous = this.database.query<{ snapshot_id: string | null }, [string]>(
        "SELECT snapshot_id FROM local_snapshots WHERE source_id = ?"
      ).get(batch.source.id)
      this.database.query(`
        INSERT INTO local_snapshots (
          source_id, provider, session_id, snapshot_id, payload, project_origin, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
          provider = excluded.provider,
          session_id = excluded.session_id,
          snapshot_id = excluded.snapshot_id,
          payload = excluded.payload,
          project_origin = excluded.project_origin,
          updated_at = excluded.updated_at
      `).run(
        batch.source.id,
        provider,
        batch.session.id,
        batch.snapshotId,
        JSON.stringify(batch),
        projectOrigin,
        now
      )
      // Every destination that has seen this source must receive the newest
      // generation. Acknowledged delivery history is retained for auditability.
      this.database.query(`
        UPDATE export_deliveries
        SET snapshot_id = ?, attempts = 0, next_attempt_at = 0,
          last_error = NULL, delivered_at = NULL, quarantined_at = NULL,
          updated_at = ?
        WHERE source_id = ? AND snapshot_id <> ?
      `).run(batch.snapshotId, now, batch.source.id, batch.snapshotId)
      this.upsertSource(
        batch.source.id,
        provider,
        batch.source.pathHash,
        source,
        batch.source.revision,
        batch.source.generation
      )
      return previous?.snapshot_id !== batch.snapshotId
    })
    return operation()
  }

  snapshots(provider?: Provider): SessionSnapshot[] {
    const rows = provider
      ? this.database.query<{ payload: string }, [Provider]>(`
          SELECT payload FROM local_snapshots
          WHERE provider = ?
          ORDER BY updated_at DESC
        `).all(provider)
      : this.database.query<{ payload: string }, []>(`
          SELECT payload FROM local_snapshots
          ORDER BY updated_at DESC
        `).all()
    return rows.map((row) => JSON.parse(row.payload) as SessionSnapshot)
  }

  snapshotCount(provider?: Provider): number {
    const row = provider
      ? this.database.query<{ count: number }, [Provider]>(`
          SELECT count(*) AS count FROM local_snapshots WHERE provider = ?
        `).get(provider)
      : this.database.query<{ count: number }, []>(`
          SELECT count(*) AS count FROM local_snapshots
        `).get()
    return Number(row?.count ?? 0)
  }

  prepareDestination(destinationId: string, provider?: Provider): number {
    const now = new Date().toISOString()
    const selection = provider
      ? "WHERE snapshot_id IS NOT NULL AND provider = ?"
      : "WHERE snapshot_id IS NOT NULL"
    const query = `
      INSERT INTO export_deliveries (
        destination_id, source_id, snapshot_id, attempts, next_attempt_at,
        created_at, updated_at
      )
      SELECT ?, source_id, snapshot_id, 0, 0, ?, ?
      FROM local_snapshots
      ${selection}
      ON CONFLICT(destination_id, source_id) DO UPDATE SET
        snapshot_id = excluded.snapshot_id,
        attempts = 0,
        next_attempt_at = 0,
        last_error = NULL,
        delivered_at = NULL,
        quarantined_at = NULL,
        updated_at = excluded.updated_at
      WHERE export_deliveries.snapshot_id <> excluded.snapshot_id
    `
    return provider
      ? this.database.query(query).run(destinationId, now, now, provider).changes
      : this.database.query(query).run(destinationId, now, now).changes
  }

  deliveryCandidates(destinationId: string, provider?: Provider): DeliveryCandidate[] {
    const rows = provider
      ? this.database.query<{
        source_id: string
        snapshot_id: string
        payload: string
        project_origin: string | null
        delivery_snapshot_id: string | null
        delivered_at: string | null
        quarantined_at: string | null
      }, [string, Provider]>(`
        SELECT snapshots.source_id, snapshots.snapshot_id, snapshots.payload,
          snapshots.project_origin,
          deliveries.snapshot_id AS delivery_snapshot_id,
          deliveries.delivered_at, deliveries.quarantined_at
        FROM local_snapshots AS snapshots
        LEFT JOIN export_deliveries AS deliveries
          ON deliveries.destination_id = ? AND deliveries.source_id = snapshots.source_id
        WHERE snapshots.provider = ? AND snapshots.snapshot_id IS NOT NULL
        ORDER BY snapshots.updated_at
      `).all(destinationId, provider)
      : this.database.query<{
        source_id: string
        snapshot_id: string
        payload: string
        project_origin: string | null
        delivery_snapshot_id: string | null
        delivered_at: string | null
        quarantined_at: string | null
      }, [string]>(`
        SELECT snapshots.source_id, snapshots.snapshot_id, snapshots.payload,
          snapshots.project_origin,
          deliveries.snapshot_id AS delivery_snapshot_id,
          deliveries.delivered_at, deliveries.quarantined_at
        FROM local_snapshots AS snapshots
        LEFT JOIN export_deliveries AS deliveries
          ON deliveries.destination_id = ? AND deliveries.source_id = snapshots.source_id
        WHERE snapshots.snapshot_id IS NOT NULL
        ORDER BY snapshots.updated_at
      `).all(destinationId)
    return rows.map((row) => ({
      sourceId: row.source_id,
      snapshotId: row.snapshot_id,
      payload: JSON.parse(row.payload) as SessionSnapshot,
      projectOrigin: row.project_origin,
      state: row.delivery_snapshot_id !== row.snapshot_id
        ? "new"
        : row.quarantined_at
          ? "quarantined"
          : row.delivered_at
            ? "delivered"
            : "pending"
    }))
  }

  pending(
    destinationId: string,
    limit = 25,
    now = Date.now(),
    provider?: Provider
  ): PendingExport[] {
    return this.due(destinationId, now, provider).slice(0, limit)
  }

  due(destinationId: string, now = Date.now(), provider?: Provider): PendingExport[] {
    const rows = provider
      ? this.database.query<{
        id: number
        destination_id: string
        snapshot_id: string
        source_id: string
        payload: string
        project_origin: string | null
        attempts: number
      }, [string, number, Provider]>(`
        SELECT deliveries.id, deliveries.destination_id, deliveries.snapshot_id,
          deliveries.source_id, snapshots.payload, snapshots.project_origin,
          deliveries.attempts
        FROM export_deliveries AS deliveries
        JOIN local_snapshots AS snapshots ON snapshots.source_id = deliveries.source_id
        WHERE deliveries.destination_id = ?
          AND deliveries.delivered_at IS NULL
          AND deliveries.quarantined_at IS NULL
          AND deliveries.next_attempt_at <= ?
          AND snapshots.provider = ?
        ORDER BY deliveries.id
      `).all(destinationId, now, provider)
      : this.database.query<{
      id: number
      destination_id: string
      snapshot_id: string
      source_id: string
      payload: string
      project_origin: string | null
      attempts: number
      }, [string, number]>(`
        SELECT deliveries.id, deliveries.destination_id, deliveries.snapshot_id,
          deliveries.source_id, snapshots.payload, snapshots.project_origin,
          deliveries.attempts
        FROM export_deliveries AS deliveries
        JOIN local_snapshots AS snapshots ON snapshots.source_id = deliveries.source_id
        WHERE deliveries.destination_id = ?
          AND deliveries.delivered_at IS NULL
          AND deliveries.quarantined_at IS NULL
          AND deliveries.next_attempt_at <= ?
        ORDER BY deliveries.id
      `).all(destinationId, now)
    return rows.map((row) => ({
      id: row.id,
      destinationId: row.destination_id,
      snapshotId: row.snapshot_id,
      sourceId: row.source_id,
      payload: JSON.parse(row.payload) as SessionSnapshot,
      projectOrigin: row.project_origin,
      attempts: row.attempts
    }))
  }

  acknowledge(id: number, snapshotId: string): void {
    const now = new Date().toISOString()
    this.database.query(`
      UPDATE export_deliveries
      SET delivered_at = ?, last_error = NULL, updated_at = ?
      WHERE id = ? AND snapshot_id = ?
    `).run(now, now, id, snapshotId)
  }

  fail(id: number, snapshotId: string, attempts: number, error: string): void {
    const delay = Math.min(5 * 60_000, 1_000 * 2 ** Math.min(attempts, 8))
    this.database.query(`
      UPDATE export_deliveries
      SET attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
      WHERE id = ? AND snapshot_id = ?
    `).run(
      attempts,
      Date.now() + delay,
      error.slice(0, 2_000),
      new Date().toISOString(),
      id,
      snapshotId
    )
  }

  quarantine(id: number, snapshotId: string, attempts: number, error: string): void {
    const now = new Date().toISOString()
    this.database.query(`
      UPDATE export_deliveries
      SET attempts = ?, last_error = ?, quarantined_at = ?, updated_at = ?
      WHERE id = ? AND snapshot_id = ?
    `).run(attempts, error.slice(0, 2_000), now, now, id, snapshotId)
  }

  retryDestination(
    destinationId: string,
    provider?: Provider,
    includeQuarantined = false
  ): number {
    if (this.hasIncrementalPipeline()) {
      const providerClause = provider
        ? "AND source_id IN (SELECT source_id FROM pipeline_sources WHERE provider = ?)"
        : ""
      const query = `
        UPDATE pipeline_deliveries
        SET next_attempt_at = 0,
          quarantined_at = CASE WHEN ? THEN NULL ELSE quarantined_at END,
          updated_at = ?
        WHERE destination_id = ? AND delivered_at IS NULL
          AND (quarantined_at IS NULL OR ?)
          ${providerClause}
      `
      return provider
        ? this.database.query(query).run(
            includeQuarantined,
            new Date().toISOString(),
            destinationId,
            includeQuarantined,
            provider
          ).changes
        : this.database.query(query).run(
            includeQuarantined,
            new Date().toISOString(),
            destinationId,
            includeQuarantined
          ).changes
    }
    const providerClause = provider
      ? `AND source_id IN (
          SELECT source_id FROM local_snapshots WHERE provider = ?
        )`
      : ""
    const query = `
      UPDATE export_deliveries
      SET next_attempt_at = 0,
        quarantined_at = CASE WHEN ? THEN NULL ELSE quarantined_at END,
        updated_at = ?
      WHERE destination_id = ? AND delivered_at IS NULL
        AND (quarantined_at IS NULL OR ?)
      ${providerClause}
    `
    return provider
      ? this.database.query(query).run(
        includeQuarantined,
        new Date().toISOString(),
        destinationId,
        includeQuarantined,
        provider
      ).changes
      : this.database.query(query).run(
        includeQuarantined,
        new Date().toISOString(),
        destinationId,
        includeQuarantined
      ).changes
  }

  deliveryIssues(destinationId: string, limit = 10): DeliveryIssue[] {
    if (this.hasIncrementalPipeline()) {
      const rows = this.database.query<{
        source_id: string
        batch_id: string
        attempts: number
        last_error: string
        next_attempt_at: number
        quarantined_at: string | null
        updated_at: string
      }, [string, number]>(`
        SELECT source_id, batch_id, attempts, last_error, next_attempt_at, quarantined_at, updated_at
        FROM pipeline_deliveries
        WHERE destination_id = ? AND delivered_at IS NULL
          AND last_error IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT ?
      `).all(destinationId, limit)
      return rows.map((row) => ({
        sourceId: row.source_id,
        snapshotId: row.batch_id,
        state: row.quarantined_at ? "quarantined" : "retrying",
        attempts: row.attempts,
        lastError: row.last_error,
        updatedAt: row.updated_at,
        nextRetryAt: row.quarantined_at ? null : retryAt(row.next_attempt_at)
      }))
    }
    const rows = this.database.query<{
      source_id: string
      snapshot_id: string
      attempts: number
      last_error: string
      next_attempt_at: number
      quarantined_at: string | null
      updated_at: string
    }, [string, number]>(`
      SELECT source_id, snapshot_id, attempts, last_error, next_attempt_at, quarantined_at, updated_at
      FROM export_deliveries
      WHERE destination_id = ? AND delivered_at IS NULL
        AND last_error IS NOT NULL
      ORDER BY updated_at DESC
      LIMIT ?
    `).all(destinationId, limit)
    return rows.map((row) => ({
      sourceId: row.source_id,
      snapshotId: row.snapshot_id,
      state: row.quarantined_at ? "quarantined" : "retrying",
      attempts: row.attempts,
      lastError: row.last_error,
      updatedAt: row.updated_at,
      nextRetryAt: row.quarantined_at ? null : retryAt(row.next_attempt_at)
    }))
  }

  stats(destinationId?: string): StoreStats {
    if (this.hasIncrementalPipeline()) {
      const pipeline = new IncrementalPipelineStore(this.database)
      const current = pipeline.stats(destinationId)
      const sessions = this.database.query<{ count: number }, []>(
        "SELECT count(*) AS count FROM local_snapshots"
      ).get()?.count ?? 0
      return {
        sources: current.sources,
        sessions: Number(sessions),
        pending: current.pending,
        failed: current.retrying,
        quarantined: current.quarantined,
        delivered: current.delivered
      }
    }
    const delivery = destinationId
      ? this.deliveryStats(destinationId)
      : this.database.query<DeliveryStats, []>(`
        SELECT
          count(*) FILTER (
            WHERE delivered_at IS NULL AND quarantined_at IS NULL
          ) AS pending,
          count(*) FILTER (
            WHERE delivered_at IS NULL AND quarantined_at IS NULL AND attempts > 0
          ) AS failed,
          count(*) FILTER (WHERE quarantined_at IS NOT NULL) AS quarantined,
          count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered
        FROM export_deliveries
      `).get() ?? { pending: 0, failed: 0, quarantined: 0, delivered: 0 }
    const local = this.database.query<{
      sources: number
      sessions: number
    }, []>(`
      SELECT
        (SELECT count(*) FROM source_state) AS sources,
        (SELECT count(*) FROM local_snapshots) AS sessions
    `).get() ?? { sources: 0, sessions: 0 }
    return { ...local, ...delivery }
  }

  close(): void {
    this.database.close()
  }

  private upsertSource(
    sourceId: string,
    provider: Provider,
    pathHash: string,
    source: SourceFile,
    revision: string | null,
    generation: number
  ): void {
    this.database.query(`
      INSERT INTO source_state (
        source_id, provider, local_path, path_hash, size, modified_ms,
        metadata_revision, normalizer_version, revision, generation, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(provider, path_hash) DO UPDATE SET
        source_id = excluded.source_id,
        local_path = excluded.local_path,
        size = excluded.size,
        modified_ms = excluded.modified_ms,
        metadata_revision = excluded.metadata_revision,
        normalizer_version = excluded.normalizer_version,
        revision = excluded.revision,
        generation = excluded.generation,
        updated_at = excluded.updated_at
    `).run(
      sourceId,
      provider,
      source.path,
      pathHash,
      source.size,
      Math.floor(source.modifiedAt.getTime()),
      source.metadataRevision ?? null,
      source.normalizerVersion ?? null,
      revision,
      generation,
      new Date().toISOString()
    )
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const columns = this.database.query<{ name: string }, []>(`PRAGMA table_info(${table})`).all()
    if (columns.some((candidate) => candidate.name === column)) return
    this.database.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
  }

  private deliveryStats(destinationId: string): DeliveryStats {
    return this.database.query<DeliveryStats, [string]>(`
      SELECT
        count(*) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL
        ) AS pending,
        count(*) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL AND attempts > 0
        ) AS failed,
        count(*) FILTER (WHERE quarantined_at IS NOT NULL) AS quarantined,
        count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered
      FROM export_deliveries
      WHERE destination_id = ?
    `).get(destinationId) ?? { pending: 0, failed: 0, quarantined: 0, delivered: 0 }
  }

  private migrateLocalSnapshotMetadata(): void {
    const completed = this.database.query<{ found: number }, [string]>(`
      SELECT 1 AS found FROM collector_meta WHERE key = ?
    `).get(localSnapshotMetadataMigration)
    if (completed) return

    const operation = this.database.transaction(() => {
      const rows = this.database.query<{
        source_id: string
        payload: string
      }, []>(`
        SELECT source_id, payload
        FROM local_snapshots
        WHERE snapshot_id IS NULL
      `).all()
      const update = this.database.query(`
        UPDATE local_snapshots
        SET snapshot_id = ?
        WHERE source_id = ?
      `)
      for (const row of rows) {
        const snapshot = JSON.parse(row.payload) as SessionSnapshot
        update.run(snapshot.snapshotId, row.source_id)
      }
      this.database.exec(`
        UPDATE local_snapshots
        SET project_origin = (
          SELECT project_origin
          FROM export_outbox
          WHERE export_outbox.source_id = local_snapshots.source_id
            AND export_outbox.project_origin IS NOT NULL
          ORDER BY export_outbox.id DESC
          LIMIT 1
        )
        WHERE project_origin IS NULL
          AND EXISTS (
            SELECT 1
            FROM export_outbox
            WHERE export_outbox.source_id = local_snapshots.source_id
              AND export_outbox.project_origin IS NOT NULL
          )
      `)
      this.database.query(`
        INSERT INTO collector_meta (key, value) VALUES (?, 'complete')
      `).run(localSnapshotMetadataMigration)
    })
    operation()
  }

  private hasIncrementalPipeline(): boolean {
    const table = this.database.query<{ found: number }, []>(`
      SELECT 1 AS found
      FROM sqlite_master
      WHERE type = 'table' AND name = 'pipeline_sources'
    `).get()
    if (!table) return false
    return (this.database.query<{ count: number }, []>(
      "SELECT count(*) AS count FROM pipeline_sources"
    ).get()?.count ?? 0) > 0
  }
}

const fromSourceRow = (row: {
  source_id: string
  provider: Provider
  local_path: string
  path_hash: string
  size: number
  modified_ms: number
  metadata_revision: string | null
  normalizer_version: string | null
  revision: string | null
  generation: number
}): SourceState => ({
  sourceId: row.source_id,
  provider: row.provider,
  localPath: row.local_path,
  pathHash: row.path_hash,
  size: Number(row.size),
  modifiedMs: Number(row.modified_ms),
  metadataRevision: row.metadata_revision,
  normalizerVersion: row.normalizer_version,
  revision: row.revision,
  generation: Number(row.generation)
})

const retryAt = (milliseconds: number): string | null =>
  Number.isFinite(milliseconds) && milliseconds > 0
    ? new Date(milliseconds).toISOString()
    : null
