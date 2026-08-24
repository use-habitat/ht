import type { Database } from "bun:sqlite"

import { hash, parserVersion, type Provider, type SourceFile } from "./domain.ts"
import {
  baselineBatch,
  chunkIngestBatch,
  deltaBatch,
  type IngestBatch
} from "./ingest-batch.ts"
import type { SessionSnapshot } from "./snapshot.ts"

export type DeliveryPriority = "backfill" | "live"

export interface PipelineSource {
  readonly sourceId: string
  readonly provider: Provider
  readonly nativeSessionId: string
  readonly localPath: string
  readonly pathHash: string
  readonly classification: SourceFile["classification"]
  readonly projectOrigin: string | null
  readonly epoch: number
  readonly cursorOffset: number
  readonly sourceSize: number
  readonly modifiedMs: number
  readonly metadataRevision: string | null
  readonly normalizerVersion: string
  readonly snapshot: SessionSnapshot
  readonly finalizedAt: string | null
}

export interface PipelineProjectOriginCount {
  readonly origin: string
  readonly sessions: number
}

export interface PipelineCapture {
  readonly captured: boolean
  readonly sourceId: string
  readonly mode: IngestBatch["mode"] | null
  readonly events: number
  readonly cursor: number
  readonly destinations: number
}

export interface PendingBatchDelivery {
  readonly id: number
  readonly destinationId: string
  readonly sourceId: string
  readonly batch: IngestBatch
  readonly attempts: number
}

export interface PipelineRequest {
  readonly id: number
  readonly provider: Provider
  readonly transcriptPath: string
  readonly nativeSessionId: string | null
  readonly projectOrigin: string | null
  readonly reason: string
  readonly attempts: number
  readonly requestedAt: string
}

export type BackfillProviderScope = Provider | "all"

export interface PipelineBackfillRequest {
  readonly destinationId: string
  readonly provider: Provider | null
  readonly reason: string
  readonly attempts: number
  readonly requestedAt: string
}

export interface PipelineBackfillStatus {
  readonly state: "not-scheduled" | "pending" | "retrying" | "complete"
  readonly pending: number
  readonly retrying: number
  readonly nextRetryAt: string | null
  readonly requestedAt: string | null
  readonly completedAt: string | null
  readonly lastError: string | null
}

export interface PipelineStats {
  readonly sources: number
  readonly finalizedSources: number
  readonly requests: number
  readonly backfills: number
  readonly pending: number
  readonly retrying: number
  readonly nextRetryAt: string | null
  readonly quarantined: number
  readonly delivered: number
}

export interface PipelineHarnessState {
  readonly provider: Provider
  readonly selected: boolean
  readonly configuredAt: string
  readonly reviewedAt: string | null
  readonly lastHookReceivedAt: string | null
  readonly lastEvent: string | null
  readonly lastSessionId: string | null
}

export type PipelineDeliveryState = "new" | "pending" | "delivered" | "quarantined"

export class IncrementalPipelineStore {
  constructor(readonly database: Database) {
    database.exec(`
      CREATE TABLE IF NOT EXISTS pipeline_sources (
        source_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        native_session_id TEXT NOT NULL,
        local_path TEXT NOT NULL,
        path_hash TEXT NOT NULL,
        classification TEXT NOT NULL,
        project_origin TEXT,
        epoch INTEGER NOT NULL,
        cursor_offset INTEGER NOT NULL,
        source_size INTEGER NOT NULL,
        modified_ms INTEGER NOT NULL,
        metadata_revision TEXT,
        normalizer_version TEXT NOT NULL,
        snapshot_payload TEXT NOT NULL,
        finalized_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS pipeline_sources_path_idx
        ON pipeline_sources(provider, path_hash);
      CREATE INDEX IF NOT EXISTS pipeline_sources_project_idx
        ON pipeline_sources(project_origin, provider);

      CREATE TABLE IF NOT EXISTS pipeline_batches (
        batch_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        mode TEXT NOT NULL,
        cursor_from INTEGER NOT NULL,
        cursor_to INTEGER NOT NULL,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(source_id) REFERENCES pipeline_sources(source_id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS pipeline_batches_source_cursor_idx
        ON pipeline_batches(source_id, epoch, cursor_from, cursor_to);

      CREATE TABLE IF NOT EXISTS pipeline_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        destination_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        priority INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        delivered_at TEXT,
        quarantined_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(destination_id, batch_id),
        FOREIGN KEY(source_id) REFERENCES pipeline_sources(source_id) ON DELETE CASCADE,
        FOREIGN KEY(batch_id) REFERENCES pipeline_batches(batch_id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS pipeline_deliveries_due_idx
        ON pipeline_deliveries(
          destination_id, delivered_at, quarantined_at, next_attempt_at, id
        );
      CREATE TABLE IF NOT EXISTS pipeline_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT NOT NULL,
        transcript_path TEXT NOT NULL,
        native_session_id TEXT,
        project_origin TEXT,
        reason TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        requested_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(provider, transcript_path)
      );
      CREATE INDEX IF NOT EXISTS pipeline_requests_due_idx
        ON pipeline_requests(next_attempt_at, id);

      CREATE TABLE IF NOT EXISTS pipeline_backfills (
        destination_id TEXT NOT NULL,
        provider_scope TEXT NOT NULL,
        reason TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        requested_at TEXT NOT NULL,
        completed_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(destination_id, provider_scope)
      );
      CREATE INDEX IF NOT EXISTS pipeline_backfills_due_idx
        ON pipeline_backfills(completed_at, next_attempt_at, destination_id);

      CREATE TABLE IF NOT EXISTS pipeline_harnesses (
        provider TEXT PRIMARY KEY,
        selected INTEGER NOT NULL DEFAULT 0,
        configured_at TEXT NOT NULL,
        reviewed_at TEXT,
        last_hook_received_at TEXT,
        last_event TEXT,
        last_session_id TEXT
      );
    `)
    this.ensureColumn("pipeline_deliveries", "priority", "INTEGER NOT NULL DEFAULT 0")
    database.exec(`
      CREATE INDEX IF NOT EXISTS pipeline_deliveries_live_idx
        ON pipeline_deliveries(
          destination_id, delivered_at, quarantined_at, priority DESC, id
        );
    `)
  }

  source(sourceId: string): PipelineSource | null {
    const row = this.database.query<PipelineSourceRow, [string]>(`
      SELECT source_id, provider, native_session_id, local_path, path_hash,
        classification, project_origin, epoch, cursor_offset, source_size,
        modified_ms, metadata_revision, normalizer_version, snapshot_payload,
        finalized_at
      FROM pipeline_sources
      WHERE source_id = ?
    `).get(sourceId)
    return row ? sourceFromRow(row) : null
  }

  sourceAtPath(provider: Provider, pathHash: string): PipelineSource | null {
    const row = this.database.query<PipelineSourceRow, [Provider, string]>(`
      SELECT source_id, provider, native_session_id, local_path, path_hash,
        classification, project_origin, epoch, cursor_offset, source_size,
        modified_ms, metadata_revision, normalizer_version, snapshot_payload,
        finalized_at
      FROM pipeline_sources
      WHERE provider = ? AND path_hash = ?
      ORDER BY updated_at DESC
      LIMIT 1
    `).get(provider, pathHash)
    return row ? sourceFromRow(row) : null
  }

  shouldSkip(provider: Provider, source: SourceFile): boolean {
    const current = this.sourceAtPath(provider, hash(source.path))
    if (!current) return false
    if (current.finalizedAt && source.classification === "archived") return true
    return current.sourceSize === source.size &&
      current.modifiedMs === Math.floor(source.modifiedAt.getTime()) &&
      current.normalizerVersion === (source.normalizerVersion ?? parserVersion) &&
      current.metadataRevision === (source.metadataRevision ?? null)
  }

  capture(input: {
    readonly provider: Provider
    readonly source: SourceFile
    readonly snapshot: SessionSnapshot
    readonly projectOrigin: string | null
    readonly cursorTo: number
    readonly destinationIds?: readonly string[]
    readonly priority?: DeliveryPriority
  }): PipelineCapture {
    const previous = this.source(input.snapshot.source.id)
    const destinationIds = [...new Set(input.destinationIds ?? [])]
    const priority = deliveryPriority(input.priority)
    const previousEvents = new Map(
      previous?.snapshot.events.map((event) => [event.id, JSON.stringify(event)]) ?? []
    )
    const nextEvents = new Map(
      input.snapshot.events.map((event) => [event.id, JSON.stringify(event)])
    )
    const rewritten = previous !== null && (
      input.cursorTo < previous.cursorOffset ||
      [...previousEvents].some(([id, payload]) => nextEvents.get(id) !== payload)
    )
    const epoch = previous
      ? previous.epoch + (rewritten ? 1 : 0)
      : this.initialEpoch(input.provider, input.snapshot.source.pathHash)
    const newlyTerminal = isTerminal(input.snapshot.session.status) &&
      !isTerminal(previous?.snapshot.session.status)
    const events = previous && !rewritten
      ? input.snapshot.events.filter((event) => !previousEvents.has(event.id))
      : input.snapshot.events
    const changed = !previous || rewritten || newlyTerminal ||
      input.cursorTo !== previous.cursorOffset ||
      events.length > 0 ||
      input.snapshot.snapshotId !== previous.snapshot.snapshotId ||
      JSON.stringify(input.snapshot.session) !== JSON.stringify(previous.snapshot.session)
    const mode: IngestBatch["mode"] | null = !changed
      ? null
      : !previous || rewritten
        ? "baseline"
        : newlyTerminal
          ? "final"
          : "delta"
    const batch = mode === "baseline"
      ? baselineBatch(input.snapshot, input.cursorTo, epoch)
      : mode
        ? deltaBatch(input.snapshot, {
            cursorFrom: previous!.cursorOffset,
            cursorTo: input.cursorTo,
            epoch,
            events,
            final: mode === "final"
          })
        : null

    const now = new Date().toISOString()
    const finalizedAt = isTerminal(input.snapshot.session.status)
      ? previous?.finalizedAt ?? now
      : null
    let destinations = 0
    const transaction = this.database.transaction(() => {
      this.database.query(`
        INSERT INTO pipeline_sources (
          source_id, provider, native_session_id, local_path, path_hash,
          classification, project_origin, epoch, cursor_offset, source_size,
          modified_ms, metadata_revision, normalizer_version, snapshot_payload,
          finalized_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
          provider = excluded.provider,
          native_session_id = excluded.native_session_id,
          local_path = excluded.local_path,
          path_hash = excluded.path_hash,
          classification = excluded.classification,
          project_origin = excluded.project_origin,
          epoch = excluded.epoch,
          cursor_offset = excluded.cursor_offset,
          source_size = excluded.source_size,
          modified_ms = excluded.modified_ms,
          metadata_revision = excluded.metadata_revision,
          normalizer_version = excluded.normalizer_version,
          snapshot_payload = excluded.snapshot_payload,
          finalized_at = excluded.finalized_at,
          updated_at = excluded.updated_at
      `).run(
        input.snapshot.source.id,
        input.provider,
        input.snapshot.source.nativeSessionId,
        input.source.path,
        input.snapshot.source.pathHash,
        input.source.classification,
        input.projectOrigin,
        epoch,
        input.cursorTo,
        input.source.size,
        Math.floor(input.source.modifiedAt.getTime()),
        input.source.metadataRevision ?? null,
        input.source.normalizerVersion ?? parserVersion,
        JSON.stringify(input.snapshot),
        finalizedAt,
        now,
        now
      )
      this.saveBrowsableSnapshot(input.provider, input.snapshot, input.projectOrigin, now)

      if (batch) {
        const batches = chunkIngestBatch(batch)
        const established = destinationIds.filter((destinationId) =>
          this.hasDeliveryHistory(destinationId, input.snapshot.source.id)
        )
        if (established.length > 0) {
          this.saveBatches(batches, now)
          for (const destinationId of established) {
            this.promotePendingSource(destinationId, input.snapshot.source.id, priority, now)
            this.queueBatches(destinationId, batches, now, priority)
            destinations += batches.length
          }
        }
      }
      for (const destinationId of destinationIds) {
        if (this.hasDeliveryHistory(destinationId, input.snapshot.source.id)) continue
        const batches = chunkIngestBatch(
          baselineBatch(input.snapshot, input.cursorTo, epoch)
        )
        this.saveBatches(batches, now)
        this.promotePendingSource(destinationId, input.snapshot.source.id, priority, now)
        this.queueBatches(destinationId, batches, now, priority)
        destinations += batches.length
      }
    })
    transaction()
    return {
      captured: batch !== null,
      sourceId: input.snapshot.source.id,
      mode,
      events: batch?.events.length ?? 0,
      cursor: input.cursorTo,
      destinations
    }
  }

  prepareDestination(
    destinationId: string,
    options: {
      readonly provider?: Provider
      readonly allows?: (snapshot: SessionSnapshot, projectOrigin: string | null) => boolean
    } = {}
  ): number {
    const now = new Date().toISOString()
    let queued = 0
    for (const sourceId of this.sourceIds(options.provider)) {
      const originalSource = this.source(sourceId)
      if (!originalSource || options.allows && !options.allows(
        originalSource.snapshot,
        originalSource.projectOrigin
      )) continue
      const transaction = this.database.transaction(() => {
        const source = this.rechunkPendingBaseline(destinationId, originalSource, now)
        const progress = this.destinationProgress(destinationId, source.sourceId)
        if (
          progress &&
          progress.epoch >= source.epoch &&
          progress.cursor >= source.cursorOffset
        ) return 0
        const epoch = progress
          ? Math.max(source.epoch, progress.epoch + 1)
          : source.epoch
        if (epoch !== source.epoch) {
          this.database.query(`
            UPDATE pipeline_sources
            SET epoch = ?, updated_at = ?
            WHERE source_id = ?
          `).run(epoch, now, source.sourceId)
        }
        const batches = chunkIngestBatch(
          baselineBatch(source.snapshot, source.cursorOffset, epoch)
        )
        this.saveBatches(batches, now)
        this.queueBatches(destinationId, batches, now)
        return batches.length
      })
      queued += transaction()
    }
    return queued
  }

  removePendingOutside(
    destinationId: string,
    allows: (snapshot: SessionSnapshot, projectOrigin: string | null) => boolean,
    provider?: Provider
  ): number {
    let removed = 0
    for (const sourceId of this.pendingSourceIds(destinationId, provider)) {
      const source = this.source(sourceId)
      if (!source || allows(source.snapshot, source.projectOrigin)) continue
      removed += this.database.query(`
        DELETE FROM pipeline_deliveries
        WHERE destination_id = ? AND source_id = ?
          AND delivered_at IS NULL
      `).run(destinationId, source.sourceId).changes
    }
    return removed
  }

  countOutside(
    allows: (snapshot: SessionSnapshot, projectOrigin: string | null) => boolean,
    provider?: Provider
  ): number {
    let withheld = 0
    for (const sourceId of this.sourceIds(provider)) {
      const source = this.source(sourceId)
      if (source && !allows(source.snapshot, source.projectOrigin)) withheld += 1
    }
    return withheld
  }

  deliveryState(destinationId: string, sourceId: string): PipelineDeliveryState {
    const row = this.database.query<{
      pending: number
      delivered: number
      quarantined: number
    }, [string, string]>(`
      SELECT
        count(*) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL
        ) AS pending,
        count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered,
        count(*) FILTER (WHERE quarantined_at IS NOT NULL) AS quarantined
      FROM pipeline_deliveries
      WHERE destination_id = ? AND source_id = ?
    `).get(destinationId, sourceId)
    if (Number(row?.pending ?? 0) > 0) return "pending"
    if (Number(row?.quarantined ?? 0) > 0) return "quarantined"
    if (Number(row?.delivered ?? 0) > 0) return "delivered"
    return "new"
  }

  due(destinationId: string, limit = 25, provider?: Provider): PendingBatchDelivery[] {
    const providerClause = provider ? "AND sources.provider = ?" : ""
    const query = `
      SELECT deliveries.id, deliveries.destination_id, deliveries.source_id,
        deliveries.attempts, batches.payload
      FROM pipeline_deliveries AS deliveries
      JOIN pipeline_batches AS batches ON batches.batch_id = deliveries.batch_id
      JOIN pipeline_sources AS sources ON sources.source_id = deliveries.source_id
      WHERE deliveries.destination_id = ?
        AND deliveries.delivered_at IS NULL
        AND deliveries.quarantined_at IS NULL
        AND deliveries.next_attempt_at <= ?
        ${providerClause}
        AND NOT EXISTS (
          SELECT 1
          FROM pipeline_deliveries AS earlier
          WHERE earlier.destination_id = deliveries.destination_id
            AND earlier.source_id = deliveries.source_id
            AND earlier.id < deliveries.id
            AND earlier.delivered_at IS NULL
            AND earlier.quarantined_at IS NULL
        )
      ORDER BY deliveries.priority DESC, deliveries.id
      LIMIT ?
    `
    const rows = provider
      ? this.database.query<PendingDeliveryRow, [string, number, Provider, number]>(
          query
        ).all(destinationId, Date.now(), provider, limit)
      : this.database.query<PendingDeliveryRow, [string, number, number]>(
          query
        ).all(destinationId, Date.now(), limit)
    return rows.map((row) => ({
      id: row.id,
      destinationId: row.destination_id,
      sourceId: row.source_id,
      batch: JSON.parse(row.payload) as IngestBatch,
      attempts: row.attempts
    }))
  }

  acknowledge(delivery: PendingBatchDelivery): void {
    const now = new Date().toISOString()
    this.database.query(`
      UPDATE pipeline_deliveries
      SET delivered_at = ?, last_error = NULL, updated_at = ?
      WHERE id = ? AND batch_id = ?
    `).run(now, now, delivery.id, delivery.batch.batchId)
  }

  fail(
    delivery: PendingBatchDelivery,
    error: string,
    retryable: boolean
  ): void {
    const attempts = delivery.attempts + 1
    const now = new Date().toISOString()
    if (!retryable) {
      this.database.query(`
        UPDATE pipeline_deliveries
        SET attempts = ?, last_error = ?, quarantined_at = ?, updated_at = ?
        WHERE id = ? AND batch_id = ?
      `).run(
        attempts,
        error.slice(0, 2_000),
        now,
        now,
        delivery.id,
        delivery.batch.batchId
      )
      return
    }
    const delay = retryDelay(attempts)
    this.database.query(`
      UPDATE pipeline_deliveries
      SET attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
      WHERE id = ? AND batch_id = ?
    `).run(
      attempts,
      Date.now() + delay,
      error.slice(0, 2_000),
      now,
      delivery.id,
      delivery.batch.batchId
    )
  }

  retryDestination(destinationId: string, includeQuarantined = false): number {
    const now = new Date().toISOString()
    return this.database.query(`
      UPDATE pipeline_deliveries
      SET next_attempt_at = 0,
        quarantined_at = CASE WHEN ? THEN NULL ELSE quarantined_at END,
        updated_at = ?
      WHERE destination_id = ? AND delivered_at IS NULL
        AND (quarantined_at IS NULL OR ?)
    `).run(includeQuarantined, now, destinationId, includeQuarantined).changes
  }

  rebaseDestination(
    destinationId: string,
    sourceId: string,
    expectedEpoch?: number
  ): readonly IngestBatch[] {
    const source = this.source(sourceId)
    if (!source) throw new Error(`Local source ${sourceId} is missing.`)
    const epoch = Math.max(
      source.epoch + 1,
      Number.isInteger(expectedEpoch) ? expectedEpoch! : 1
    )
    const now = new Date().toISOString()
    const priority = this.pendingSourcePriority(destinationId, sourceId)
    const batches = chunkIngestBatch(
      baselineBatch(source.snapshot, source.cursorOffset, epoch)
    )
    const transaction = this.database.transaction(() => {
      this.database.query(`
        UPDATE pipeline_sources
        SET epoch = ?, updated_at = ?
        WHERE source_id = ?
      `).run(epoch, now, sourceId)
      this.database.query(`
        DELETE FROM pipeline_deliveries
        WHERE destination_id = ? AND source_id = ? AND delivered_at IS NULL
      `).run(destinationId, sourceId)
      this.saveBatches(batches, now)
      this.queueBatches(destinationId, batches, now, priority)
    })
    transaction()
    return batches
  }

  enqueueRequest(input: {
    readonly provider: Provider
    readonly transcriptPath: string
    readonly nativeSessionId?: string | null
    readonly projectOrigin?: string | null
    readonly reason?: string
  }): void {
    const now = new Date().toISOString()
    this.database.query(`
      INSERT INTO pipeline_requests (
        provider, transcript_path, native_session_id, project_origin, reason,
        attempts, next_attempt_at, requested_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
      ON CONFLICT(provider, transcript_path) DO UPDATE SET
        native_session_id = COALESCE(excluded.native_session_id, native_session_id),
        project_origin = COALESCE(excluded.project_origin, project_origin),
        reason = excluded.reason,
        next_attempt_at = 0,
        last_error = NULL,
        requested_at = excluded.requested_at,
        updated_at = excluded.updated_at
    `).run(
      input.provider,
      input.transcriptPath,
      input.nativeSessionId ?? null,
      input.projectOrigin ?? null,
      input.reason ?? "hook",
      now,
      now
    )
  }

  enqueueBackfill(input: {
    readonly destinationId: string
    readonly provider?: Provider
    readonly reason?: string
  }): void {
    const now = new Date().toISOString()
    this.database.query(`
      INSERT INTO pipeline_backfills (
        destination_id, provider_scope, reason, attempts, next_attempt_at,
        requested_at, completed_at, updated_at
      ) VALUES (?, ?, ?, 0, 0, ?, NULL, ?)
      ON CONFLICT(destination_id, provider_scope) DO UPDATE SET
        reason = excluded.reason,
        attempts = 0,
        next_attempt_at = 0,
        last_error = NULL,
        requested_at = excluded.requested_at,
        completed_at = NULL,
        updated_at = excluded.updated_at
    `).run(
      input.destinationId,
      input.provider ?? "all",
      input.reason ?? "setup",
      now,
      now
    )
  }

  configureHarnesses(providers: readonly Provider[]): void {
    const now = new Date().toISOString()
    const selected = new Set(providers)
    const transaction = this.database.transaction(() => {
      for (const provider of selected) {
        this.database.query(`
          INSERT INTO pipeline_harnesses (
            provider, selected, configured_at
          ) VALUES (?, 1, ?)
          ON CONFLICT(provider) DO UPDATE SET
            selected = 1,
            configured_at = CASE
              WHEN pipeline_harnesses.selected = 0 THEN excluded.configured_at
              ELSE pipeline_harnesses.configured_at
            END,
            reviewed_at = CASE
              WHEN pipeline_harnesses.selected = 0 THEN NULL
              ELSE pipeline_harnesses.reviewed_at
            END,
            last_hook_received_at = CASE
              WHEN pipeline_harnesses.selected = 0 THEN NULL
              ELSE pipeline_harnesses.last_hook_received_at
            END,
            last_event = CASE
              WHEN pipeline_harnesses.selected = 0 THEN NULL
              ELSE pipeline_harnesses.last_event
            END,
            last_session_id = CASE
              WHEN pipeline_harnesses.selected = 0 THEN NULL
              ELSE pipeline_harnesses.last_session_id
            END
        `).run(provider, now)
      }
      for (const state of this.harnesses(true)) {
        if (selected.has(state.provider)) continue
        this.database.query(`
          UPDATE pipeline_harnesses
          SET selected = 0
          WHERE provider = ?
        `).run(state.provider)
      }
    })
    transaction()
  }

  recordHookObservation(
    provider: Provider,
    event: string,
    nativeSessionId: string | null
  ): void {
    const now = new Date().toISOString()
    this.database.query(`
      INSERT INTO pipeline_harnesses (
        provider, selected, configured_at, reviewed_at,
        last_hook_received_at, last_event, last_session_id
      ) VALUES (?, 0, ?, ?, ?, ?, ?)
      ON CONFLICT(provider) DO UPDATE SET
        reviewed_at = COALESCE(pipeline_harnesses.reviewed_at, excluded.reviewed_at),
        last_hook_received_at = excluded.last_hook_received_at,
        last_event = excluded.last_event,
        last_session_id = excluded.last_session_id
    `).run(provider, now, now, now, event, nativeSessionId)
  }

  harnesses(selectedOnly = false): PipelineHarnessState[] {
    const where = selectedOnly ? "WHERE selected = 1" : ""
    return this.database.query<PipelineHarnessRow, []>(`
      SELECT provider, selected, configured_at, reviewed_at,
        last_hook_received_at, last_event, last_session_id
      FROM pipeline_harnesses
      ${where}
      ORDER BY provider
    `).all().map((row) => ({
      provider: row.provider,
      selected: row.selected === 1,
      configuredAt: row.configured_at,
      reviewedAt: row.reviewed_at,
      lastHookReceivedAt: row.last_hook_received_at,
      lastEvent: row.last_event,
      lastSessionId: row.last_session_id
    }))
  }

  dueRequests(limit = 100): PipelineRequest[] {
    return this.database.query<PipelineRequestRow, [number, number]>(`
      SELECT id, provider, transcript_path, native_session_id, project_origin,
        reason, attempts, requested_at
      FROM pipeline_requests
      WHERE next_attempt_at <= ?
      ORDER BY id
      LIMIT ?
    `).all(Date.now(), limit).map((row) => ({
      id: row.id,
      provider: row.provider,
      transcriptPath: row.transcript_path,
      nativeSessionId: row.native_session_id,
      projectOrigin: row.project_origin,
      reason: row.reason,
      attempts: row.attempts,
      requestedAt: row.requested_at
    }))
  }

  acknowledgeRequest(request: PipelineRequest): void {
    this.database.query(`
      DELETE FROM pipeline_requests
      WHERE id = ? AND requested_at = ?
    `).run(request.id, request.requestedAt)
  }

  failRequest(request: PipelineRequest, error: string): void {
    const attempts = request.attempts + 1
    const delay = retryDelay(attempts)
    this.database.query(`
      UPDATE pipeline_requests
      SET attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
      WHERE id = ? AND requested_at = ?
    `).run(
      attempts,
      Date.now() + delay,
      error.slice(0, 2_000),
      new Date().toISOString(),
      request.id,
      request.requestedAt
    )
  }

  dueBackfills(limit = 100): PipelineBackfillRequest[] {
    return this.database.query<PipelineBackfillRow, [number, number]>(`
      SELECT destination_id, provider_scope, reason, attempts, requested_at
      FROM pipeline_backfills
      WHERE completed_at IS NULL AND next_attempt_at <= ?
      ORDER BY requested_at, destination_id, provider_scope
      LIMIT ?
    `).all(Date.now(), limit).map((row) => ({
      destinationId: row.destination_id,
      provider: row.provider_scope === "all" ? null : row.provider_scope,
      reason: row.reason,
      attempts: row.attempts,
      requestedAt: row.requested_at
    }))
  }

  acknowledgeBackfill(request: PipelineBackfillRequest): void {
    const now = new Date().toISOString()
    this.database.query(`
      UPDATE pipeline_backfills
      SET completed_at = ?, last_error = NULL, updated_at = ?
      WHERE destination_id = ? AND provider_scope = ?
        AND requested_at = ? AND completed_at IS NULL
    `).run(
      now,
      now,
      request.destinationId,
      request.provider ?? "all",
      request.requestedAt
    )
  }

  failBackfill(request: PipelineBackfillRequest, error: string): void {
    const attempts = request.attempts + 1
    this.database.query(`
      UPDATE pipeline_backfills
      SET attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
      WHERE destination_id = ? AND provider_scope = ?
        AND requested_at = ? AND completed_at IS NULL
    `).run(
      attempts,
      Date.now() + retryDelay(attempts),
      error.slice(0, 2_000),
      new Date().toISOString(),
      request.destinationId,
      request.provider ?? "all",
      request.requestedAt
    )
  }

  backfillStatus(destinationId: string): PipelineBackfillStatus {
    const rows = this.database.query<PipelineBackfillStatusRow, [string]>(`
      SELECT attempts, next_attempt_at, last_error, requested_at, completed_at
      FROM pipeline_backfills
      WHERE destination_id = ?
      ORDER BY requested_at DESC
    `).all(destinationId)
    if (rows.length === 0) {
      return {
        state: "not-scheduled",
        pending: 0,
        retrying: 0,
        nextRetryAt: null,
        requestedAt: null,
        completedAt: null,
        lastError: null
      }
    }
    const pending = rows.filter((row) => row.completed_at === null)
    const retrying = pending.filter((row) => row.attempts > 0)
    const latest = rows[0]!
    const nextRetryAt = pending.reduce<number | null>((earliest, row) => {
      const candidate = Number(row.next_attempt_at)
      if (!Number.isFinite(candidate) || candidate <= 0) return earliest
      return earliest === null ? candidate : Math.min(earliest, candidate)
    }, null)
    return {
      state: pending.length === 0
        ? "complete"
        : retrying.length > 0
          ? "retrying"
          : "pending",
      pending: pending.length,
      retrying: retrying.length,
      nextRetryAt: retryTimestamp(nextRetryAt),
      requestedAt: latest.requested_at,
      completedAt: latest.completed_at,
      lastError: pending.find((row) => row.last_error)?.last_error ?? null
    }
  }

  stats(destinationId?: string): PipelineStats {
    const source = this.database.query<{
      sources: number
      finalized_sources: number
    }, []>(`
      SELECT count(*) AS sources,
        count(*) FILTER (WHERE finalized_at IS NOT NULL) AS finalized_sources
      FROM pipeline_sources
    `).get() ?? { sources: 0, finalized_sources: 0 }
    const requests = this.database.query<{ count: number }, []>(
      "SELECT count(*) AS count FROM pipeline_requests"
    ).get()?.count ?? 0
    const backfills = destinationId
      ? this.database.query<{ count: number }, [string]>(`
          SELECT count(*) AS count
          FROM pipeline_backfills
          WHERE completed_at IS NULL AND destination_id = ?
        `).get(destinationId)?.count ?? 0
      : this.database.query<{ count: number }, []>(`
          SELECT count(*) AS count
          FROM pipeline_backfills
          WHERE completed_at IS NULL
        `).get()?.count ?? 0
    const destinationClause = destinationId ? "WHERE destination_id = ?" : ""
    const deliveryQuery = `
      SELECT
        count(*) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL
        ) AS pending,
        count(*) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL AND attempts > 0
        ) AS retrying,
        min(next_attempt_at) FILTER (
          WHERE delivered_at IS NULL AND quarantined_at IS NULL AND attempts > 0
        ) AS next_retry_at,
        count(*) FILTER (WHERE quarantined_at IS NOT NULL) AS quarantined,
        count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered
      FROM pipeline_deliveries
      ${destinationClause}
    `
    const delivery = destinationId
      ? this.database.query<PipelineDeliveryStats, [string]>(deliveryQuery).get(destinationId)
      : this.database.query<PipelineDeliveryStats, []>(deliveryQuery).get()
    return {
      sources: Number(source.sources),
      finalizedSources: Number(source.finalized_sources),
      requests: Number(requests),
      backfills: Number(backfills),
      pending: Number(delivery?.pending ?? 0),
      retrying: Number(delivery?.retrying ?? 0),
      nextRetryAt: retryTimestamp(delivery?.next_retry_at),
      quarantined: Number(delivery?.quarantined ?? 0),
      delivered: Number(delivery?.delivered ?? 0)
    }
  }

  sources(provider?: Provider): PipelineSource[] {
    const rows = provider
      ? this.database.query<PipelineSourceRow, [Provider]>(`
          SELECT source_id, provider, native_session_id, local_path, path_hash,
            classification, project_origin, epoch, cursor_offset, source_size,
            modified_ms, metadata_revision, normalizer_version, snapshot_payload,
            finalized_at
          FROM pipeline_sources
          WHERE provider = ?
          ORDER BY updated_at
        `).all(provider)
      : this.database.query<PipelineSourceRow, []>(`
          SELECT source_id, provider, native_session_id, local_path, path_hash,
            classification, project_origin, epoch, cursor_offset, source_size,
            modified_ms, metadata_revision, normalizer_version, snapshot_payload,
            finalized_at
          FROM pipeline_sources
          ORDER BY updated_at
        `).all()
    return rows.map(sourceFromRow)
  }

  projectOriginCounts(provider?: Provider): PipelineProjectOriginCount[] {
    const rows = provider
      ? this.database.query<{
          project_origin: string
          sessions: number
        }, [Provider]>(`
          SELECT project_origin, count(*) AS sessions
          FROM pipeline_sources
          WHERE provider = ? AND project_origin IS NOT NULL
            AND trim(project_origin) != ''
          GROUP BY project_origin
          ORDER BY sessions DESC, project_origin
        `).all(provider)
      : this.database.query<{
          project_origin: string
          sessions: number
        }, []>(`
          SELECT project_origin, count(*) AS sessions
          FROM pipeline_sources
          WHERE project_origin IS NOT NULL AND trim(project_origin) != ''
          GROUP BY project_origin
          ORDER BY sessions DESC, project_origin
        `).all()
    return rows.map((row) => ({
      origin: row.project_origin,
      sessions: Number(row.sessions)
    }))
  }

  private sourceIds(provider?: Provider): string[] {
    const rows = provider
      ? this.database.query<{ source_id: string }, [Provider]>(`
          SELECT source_id FROM pipeline_sources WHERE provider = ? ORDER BY updated_at
        `).all(provider)
      : this.database.query<{ source_id: string }, []>(`
          SELECT source_id FROM pipeline_sources ORDER BY updated_at
        `).all()
    return rows.map((row) => row.source_id)
  }

  private pendingSourceIds(destinationId: string, provider?: Provider): string[] {
    const providerClause = provider ? "AND sources.provider = ?" : ""
    const query = `
      SELECT DISTINCT sources.source_id
      FROM pipeline_sources AS sources
      JOIN pipeline_deliveries AS deliveries
        ON deliveries.source_id = sources.source_id
      WHERE deliveries.destination_id = ?
        AND deliveries.delivered_at IS NULL
        ${providerClause}
    `
    const rows = provider
      ? this.database.query<{ source_id: string }, [string, Provider]>(query)
          .all(destinationId, provider)
      : this.database.query<{ source_id: string }, [string]>(query)
          .all(destinationId)
    return rows.map((row) => row.source_id)
  }

  private initialEpoch(provider: Provider, pathHash: string): number {
    const legacy = this.database.query<{ generation: number }, [Provider, string]>(`
      SELECT generation
      FROM source_state
      WHERE provider = ? AND path_hash = ?
    `).get(provider, pathHash)
    return Math.max(1, Number(legacy?.generation ?? 0) + 1)
  }

  private hasDeliveryHistory(destinationId: string, sourceId: string): boolean {
    return this.database.query<{ found: number }, [string, string]>(`
      SELECT 1 AS found
      FROM pipeline_deliveries
      WHERE destination_id = ? AND source_id = ?
      LIMIT 1
    `).get(destinationId, sourceId) !== null
  }

  private destinationProgress(
    destinationId: string,
    sourceId: string
  ): { epoch: number; cursor: number } | null {
    const row = this.database.query<{
      epoch: number | null
      cursor: number | null
    }, [string, string]>(`
      SELECT batches.epoch, batches.cursor_to AS cursor
      FROM pipeline_deliveries AS deliveries
      JOIN pipeline_batches AS batches ON batches.batch_id = deliveries.batch_id
      WHERE deliveries.destination_id = ? AND deliveries.source_id = ?
        AND deliveries.quarantined_at IS NULL
      ORDER BY batches.epoch DESC, deliveries.id DESC
      LIMIT 1
    `).get(destinationId, sourceId)
    return row?.epoch === null || row?.epoch === undefined
      ? null
      : { epoch: Number(row.epoch), cursor: Number(row.cursor ?? 0) }
  }

  /**
   * Upgrade the only unsafe v0.4 queue shape in place. A setup that was
   * interrupted before its first upload can contain a single huge baseline;
   * replace it with ordered chunks and a new epoch before retrying it.
   */
  private rechunkPendingBaseline(
    destinationId: string,
    source: PipelineSource,
    now: string
  ): PipelineSource {
    const hasDelivered = this.database.query<{ found: number }, [string]>(`
      SELECT 1 AS found
      FROM pipeline_deliveries
      WHERE source_id = ? AND delivered_at IS NOT NULL
      LIMIT 1
    `).get(source.sourceId) !== null
    if (hasDelivered) return source
    const pending = this.database.query<{ payload: string }, [string, string]>(`
      SELECT batches.payload
      FROM pipeline_deliveries AS deliveries
      JOIN pipeline_batches AS batches ON batches.batch_id = deliveries.batch_id
      WHERE deliveries.destination_id = ? AND deliveries.source_id = ?
        AND deliveries.delivered_at IS NULL
        AND deliveries.quarantined_at IS NULL
    `).all(destinationId, source.sourceId)
    const containsMonolith = pending.some(({ payload }) => {
      const batch = JSON.parse(payload) as IngestBatch
      return batch.mode === "baseline" && chunkIngestBatch(batch).length > 1
    })
    if (!containsMonolith) return source

    const epoch = source.epoch + 1
    // The old baseline may have reached the server just before the process was
    // interrupted. A new epoch makes the replacement safe and idempotent.
    this.database.query(`
      DELETE FROM pipeline_deliveries
      WHERE source_id = ? AND delivered_at IS NULL
    `).run(source.sourceId)
    this.database.query(`
      UPDATE pipeline_sources
      SET epoch = ?, updated_at = ?
      WHERE source_id = ?
    `).run(epoch, now, source.sourceId)
    return { ...source, epoch }
  }

  private saveBatches(batches: readonly IngestBatch[], now: string): void {
    for (const batch of batches) this.saveBatch(batch, now)
  }

  private queueBatches(
    destinationId: string,
    batches: readonly IngestBatch[],
    now: string,
    priority = 0
  ): void {
    for (const batch of batches) this.queueDelivery(destinationId, batch, now, priority)
  }

  private saveBatch(batch: IngestBatch, now: string): void {
    if (batch.mode === "baseline") {
      this.database.query(`
        DELETE FROM pipeline_deliveries
        WHERE source_id = ? AND delivered_at IS NULL
          AND batch_id IN (
            SELECT batch_id FROM pipeline_batches
            WHERE source_id = ? AND epoch < ?
          )
      `).run(batch.source.id, batch.source.id, batch.source.epoch)
    }
    this.database.query(`
      INSERT INTO pipeline_batches (
        batch_id, source_id, epoch, mode, cursor_from, cursor_to, payload, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(batch_id) DO NOTHING
    `).run(
      batch.batchId,
      batch.source.id,
      batch.source.epoch,
      batch.mode,
      batch.cursor.from,
      batch.cursor.to,
      JSON.stringify(batch),
      now
    )
  }

  private queueDelivery(
    destinationId: string,
    batch: IngestBatch,
    now: string,
    priority: number
  ): void {
    this.database.query(`
      INSERT INTO pipeline_deliveries (
        destination_id, source_id, batch_id, attempts, priority, next_attempt_at,
        created_at, updated_at
      ) VALUES (?, ?, ?, 0, ?, 0, ?, ?)
      ON CONFLICT(destination_id, batch_id) DO UPDATE SET
        priority = MAX(pipeline_deliveries.priority, excluded.priority)
    `).run(destinationId, batch.source.id, batch.batchId, priority, now, now)
  }

  nextAttemptAt(): number | null {
    const deliveries = this.database.query<{ next_attempt_at: number | null }, []>(`
      SELECT min(deliveries.next_attempt_at) AS next_attempt_at
      FROM pipeline_deliveries AS deliveries
      WHERE deliveries.delivered_at IS NULL AND deliveries.quarantined_at IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM pipeline_deliveries AS earlier
          WHERE earlier.destination_id = deliveries.destination_id
            AND earlier.source_id = deliveries.source_id
            AND earlier.id < deliveries.id
            AND earlier.delivered_at IS NULL
            AND earlier.quarantined_at IS NULL
        )
    `).get()?.next_attempt_at
    const requests = this.database.query<{ next_attempt_at: number | null }, []>(`
      SELECT min(next_attempt_at) AS next_attempt_at
      FROM pipeline_requests
    `).get()?.next_attempt_at
    const backfills = this.database.query<{ next_attempt_at: number | null }, []>(`
      SELECT min(next_attempt_at) AS next_attempt_at
      FROM pipeline_backfills
      WHERE completed_at IS NULL
    `).get()?.next_attempt_at
    const candidates = [deliveries, requests, backfills]
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value >= 0)
    return candidates.length > 0 ? Math.min(...candidates) : null
  }

  private promotePendingSource(
    destinationId: string,
    sourceId: string,
    priority: number,
    now: string
  ): void {
    if (priority === 0) return
    this.database.query(`
      UPDATE pipeline_deliveries
      SET priority = MAX(priority, ?), updated_at = ?
      WHERE destination_id = ? AND source_id = ?
        AND delivered_at IS NULL AND quarantined_at IS NULL
    `).run(priority, now, destinationId, sourceId)
  }

  private pendingSourcePriority(destinationId: string, sourceId: string): number {
    const row = this.database.query<{ priority: number | null }, [string, string]>(`
      SELECT max(priority) AS priority
      FROM pipeline_deliveries
      WHERE destination_id = ? AND source_id = ?
        AND delivered_at IS NULL AND quarantined_at IS NULL
    `).get(destinationId, sourceId)
    return Number(row?.priority ?? 0)
  }

  private ensureColumn(table: "pipeline_deliveries", column: string, definition: string): void {
    const columns = this.database.query<{ name: string }, []>(`PRAGMA table_info(${table})`).all()
    if (columns.some((candidate) => candidate.name === column)) return
    this.database.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
  }

  private saveBrowsableSnapshot(
    provider: Provider,
    snapshot: SessionSnapshot,
    projectOrigin: string | null,
    now: string
  ): void {
    this.database.query(`
      DELETE FROM local_snapshots
      WHERE provider = ? AND session_id = ? AND source_id <> ?
    `).run(provider, snapshot.session.id, snapshot.source.id)
    this.database.query(`
      INSERT INTO local_snapshots (
        source_id, provider, session_id, snapshot_id, payload, project_origin, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(source_id) DO UPDATE SET
        provider = excluded.provider,
        session_id = excluded.session_id,
        snapshot_id = excluded.snapshot_id,
        payload = excluded.payload,
        project_origin = excluded.project_origin,
        updated_at = excluded.updated_at
    `).run(
      snapshot.source.id,
      provider,
      snapshot.session.id,
      snapshot.snapshotId,
      JSON.stringify(snapshot),
      projectOrigin,
      now
    )
  }
}

interface PipelineSourceRow {
  readonly source_id: string
  readonly provider: Provider
  readonly native_session_id: string
  readonly local_path: string
  readonly path_hash: string
  readonly classification: SourceFile["classification"]
  readonly project_origin: string | null
  readonly epoch: number
  readonly cursor_offset: number
  readonly source_size: number
  readonly modified_ms: number
  readonly metadata_revision: string | null
  readonly normalizer_version: string
  readonly snapshot_payload: string
  readonly finalized_at: string | null
}

interface PendingDeliveryRow {
  readonly id: number
  readonly destination_id: string
  readonly source_id: string
  readonly payload: string
  readonly attempts: number
}

interface PipelineRequestRow {
  readonly id: number
  readonly provider: Provider
  readonly transcript_path: string
  readonly native_session_id: string | null
  readonly project_origin: string | null
  readonly reason: string
  readonly attempts: number
  readonly requested_at: string
}

interface PipelineBackfillRow {
  readonly destination_id: string
  readonly provider_scope: BackfillProviderScope
  readonly reason: string
  readonly attempts: number
  readonly requested_at: string
}

interface PipelineBackfillStatusRow {
  readonly attempts: number
  readonly next_attempt_at: number
  readonly last_error: string | null
  readonly requested_at: string
  readonly completed_at: string | null
}

interface PipelineDeliveryStats {
  readonly pending: number
  readonly retrying: number
  readonly next_retry_at: number | null
  readonly quarantined: number
  readonly delivered: number
}

interface PipelineHarnessRow {
  readonly provider: Provider
  readonly selected: number
  readonly configured_at: string
  readonly reviewed_at: string | null
  readonly last_hook_received_at: string | null
  readonly last_event: string | null
  readonly last_session_id: string | null
}

const sourceFromRow = (row: PipelineSourceRow): PipelineSource => ({
  sourceId: row.source_id,
  provider: row.provider,
  nativeSessionId: row.native_session_id,
  localPath: row.local_path,
  pathHash: row.path_hash,
  classification: row.classification,
  projectOrigin: row.project_origin,
  epoch: Number(row.epoch),
  cursorOffset: Number(row.cursor_offset),
  sourceSize: Number(row.source_size),
  modifiedMs: Number(row.modified_ms),
  metadataRevision: row.metadata_revision,
  normalizerVersion: row.normalizer_version,
  snapshot: JSON.parse(row.snapshot_payload) as SessionSnapshot,
  finalizedAt: row.finalized_at
})

const deliveryPriority = (priority: DeliveryPriority | undefined): number =>
  priority === "live" ? 1 : 0

const isTerminal = (
  status: SessionSnapshot["session"]["status"] | undefined
): boolean => status === "completed" || status === "archived"

const retryDelay = (attempts: number): number => {
  const cap = Math.min(5 * 60_000, 1_000 * 2 ** Math.min(attempts, 8))
  const floor = Math.max(1_000, Math.floor(cap / 2))
  return floor + Math.floor(Math.random() * (cap - floor + 1))
}

const retryTimestamp = (value: number | null | undefined): string | null => {
  const milliseconds = Number(value ?? 0)
  return Number.isFinite(milliseconds) && milliseconds > 0
    ? new Date(milliseconds).toISOString()
    : null
}
