import { basename, join } from "node:path"
import { stat } from "node:fs/promises"

import {
  hash,
  parserVersion,
  type ParseDiagnostic,
  type Provider,
  type SourceFile
} from "./domain.ts"
import { SnapshotExportError, type SnapshotExporter } from "./exporter.ts"
import { parseJsonlSource } from "./jsonl.ts"
import { normalizeCollectedSnapshot } from "./normalize.ts"
import {
  IncrementalPipelineStore,
  type DeliveryPriority
} from "./pipeline-store.ts"
import { defaultClaudeHome, discoverClaudeSources } from "./providers/claude.ts"
import {
  codexSourceAtPath,
  defaultCodexHome,
  discoverCodexSources
} from "./providers/codex.ts"
import { LocalSessionStore } from "./store.ts"
import { allowsProjectUpload, type ProjectUploadPolicy } from "./user-config.ts"

export interface CollectorOptions {
  readonly exporter?: SnapshotExporter
  readonly destinations?: readonly CollectorDestination[]
  readonly autoPrepareDestinations?: boolean
  readonly statePath?: string
  readonly codexHome?: string
  readonly claudeHome?: string
  readonly uploadPolicy?: ProjectUploadPolicy
}

export interface CollectorDestination {
  readonly exporter: SnapshotExporter
  readonly allows?: (snapshot: import("./snapshot.ts").SessionSnapshot, projectOrigin: string | null) => boolean
}

export interface ScanReport {
  readonly scanned: number
  readonly unchanged: number
  readonly captured: number
  readonly ignored: number
  readonly malformed: number
  readonly diagnostics: readonly ParseDiagnostic[]
}

export interface FlushReport {
  readonly exportEnabled: boolean
  readonly exporter: string | null
  readonly destinationId: string | null
  readonly attempted: number
  readonly exported: number
  readonly withheld: number
  readonly failed: number
  readonly quarantined: number
  readonly remaining: number
}

export interface BackfillPreview {
  readonly destinationId: string
  readonly discovered: number
  readonly eligible: number
  readonly withheld: number
  readonly alreadyDelivered: number
  readonly pending: number
  readonly quarantined: number
}

export class SessionCollector {
  readonly store: LocalSessionStore
  readonly pipeline: IncrementalPipelineStore
  readonly deviceId: string

  constructor(readonly options: CollectorOptions) {
    this.store = new LocalSessionStore(
      options.statePath ?? join(process.env.HOME ?? ".", ".ht", "sessions.sqlite")
    )
    this.pipeline = new IncrementalPipelineStore(this.store.database)
    this.deviceId = this.store.deviceId()
  }

  async scan(provider?: Provider): Promise<ScanReport> {
    const sources: Array<{ provider: Provider; source: SourceFile }> = []
    if (!provider || provider === "codex") {
      const discovered = await discoverCodexSources(this.options.codexHome ?? defaultCodexHome())
      sources.push(...discovered.map((source) => ({ provider: "codex" as const, source })))
    }
    if (!provider || provider === "claude") {
      const discovered = await discoverClaudeSources(this.options.claudeHome ?? defaultClaudeHome())
      sources.push(...discovered.map((source) => ({ provider: "claude" as const, source })))
    }

    let unchanged = 0
    let captured = 0
    let ignored = 0
    const diagnostics: ParseDiagnostic[] = []
    for (const discovered of sources) {
      const source = {
        ...discovered.source,
        normalizerVersion: parserVersion
      }
      if (this.pipeline.shouldSkip(discovered.provider, source)) {
        unchanged += 1
        continue
      }
      const result = await this.captureSource(discovered.provider, source)
      diagnostics.push(...result.diagnostics)
      if (result.ignored) {
        ignored += 1
        continue
      }
      if (result.captured) captured += 1
    }
    return {
      scanned: sources.length,
      unchanged,
      captured,
      ignored,
      malformed: diagnostics.length,
      diagnostics
    }
  }

  async flush(
    limit = 25,
    provider?: Provider,
    options: { readonly paceMs?: number } = {}
  ): Promise<FlushReport> {
    const destinations = this.destinations()
    if (destinations.length === 0) {
      return {
        exportEnabled: false,
        exporter: null,
        destinationId: null,
        attempted: 0,
        exported: 0,
        withheld: 0,
        failed: 0,
        quarantined: 0,
        remaining: this.pipeline.sources(provider).length
      }
    }
    let exported = 0
    let failed = 0
    let quarantined = 0
    let attempted = 0
    let withheld = 0
    for (const destination of destinations) {
      this.pipeline.removePendingOutside(
        destination.exporter.destinationId,
        (snapshot, projectOrigin) =>
          this.allows(destination, snapshot, projectOrigin),
        provider
      )
      if (this.options.autoPrepareDestinations !== false) {
        this.pipeline.prepareDestination(destination.exporter.destinationId, {
          provider,
          allows: (snapshot, projectOrigin) =>
            this.allows(destination, snapshot, projectOrigin)
        })
        withheld += this.pipeline.countOutside(
          (snapshot, projectOrigin) =>
            this.allows(destination, snapshot, projectOrigin),
          provider
        )
      }
      const pending = this.pipeline.due(
        destination.exporter.destinationId,
        Math.max(0, limit - attempted),
        provider
      )
      for (const item of pending) {
        attempted += 1
        try {
          if (destination.exporter.exportBatch) {
            await destination.exporter.exportBatch(item.batch)
          } else if (item.batch.mode === "baseline") {
            const source = this.pipeline.source(item.sourceId)
            if (!source) throw new Error(`Local source ${item.sourceId} is missing.`)
            await destination.exporter.exportSnapshot(source.snapshot)
          } else {
            throw new SnapshotExportError(
              `Exporter ${destination.exporter.kind} does not support incremental batches.`,
              false
            )
          }
          this.pipeline.acknowledge(item)
          exported += 1
        } catch (error) {
          failed += 1
          const message = error instanceof Error ? error.message : "Upload failed"
          if (
            error instanceof SnapshotExportError &&
            cursorRecoveryCodes.has(error.code ?? "")
          ) {
            const expectedEpoch = typeof error.details.expectedEpoch === "number"
              ? error.details.expectedEpoch
              : undefined
            this.pipeline.rebaseDestination(
              destination.exporter.destinationId,
              item.sourceId,
              expectedEpoch
            )
            continue
          }
          const retryable = !(error instanceof SnapshotExportError) || error.retryable
          this.pipeline.fail(item, message, retryable)
          if (!retryable) quarantined += 1
        }
        if (options.paceMs && options.paceMs > 0) await delay(options.paceMs)
      }
    }
    const pendingRemaining = destinations.reduce(
      (total, destination) =>
        total + this.pipeline.stats(destination.exporter.destinationId).pending,
      0
    )
    const remaining = pendingRemaining
    return {
      exportEnabled: true,
      exporter: destinations.length === 1 ? destinations[0]!.exporter.kind : "multiple",
      destinationId: destinations.length === 1
        ? destinations[0]!.exporter.destinationId
        : null,
      attempted,
      exported,
      withheld,
      failed,
      quarantined,
      remaining
    }
  }

  backfillPreview(provider?: Provider): BackfillPreview {
    const destination = this.destinations()[0]
    if (!destination) {
      throw new Error("A configured exporter is required to preview a backfill.")
    }
    const candidates = this.pipeline.sources(provider)
    const withState = candidates.map((item) => ({
      item,
      state: this.pipeline.deliveryState(destination.exporter.destinationId, item.sourceId)
    }))
    const eligible = withState.filter(({ item, state }) =>
      state !== "delivered" &&
      state !== "quarantined" &&
      this.allows(destination, item.snapshot, item.projectOrigin)
    )
    return {
      destinationId: destination.exporter.destinationId,
      discovered: candidates.length,
      eligible: eligible.length,
      withheld: withState.filter(({ item, state }) =>
        state !== "delivered" &&
        state !== "quarantined" &&
        !this.allows(destination, item.snapshot, item.projectOrigin)
      ).length,
      alreadyDelivered: withState.filter(({ state }) => state === "delivered").length,
      pending: withState.filter(({ state }) => state === "pending").length,
      quarantined: withState.filter(({ state }) => state === "quarantined").length
    }
  }

  prepareBackfill(provider?: Provider): number {
    const destination = this.destinations()[0]
    if (!destination) {
      throw new Error("A configured exporter is required to prepare a backfill.")
    }
    return this.pipeline.prepareDestination(destination.exporter.destinationId, {
      provider,
      allows: (snapshot, projectOrigin) => this.allows(destination, snapshot, projectOrigin)
    })
  }

  async runOnce(provider?: Provider): Promise<{ scan: ScanReport; flush: FlushReport }> {
    const scan = await this.scan(provider)
    const flush = await this.flush()
    return { scan, flush }
  }

  async processPath(
    provider: Provider,
    path: string,
    options: {
      readonly classification?: SourceFile["classification"]
      readonly projectOrigin?: string | null
      readonly completed?: boolean
    } = {}
  ): Promise<PipelineCaptureResult> {
    const classification = options.classification ??
      classificationFromPath(provider, path)
    let source: SourceFile
    if (provider === "codex") {
      const codexHome = this.options.codexHome ?? defaultCodexHome()
      try {
        source = await codexSourceAtPath(codexHome, path, classification)
      } catch (error) {
        if (!isMissing(error)) throw error
        source = await codexSourceAtPath(
          codexHome,
          join(codexHome, "archived_sessions", basename(path)),
          "archived"
        )
      }
      source = { ...source, normalizerVersion: parserVersion }
    } else {
      const details = await stat(path)
      source = {
        path,
        classification,
        size: details.size,
        modifiedAt: details.mtime,
        normalizerVersion: parserVersion
      }
    }
    // Paths delivered by a hook are the user's current work. Promote their
    // ordered batches above historical backfill without violating per-source
    // cursor ordering.
    return this.captureSource(
      provider,
      source,
      options.projectOrigin,
      "live",
      options.completed
    )
  }

  close(): void {
    this.store.close()
  }

  private async captureSource(
    provider: Provider,
    source: SourceFile,
    projectOriginHint?: string | null,
    priority: DeliveryPriority = "backfill",
    completed = false
  ): Promise<PipelineCaptureResult> {
    const parsed = await parseJsonlSource(source)
    const collected = normalizeCollectedSnapshot({
      provider,
      deviceId: this.deviceId,
      generation: this.pipeline.sourceAtPath(provider, hash(source.path))?.epoch ?? 1,
      source,
      records: parsed.records
    })
    if (!collected) {
      return {
        captured: false,
        ignored: true,
        diagnostics: parsed.diagnostics
      }
    }
    const snapshot = completed && collected.snapshot.session.status === "active"
      ? {
          ...collected.snapshot,
          session: { ...collected.snapshot.session, status: "completed" as const }
        }
      : collected.snapshot
    const projectOrigin = collected.projectOrigin ?? projectOriginHint ?? null
    const destinations = this.destinations().filter((destination) =>
      this.allows(destination, snapshot, projectOrigin)
    )
    const captured = this.pipeline.capture({
      provider,
      source,
      snapshot,
      projectOrigin,
      cursorTo: parsed.nextOffset,
      destinationIds: destinations.map((destination) => destination.exporter.destinationId),
      priority
    })
    return {
      captured: captured.captured,
      ignored: false,
      diagnostics: parsed.diagnostics
    }
  }

  private destinations(): CollectorDestination[] {
    if (this.options.destinations) return [...this.options.destinations]
    return this.options.exporter ? [{ exporter: this.options.exporter }] : []
  }

  private allows(
    destination: CollectorDestination,
    snapshot: import("./snapshot.ts").SessionSnapshot,
    projectOrigin: string | null
  ): boolean {
    if (destination.allows && !destination.allows(snapshot, projectOrigin)) return false
    return !this.options.uploadPolicy ||
      allowsProjectUpload(this.options.uploadPolicy, snapshot, projectOrigin)
  }
}

interface PipelineCaptureResult {
  readonly captured: boolean
  readonly ignored: boolean
  readonly diagnostics: readonly ParseDiagnostic[]
}

const classificationFromPath = (
  provider: Provider,
  path: string
): SourceFile["classification"] =>
  provider === "codex" && path.replaceAll("\\", "/").includes("/archived_sessions/")
    ? "archived"
    : "active"

const cursorRecoveryCodes = new Set([
  "baseline-required",
  "baseline-conflict",
  "cursor-gap",
  "cursor-conflict"
])

export const watchCollector = async (
  collector: SessionCollector,
  options: { readonly intervalMs?: number; readonly signal?: AbortSignal; readonly onCycle?: (result: unknown) => void } = {}
): Promise<void> => {
  const intervalMs = options.intervalMs ?? 2_000
  while (!options.signal?.aborted) {
    const result = await collector.runOnce()
    options.onCycle?.(result)
    await abortableDelay(intervalMs, options.signal)
  }
}

const abortableDelay = async (milliseconds: number, signal?: AbortSignal): Promise<void> => {
  if (signal?.aborted) return
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, milliseconds)
    signal?.addEventListener("abort", () => {
      clearTimeout(timer)
      resolve()
    }, { once: true })
  })
}

const delay = async (milliseconds: number): Promise<void> => {
  await new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
}

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
