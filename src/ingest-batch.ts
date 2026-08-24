import { hash } from "./domain.ts"
import type { CanonicalEvent, SessionSnapshot } from "./snapshot.ts"

export type IngestMode = "baseline" | "delta" | "final"

export interface IngestBatch {
  readonly schemaVersion: 2
  readonly batchId: string
  readonly mode: IngestMode
  readonly cursor: {
    readonly from: number
    readonly to: number
  }
  readonly source: {
    readonly id: string
    readonly deviceId: string
    readonly provider: string
    readonly nativeSessionId: string
    readonly nativeParentSessionId: string | null
    readonly classification: "active" | "archived" | "fixture"
    readonly pathHash: string
    readonly epoch: number
    readonly sourceSize: number
    readonly capturedAt: string
  }
  readonly session: SessionSnapshot["session"]
  readonly events: readonly CanonicalEvent[]
  readonly redaction: SessionSnapshot["redaction"]
}

// Keep individual requests comfortably below the API limit and keep each
// server-side transaction bounded for sessions with many events.
export const defaultIngestBatchChunkLimits = {
  maxEvents: 200,
  maxSerializedEventBytes: 768 * 1024
} as const

export const baselineBatch = (
  snapshot: SessionSnapshot,
  cursorTo = snapshot.source.sourceSize,
  epoch = snapshot.source.generation
): IngestBatch => batchFromSnapshot(snapshot, {
  mode: "baseline",
  cursorFrom: 0,
  cursorTo,
  epoch,
  events: snapshot.events
})

export const deltaBatch = (
  snapshot: SessionSnapshot,
  input: {
    readonly cursorFrom: number
    readonly cursorTo: number
    readonly epoch: number
    readonly events: readonly CanonicalEvent[]
    readonly final?: boolean
  }
): IngestBatch => batchFromSnapshot(snapshot, {
  mode: input.final ? "final" : "delta",
  cursorFrom: input.cursorFrom,
  cursorTo: input.cursorTo,
  epoch: input.epoch,
  events: input.events
})

/**
 * Split a batch into independently durable, cursor-ordered uploads. The first
 * baseline chunk remains a baseline; a terminal session's final chunk is
 * marked `final`, so completion work cannot begin before all events arrive.
 */
export const chunkIngestBatch = (
  batch: IngestBatch,
  limits: {
    readonly maxEvents?: number
    readonly maxSerializedEventBytes?: number
  } = {}
): readonly IngestBatch[] => {
  const maxEvents = limits.maxEvents ?? defaultIngestBatchChunkLimits.maxEvents
  const maxSerializedEventBytes = limits.maxSerializedEventBytes ??
    defaultIngestBatchChunkLimits.maxSerializedEventBytes
  if (batch.events.length <= maxEvents && serializedEventBytes(batch.events) <= maxSerializedEventBytes) {
    return [batch]
  }

  const chunks: Array<{ events: IngestBatch["events"]; bytes: number }> = []
  let events: IngestBatch["events"] = []
  let bytes = 0
  for (const event of batch.events) {
    const eventBytes = serializedEventBytes([event])
    if (
      events.length > 0 &&
      (events.length >= maxEvents || bytes + eventBytes > maxSerializedEventBytes)
    ) {
      chunks.push({ events, bytes })
      events = []
      bytes = 0
    }
    events = [...events, event]
    bytes += eventBytes
  }
  if (events.length > 0) chunks.push({ events, bytes })
  if (chunks.length <= 1) return [batch]

  const totalBytes = chunks.reduce((total, chunk) => total + chunk.bytes, 0)
  const cursorRange = batch.cursor.to - batch.cursor.from
  let bytesThrough = 0
  let cursorFrom = batch.cursor.from
  return chunks.map((chunk, index) => {
    bytesThrough += chunk.bytes
    const last = index === chunks.length - 1
    const cursorTo = last
      ? batch.cursor.to
      : Math.max(
          cursorFrom,
          Math.min(
            batch.cursor.to,
            batch.cursor.from + Math.floor(cursorRange * bytesThrough / totalBytes)
          )
        )
    const mode: IngestMode = index === 0 && batch.mode === "baseline"
      ? "baseline"
      : last && (batch.mode === "final" || isTerminal(batch.session.status))
        ? "final"
        : "delta"
    const chunked = batchFromBatch(batch, {
      mode,
      cursorFrom,
      cursorTo,
      events: chunk.events
    })
    cursorFrom = cursorTo
    return chunked
  })
}

const batchFromSnapshot = (
  snapshot: SessionSnapshot,
  input: {
    readonly mode: IngestMode
    readonly cursorFrom: number
    readonly cursorTo: number
    readonly epoch: number
    readonly events: readonly CanonicalEvent[]
  }
): IngestBatch => {
  const source = {
    id: snapshot.source.id,
    deviceId: snapshot.source.deviceId,
    provider: snapshot.source.provider,
    nativeSessionId: snapshot.source.nativeSessionId,
    nativeParentSessionId: snapshot.source.nativeParentSessionId,
    classification: snapshot.source.classification,
    pathHash: snapshot.source.pathHash,
    epoch: input.epoch,
    sourceSize: snapshot.source.sourceSize,
    capturedAt: snapshot.source.capturedAt
  }
  const identity = JSON.stringify({
    mode: input.mode,
    source,
    cursor: [input.cursorFrom, input.cursorTo],
    session: snapshot.session,
    events: input.events,
    redaction: snapshot.redaction
  })
  return {
    schemaVersion: 2,
    batchId: hash(identity),
    mode: input.mode,
    cursor: { from: input.cursorFrom, to: input.cursorTo },
    source,
    session: snapshot.session,
    events: input.events,
    redaction: snapshot.redaction
  }
}

const batchFromBatch = (
  batch: IngestBatch,
  input: {
    readonly mode: IngestMode
    readonly cursorFrom: number
    readonly cursorTo: number
    readonly events: readonly CanonicalEvent[]
  }
): IngestBatch => {
  const identity = JSON.stringify({
    mode: input.mode,
    source: batch.source,
    cursor: [input.cursorFrom, input.cursorTo],
    session: batch.session,
    events: input.events,
    redaction: batch.redaction
  })
  return {
    ...batch,
    batchId: hash(identity),
    mode: input.mode,
    cursor: { from: input.cursorFrom, to: input.cursorTo },
    events: input.events
  }
}

const serializedEventBytes = (events: readonly CanonicalEvent[]): number =>
  Buffer.byteLength(JSON.stringify(events))

const isTerminal = (status: SessionSnapshot["session"]["status"]): boolean =>
  status === "completed" || status === "archived"
