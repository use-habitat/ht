import { SnapshotExportError, type SnapshotExporter } from "../exporter.ts"
import { baselineBatch, type IngestBatch } from "../ingest-batch.ts"
import type { SessionSnapshot } from "../snapshot.ts"

export interface HabitatExporterOptions {
  readonly apiUrl: string
  readonly apiKey: string
  readonly destinationId?: string
  readonly requestTimeoutMs?: number
  readonly fetch?: typeof globalThis.fetch
}

export class HabitatExporter implements SnapshotExporter {
  readonly kind = "habitat"
  readonly destinationId: string
  private readonly fetcher: typeof globalThis.fetch

  constructor(readonly options: HabitatExporterOptions) {
    this.fetcher = options.fetch ?? globalThis.fetch
    this.destinationId = options.destinationId ?? `habitat:${normalizedUrl(options.apiUrl)}`
  }

  async exportSnapshot(snapshot: SessionSnapshot): Promise<void> {
    await this.exportBatch(baselineBatch(snapshot))
  }

  async exportBatch(batch: IngestBatch): Promise<void> {
    let response: Response
    try {
      response = await this.fetcher(
        new URL("/v1/ingest/batches", trailingSlash(this.options.apiUrl)),
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.options.apiKey}`,
            "Content-Type": "application/vnd.habitat.batch+gzip",
            "Idempotency-Key": batch.batchId
          },
          body: new Blob([Bun.gzipSync(Buffer.from(JSON.stringify(batch)))]),
          signal: AbortSignal.timeout(this.options.requestTimeoutMs ?? 45_000)
        }
      )
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause)
      throw new SnapshotExportError(
        `Habitat upload did not complete: ${message}`,
        true
      )
    }
    if (!response.ok) {
      const body = await response.text()
      const parsed = errorBody(body)
      throw new SnapshotExportError(
        `HTTP ${response.status}: ${parsed.message ?? body.slice(0, 1_000)}`,
        retryableStatus(response.status),
        response.status,
        parsed.code,
        parsed.details
      )
    }
  }
}

const trailingSlash = (value: string): string => value.endsWith("/") ? value : `${value}/`
const normalizedUrl = (value: string): string => value.replace(/\/$/, "")
const retryableStatus = (status: number): boolean =>
  status >= 500 || [401, 403, 408, 409, 425, 429].includes(status)

const errorBody = (body: string): {
  code?: string
  message?: string
  details: Readonly<Record<string, unknown>>
} => {
  try {
    const parsed = JSON.parse(body) as {
      error?: Record<string, unknown>
    }
    const error = parsed.error
    if (!error) return { details: {} }
    const { code, message, requestId: _requestId, ...details } = error
    return {
      ...(typeof code === "string" ? { code } : {}),
      ...(typeof message === "string" ? { message } : {}),
      details
    }
  } catch {
    return { details: {} }
  }
}
