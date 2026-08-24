import type { SessionSnapshot } from "./snapshot.ts"
import type { IngestBatch } from "./ingest-batch.ts"

export interface SnapshotExporter {
  readonly kind: string
  /**
   * Stable, non-secret identity for the remote destination.
   *
   * Delivery state is scoped to this value so changing workspaces never causes
   * an acknowledged snapshot to be mistaken for one delivered elsewhere.
   */
  readonly destinationId: string
  exportSnapshot(snapshot: SessionSnapshot): Promise<void>
  exportBatch?(batch: IngestBatch): Promise<void>
}

export class SnapshotExportError extends Error {
  constructor(
    message: string,
    readonly retryable: boolean,
    readonly status?: number,
    readonly code?: string,
    readonly details: Readonly<Record<string, unknown>> = {}
  ) {
    super(message)
    this.name = "SnapshotExportError"
  }
}
