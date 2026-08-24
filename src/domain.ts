import { createHash, randomUUID } from "node:crypto"

export const parserVersion = "0.3.0"
export type Provider = "codex" | "claude"
export type SessionKind = "root" | "agent"
export type EventKind =
  | "session_started"
  | "user_message"
  | "assistant_message"
  | "reasoning"
  | "tool_call"
  | "tool_result"
  | "token_usage"
  | "unknown"

export interface SourceFile {
  readonly path: string
  readonly classification: "active" | "archived" | "fixture"
  readonly size: number
  readonly modifiedAt: Date
  readonly nativeTitle?: string
  readonly metadataRevision?: string
  readonly normalizerVersion?: string
}

export interface ParsedRecord {
  readonly line: number
  readonly offset: number
  readonly raw: Record<string, unknown>
  readonly rawText: string
  readonly occurredAt: Date | null
}

export interface ParseDiagnostic {
  readonly sourcePath: string
  readonly line: number
  readonly message: string
}

export interface ImportReport {
  readonly runId: string
  readonly provider: Provider
  readonly sourcesScanned: number
  readonly recordsImported: number
  readonly duplicates: number
  readonly malformedRecords: number
  readonly normalizedEvents: number
  readonly startedAt: Date
  readonly finishedAt: Date
}

export interface SessionFilters {
  readonly limit?: number
  readonly project?: string
  readonly provider?: Provider
  readonly since?: Date
  readonly until?: Date
}

export type AggregateMetric = "tokens" | "tool_calls" | "sessions" | "messages"
export type AggregateGroup = "provider" | "project" | "model" | "tool" | "day"

export interface Evidence {
  readonly sessionId: string
  readonly eventId?: string
}

export interface QueryResult<T> {
  readonly data: T
  readonly evidence: readonly Evidence[]
  readonly freshness: { readonly importedThrough: string | null }
  readonly notices: readonly string[]
}

export const hash = (value: string): string =>
  createHash("sha256").update(value).digest("hex")

export const createId = (): string => randomUUID()

export const record = (value: unknown): Record<string, unknown> | null =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null

export const string = (value: unknown): string | null =>
  typeof value === "string" && value.length > 0 ? value : null

export const number = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0

export const timestamp = (value: unknown): Date | null => {
  const input = string(value)
  if (!input) return null
  const date = new Date(input)
  return Number.isNaN(date.getTime()) ? null : date
}

export const iso = (date: Date | null | undefined): string | null =>
  date ? date.toISOString() : null
