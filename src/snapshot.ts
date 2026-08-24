import { z } from "zod"

const isoDate = z.string().datetime({ offset: true })
const identifier = z.string().min(1).max(256)
const sha256 = z.string().regex(/^[a-f0-9]{64}$/)
const jsonObject = z.record(z.string(), z.unknown())

export const canonicalContentBlockSchema = z.object({
  type: identifier
}).catchall(z.unknown())

export const usageSchema = z.object({
  inputTokens: z.number().int().nonnegative().default(0),
  outputTokens: z.number().int().nonnegative().default(0),
  cachedInputTokens: z.number().int().nonnegative().default(0),
  cacheCreationInputTokens: z.number().int().nonnegative().default(0),
  reasoningOutputTokens: z.number().int().nonnegative().default(0)
})

export const canonicalEventSchema = z.object({
  id: identifier,
  sequence: z.number().int().nonnegative(),
  kind: identifier,
  actor: identifier,
  occurredAt: isoDate,
  content: z.array(canonicalContentBlockSchema).max(10_000).default([]),
  text: z.string().nullable().default(null),
  toolName: z.string().max(512).nullable().default(null),
  attributes: jsonObject.default({}),
  usage: usageSchema.optional()
})

const sessionPayloadSchema = z.object({
  schemaVersion: z.literal(1),
  source: z.object({
    id: identifier,
    deviceId: identifier,
    provider: identifier,
    nativeSessionId: identifier,
    nativeParentSessionId: z.string().max(512).nullable().default(null),
    classification: z.enum(["active", "archived", "fixture"]),
    pathHash: sha256,
    generation: z.number().int().positive(),
    revision: sha256,
    sourceSize: z.number().int().nonnegative(),
    capturedAt: isoDate
  }),
  session: z.object({
    id: identifier,
    kind: z.enum(["root", "agent"]),
    title: z.string().max(4_000).nullable().default(null),
    project: z.string().max(1_000).nullable().default(null),
    model: z.string().max(512).nullable().default(null),
    status: z.enum(["active", "completed", "archived"]),
    startedAt: isoDate,
    updatedAt: isoDate,
    attributes: jsonObject.default({})
  }),
  events: z.array(canonicalEventSchema).max(100_000),
  redaction: z.object({
    version: identifier,
    replacements: z.number().int().nonnegative()
  })
})

export const sessionSnapshotSchema = sessionPayloadSchema.extend({ snapshotId: sha256 })

export type Usage = z.infer<typeof usageSchema>
export type CanonicalContentBlock = z.infer<typeof canonicalContentBlockSchema>
export type CanonicalEvent = z.infer<typeof canonicalEventSchema>
export type SessionSnapshot = z.infer<typeof sessionSnapshotSchema>
