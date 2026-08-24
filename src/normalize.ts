import { basename } from "node:path"

import {
  claudeParentIdFromPath,
  claudeSessionIdFromPath,
  isClaudeSubagentPath
} from "./providers/claude.ts"
import { sessionIdFromPath } from "./providers/codex.ts"
import {
  hash,
  number,
  record,
  string,
  timestamp,
  type ParsedRecord,
  type Provider,
  type SourceFile
} from "./domain.ts"
import { redactValue } from "./redaction.ts"
import {
  sessionSnapshotSchema,
  type CanonicalContentBlock,
  type CanonicalEvent,
  type SessionSnapshot,
  type Usage
} from "./snapshot.ts"

export interface SnapshotInput {
  readonly provider: Provider
  readonly deviceId: string
  readonly generation: number
  readonly source: SourceFile
  readonly records: readonly ParsedRecord[]
}

interface NormalizedSession {
  readonly nativeSessionId: string
  readonly nativeParentSessionId: string | null
  readonly kind: "root" | "agent"
  readonly title: string | null
  readonly projectOrigin: string | null
  readonly project: string | null
  readonly model: string | null
  readonly startedAt: Date
  readonly updatedAt: Date
  readonly events: readonly CanonicalEvent[]
}

export interface CollectedSnapshot {
  readonly snapshot: SessionSnapshot
  readonly projectOrigin: string | null
}

export const normalizeSnapshot = (input: SnapshotInput): SessionSnapshot | null =>
  normalizeCollectedSnapshot(input)?.snapshot ?? null

export const normalizeCollectedSnapshot = (input: SnapshotInput): CollectedSnapshot | null => {
  if (input.records.length === 0) return null
  const pathHash = hash(input.source.path)
  const provisionalSourceId = hash(`${input.deviceId}:${input.provider}:${pathHash}`)
  const provisional = input.provider === "codex"
    ? normalizeCodex(input.source, input.records, provisionalSourceId)
    : normalizeClaude(input.source, input.records, provisionalSourceId)
  if (!provisional || provisional.events.length === 0) return null
  // A logical source follows one provider session across active/archive path
  // moves. Normalize once more with that stable identity so fallback event IDs
  // do not change when the provider relocates a transcript.
  const sourceId = hash([
    input.deviceId,
    input.provider,
    provisional.nativeSessionId
  ].join(":"))
  const snapshot = sourceId === provisionalSourceId
    ? provisional
    : input.provider === "codex"
      ? normalizeCodex(input.source, input.records, sourceId)
      : normalizeClaude(input.source, input.records, sourceId)
  if (!snapshot || snapshot.events.length === 0) return null

  const revision = revisionOf(input.records)
  // A provider session keeps one identity when its file moves (for example from
  // Codex's active directory to its archive) or another authenticated device
  // uploads the same transcript.
  const sessionId = hash([
    input.provider,
    snapshot.nativeSessionId
  ].join(":"))
  const unredacted = {
    schemaVersion: 1 as const,
    snapshotId: hash(`${sourceId}:${input.generation}:${revision}`),
    source: {
      id: sourceId,
      deviceId: input.deviceId,
      provider: input.provider,
      nativeSessionId: snapshot.nativeSessionId,
      nativeParentSessionId: snapshot.nativeParentSessionId,
      classification: input.source.classification,
      pathHash,
      generation: input.generation,
      revision,
      sourceSize: input.source.size,
      capturedAt: input.source.modifiedAt.toISOString()
    },
    session: {
      id: sessionId,
      kind: snapshot.kind,
      title: snapshot.title,
      project: snapshot.project,
      model: snapshot.model,
      status: input.source.classification === "archived" ? "archived" as const : "active" as const,
      startedAt: snapshot.startedAt.toISOString(),
      updatedAt: snapshot.updatedAt.toISOString(),
      attributes: {}
    },
    events: snapshot.events,
    redaction: { version: "local-v1", replacements: 0 }
  }
  const redacted = redactValue(unredacted)
  redacted.value.redaction.replacements = redacted.replacements
  return {
    snapshot: sessionSnapshotSchema.parse(redacted.value),
    projectOrigin: snapshot.projectOrigin
  }
}

const normalizeCodex = (
  source: SourceFile,
  records: readonly ParsedRecord[],
  sourceId: string
): NormalizedSession | null => {
  const pathId = sessionIdFromPath(source.path)
  const metadataRecords = records.filter((entry) => entry.raw.type === "session_meta")
  const metadata = metadataRecords.find((entry) => string(record(entry.raw.payload)?.id) === pathId)
    ?? metadataRecords.at(-1)
  const payload = record(metadata?.raw.payload)
  const projectOrigin = string(payload?.cwd)
  const nativeSessionId = string(payload?.id) ?? pathId ?? hash(source.path).slice(0, 32)
  const sourceMetadata = record(payload?.source)
  const threadSpawn = record(record(sourceMetadata?.subagent)?.thread_spawn)
  const nativeParentSessionId = string(threadSpawn?.parent_thread_id) ??
    string(payload?.parent_thread_id) ?? string(sourceMetadata?.parent_id)
  const threadSource = string(payload?.thread_source) ?? string(sourceMetadata?.thread_source)
  const kind = threadSpawn || threadSource === "subagent" || nativeParentSessionId ? "agent" : "root"
  const startedAt = metadata?.occurredAt ?? timestamp(payload?.timestamp) ?? earliest(records)
  const updatedAt = latest(records, startedAt)
  const ownershipStartLine = codexOwnershipStart(records, nativeSessionId, startedAt)
  const owned = records.filter((entry) =>
    entry.line >= ownershipStartLine ||
    (entry.raw.type === "session_meta" && string(record(entry.raw.payload)?.id) === nativeSessionId)
  )

  const hasEventUser = owned.some((entry) => {
    const body = record(entry.raw.payload)
    return entry.raw.type === "event_msg" && body?.type === "user_message"
  })
  const hasEventAssistant = owned.some((entry) => {
    const body = record(entry.raw.payload)
    return entry.raw.type === "event_msg" && body?.type === "agent_message"
  })
  const models = owned.flatMap((entry) => {
    const model = entry.raw.type === "turn_context" ? string(record(entry.raw.payload)?.model) : null
    return model ? [model] : []
  })
  const firstPrompt = owned.flatMap((entry) => {
    const body = record(entry.raw.payload)
    return entry.raw.type === "event_msg" && body?.type === "user_message"
      ? string(body.message) ?? []
      : []
  })[0] ?? null
  const updatedTitle = owned.flatMap((entry) => {
    const body = record(entry.raw.payload)
    const threadId = string(body?.thread_id)
    return entry.raw.type === "event_msg" && body?.type === "thread_name_updated" &&
        (threadId === null || threadId === nativeSessionId)
      ? string(body.thread_name) ?? []
      : []
  }).at(-1) ?? null
  const title = clean(source.nativeTitle ?? updatedTitle ?? firstPrompt)
  const subagentsByCallId = codexSubagentLinks(owned)
  const events: CanonicalEvent[] = []
  let previousUsage: Usage | null = null
  let activeTurnId: string | null = null
  let turnUsageIndex = 0

  for (const entry of owned) {
    const body = record(entry.raw.payload)
    const occurredAt = (entry.occurredAt ?? startedAt).toISOString()
    const rawId = rawEventId(sourceId, entry)
    const add = (
      discriminator: string,
      event: Omit<CanonicalEvent, "id" | "sequence" | "occurredAt">
    ): void => {
      events.push({
        ...event,
        id: hash(`${rawId}:${discriminator}`),
        sequence: events.length,
        occurredAt
      })
    }

    if (entry.raw.type === "turn_context") {
      activeTurnId = string(body?.turn_id)
      turnUsageIndex = 0
    }

    if (entry.raw.type === "session_meta" && string(body?.id) === nativeSessionId) {
      add("session", event("session_started", "system"))
    } else if (entry.raw.type === "event_msg" && body?.type === "user_message") {
      add("user", event("user_message", "user", string(body.message)))
    } else if (entry.raw.type === "event_msg" && body?.type === "agent_message") {
      add("assistant", event("assistant_message", "assistant", string(body.message)))
    } else if (entry.raw.type === "response_item") {
      const type = string(body?.type)
      if (type === "message") {
        const role = string(body?.role)
        if (role === "user" && !hasEventUser) {
          add("user", event("user_message", "user", messageText(body?.content)))
        } else if (role === "assistant" && !hasEventAssistant) {
          add("assistant", event("assistant_message", "assistant", messageText(body?.content)))
        }
      } else if (type === "reasoning") {
        const text = codexReasoningText(body)
        add("reasoning", event("reasoning", "assistant", text, null, {
          providerReasoningId: string(body?.id),
          contentOmitted: text === null,
          encryptedContentAvailable: string(body?.encrypted_content) !== null
        }))
      } else if (type === "function_call" || type === "custom_tool_call") {
        const callId = string(body?.call_id)
        add("tool-call", event("tool_call", "assistant", null, string(body?.name) ?? type, {
          callId,
          childNativeSessionId: callId ? subagentsByCallId.get(callId) ?? null : null,
          // Standard function calls use `arguments`; Code Mode custom tools
          // (including the `exec` wrapper) put their freeform source in `input`.
          arguments: type === "custom_tool_call"
            ? body?.input ?? body?.arguments ?? null
            : body?.arguments ?? null
        }))
      } else if (type === "function_call_output" || type === "custom_tool_call_output") {
        const content = codexToolOutputContent(body?.output)
        add("tool-result", event("tool_result", "tool", codexToolOutputText(content), null, {
          callId: string(body?.call_id)
        }, content))
      }
    }

    if (entry.raw.type === "event_msg" && body?.type === "token_count") {
      const total = codexUsage(record(record(body.info)?.total_token_usage))
      if (total) {
        const delta = previousUsage && monotonic(total, previousUsage)
          ? subtractUsage(total, previousUsage)
          : total
        previousUsage = total
        if (hasUsage(delta)) {
          add("usage", {
            ...event("token_usage", "system", usageText(delta), null, {
              providerUsageId: activeTurnId
                ? `codex:${activeTurnId}:${turnUsageIndex++}`
                : null
            }),
            usage: delta
          })
        }
      }
    }
  }
  if (!events.some((entry) => entry.kind !== "session_started")) return null
  return {
    nativeSessionId,
    nativeParentSessionId: nativeParentSessionId ?? null,
    kind,
    title,
    projectOrigin,
    project: projectName(projectOrigin),
    model: models[0] ?? null,
    startedAt,
    updatedAt,
    events
  }
}

const normalizeClaude = (
  source: SourceFile,
  records: readonly ParsedRecord[],
  sourceId: string
): NormalizedSession | null => {
  const nativeSessionId = claudeSessionIdFromPath(source.path)
  let nativeParentSessionId = claudeParentIdFromPath(source.path)
  const isAgent = isClaudeSubagentPath(source.path)
  const startedAt = earliest(records)
  const updatedAt = latest(records, startedAt)
  let cwd: string | null = null
  let title: string | null = null
  let firstPrompt: string | null = null
  let model: string | null = null
  const groups = new Map<string, ParsedRecord[]>()
  const subagentsByToolUseId = claudeSubagentLinks(records)

  for (const entry of records) {
    if (isAgent) nativeParentSessionId ??= string(entry.raw.sessionId)
    cwd ??= string(entry.raw.cwd)
    if (entry.raw.type === "ai-title") title = clean(string(entry.raw.aiTitle)) ?? title
    const message = record(entry.raw.message)
    if (isClaudeHuman(entry.raw) && firstPrompt === null) firstPrompt = clean(humanText(message?.content))
    if (entry.raw.type === "assistant") {
      model ??= string(message?.model)
      const key = string(message?.id) ?? string(entry.raw.requestId) ?? string(entry.raw.uuid) ?? `line:${entry.line}`
      const group = groups.get(key) ?? []
      group.push(entry)
      groups.set(key, group)
    }
  }

  const candidates: EventCandidate[] = [{
    line: records[0]!.line,
    order: -1,
    at: records[0]!.occurredAt ?? startedAt,
    discriminator: "session",
    value: event("session_started", "system")
  }]
  const seenResults = new Set<string>()
  for (const entry of records) {
    const message = record(entry.raw.message)
    if (isClaudeHuman(entry.raw)) {
      candidates.push({
        line: entry.line,
        order: 20,
        at: entry.occurredAt ?? startedAt,
        discriminator: `user:${string(entry.raw.uuid) ?? entry.line}`,
        value: event("user_message", "user", humanText(message?.content))
      })
    }
    for (const [index, block] of contentBlocks(message?.content).entries()) {
      if (block.type !== "tool_result") continue
      const callId = string(block.tool_use_id) ?? `${entry.line}:${index}`
      if (seenResults.has(callId)) continue
      seenResults.add(callId)
      candidates.push({
        line: entry.line,
        order: 10 + index,
        at: entry.occurredAt ?? startedAt,
        discriminator: `result:${callId}`,
        value: event("tool_result", "tool", toolResultText(block.content), null, {
          callId,
          isError: block.is_error === true
        })
      })
    }
  }

  for (const [messageId, group] of groups) {
    group.sort((left, right) => left.line - right.line)
    const first = group[0]!
    const seenCalls = new Set<string>()
    let order = 0
    for (const entry of group) {
      const message = record(entry.raw.message)
      for (const [index, block] of contentBlocks(message?.content).entries()) {
        if (block.type !== "tool_use") continue
        const callId = string(block.id) ?? `${entry.line}:${index}`
        if (seenCalls.has(callId)) continue
        seenCalls.add(callId)
        const toolName = string(block.name) ?? "unknown"
        candidates.push({
          line: first.line,
          order: 40 + order++,
          at: first.occurredAt ?? startedAt,
          discriminator: `call:${callId}`,
          value: event("tool_call", "assistant", null, toolName, {
            callId,
            arguments: block.input ?? null,
            childNativeSessionId: isClaudeSubagentTool(toolName)
              ? subagentsByToolUseId.get(callId) ?? null
              : null
          })
        })
      }
    }
    const text = claudeAssistantText(group)
    const reasoning = claudeReasoning(group)
    if (reasoning) {
      candidates.push({
        line: first.line,
        order: 25,
        at: first.occurredAt ?? startedAt,
        discriminator: `reasoning:${messageId}`,
        value: event("reasoning", "assistant", reasoning.text, null, {
          providerMessageId: messageId,
          contentOmitted: reasoning.text === null,
          redacted: reasoning.redacted
        })
      })
    }
    if (text) {
      candidates.push({
        line: first.line,
        order: 30,
        at: first.occurredAt ?? startedAt,
        discriminator: `assistant:${messageId}`,
        value: event("assistant_message", "assistant", text, null, { providerMessageId: messageId })
      })
    }
    const usage = claudeUsage(group)
    if (usage && hasUsage(usage.value)) {
      const providerUsageId = claudeProviderUsageId(group)
      candidates.push({
        line: usage.entry.line,
        order: 90,
        at: usage.entry.occurredAt ?? startedAt,
        discriminator: `usage:${messageId}`,
        value: {
          ...event("token_usage", "system", usageText(usage.value), null, {
            providerUsageId
          }),
          usage: usage.value
        }
      })
    }
  }
  candidates.sort((left, right) => left.line - right.line || left.order - right.order)
  if (candidates.length === 1) return null
  const events = candidates.map((candidate, sequence) => ({
    ...candidate.value,
    id: hash(`${sourceId}:${candidate.line}:${candidate.discriminator}`),
    sequence,
    occurredAt: candidate.at.toISOString()
  }))
  return {
    nativeSessionId,
    nativeParentSessionId,
    kind: isAgent ? "agent" : "root",
    title: title ?? firstPrompt ?? (isAgent ? `Claude agent ${nativeSessionId}` : null),
    projectOrigin: cwd,
    project: projectName(cwd),
    model,
    startedAt,
    updatedAt,
    events
  }
}

interface EventCandidate {
  readonly line: number
  readonly order: number
  readonly at: Date
  readonly discriminator: string
  readonly value: Omit<CanonicalEvent, "id" | "sequence" | "occurredAt">
}

const event = (
  kind: string,
  actor: string,
  text: string | null = null,
  toolName: string | null = null,
  attributes: Record<string, unknown> = {},
  content: ReadonlyArray<CanonicalContentBlock> = []
): Omit<CanonicalEvent, "id" | "sequence" | "occurredAt"> => ({
  kind,
  actor,
  content: [...content],
  text,
  toolName,
  attributes
})

const earliest = (records: readonly ParsedRecord[]): Date => {
  const times = records.flatMap((entry) => entry.occurredAt ? [entry.occurredAt.getTime()] : [])
  return new Date(times.length > 0 ? Math.min(...times) : Date.now())
}

const latest = (records: readonly ParsedRecord[], fallback: Date): Date => {
  const times = records.flatMap((entry) => entry.occurredAt ? [entry.occurredAt.getTime()] : [])
  return new Date(times.length > 0 ? Math.max(...times) : fallback.getTime())
}

const revisionOf = (records: readonly ParsedRecord[]): string => {
  let value = ""
  for (const entry of records) value += `${entry.rawText}\n`
  return hash(value)
}

const rawEventId = (sourceId: string, entry: ParsedRecord): string =>
  hash(`${sourceId}:${entry.offset}:${entry.rawText}`)

const projectName = (cwd: string | null): string | null => cwd ? basename(cwd) || cwd : null

const clean = (value: string | null): string | null =>
  value ? value.replace(/\s+/g, " ").trim().slice(0, 500) : null

const messageText = (value: unknown): string | null => {
  if (typeof value === "string") return value
  return clean(contentBlocks(value).flatMap((block) => string(block.text) ?? []).join("\n"))
}

const reasoningText = (value: string | null): string | null =>
  value ? value.replace(/\s+/g, " ").trim().slice(0, 4_000) || null : null

const codexReasoningText = (value: Record<string, unknown> | null): string | null =>
  reasoningText(contentBlocks(value?.summary)
    .filter((block) => block.type === "summary_text")
    .flatMap((block) => string(block.text) ?? [])
    .join("\n"))

const codexToolOutputContent = (value: unknown): CanonicalContentBlock[] => {
  const block = (candidate: unknown): CanonicalContentBlock | null => {
    if (typeof candidate === "string") return { type: "text", text: candidate }
    const candidateRecord = record(candidate)
    if (candidateRecord) {
      const type = string(candidateRecord.type)
      return type
        ? { ...candidateRecord, type }
        : { type: "json", value: candidateRecord }
    }
    return candidate === null || candidate === undefined
      ? null
      : { type: "json", value: candidate }
  }

  return (Array.isArray(value) ? value : [value]).flatMap((candidate) => block(candidate) ?? [])
}

const codexToolOutputText = (content: ReadonlyArray<CanonicalContentBlock>): string | null => {
  const text = content.flatMap((block) =>
    string(block.text) ?? string(block.content) ?? string(block.output) ?? []
  ).join("\n")
  return text || null
}

const codexSubagentLinks = (records: readonly ParsedRecord[]): Map<string, string> => {
  const callNames = new Map<string, string>()
  const links = new Map<string, string>()
  for (const entry of records) {
    if (entry.raw.type !== "response_item") continue
    const body = record(entry.raw.payload)
    if (body?.type !== "function_call" && body?.type !== "custom_tool_call") continue
    const callId = string(body.call_id)
    const name = string(body.name)
    if (callId && name) callNames.set(callId, name)
  }
  for (const entry of records) {
    const body = record(entry.raw.payload)
    if (entry.raw.type === "response_item" &&
      (body?.type === "function_call_output" || body?.type === "custom_tool_call_output")) {
      const callId = string(body.call_id)
      if (!callId || callNames.get(callId) !== "spawn_agent") continue
      const childId = string(jsonRecord(body.output)?.agent_id)
      if (childId) links.set(callId, childId)
      continue
    }
    if (entry.raw.type !== "event_msg") continue
    const callId = string(body?.call_id) ?? string(body?.event_id)
    const childId = body?.type === "collab_agent_spawn_end"
      ? string(body.new_thread_id)
      : body?.type === "subagent_activity" && body.kind === "started"
        ? string(body.agent_thread_id)
        : null
    if (callId && childId) links.set(callId, childId)
  }
  return links
}

const jsonRecord = (value: unknown): Record<string, unknown> | null => {
  const direct = record(value)
  if (direct) return direct
  if (typeof value !== "string") return null
  try {
    return record(JSON.parse(value))
  } catch {
    return null
  }
}

const contentBlocks = (value: unknown): Record<string, unknown>[] =>
  Array.isArray(value)
    ? value.flatMap((candidate) => {
      const block = record(candidate)
      return block ? [block] : []
    })
    : []

const claudeSubagentLinks = (records: readonly ParsedRecord[]): Map<string, string> => {
  const links = new Map<string, string>()
  for (const entry of records) {
    if (entry.raw.type === "queue-operation") {
      const queued = jsonRecord(entry.raw.content)
      const toolUseId = string(queued?.tool_use_id)
      const taskId = string(queued?.task_id)
      if (toolUseId && taskId) links.set(toolUseId, claudeAgentSessionId(taskId))
      continue
    }
    if (entry.raw.type !== "user") continue
    const agentId = string(record(entry.raw.toolUseResult)?.agentId)
    if (!agentId) continue
    const results = contentBlocks(record(entry.raw.message)?.content)
      .filter((block) => block.type === "tool_result")
    if (results.length !== 1) continue
    const toolUseId = string(results[0]?.tool_use_id)
    if (toolUseId && !links.has(toolUseId)) {
      links.set(toolUseId, claudeAgentSessionId(agentId))
    }
  }
  return links
}

const claudeAgentSessionId = (value: string): string =>
  value.startsWith("agent-") ? value : `agent-${value}`

const isClaudeSubagentTool = (name: string): boolean => {
  const normalized = name.toLocaleLowerCase()
  return normalized === "task" || normalized === "agent" || normalized.includes("subagent")
}

const claudeProviderUsageId = (records: readonly ParsedRecord[]): string | null => {
  for (const entry of records) {
    const messageId = string(record(entry.raw.message)?.id)
    if (messageId) return `claude:${messageId}`
  }
  for (const entry of records) {
    const requestId = string(entry.raw.requestId)
    if (requestId) return `claude-request:${requestId}`
  }
  return null
}

const isClaudeHuman = (raw: Record<string, unknown>): boolean => {
  if (raw.type !== "user" || raw.isMeta === true || string(raw.sourceToolAssistantUUID)) return false
  const content = record(raw.message)?.content
  if (typeof content === "string") return content.trim().length > 0
  const blocks = contentBlocks(content)
  return blocks.some((block) => block.type === "text" && string(block.text)) &&
    !blocks.some((block) => block.type === "tool_result")
}

const humanText = (value: unknown): string | null => {
  if (typeof value === "string") return clean(value)
  return clean(contentBlocks(value)
    .filter((block) => block.type === "text")
    .flatMap((block) => string(block.text) ?? [])
    .join("\n"))
}

const toolResultText = (value: unknown): string | null => {
  if (typeof value === "string") return clean(value)
  return clean(contentBlocks(value)
    .flatMap((block) => string(block.text) ?? string(block.content) ?? [])
    .join("\n"))
}

const claudeAssistantText = (records: readonly ParsedRecord[]): string | null => {
  const seen = new Set<string>()
  const texts: string[] = []
  for (const entry of records) {
    const content = record(entry.raw.message)?.content
    const candidates = typeof content === "string"
      ? [content]
      : contentBlocks(content).filter((block) => block.type === "text").flatMap((block) => string(block.text) ?? [])
    for (const candidate of candidates) {
      const text = clean(candidate)
      if (text && !seen.has(text)) {
        seen.add(text)
        texts.push(text)
      }
    }
  }
  return texts.join("\n") || null
}

const claudeReasoning = (records: readonly ParsedRecord[]): {
  readonly text: string | null
  readonly redacted: boolean
} | null => {
  let found = false
  let redacted = false
  const texts: string[] = []
  for (const entry of records) {
    for (const block of contentBlocks(record(entry.raw.message)?.content)) {
      if (block.type === "redacted_thinking") {
        found = true
        redacted = true
        continue
      }
      if (block.type !== "thinking") continue
      found = true
      const candidate = reasoningText(string(block.thinking))
      if (!candidate || texts.some((text) => text.includes(candidate))) continue
      for (let index = texts.length - 1; index >= 0; index -= 1) {
        if (candidate.includes(texts[index]!)) texts.splice(index, 1)
      }
      texts.push(candidate)
    }
  }
  return found ? { text: reasoningText(texts.join("\n")), redacted } : null
}

const codexUsage = (value: Record<string, unknown> | null): Usage | null => {
  if (!value) return null
  const result = {
    inputTokens: number(value.input_tokens),
    outputTokens: number(value.output_tokens),
    cachedInputTokens: number(value.cached_input_tokens),
    cacheCreationInputTokens: 0,
    reasoningOutputTokens: number(value.reasoning_output_tokens)
  }
  return hasUsage(result) ? result : null
}

const claudeUsage = (records: readonly ParsedRecord[]): { entry: ParsedRecord; value: Usage } | null => {
  let usageEntry: ParsedRecord | null = null
  let input = 0
  let output = 0
  let cached = 0
  let cacheCreation = 0
  for (const entry of records) {
    const usage = record(record(entry.raw.message)?.usage)
    if (!usage) continue
    usageEntry = entry
    input = Math.max(input, number(usage.input_tokens))
    output = Math.max(output, number(usage.output_tokens))
    cached = Math.max(cached, number(usage.cache_read_input_tokens))
    cacheCreation = Math.max(cacheCreation, number(usage.cache_creation_input_tokens))
  }
  if (!usageEntry) return null
  return {
    entry: usageEntry,
    value: {
      inputTokens: input + cached + cacheCreation,
      outputTokens: output,
      cachedInputTokens: cached,
      cacheCreationInputTokens: cacheCreation,
      reasoningOutputTokens: 0
    }
  }
}

const subtractUsage = (next: Usage, previous: Usage): Usage => ({
  inputTokens: Math.max(0, next.inputTokens - previous.inputTokens),
  outputTokens: Math.max(0, next.outputTokens - previous.outputTokens),
  cachedInputTokens: Math.max(0, next.cachedInputTokens - previous.cachedInputTokens),
  cacheCreationInputTokens: Math.max(0, next.cacheCreationInputTokens - previous.cacheCreationInputTokens),
  reasoningOutputTokens: Math.max(0, next.reasoningOutputTokens - previous.reasoningOutputTokens)
})

const monotonic = (next: Usage, previous: Usage): boolean =>
  next.inputTokens >= previous.inputTokens && next.outputTokens >= previous.outputTokens &&
  next.cachedInputTokens >= previous.cachedInputTokens &&
  next.cacheCreationInputTokens >= previous.cacheCreationInputTokens &&
  next.reasoningOutputTokens >= previous.reasoningOutputTokens

const hasUsage = (usage: Usage): boolean => Object.values(usage).some((value) => value > 0)

const usageText = (usage: Usage): string =>
  `input ${usage.inputTokens}, output ${usage.outputTokens}, cached ${usage.cachedInputTokens}, cache creation ${usage.cacheCreationInputTokens}, reasoning ${usage.reasoningOutputTokens}`

const codexOwnershipStart = (
  records: readonly ParsedRecord[],
  nativeSessionId: string,
  startedAt: Date
): number => {
  const lastForeignMetadataLine = records.reduce((latestLine, entry) => {
    if (entry.raw.type !== "session_meta") return latestLine
    const id = string(record(entry.raw.payload)?.id)
    return id && id !== nativeSessionId ? Math.max(latestLine, entry.line) : latestLine
  }, 0)
  if (lastForeignMetadataLine === 0) return 1
  const creationSecond = Math.floor(startedAt.getTime() / 1_000) * 1_000
  const taskStarts = records.filter((entry) =>
    entry.line > lastForeignMetadataLine && entry.raw.type === "event_msg" &&
    record(entry.raw.payload)?.type === "task_started"
  )
  const modern = taskStarts.find((entry) => {
    const value = record(entry.raw.payload)?.started_at
    const millis = typeof value === "number" && Number.isFinite(value)
      ? (value < 1_000_000_000_000 ? value * 1_000 : value)
      : timestamp(value)?.getTime() ?? null
    return millis !== null && millis >= creationSecond
  })
  return (modern ?? taskStarts[0])?.line ?? Number.POSITIVE_INFINITY
}
