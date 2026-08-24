import { open } from "node:fs/promises"

import {
  type ParseDiagnostic,
  type ParsedRecord,
  type SourceFile,
  record,
  timestamp
} from "./domain.ts"

export const parseJsonlSource = async (
  source: SourceFile,
  startOffset = 0,
  startLine = 1
): Promise<{
  records: ParsedRecord[]
  diagnostics: ParseDiagnostic[]
  nextOffset: number
  nextLine: number
}> => {
  const safeStart = Math.max(0, Math.min(startOffset, source.size))
  const body = Buffer.alloc(Math.max(0, source.size - safeStart))
  const file = await open(source.path, "r")
  let bytesRead = 0
  try {
    while (bytesRead < body.byteLength) {
      const result = await file.read(
        body,
        bytesRead,
        body.byteLength - bytesRead,
        safeStart + bytesRead
      )
      if (result.bytesRead === 0) break
      bytesRead += result.bytesRead
    }
  } finally {
    await file.close()
  }
  const tail = body.subarray(0, bytesRead)
  const records: ParsedRecord[] = []
  const diagnostics: ParseDiagnostic[] = []
  let cursor = 0
  let lineNumber = startLine

  while (cursor < tail.byteLength) {
    const newline = tail.indexOf(10, cursor)
    // Provider logs are append-only. Leave an incomplete final record for the
    // next scan instead of checkpointing bytes that cannot yet be parsed.
    if (newline < 0) break
    const line = tail.subarray(cursor, newline).toString("utf8").replace(/\r$/, "")
    const trimmed = line.trim()
    if (trimmed.length === 0) {
      cursor = newline + 1
      lineNumber += 1
      continue
    }
    try {
      const raw = JSON.parse(trimmed) as unknown
      const parsed = record(raw)
      if (!parsed) throw new Error("Record is not an object")
      records.push({
        line: lineNumber,
        offset: safeStart + cursor,
        raw: parsed,
        rawText: trimmed,
        occurredAt: timestamp(parsed.timestamp)
      })
    } catch (error) {
      diagnostics.push({
        sourcePath: source.path,
        line: lineNumber,
        message: error instanceof Error ? error.message : "Invalid JSON"
      })
    }
    cursor = newline + 1
    lineNumber += 1
  }
  return {
    records,
    diagnostics,
    nextOffset: safeStart + cursor,
    nextLine: lineNumber
  }
}
