import { readFile, readdir, stat } from "node:fs/promises"
import { basename, join } from "node:path"
import { Database } from "bun:sqlite"

import { hash, record, string, type SourceFile } from "../domain.ts"

const uuidAtEnd = /([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\.jsonl$/i

export const defaultCodexHome = (): string =>
  process.env.HT_CODEX_HOME ?? process.env.CODEX_HOME ?? `${process.env.HOME}/.codex`

export const sessionIdFromPath = (path: string): string | null =>
  uuidAtEnd.exec(basename(path))?.[1] ?? null

export const codexSourceAtPath = async (
  home: string,
  path: string,
  classification: SourceFile["classification"]
): Promise<SourceFile> => {
  const details = await stat(path)
  const titles = await codexThreadTitles(home)
  const nativeTitle = titles.get(sessionIdFromPath(path) ?? "")
  return {
    path,
    classification,
    size: details.size,
    modifiedAt: details.mtime,
    ...(nativeTitle ? { nativeTitle } : {}),
    metadataRevision: hash(nativeTitle ?? "")
  }
}

export const discoverCodexSources = async (home: string): Promise<SourceFile[]> => {
  const sources: SourceFile[] = []
  for (const [directory, classification] of [
    [join(home, "sessions"), "active"],
    [join(home, "archived_sessions"), "archived"]
  ] as const) {
    await walkJsonl(directory, classification, sources)
  }

  // A fixture directory can contain session JSONL files directly. This also makes
  // custom test imports convenient without pretending it is a Codex home.
  if (sources.length === 0) await walkJsonl(home, "fixture", sources)
  const titles = await codexThreadTitles(home)
  return sources
    .map((source) => {
      const nativeTitle = titles.get(sessionIdFromPath(source.path) ?? "")
      return {
        ...source,
        ...(nativeTitle ? { nativeTitle } : {}),
        metadataRevision: hash(nativeTitle ?? "")
      }
    })
    .sort((left, right) => left.path.localeCompare(right.path))
}

const codexThreadTitles = async (home: string): Promise<Map<string, string>> => {
  const titles = await sessionIndexTitles(join(home, "session_index.jsonl"))
  const databases = await stateDatabasePaths(home)

  for (const path of databases) {
    let database: Database | null = null
    try {
      database = new Database(path, { readonly: true })
      const rows = database.query<{ id: string; title: string }, []>(`
        SELECT id, title
        FROM threads
        WHERE length(trim(title)) > 0
      `).all()
      // session_index.jsonl contains Codex's generated or user-renamed thread
      // names. The state database title can still be the first prompt, so it is
      // only a fallback when the portable index has no title for the thread.
      for (const row of rows) {
        if (!titles.has(row.id)) titles.set(row.id, row.title)
      }
      return titles
    } catch {
      // Try an older state schema before falling back to the portable index.
    } finally {
      database?.close()
    }
  }

  return titles
}

const stateDatabasePaths = async (home: string): Promise<string[]> => {
  try {
    const entries = await readdir(home, { withFileTypes: true })
    return entries
      .flatMap((entry) => {
        if (!entry.isFile()) return []
        const match = /^state_(\d+)\.sqlite$/.exec(entry.name)
        return match ? [{ path: join(home, entry.name), version: Number(match[1]) }] : []
      })
      .sort((left, right) => right.version - left.version)
      .map((entry) => entry.path)
  } catch (error) {
    if (isMissing(error)) return []
    throw error
  }
}

const sessionIndexTitles = async (path: string): Promise<Map<string, string>> => {
  const titles = new Map<string, string>()
  let content: string
  try {
    content = await readFile(path, "utf8")
  } catch (error) {
    if (isMissing(error)) return titles
    throw error
  }

  for (const line of content.split("\n")) {
    if (!line.trim()) continue
    try {
      const row = record(JSON.parse(line))
      const id = string(row?.id)
      const title = string(row?.thread_name)
      if (id && title) titles.set(id, title)
    } catch {
      // One malformed index line should not discard other harness titles.
    }
  }
  return titles
}

const walkJsonl = async (
  directory: string,
  classification: SourceFile["classification"],
  into: SourceFile[]
): Promise<void> => {
  let entries
  try {
    entries = await readdir(directory, { withFileTypes: true })
  } catch (error) {
    if (isMissing(error)) return
    throw error
  }
  for (const entry of entries) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) {
      await walkJsonl(path, classification, into)
    } else if (entry.isFile() && entry.name.endsWith(".jsonl")) {
      const details = await stat(path)
      into.push({ path, classification, size: details.size, modifiedAt: details.mtime })
    }
  }
}

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
