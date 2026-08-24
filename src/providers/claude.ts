import { readdir, stat } from "node:fs/promises"
import { basename, join, sep } from "node:path"

import type { SourceFile } from "../domain.ts"

export const defaultClaudeHome = (): string =>
  process.env.HT_CLAUDE_HOME ?? process.env.CLAUDE_CONFIG_DIR ?? `${process.env.HOME}/.claude`

export const claudeSessionIdFromPath = (path: string): string => basename(path, ".jsonl")

export const claudeParentIdFromPath = (path: string): string | null => {
  const parts = path.split(sep)
  const subagents = parts.lastIndexOf("subagents")
  return subagents > 0 ? parts[subagents - 1] ?? null : null
}

export const isClaudeSubagentPath = (path: string): boolean =>
  path.split(sep).includes("subagents")

export const discoverClaudeSources = async (home: string): Promise<SourceFile[]> => {
  const projects = join(home, "projects")
  let projectDirectories
  try {
    projectDirectories = (await readdir(projects, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory())
      .map((entry) => join(projects, entry.name))
  } catch (error) {
    if (isMissing(error)) return []
    throw error
  }

  const sources: SourceFile[] = []
  for (const project of projectDirectories) {
    const entries = await readdir(project, { withFileTypes: true })
    for (const entry of entries) {
      const path = join(project, entry.name)
      if (entry.isFile() && entry.name.endsWith(".jsonl")) {
        await addSource(path, "active", sources)
      } else if (entry.isDirectory()) {
        await walkSubagents(path, sources)
      }
    }
  }
  return sources.sort((left, right) => left.path.localeCompare(right.path))
}

const walkSubagents = async (directory: string, into: SourceFile[]): Promise<void> => {
  let entries
  try {
    entries = await readdir(directory, { withFileTypes: true })
  } catch (error) {
    if (isMissing(error)) return
    throw error
  }
  for (const entry of entries) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) await walkSubagents(path, into)
    else if (entry.isFile() && entry.name.endsWith(".jsonl") && isClaudeSubagentPath(path)) {
      await addSource(path, "active", into)
    }
  }
}

const addSource = async (
  path: string,
  classification: SourceFile["classification"],
  into: SourceFile[]
): Promise<void> => {
  const details = await stat(path)
  into.push({ path, classification, size: details.size, modifiedAt: details.mtime })
}

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
