import { realpathSync } from "node:fs"
import { readFile, stat } from "node:fs/promises"
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path"

import fuzzysort from "fuzzysort"

import { defaultClaudeHome } from "./providers/claude.ts"
import { defaultCodexHome } from "./providers/codex.ts"

export type ProjectSuggestionSource = "codex-desktop" | "claude"

export interface ProjectSuggestion {
  readonly origin: string
  readonly name: string
  readonly sources: readonly ProjectSuggestionSource[]
}

export interface GitWorktree {
  readonly origin: string
  readonly projectOrigin: string
}

export const rankProjectSuggestions = <
  Project extends Pick<ProjectSuggestion, "origin" | "name">
>(
  query: string,
  projects: readonly Project[]
): Project[] => {
  const search = query.trim()
  if (!search) return [...projects]
  const results = fuzzysort.go(search, projects, {
    key: (project) => `${project.name} ${project.origin}`
  })
  return [...results].sort((left, right) => {
    const boost = projectMatchBoost(search, right.obj) -
      projectMatchBoost(search, left.obj)
    return boost || right.score - left.score
  }).map((result) => result.obj)
}

export const discoverProjectSuggestions = async (
  options: {
    readonly codexHome?: string
    readonly claudeHome?: string
  } = {}
): Promise<ProjectSuggestion[]> => {
  const codexHome = options.codexHome ?? defaultCodexHome()
  const claudeHome = options.claudeHome ?? defaultClaudeHome()
  const [codex, claude] = await Promise.all([
    codexDesktopProjects(join(codexHome, ".codex-global-state.json")),
    claudeProjects(join(dirname(claudeHome), ".claude.json"))
  ])
  const grouped = new Map<string, {
    name: string
    sources: Set<ProjectSuggestionSource>
  }>()

  for (const project of [...codex, ...claude]) {
    if (isCodexWorktreePath(project.origin, codexHome)) continue
    if (!await isDirectory(project.origin)) continue
    const origin = normalizeProjectPath(project.origin)
    const current = grouped.get(origin) ?? {
      name: project.name || basename(origin),
      sources: new Set<ProjectSuggestionSource>()
    }
    current.sources.add(project.source)
    grouped.set(origin, current)
  }

  return [...grouped].map(([origin, project]) => ({
    origin,
    name: project.name,
    sources: [...project.sources].sort()
  })).sort((left, right) =>
    left.name.localeCompare(right.name) || left.origin.localeCompare(right.origin)
  )
}

export const discoverGitWorktrees = async (
  projectOrigins: readonly string[]
): Promise<GitWorktree[]> => {
  const worktrees = new Map<string, GitWorktree>()
  for (const projectOrigin of projectOrigins) {
    const project = realProjectPath(projectOrigin)
    const roots = await gitWorktrees(projectOrigin)
    const projectRoot = roots.filter((root) => pathIsWithin(project, root))
      .sort((left, right) => right.length - left.length)[0]
    if (!projectRoot) continue
    const projectRelativePath = relative(projectRoot, project)
    for (const root of roots) {
      const origin = normalizeProjectPath(resolve(root, projectRelativePath))
      if (origin === project || !await isDirectory(origin)) continue
      worktrees.set(`${projectOrigin}\0${origin}`, { origin, projectOrigin })
    }
  }
  return [...worktrees.values()].sort((left, right) =>
    left.origin.localeCompare(right.origin)
  )
}

export const isCodexWorktreePath = (
  origin: string,
  codexHome = defaultCodexHome()
): boolean => codexWorktreePath(origin, codexHome) !== null

const codexWorktreePath = (origin: string, codexHome: string): string | null => {
  const worktrees = resolve(codexHome, "worktrees")
  const roots = [worktrees]
  try {
    roots.push(resolve(realpathSync(codexHome), "worktrees"))
  } catch {
    // A missing Codex home has no worktrees.
  }
  for (const root of new Set(roots)) {
    const candidate = relative(root, resolve(origin))
    if (candidate && pathIsWithin(resolve(origin), root)) {
      return normalizeProjectPath(resolve(worktrees, candidate))
    }
  }
  return null
}

const pathIsWithin = (path: string, root: string): boolean => {
  const candidate = relative(root, path)
  return candidate === "" || (
    candidate !== ".." &&
    !candidate.startsWith(`..${sep}`) &&
    !isAbsolute(candidate)
  )
}

const codexDesktopProjects = async (
  statePath: string
): Promise<{
  readonly origin: string
  readonly name: string
  readonly source: "codex-desktop"
}[]> => {
  const projects = record(record(await readJson(statePath))?.["local-projects"])
  if (!projects) return []
  return Object.values(projects).flatMap((value) => {
    const project = record(value)
    if (!project || !Array.isArray(project.rootPaths)) return []
    const name = typeof project.name === "string" ? project.name.trim() : ""
    return project.rootPaths.flatMap((rootPath) =>
      typeof rootPath === "string" && rootPath.trim()
        ? [{
            origin: rootPath,
            name: name || basename(rootPath),
            source: "codex-desktop" as const
          }]
        : []
    )
  })
}

const claudeProjects = async (
  statePath: string
): Promise<{
  readonly origin: string
  readonly name: string
  readonly source: "claude"
}[]> => {
  const projects = record(record(await readJson(statePath))?.projects)
  if (!projects) return []
  return Object.keys(projects).filter((origin) => origin.trim()).map((origin) => ({
    origin,
    name: basename(origin),
    source: "claude" as const
  }))
}

const gitWorktrees = async (projectOrigin: string): Promise<string[]> => {
  try {
    const child = Bun.spawn([
      "git", "-C", projectOrigin, "worktree", "list", "--porcelain", "-z"
    ], { stdout: "pipe", stderr: "ignore" })
    const [output, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      child.exited
    ])
    if (exitCode !== 0) return []
    return output.split("\0").flatMap((line) =>
      line.startsWith("worktree ")
        ? [normalizeProjectPath(line.slice("worktree ".length))]
        : []
    )
  } catch {
    return []
  }
}

const projectMatchBoost = (
  search: string,
  project: Pick<ProjectSuggestion, "origin" | "name">
): number => {
  const query = search.toLocaleLowerCase()
  const name = project.name.toLocaleLowerCase()
  const folder = basename(project.origin).toLocaleLowerCase()
  if (name === query) return 4
  if (name.startsWith(query)) return 3
  if (folder === query) return 2.5
  if (folder.startsWith(query)) return 2
  if (name.split(/\W+/).some((word) => word.startsWith(query))) return 1
  return 0
}

const normalizeProjectPath = (value: string): string => {
  const normalized = value.replaceAll("\\", "/")
  return normalized.length > 1 ? normalized.replace(/\/+$/, "") : normalized
}

const realProjectPath = (value: string): string => {
  try {
    return normalizeProjectPath(realpathSync(value))
  } catch {
    return normalizeProjectPath(resolve(value))
  }
}

const isDirectory = async (path: string): Promise<boolean> => {
  try {
    return (await stat(path)).isDirectory()
  } catch {
    return false
  }
}

const readJson = async (path: string): Promise<unknown> => {
  try {
    return JSON.parse(await readFile(path, "utf8")) as unknown
  } catch {
    return null
  }
}

const record = (value: unknown): Record<string, unknown> | null =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null
