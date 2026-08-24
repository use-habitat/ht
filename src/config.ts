import { createHash } from "node:crypto"
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { homedir } from "node:os"
import { dirname, isAbsolute, join, resolve } from "node:path"

import { z } from "zod"

import { terminateOwnedSubprocess } from "./owned-subprocess.ts"
import {
  discoverGitWorktrees,
  isCodexWorktreePath
} from "./project-suggestions.ts"

export interface HabitatWorkspaceConfig {
  readonly kind: "habitat"
  readonly apiUrl: string
  readonly workspace: { readonly id: string; readonly slug: string; readonly name: string }
  readonly principal: { readonly id: string; readonly kind: string; readonly name: string }
  readonly credentialStorage: "keychain" | "file"
  readonly loggedInAt: string
}

export interface ProjectRoute {
  readonly origin: string
  readonly name: string
  readonly workspaceId: string
  readonly providers: readonly ("codex" | "claude")[]
  readonly selectedAt: string
}

export interface HTConfig {
  readonly $schema?: string
  readonly schemaVersion: 2
  readonly daemonPort: number
  readonly activeWorkspaceId: string | null
  readonly workspaces: readonly HabitatWorkspaceConfig[]
  readonly projects: readonly ProjectRoute[]
  readonly backfillWindow: "30d" | "90d" | "all"
  readonly upload: {
    readonly projectOrigins: {
      readonly include?: readonly string[]
      readonly exclude?: readonly string[]
    }
  }
}

export interface HTPaths {
  readonly home: string
  readonly config: string
  readonly legacyConfig: string
  readonly credentials: string
  readonly database: string
  readonly status: string
  readonly bin: string
  readonly launcher: string
  readonly logs: string
}

export interface HabitatExportConnection {
  readonly kind: "habitat"
  readonly apiUrl: string
  readonly apiKey: string
  readonly destinationId: string
  readonly config: HabitatWorkspaceConfig | null
}

export interface HabitatLogoutResult {
  readonly workspaces: readonly {
    readonly id: string
    readonly slug: string
    readonly name: string
  }[]
  readonly activeWorkspaceId: string | null
  readonly remainingWorkspaces: number
}

const patternsSchema = z.array(z.string().trim().min(1).max(2_000)).max(1_000)
const uploadSchema = z.object({
  projectOrigins: z.object({
    include: patternsSchema.optional(),
    exclude: patternsSchema.optional()
  }).strict().default({})
}).strict().default({ projectOrigins: {} })
const workspaceSchema = z.object({
  kind: z.literal("habitat"),
  apiUrl: z.string().url(),
  workspace: z.object({
    id: z.string().min(1),
    slug: z.string().min(1),
    name: z.string().min(1)
  }).strict(),
  principal: z.object({
    id: z.string().min(1),
    kind: z.string().min(1),
    name: z.string().min(1)
  }).strict(),
  credentialStorage: z.enum(["keychain", "file"]),
  loggedInAt: z.string().min(1)
}).strict()
const projectSchema = z.object({
  origin: z.string().min(1),
  name: z.string().min(1),
  workspaceId: z.string().min(1),
  providers: z.array(z.enum(["codex", "claude"])),
  selectedAt: z.string().min(1)
}).strict()
const configSchema = z.object({
  $schema: z.string().optional(),
  schemaVersion: z.literal(2),
  daemonPort: z.number().int().min(1).max(65_535),
  activeWorkspaceId: z.string().min(1).nullable(),
  workspaces: z.array(workspaceSchema),
  projects: z.array(projectSchema),
  backfillWindow: z.enum(["30d", "90d", "all"]),
  upload: uploadSchema
}).strict()
const uploadPolicySchema = z.object({
  $schema: z.string().optional(),
  schemaVersion: z.literal(1),
  upload: uploadSchema
}).strict()

const emptyConfig = (): HTConfig => ({
  schemaVersion: 2,
  daemonPort: 4322,
  activeWorkspaceId: null,
  workspaces: [],
  projects: [],
  backfillWindow: "30d",
  upload: { projectOrigins: {} }
})

export interface ConfigPathOptions {
  readonly env?: Readonly<Record<string, string | undefined>>
  readonly home?: string
  readonly cwd?: string
}

export const configPath = (options: ConfigPathOptions = {}): string => {
  const environment = options.env ?? process.env
  const home = options.home ?? homedir()
  const cwd = options.cwd ?? process.cwd()
  const explicit = environment.HT_CONFIG?.trim()
  if (explicit) return isAbsolute(explicit) ? explicit : resolve(cwd, explicit)
  const stateHome = environment.HT_HOME?.trim()
  if (stateHome) {
    const absoluteStateHome = isAbsolute(stateHome) ? stateHome : resolve(cwd, stateHome)
    return join(absoluteStateHome, "ht.config.json")
  }
  const configHome = environment.XDG_CONFIG_HOME?.trim()
  return join(
    configHome && isAbsolute(configHome) ? configHome : join(home, ".config"),
    "ht",
    "ht.config.json"
  )
}

export const htPaths = (explicitHome?: string): HTPaths => {
  const home = explicitHome ?? process.env.HT_HOME ?? join(homedir(), ".ht")
  return {
    home,
    config: explicitHome === undefined ? configPath() : join(home, "ht.config.json"),
    legacyConfig: join(home, "config.json"),
    credentials: join(home, "credentials.json"),
    database: join(home, "sessions.sqlite"),
    status: join(home, "daemon-status.json"),
    bin: join(home, "bin"),
    launcher: join(home, "bin", "ht"),
    logs: join(home, "logs")
  }
}

export const configPaths = (options: ConfigPathOptions = {}): HTPaths => {
  const environment = options.env ?? process.env
  const cwd = options.cwd ?? process.cwd()
  const userHome = options.home ?? homedir()
  const configuredHome = environment.HT_HOME?.trim()
  const stateHome = configuredHome
    ? isAbsolute(configuredHome) ? configuredHome : resolve(cwd, configuredHome)
    : join(userHome, ".ht")
  return { ...htPaths(stateHome), config: configPath(options) }
}

export const readConfig = async (paths = htPaths()): Promise<HTConfig> => {
  let candidate: unknown
  try {
    candidate = JSON.parse(await readFile(paths.config, "utf8")) as unknown
  } catch (error) {
    if (!isMissing(error)) throw configReadError(error, paths.config)
    const legacy = await readLegacyConfig(paths)
    if (!legacy) return emptyConfig()
    await writeConfig(legacy, paths)
    await removeLegacyConfig(paths)
    return legacy
  }

  try {
    const config = isUploadPolicyConfig(candidate)
      ? {
          ...(await readLegacyConfig(paths) ?? emptyConfig()),
          upload: uploadConfig(candidate)
        }
      : migrateConfig(candidate)
    if (!isCurrentConfig(candidate)) await writeConfig(config, paths)
    await removeLegacyConfig(paths)
    return config
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Invalid HT config at ")) {
      throw error
    }
    throw new Error(`Invalid HT config at ${paths.config}: ${message(error)}`)
  }
}

const readLegacyConfig = async (paths: HTPaths): Promise<HTConfig | null> => {
  if (paths.legacyConfig === paths.config) return null
  try {
    return migrateConfig(JSON.parse(await readFile(paths.legacyConfig, "utf8")) as unknown)
  } catch (error) {
    if (isMissing(error)) return null
    throw error
  }
}

const removeLegacyConfig = async (paths: HTPaths): Promise<void> => {
  if (paths.legacyConfig !== paths.config) {
    await rm(paths.legacyConfig, { force: true })
  }
}

export const writeConfig = async (
  config: HTConfig,
  paths = htPaths()
): Promise<void> => {
  await mkdir(dirname(paths.config), { recursive: true, mode: 0o700 })
  await atomicJson(paths.config, config, 0o600)
}

export const saveHabitatLogin = async (
  value: Omit<HabitatWorkspaceConfig, "kind" | "credentialStorage" | "loggedInAt">,
  apiKey: string,
  paths = htPaths()
): Promise<HTConfig> => {
  await mkdir(paths.home, { recursive: true, mode: 0o700 })
  const credentialStorage = await saveApiKey(
    value.apiUrl,
    value.workspace.id,
    apiKey,
    paths
  )
  const current = await readConfig(paths)
  const workspace: HabitatWorkspaceConfig = {
    kind: "habitat",
    ...value,
    credentialStorage,
    loggedInAt: new Date().toISOString()
  }
  const config: HTConfig = {
    ...current,
    schemaVersion: 2,
    daemonPort: validPort(Number(process.env.HT_DAEMON_PORT)) ?? current.daemonPort,
    activeWorkspaceId: workspace.workspace.id,
    workspaces: [
      ...current.workspaces.filter((candidate) =>
        candidate.workspace.id !== workspace.workspace.id
      ),
      workspace
    ]
  }
  await writeConfig(config, paths)
  return config
}

export const saveSetupSelection = async (
  input: {
    readonly activeWorkspaceId: string
    readonly projects: readonly Omit<ProjectRoute, "selectedAt">[]
    readonly backfillWindow: HTConfig["backfillWindow"]
  },
  paths = htPaths()
): Promise<HTConfig> => {
  const current = await readConfig(paths)
  if (!current.workspaces.some((workspace) =>
    workspace.workspace.id === input.activeWorkspaceId
  )) {
    throw new Error(`Habitat workspace ${input.activeWorkspaceId} is not logged in.`)
  }
  const selectedAt = new Date().toISOString()
  const selectedOrigins = new Set(input.projects.map((project) => project.origin))
  const config: HTConfig = {
    ...current,
    activeWorkspaceId: input.activeWorkspaceId,
    backfillWindow: input.backfillWindow,
    projects: [
      ...current.projects.filter((project) =>
        project.workspaceId !== input.activeWorkspaceId &&
        !selectedOrigins.has(project.origin)
      ),
      ...input.projects.map((project) => ({ ...project, selectedAt }))
    ]
  }
  await writeConfig(config, paths)
  return config
}

export const logoutHabitat = async (
  input: {
    readonly workspaceId?: string
    readonly all?: boolean
  } = {},
  paths = htPaths()
): Promise<HabitatLogoutResult> => {
  const current = await readConfig(paths)
  const targets = input.all
    ? [...current.workspaces]
    : (() => {
        const workspaceId = input.workspaceId ?? current.activeWorkspaceId
        if (!workspaceId) {
          throw new Error("No Habitat workspace is logged in.")
        }
        const workspace = current.workspaces.find((candidate) =>
          candidate.workspace.id === workspaceId
        )
        if (!workspace) {
          throw new Error(`Habitat workspace ${workspaceId} is not logged in.`)
        }
        return [workspace]
      })()
  const targetIds = new Set(targets.map((workspace) => workspace.workspace.id))
  const remaining = current.workspaces.filter((workspace) =>
    !targetIds.has(workspace.workspace.id)
  )
  const activeWorkspaceId = current.activeWorkspaceId &&
    !targetIds.has(current.activeWorkspaceId)
    ? current.activeWorkspaceId
    : remaining[0]?.workspace.id ?? null

  await removeApiKeys(
    targets,
    targets.length === current.workspaces.length,
    paths
  )
  await writeConfig({
    ...current,
    activeWorkspaceId,
    workspaces: remaining,
    projects: current.projects.filter((project) =>
      !targetIds.has(project.workspaceId)
    )
  }, paths)

  return {
    workspaces: targets.map((workspace) => workspace.workspace),
    activeWorkspaceId,
    remainingWorkspaces: remaining.length
  }
}

export const resolveHabitatExport = async (
  paths = htPaths(),
  workspaceId?: string
): Promise<HabitatExportConnection | null> => {
  if (workspaceId) {
    const config = await readConfig(paths)
    const workspace = config.workspaces.find((candidate) =>
      candidate.workspace.id === workspaceId
    )
    if (!workspace) return null
    return {
      kind: "habitat",
      apiUrl: workspace.apiUrl,
      apiKey: await readApiKey(workspace, paths),
      destinationId: habitatDestinationId(
        workspace.apiUrl,
        `workspace-${workspace.workspace.id}`
      ),
      config: workspace
    }
  }
  const connections = await resolveHabitatExports(paths)
  const config = await readConfig(paths)
  return connections.find((connection) =>
    connection.config?.workspace.id === config.activeWorkspaceId
  ) ?? connections[0] ?? null
}

export const resolveHabitatExports = async (
  paths = htPaths()
): Promise<HabitatExportConnection[]> => {
  const environmentUrl = process.env.HABITAT_API_URL
  const environmentKey = process.env.HABITAT_API_KEY
  if (environmentUrl || environmentKey) {
    if (!environmentUrl || !environmentKey) {
      throw new Error("Habitat export requires HABITAT_API_URL and HABITAT_API_KEY.")
    }
    return [{
      kind: "habitat",
      apiUrl: environmentUrl.replace(/\/$/, ""),
      apiKey: environmentKey,
      destinationId: habitatDestinationId(
        environmentUrl,
        `credential-${createHash("sha256").update(environmentKey).digest("hex").slice(0, 16)}`
      ),
      config: null
    }]
  }
  const config = await readConfig(paths)
  return Promise.all(config.workspaces.map(async (workspace) => ({
    kind: "habitat" as const,
    apiUrl: workspace.apiUrl,
    apiKey: await readApiKey(workspace, paths),
    destinationId: habitatDestinationId(
      workspace.apiUrl,
      `workspace-${workspace.workspace.id}`
    ),
    config: workspace
  })))
}

export const projectWorkspaceId = (
  config: HTConfig,
  projectOrigin: string | null,
  provider?: string
): string | null => {
  if (!projectOrigin) return null
  const normalized = normalizeOrigin(projectOrigin)
  return config.projects
    .filter((project) => {
      const projectRoot = normalizeOrigin(project.origin)
      return originIsWithin(normalized, projectRoot) &&
        (provider === undefined || project.providers.some((candidate) =>
          candidate === provider
        ))
    })
    .sort((left, right) =>
      normalizeOrigin(right.origin).length - normalizeOrigin(left.origin).length
    )[0]?.workspaceId ?? null
}

export const expandGitWorktreeRoutes = async (
  config: HTConfig,
  options: { readonly codexHome?: string } = {}
): Promise<HTConfig> => {
  const worktrees = await discoverGitWorktrees(
    config.projects.filter((project) =>
      !isCodexWorktreePath(project.origin, options.codexHome)
    ).map((project) => project.origin)
  )
  if (worktrees.length === 0) return config

  const projects = new Map(config.projects.map((project) => [
    normalizeOrigin(project.origin),
    project
  ]))
  for (const worktree of worktrees) {
    const origin = normalizeOrigin(worktree.origin)
    if (projects.has(origin)) continue
    const project = projects.get(normalizeOrigin(worktree.projectOrigin))
    if (!project) continue
    projects.set(origin, {
      ...project,
      origin: worktree.origin
    })
  }
  return { ...config, projects: [...projects.values()] }
}

export const projectsForWorkspace = (
  config: HTConfig,
  workspaceId: string
): readonly ProjectRoute[] =>
  config.projects.filter((project) => project.workspaceId === workspaceId)

export const daemonPort = async (paths = htPaths()): Promise<number> =>
  validPort(Number(process.env.HT_DAEMON_PORT)) ?? (await readConfig(paths)).daemonPort

export const writeDaemonStatus = async (value: unknown, paths = htPaths()): Promise<void> => {
  await mkdir(paths.home, { recursive: true, mode: 0o700 })
  await atomicJson(paths.status, value, 0o600)
}

export const readDaemonStatus = async (paths = htPaths()): Promise<unknown | null> => {
  try {
    return JSON.parse(await readFile(paths.status, "utf8")) as unknown
  } catch (error) {
    if (isMissing(error)) return null
    throw error
  }
}

const readApiKey = async (
  workspace: HabitatWorkspaceConfig,
  paths: HTPaths
): Promise<string> => {
  if (workspace.credentialStorage === "keychain") {
    const result = await run([
      "security", "find-generic-password",
      "-s", keychainService(workspace.apiUrl, workspace.workspace.id),
      "-a", "ht", "-w"
    ])
    if (result.ok && result.stdout.trim()) return result.stdout.trim()
    const legacy = await run([
      "security", "find-generic-password",
      "-s", legacyKeychainService(workspace.apiUrl),
      "-a", "ht", "-w"
    ])
    if (legacy.ok && legacy.stdout.trim()) return legacy.stdout.trim()
  }
  try {
    const parsed = JSON.parse(await readFile(paths.credentials, "utf8")) as {
      habitatApiKey?: unknown
      habitatApiKeys?: Record<string, unknown>
    }
    const scoped = parsed.habitatApiKeys?.[workspace.workspace.id]
    if (typeof scoped === "string" && scoped.length > 0) return scoped
    if (typeof parsed.habitatApiKey === "string" && parsed.habitatApiKey.length > 0) {
      return parsed.habitatApiKey
    }
  } catch {
    // Normalized error below.
  }
  throw new Error(
    `Habitat credentials for ${workspace.workspace.name} are missing. Run \`ht setup\` again.`
  )
}

const saveApiKey = async (
  apiUrl: string,
  workspaceId: string,
  apiKey: string,
  paths: HTPaths
): Promise<"keychain" | "file"> => {
  if (process.platform === "darwin" && process.env.HT_DISABLE_KEYCHAIN !== "1") {
    const stored = await run([
      "security", "add-generic-password", "-U",
      "-s", keychainService(apiUrl, workspaceId),
      "-a", "ht", "-w", apiKey
    ])
    if (stored.ok) return "keychain"
  }
  let existing: Record<string, unknown> = {}
  try {
    existing = JSON.parse(await readFile(paths.credentials, "utf8")) as Record<string, unknown>
  } catch (error) {
    if (!isMissing(error)) throw error
  }
  const current = typeof existing.habitatApiKeys === "object" &&
    existing.habitatApiKeys !== null &&
    !Array.isArray(existing.habitatApiKeys)
    ? existing.habitatApiKeys as Record<string, unknown>
    : {}
  await atomicJson(paths.credentials, {
    ...existing,
    habitatApiKeys: { ...current, [workspaceId]: apiKey }
  }, 0o600)
  return "file"
}

const removeApiKeys = async (
  workspaces: readonly HabitatWorkspaceConfig[],
  removeLegacy: boolean,
  paths: HTPaths
): Promise<void> => {
  if (process.platform === "darwin") {
    const keychainWorkspaces = workspaces.filter((workspace) =>
      workspace.credentialStorage === "keychain"
    )
    for (const workspace of keychainWorkspaces) {
      await deleteKeychainApiKey(
        keychainService(workspace.apiUrl, workspace.workspace.id)
      )
    }
    if (removeLegacy) {
      for (const apiUrl of new Set(
        keychainWorkspaces.map((workspace) => workspace.apiUrl)
      )) {
        await deleteKeychainApiKey(legacyKeychainService(apiUrl))
      }
    }
  }

  let existing: Record<string, unknown>
  try {
    existing = JSON.parse(await readFile(paths.credentials, "utf8")) as Record<string, unknown>
  } catch (error) {
    if (isMissing(error)) return
    throw error
  }
  const next = { ...existing }
  if (
    typeof existing.habitatApiKeys === "object" &&
    existing.habitatApiKeys !== null &&
    !Array.isArray(existing.habitatApiKeys)
  ) {
    const habitatApiKeys = {
      ...existing.habitatApiKeys as Record<string, unknown>
    }
    for (const workspace of workspaces) {
      delete habitatApiKeys[workspace.workspace.id]
    }
    if (Object.keys(habitatApiKeys).length > 0) {
      next.habitatApiKeys = habitatApiKeys
    } else {
      delete next.habitatApiKeys
    }
  }
  if (removeLegacy) delete next.habitatApiKey
  await atomicJson(paths.credentials, next, 0o600)
}

const deleteKeychainApiKey = async (service: string): Promise<void> => {
  const existing = await run([
    "security", "find-generic-password",
    "-s", service,
    "-a", "ht"
  ])
  if (!existing.ok) return
  const removed = await run([
    "security", "delete-generic-password",
    "-s", service,
    "-a", "ht"
  ])
  if (!removed.ok) {
    throw new Error("Could not remove Habitat credentials from macOS Keychain.")
  }
}

const migrateConfig = (candidate: unknown): HTConfig => {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    throw new Error("Invalid HT configuration.")
  }
  const value = candidate as Record<string, unknown>
  if (value.schemaVersion === 2) {
    const result = configSchema.safeParse({
      ...value,
      upload: value.upload ?? { projectOrigins: {} }
    })
    if (!result.success) {
      throw new Error(configIssues(result.error.issues))
    }
    return result.data
  }
  if (value.schemaVersion === 1) {
    const legacy = value as {
      daemonPort?: unknown
      exporter?: HabitatWorkspaceConfig | null
    }
    const workspace = legacy.exporter ?? null
    return {
      ...emptyConfig(),
      daemonPort: validPort(Number(legacy.daemonPort)) ?? 4322,
      activeWorkspaceId: workspace?.workspace.id ?? null,
      workspaces: workspace ? [workspace] : []
    }
  }
  throw new Error("Unsupported HT configuration.")
}

const isUploadPolicyConfig = (candidate: unknown): boolean =>
  typeof candidate === "object" && candidate !== null && !Array.isArray(candidate) &&
  (candidate as Record<string, unknown>).schemaVersion === 1 &&
  "upload" in candidate

const uploadConfig = (candidate: unknown): HTConfig["upload"] => {
  const result = uploadPolicySchema.safeParse(candidate)
  if (!result.success) throw new Error(configIssues(result.error.issues))
  return result.data.upload
}

const isCurrentConfig = (candidate: unknown): boolean =>
  typeof candidate === "object" && candidate !== null && !Array.isArray(candidate) &&
  (candidate as Record<string, unknown>).schemaVersion === 2 &&
  "upload" in candidate

const configIssues = (issues: readonly z.core.$ZodIssue[]): string =>
  issues.map((issue) => {
    const location = issue.path.length > 0 ? issue.path.join(".") : "config"
    return `${location}: ${issue.message}`
  }).join("; ")

const configReadError = (error: unknown, path: string): Error =>
  error instanceof SyntaxError
    ? new Error(`Invalid JSON in HT config at ${path}: ${error.message}`)
    : error instanceof Error ? error : new Error(String(error))

const message = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const habitatDestinationId = (apiUrl: string, scope: string): string =>
  `habitat:${apiUrl.replace(/\/$/, "")}:${scope}`

const keychainService = (apiUrl: string, workspaceId: string): string =>
  `com.usehabitat.ht.${createHash("sha256")
    .update(`${apiUrl}:${workspaceId}`)
    .digest("hex")
    .slice(0, 16)}`

const legacyKeychainService = (apiUrl: string): string =>
  `com.usehabitat.ht.${createHash("sha256").update(apiUrl).digest("hex").slice(0, 16)}`

const normalizeOrigin = (value: string): string => {
  const normalized = value.replaceAll("\\", "/")
  return normalized.length > 1 ? normalized.replace(/\/+$/, "") : normalized
}

const originIsWithin = (origin: string, projectRoot: string): boolean =>
  origin === projectRoot ||
  (projectRoot === "/" ? origin.startsWith("/") : origin.startsWith(`${projectRoot}/`))

const atomicJson = async (path: string, value: unknown, mode: number): Promise<void> => {
  const temporary = `${path}.${process.pid}.tmp`
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode })
  await chmod(temporary, mode)
  await rename(temporary, path)
}

const run = async (command: string[]): Promise<{ ok: boolean; stdout: string }> => {
  let child: ReturnType<typeof Bun.spawn> | null = null
  try {
    const spawned = Bun.spawn(command, { stdout: "pipe", stderr: "ignore" })
    child = spawned
    const [stdout, exitCode] = await Promise.all([
      new Response(spawned.stdout).text(),
      spawned.exited
    ])
    return { ok: exitCode === 0, stdout }
  } catch {
    return { ok: false, stdout: "" }
  } finally {
    if (child?.exitCode === null) {
      await terminateOwnedSubprocess(child)
    }
  }
}

const validPort = (value: number): number | null =>
  Number.isInteger(value) && value > 0 && value <= 65_535 ? value : null

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
