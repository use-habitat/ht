import { statSync } from "node:fs"
import { basename, resolve } from "node:path"
import type { Writable } from "node:stream"

import {
  autocompleteMultiselect as promptAutocompleteMultiselect,
  confirm as promptConfirm,
  isCancel,
  log as promptLog,
  path as promptPath,
  select as promptSelect
} from "@clack/prompts"

import type { Provider } from "./domain.ts"
import {
  habitatAppUrl,
  habitatApiUrl,
  loginWithBrowser,
  type HabitatLoginResult
} from "./habitat-login.ts"
import {
  htPaths,
  projectsForWorkspace,
  readConfig,
  resolveHabitatExport,
  saveHabitatLogin,
  saveSetupSelection,
  type HTConfig
} from "./config.ts"
import { SessionCollector } from "./collector.ts"
import { daemonVersionMatches, requestDaemonSync } from "./daemon.ts"
import { installHT, restartHTDaemon } from "./install.ts"
import {
  IncrementalPipelineStore,
  type PipelineProjectOriginCount,
  type PipelineHarnessState
} from "./pipeline-store.ts"
import {
  discoverGitWorktrees,
  discoverProjectSuggestions,
  isCodexWorktreePath,
  rankProjectSuggestions,
  type GitWorktree,
  type ProjectSuggestion
} from "./project-suggestions.ts"
import { LocalSessionStore } from "./store.ts"

export interface SetupOptions {
  readonly entrypoint: string
  readonly configureOnly?: boolean
  readonly apiUrl?: string
  readonly appUrl?: string
  readonly apiKey?: string
  readonly workspaceId?: string
  readonly addWorkspace?: boolean
  readonly codexHome?: string
  readonly claudeHome?: string
  readonly projectOrigins?: readonly string[]
  readonly backfillWindow?: HTConfig["backfillWindow"]
  readonly noOpen?: boolean
  readonly noDaemon?: boolean
  readonly nonInteractive?: boolean
  readonly requestBackgroundSync?: typeof requestDaemonSync
  readonly output?: Writable
}

export interface SetupResult {
  readonly configured: true
  readonly repeatedSafe: true
  readonly ready: boolean
  readonly workspace: { readonly id: string; readonly slug: string; readonly name: string }
  readonly workspaceUrl: string
  readonly harnesses: readonly {
    readonly provider: Provider
    readonly state: "active" | "ready" | "verification-required"
    readonly configuredAt: string
    readonly reviewedAt: string | null
    readonly lastHookReceivedAt: string | null
  }[]
  readonly actionsRequired: readonly string[]
  readonly projects: readonly string[]
  readonly backfillWindow: HTConfig["backfillWindow"]
  readonly configPath: string
  readonly discovery: {
    readonly scanned: number
    readonly projects: number
    readonly sessions: number
  }
  readonly backfill: {
    readonly scheduled: boolean
    readonly notified: boolean
    readonly queued: number
    readonly exported: number
    readonly failed: number
    readonly remaining: number
    readonly retrying: number
    readonly quarantined: number
  }
  readonly installation: Awaited<ReturnType<typeof installHT>> | null
}

const monitoredProviders = ["codex", "claude"] as const satisfies readonly Provider[]
const browseForProject = "__ht_browse_for_project__"

export interface ProjectCandidate {
  readonly origin: string
  readonly name: string
  readonly sources: readonly string[]
  readonly sessionCount: number
}

interface ProjectSessionInventory {
  readonly scanned: number
  readonly sessions: number
  readonly origins: readonly PipelineProjectOriginCount[]
}

export const runSetup = async (options: SetupOptions): Promise<SetupResult> => {
  const paths = htPaths()
  const output = options.output ?? process.stderr
  const interactive = options.nonInteractive !== true &&
    process.stdin.isTTY === true
  try {
    let config = await readConfig(paths)
    const workspaceId = options.configureOnly
      ? await chooseExistingWorkspace(config, options.workspaceId, interactive, output)
      : await chooseOrLoginWorkspace(config, options, interactive, output)
    config = await readConfig(paths)
    const workspace = config.workspaces.find((candidate) =>
      candidate.workspace.id === workspaceId
    )
    if (!workspace) throw new Error(`Workspace ${workspaceId} is not configured.`)
    const existingRoutes = projectsForWorkspace(config, workspaceId)
    const selectedHarnesses = [...monitoredProviders]
    const existingCoreRoutes = existingRoutes.filter((route) =>
      !isCodexWorktreePath(route.origin, options.codexHome)
    )
    const existing = existingCoreRoutes.map((route) => route.origin)
    const inventory = interactive
      ? await discoverProjectSessionInventory(paths.database, options)
      : { scanned: 0, sessions: 0, origins: [] }
    progress(
      `${inventory.sessions} local session${inventory.sessions === 1 ? "" : "s"} found.`,
      interactive,
      output
    )
    const suggestions = await discoverProjectSuggestions({
      codexHome: options.codexHome,
      claudeHome: options.claudeHome
    })
    const worktrees = interactive
      ? await discoverGitWorktrees([
          ...new Set([
            ...suggestions.map((project) => project.origin),
            ...existingCoreRoutes.map((project) => project.origin)
          ])
        ])
      : []
    const projects = projectCandidates(
      suggestions,
      existingCoreRoutes,
      options.projectOrigins,
      inventory.origins,
      worktrees
    )
    const selected = await chooseProjects(
      projects,
      existing,
      options,
      interactive,
      inventory
    )
    const backfillWindow = await chooseBackfillWindow(
      options.backfillWindow ?? config.backfillWindow,
      interactive,
      output
    )
    const confirmed = interactive
      ? await confirm(
          `Ingest ${selected.length} project${selected.length === 1 ? "" : "s"} ` +
            `into ${workspace.workspace.name} (${backfillWindow})?`,
          true,
          output
        )
      : true
    if (!confirmed) throw new Error("Setup was cancelled.")
    config = await saveSetupSelection({
      activeWorkspaceId: workspaceId,
      projects: selected.map((origin) => {
        const project = projects.find((candidate) => candidate.origin === origin)
        return {
          origin,
          name: project?.name ?? basename(origin),
          workspaceId,
          providers: [...monitoredProviders]
        }
      }),
      backfillWindow
    }, paths)

    const connection = await resolveHabitatExport(paths, workspaceId)
    if (!connection) throw new Error(`Credentials for ${workspace.workspace.name} are missing.`)

    const installedHarnesses = normalizeHarnesses([
      ...config.projects.flatMap((project) => project.providers),
      ...selectedHarnesses
    ])
    const installation = await installHT({
      entrypoint: options.entrypoint,
      paths,
      codexHome: options.codexHome,
      claudeHome: options.claudeHome,
      activateDaemon: options.noDaemon !== true,
      providers: installedHarnesses
    })
    const daemonCompatible = options.noDaemon === true || !installation.daemon.active
      ? false
      : await daemonVersionMatches(paths) || await restartHTDaemon(paths)
    const backfillNotified = options.requestBackgroundSync
      ? await options.requestBackgroundSync(undefined, paths)
      : daemonCompatible
        ? await requestDaemonSync(undefined, paths)
        : false
    const harnessStates = readHarnessStates(paths.database)
    const harnesses = setupHarnesses(harnessStates, selectedHarnesses)
    const actionsRequired = harnesses.flatMap((harness) =>
      harness.state === "verification-required"
        ? [
            "Open Codex, run /hooks, and trust the installed Habitat hook, then " +
              `complete one turn. Trust only: ${paths.launcher} hook codex.`
          ]
        : []
    )
    const setupInstallation = harnesses.some((harness) =>
      harness.provider === "codex" && harness.state === "active"
    )
      ? {
          ...installation,
          notices: installation.notices.filter((notice) =>
            !notice.startsWith("Codex requires")
          )
        }
      : installation
    return {
      configured: true,
      repeatedSafe: true,
      ready: actionsRequired.length === 0,
      workspace: workspace.workspace,
      workspaceUrl: workspaceSessionsUrl(
        habitatAppUrl(options.appUrl, connection.apiUrl),
        workspace.workspace.id
      ),
      harnesses,
      actionsRequired,
      projects: selected,
      backfillWindow: config.backfillWindow,
      configPath: paths.config,
      discovery: {
        scanned: inventory.scanned,
        projects: selected.length,
        sessions: inventory.sessions
      },
      backfill: {
        scheduled: true,
        notified: backfillNotified,
        queued: 0,
        exported: 0,
        failed: 0,
        remaining: 0,
        retrying: 0,
        quarantined: 0
      },
      installation: setupInstallation
    }
  } finally {
    // Prompt libraries put stdin into flowing mode. Setup owns that foreground
    // input resource, so release it on success, cancellation, and failure. The
    // launchd-managed collector is a separate process and remains launchd-owned.
    if (interactive) process.stdin.pause()
  }
}

const normalizeHarnesses = (providers: readonly Provider[]): Provider[] => {
  const selected = new Set(providers)
  return (["codex", "claude"] as const).filter((provider) => selected.has(provider))
}

const readHarnessStates = (databasePath: string): PipelineHarnessState[] => {
  const store = new LocalSessionStore(databasePath)
  try {
    return new IncrementalPipelineStore(store.database).harnesses()
  } finally {
    store.close()
  }
}

const setupHarnesses = (
  states: readonly PipelineHarnessState[],
  providers: readonly Provider[]
): SetupResult["harnesses"] => providers.map((provider) => {
  const state = states.find((candidate) => candidate.provider === provider)
  return {
    provider,
    state: state?.lastHookReceivedAt
      ? "active"
      : provider === "codex"
        ? "verification-required"
        : "ready",
    configuredAt: state?.configuredAt ?? new Date().toISOString(),
    reviewedAt: state?.reviewedAt ?? null,
    lastHookReceivedAt: state?.lastHookReceivedAt ?? null
  }
})

const chooseOrLoginWorkspace = async (
  config: HTConfig,
  options: SetupOptions,
  interactive: boolean,
  output: Writable
): Promise<string> => {
  const explicit = options.workspaceId
  if (explicit && config.workspaces.some((workspace) =>
    workspace.workspace.id === explicit
  ) && !options.addWorkspace) return explicit

  if (
    config.workspaces.length > 0 &&
    !options.addWorkspace &&
    !options.apiKey
  ) {
    if (!interactive) {
      return explicit ?? config.activeWorkspaceId ??
        config.workspaces[0]!.workspace.id
    }
    const choices = [
      ...config.workspaces.map((workspace) =>
        `${workspace.workspace.name} (${workspace.workspace.slug})`
      ),
      "Add another workspace"
    ]
    const defaultIndex = Math.max(
      0,
      config.workspaces.findIndex((workspace) =>
        workspace.workspace.id === config.activeWorkspaceId
      )
    )
    const selected = await selectOne(
      "Choose a Habitat workspace",
      choices,
      defaultIndex,
      output
    )
    if (selected < config.workspaces.length) {
      return config.workspaces[selected]!.workspace.id
    }
  }

  if (!interactive && !options.apiKey) {
    throw new Error(
      "Non-interactive setup requires --api-key, --api-key-stdin, or HABITAT_API_KEY."
    )
  }
  const apiUrl = habitatApiUrl(options.apiUrl)
  const authenticated = options.apiKey
    ? await authenticateApiKey(apiUrl, options.apiKey)
    : await loginWithBrowser({
        apiUrl,
        appUrl: habitatAppUrl(options.appUrl, apiUrl),
        openBrowser: options.noOpen ? async () => false : undefined,
        onVerification: ({ url, userCode, browserOpened }) => {
          progress(
            browserOpened
              ? `Opened Habitat. Confirm code ${userCode}.`
              : `Open ${url} and confirm code ${userCode}.`,
            true,
            output
          )
        }
      })
  await saveHabitatLogin({
    apiUrl,
    workspace: authenticated.workspace,
    principal: authenticated.principal
  }, authenticated.apiKey)
  return authenticated.workspace.id
}

const chooseExistingWorkspace = async (
  config: HTConfig,
  explicit: string | undefined,
  interactive: boolean,
  output: Writable
): Promise<string> => {
  if (config.workspaces.length === 0) {
    throw new Error("No Habitat workspace is configured. Run `ht setup` first.")
  }
  if (explicit) {
    if (!config.workspaces.some((workspace) => workspace.workspace.id === explicit)) {
      throw new Error(`Workspace ${explicit} is not configured.`)
    }
    return explicit
  }
  if (!interactive || config.workspaces.length === 1) {
    return config.activeWorkspaceId ?? config.workspaces[0]!.workspace.id
  }
  const selected = await selectOne(
    "Configure which workspace",
    config.workspaces.map((workspace) =>
      `${workspace.workspace.name} (${workspace.workspace.slug})`
    ),
    Math.max(0, config.workspaces.findIndex((workspace) =>
      workspace.workspace.id === config.activeWorkspaceId
    )),
    output
  )
  return config.workspaces[selected]!.workspace.id
}

const chooseProjects = async (
  projects: readonly ProjectCandidate[],
  existing: readonly string[],
  options: SetupOptions,
  interactive: boolean,
  inventory: ProjectSessionInventory
): Promise<string[]> => {
  if (options.projectOrigins) {
    const selected = options.projectOrigins.map(normalizeSelectedProject)
    const warning = emptyProjectSelectionWarning(
      selected,
      projects,
      inventory.origins
    )
    if (interactive && warning && !await confirm(
      warning,
      false,
      options.output ?? process.stderr
    )) {
      throw new Error("Setup was cancelled. Choose another project folder and retry.")
    }
    return selected
  }
  if (!interactive) {
    if (options.configureOnly && existing.length > 0) return [...existing]
    throw new Error(
      "At least one project folder is required in non-interactive setup. " +
      "Pass `--project /path/to/project` (repeatable)."
    )
  }

  const projectOptions = projectPickerOptions(projects)
  const optionsByOrigin = new Map(projectOptions.map((option) => [
    option.value,
    option
  ]))
  const browseOption = {
    value: browseForProject,
    label: "Add a folder by path…",
    hint: "Browse directories not saved in Codex Desktop or Claude"
  }
  let initialValues = projects.filter((project) =>
    existing.includes(project.origin)
  ).map((project) => project.origin)
  while (true) {
    const selected = await promptValue(promptAutocompleteMultiselect<string>({
      message: "Which project folders should Habitat monitor sessions for?",
      options() {
        const search = this.userInput.trim()
        if (!search) return [...projectOptions, browseOption]
        return [
          ...rankProjectSuggestions(search, projects).map((project) =>
            optionsByOrigin.get(project.origin)!
          ),
          browseOption
        ]
      },
      placeholder: "Type to fuzzy find project folders",
      filter: () => true,
      initialValues,
      required: true,
      maxItems: 10,
      output: options.output ?? process.stderr
    }))
    const chosen = selected.filter((origin) => origin !== browseForProject)

    if (selected.includes(browseForProject)) {
      do {
        const directory = await promptValue(promptPath({
          message: "Choose a project folder",
          directory: true,
          initialValue: process.cwd(),
          validate: (value) => {
            if (!value) return "Choose an existing directory"
            try {
              if (!statSync(value).isDirectory()) return "Choose an existing directory"
            } catch {
              return "Choose an existing directory"
            }
          },
          output: options.output ?? process.stderr
        }))
        const normalized = normalizeSelectedProject(directory)
        if (!chosen.includes(normalized)) chosen.push(normalized)
      } while (await confirm(
        "Add another project folder?",
        false,
        options.output ?? process.stderr
      ))
    }

    const warning = emptyProjectSelectionWarning(
      chosen,
      projects,
      inventory.origins
    )
    if (!warning || await confirm(
      warning,
      false,
      options.output ?? process.stderr
    )) return chosen

    initialValues = chosen.filter((origin) =>
      (projects.find((project) => project.origin === origin)?.sessionCount ??
        projectSessionCount(origin, inventory.origins)) > 0
    )
  }
}

export const projectCandidates = (
  suggestions: readonly ProjectSuggestion[],
  existing: readonly {
    readonly origin: string
    readonly name: string
  }[],
  explicit: readonly string[] | undefined,
  origins: readonly PipelineProjectOriginCount[],
  worktrees: readonly GitWorktree[] = []
): ProjectCandidate[] => {
  const projects = new Map<string, ProjectCandidate>()
  const sessionCount = (origin: string): number => projectSessionCount(
    origin,
    origins,
    worktrees.filter((worktree) =>
      normalizeSelectedProject(worktree.projectOrigin) ===
        normalizeSelectedProject(origin)
    ).map((worktree) => worktree.origin)
  )
  for (const suggestion of suggestions) {
    projects.set(suggestion.origin, {
      origin: suggestion.origin,
      name: suggestion.name,
      sources: suggestion.sources.map((source) =>
        source === "codex-desktop" ? "Codex Desktop" : "Claude"
      ),
      sessionCount: sessionCount(suggestion.origin)
    })
  }
  for (const route of existing) {
    const current = projects.get(route.origin)
    projects.set(route.origin, {
      origin: route.origin,
      name: current?.name ?? route.name,
      sources: current
        ? [...current.sources, "currently monitored"]
        : ["currently monitored"],
      sessionCount: current?.sessionCount ?? sessionCount(route.origin)
    })
  }
  for (const origin of explicit ?? []) {
    const normalized = normalizeSelectedProject(origin)
    if (!projects.has(normalized)) {
      projects.set(normalized, {
        origin: normalized,
        name: basename(normalized),
        sources: ["command line"],
        sessionCount: sessionCount(normalized)
      })
    }
  }
  return [...projects.values()].sort((left, right) =>
    left.name.localeCompare(right.name) ||
    right.sessionCount - left.sessionCount ||
    left.origin.localeCompare(right.origin)
  )
}

export const projectPickerOptions = (
  projects: readonly ProjectCandidate[]
): {
  readonly value: string
  readonly label: string
  readonly hint: string
  readonly disabled: boolean
}[] => projects.map((project) => ({
  value: project.origin,
  label: `${project.name} · ${project.sessionCount} historical session${
    project.sessionCount === 1 ? "" : "s"
  }`,
  hint: `${project.origin} · ${project.sources.join(", ")}`,
  disabled: false
}))

export const emptyProjectSelectionWarning = (
  selected: readonly string[],
  projects: readonly ProjectCandidate[],
  origins: readonly PipelineProjectOriginCount[]
): string | null => {
  const empty = selected.flatMap((origin) => {
    const project = projects.find((candidate) => candidate.origin === origin)
    const sessionCount = project?.sessionCount ?? projectSessionCount(origin, origins)
    return sessionCount === 0
      ? [{ origin, name: project?.name ?? basename(origin) }]
      : []
  })
  if (empty.length === 0) return null

  const alternatives = projects.filter((project) =>
    project.sessionCount > 0 &&
    !selected.includes(project.origin) &&
    empty.some((candidate) => candidate.name === project.name)
  )
  const folders = empty.length === 1
    ? empty[0]!.origin
    : empty.map((project) => project.origin).join(", ")
  const alternative = alternatives[0]
    ? ` A same-named folder has ${alternatives[0].sessionCount} historical session${
        alternatives[0].sessionCount === 1 ? "" : "s"
      }: ${alternatives[0].origin}.`
    : ""
  const backfill = empty.length === selected.length
    ? " The initial backfill will upload nothing."
    : " Those folders will only contribute new sessions."
  return `No historical sessions were found in ${folders}.${alternative} ` +
    `Habitat can still monitor new sessions there.${backfill} Continue?`
}

export const projectSessionCount = (
  projectOrigin: string,
  origins: readonly PipelineProjectOriginCount[],
  relatedOrigins: readonly string[] = []
): number => {
  const roots = [projectOrigin, ...relatedOrigins].map(normalizeSelectedProject)
  return origins.reduce((total, origin) => {
    const candidate = normalizeSelectedProject(origin.origin)
    const matches = roots.some((root) =>
      candidate === root || (
        root === "/"
          ? candidate.startsWith("/")
          : candidate.startsWith(`${root}/`)
      )
    )
    return matches ? total + origin.sessions : total
  }, 0)
}

const discoverProjectSessionInventory = async (
  statePath: string,
  options: Pick<SetupOptions, "codexHome" | "claudeHome">
): Promise<ProjectSessionInventory> => {
  const collector = new SessionCollector({
    statePath,
    codexHome: options.codexHome,
    claudeHome: options.claudeHome,
    autoPrepareDestinations: false
  })
  try {
    const scan = await collector.scan()
    const origins = collector.pipeline.projectOriginCounts()
    return {
      scanned: scan.scanned,
      sessions: origins.reduce((total, origin) => total + origin.sessions, 0),
      origins
    }
  } finally {
    collector.close()
  }
}

const normalizeSelectedProject = (origin: string): string => {
  const absolute = resolve(origin).replaceAll("\\", "/")
  return absolute.length > 1 ? absolute.replace(/\/+$/, "") : absolute
}

const chooseBackfillWindow = async (
  current: HTConfig["backfillWindow"],
  interactive: boolean,
  output: Writable
): Promise<HTConfig["backfillWindow"]> => {
  if (!interactive) return current
  const choices: readonly HTConfig["backfillWindow"][] = ["30d", "90d", "all"]
  return choices[await selectOne(
    "How far back should Habitat ingest sessions",
    ["Last 30 days", "Last 90 days", "All time"],
    Math.max(0, choices.indexOf(current)),
    output
  )]!
}

const selectOne = async (
  label: string,
  choices: readonly string[],
  defaultIndex: number,
  output: Writable
): Promise<number> => {
  return promptValue(promptSelect<number>({
    message: label,
    options: choices.map((choice, value) => ({ value, label: choice })),
    initialValue: defaultIndex,
    output
  }))
}

const confirm = async (
  label: string,
  defaultValue: boolean,
  output: Writable
): Promise<boolean> => promptValue(promptConfirm({
  message: label,
  initialValue: defaultValue,
  output
}))

const promptValue = <Value>(value: Promise<Value | symbol>): Promise<Value> =>
  value.then((result) => {
    if (isCancel(result)) throw new Error("Setup was cancelled.")
    return result
  })

const authenticateApiKey = async (
  apiUrl: string,
  apiKey: string
): Promise<HabitatLoginResult> => {
  const response = await fetch(new URL("/v1/me", `${apiUrl}/`), {
    headers: { Authorization: `Bearer ${apiKey}` },
    signal: AbortSignal.timeout(15_000)
  })
  const body = await response.json().catch(() => null) as {
    data?: {
      workspace?: HabitatLoginResult["workspace"]
      principal?: HabitatLoginResult["principal"]
    }
    error?: { message?: unknown }
  } | null
  if (!response.ok || !body?.data?.workspace || !body.data.principal) {
    const message = typeof body?.error?.message === "string"
      ? body.error.message
      : `Habitat authentication failed (HTTP ${response.status}).`
    throw new Error(message)
  }
  return {
    apiKey,
    workspace: body.data.workspace,
    principal: body.data.principal
  }
}

const workspaceSessionsUrl = (appUrl: string, workspaceId: string): string => {
  const url = new URL(appUrl)
  url.pathname = `/w/${encodeURIComponent(workspaceId)}/sessions`
  url.search = ""
  url.hash = ""
  return url.toString()
}

const progress = (message: string, visible: boolean, output: Writable): void => {
  if (!visible) return
  promptLog.info(message, { output })
}
