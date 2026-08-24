import { fileURLToPath } from "node:url"
import type { Writable } from "node:stream"

import { Command, CommanderError, Option } from "commander"

import { CliDisplay } from "./ui/display.ts"
import type { Provider } from "./domain.ts"
import { SessionCollector, type CollectorDestination } from "./collector.ts"
import {
  notifyDaemon,
  runDaemon,
  syncThroughDaemon,
  wakeDaemon
} from "./daemon.ts"
import { HabitatExporter } from "./exporters/habitat.ts"
import {
  habitatApiUrl,
  habitatAppUrl,
  loginWithBrowser
} from "./habitat-login.ts"
import { installHT, uninstallHT } from "./install.ts"
import { inspectPreflight } from "./preflight.ts"
import {
  daemonPort,
  expandGitWorktreeRoutes,
  htPaths,
  logoutHabitat,
  projectWorkspaceId,
  projectsForWorkspace,
  readConfig,
  readDaemonStatus,
  resolveHabitatExport,
  resolveHabitatExports,
  saveHabitatLogin
} from "./config.ts"
import { IncrementalPipelineStore } from "./pipeline-store.ts"
import { LocalSessionStore } from "./store.ts"
import { runSetup } from "./setup.ts"
import type { SessionSnapshot } from "./snapshot.ts"
import { updateHT, type UpdateOptions, type UpdateResult } from "./update.ts"
import {
  allowsProjectUpload,
  projectUploadPolicy,
  readProjectUploadPolicy,
  uploadPolicySummary,
  userConfigPath
} from "./user-config.ts"
import { cliVersion } from "./version.ts"

const version = cliVersion

export interface CliRuntimeOptions {
  readonly entrypoint?: string
  readonly update?: (options: UpdateOptions) => Promise<UpdateResult>
  readonly stdout?: Writable
  readonly stderr?: Writable
}

interface SetupCommandOptions {
  readonly apiKeyStdin?: boolean
  readonly apiKey?: string
  readonly apiUrl?: string
  readonly appUrl?: string
  readonly workspace?: string
  readonly addWorkspace?: boolean
  readonly codexHome?: string
  readonly claudeHome?: string
  readonly project?: readonly string[]
  readonly backfill?: string
  readonly open?: boolean
  readonly daemon?: boolean
  readonly nonInteractive?: boolean
}

interface LoginCommandOptions {
  readonly apiKeyStdin?: boolean
  readonly apiKey?: string
  readonly apiUrl?: string
  readonly appUrl?: string
  readonly open?: boolean
}

interface LogoutCommandOptions {
  readonly workspace?: string
  readonly all?: boolean
}

interface InstallCommandOptions {
  readonly harness?: string
  readonly codexHome?: string
  readonly claudeHome?: string
  readonly daemon?: boolean
}

interface SyncCommandOptions {
  readonly provider?: string
  readonly codexHome?: string
  readonly claudeHome?: string
}

interface BackfillCommandOptions extends SyncCommandOptions {
  readonly workspace?: string
  readonly dryRun?: boolean
  readonly retryQuarantined?: boolean
}

interface SessionListCommandOptions extends SyncCommandOptions {
  readonly limit?: string
}

interface SessionShowCommandOptions {
  readonly includeContent?: boolean
}

interface SessionSearchCommandOptions {
  readonly provider?: string
  readonly limit?: string
}

interface StatusCommandOptions {
  readonly verbose?: boolean
}

interface UpdateCommandOptions {
  readonly to?: string
  readonly force?: boolean
}

export const runCli = async (
  input: string[],
  options: CliRuntimeOptions = {}
): Promise<number> => {
  const jsonRequested = input.includes("--json")
  const display = new CliDisplay(
    jsonRequested,
    options.stdout ?? process.stdout,
    options.stderr ?? process.stderr
  )
  let exitCode = 0
  try {
    for (const argument of input) {
      if (argument.trim() !== argument) {
        throw new CliError(
          "INVALID_ARGUMENT",
          `Unexpected whitespace in argument ${JSON.stringify(argument)}.`,
          "Remove the stray space or shell continuation and run the command again."
        )
      }
    }

    const program = createCliProgram({
      runtime: options,
      jsonRequested,
      display,
      setExitCode: (value) => {
        exitCode = value
      }
    })
    await program.parseAsync(input, { from: "user" })
    return exitCode
  } catch (error) {
    if (error instanceof CommanderError && error.exitCode === 0) return 0
    const normalized = commandError(error)
    if (jsonRequested) {
      display.printJson({ ok: false, error: normalized })
    } else if (!(error instanceof CommanderError)) {
      display.error(normalized)
    }
    return 1
  }
}

const createCliProgram = (input: {
  readonly runtime: CliRuntimeOptions
  readonly jsonRequested: boolean
  readonly display: CliDisplay
  readonly setExitCode: (value: number) => void
}): Command => {
  const program = new Command()
    .name("ht")
    .description("Agent Session Observability")
    .version(
      input.jsonRequested
        ? JSON.stringify({ version, command: "ht" }, null, 2)
        : `ht ${version}`,
      "-v, --version",
      "show the HT version"
    )
    .option("--json", "print machine-readable JSON")
    .allowExcessArguments(false)
    .exitOverride()
    .configureOutput({
      writeOut: input.display.writeOut,
      writeErr: input.display.writeErr,
      outputError: (value) => {
        if (!input.jsonRequested) input.display.commanderError(value)
      }
    })
    .configureHelp({ showGlobalOptions: true })
    .helpCommand("help [command]", "display help for a command")
    .showSuggestionAfterError(true)
    .addHelpText("after", `
Environment:
  HT_HOME           Local state directory (default ~/.ht)
  HT_CODEX_HOME     Codex data directory override
  HT_CLAUDE_HOME    Claude data directory override
  HT_CONFIG         User config path (default ~/.config/ht/ht.config.json)
  HABITAT_API_URL   Habitat API override
  HABITAT_APP_URL   Habitat web app override
  HABITAT_API_KEY   Non-interactive Habitat API key`)

  program.action(() => {
    program.outputHelp()
    input.setExitCode(0)
  })

  program
    .command("version")
    .description("show the HT version")
    .helpGroup("Lifecycle:")
    .allowExcessArguments(false)
    .action(() => {
      if (input.jsonRequested) input.display.printJson({ version, command: "ht" })
      else input.display.version(version)
      input.setExitCode(0)
    })

  program
    .command("update")
    .description("update HT to the latest checksum-verified release")
    .helpGroup("Lifecycle:")
    .option("--to <version>", "install a specific release such as v0.4.0")
    .option("--force", "reinstall even when the requested version is current")
    .allowExcessArguments(false)
    .action(async (commandOptions: UpdateCommandOptions) => {
      const spinner = input.display.updateStarted()
      try {
        const result = await (input.runtime.update ?? updateHT)({
          currentVersion: version,
          requestedVersion: commandOptions.to,
          force: commandOptions.force
        })
        if (input.jsonRequested) input.display.printJson(result)
        else input.display.update(result, spinner)
        input.setExitCode(result.daemonRestarted === false ? 1 : 0)
      } catch (error) {
        spinner?.clear()
        throw error
      }
    })

  const addSetupCommand = (name: "setup" | "configure"): void => {
    program
      .command(name)
      .description(name === "setup"
        ? "connect a Habitat workspace and configure ingestion"
        : "change project routing for a configured workspace")
      .helpGroup("Workspace:")
      .option("--api-key-stdin", "read a Habitat API key from standard input")
      .option("--api-key <key>", "use a Habitat API key")
      .option("--api-url <url>", "override the Habitat API URL")
      .option("--app-url <url>", "override the Habitat app URL")
      .option("--workspace <id>", "select a workspace")
      .option("--add-workspace", "connect another workspace")
      .option("--codex-home <path>", "override the Codex data directory")
      .option("--claude-home <path>", "override the Claude Code data directory")
      .option(
        "--project <origin>",
        "select a project folder (repeatable)",
        collectOption
      )
      .addOption(new Option("--backfill <window>", "select historical ingestion window")
        .choices(["30d", "90d", "all"]))
      .option("--no-open", "do not open a browser")
      .option("--no-daemon", "do not activate the background daemon")
      .option("--non-interactive", "disable terminal prompts")
      .allowExcessArguments(false)
      .action(async (commandOptions: SetupCommandOptions) => {
        if (!input.jsonRequested) {
          input.display.intro(name === "configure" ? "Configure Habitat" : "Set up Habitat")
        }
        const apiKey = commandOptions.apiKey ?? process.env.HABITAT_API_KEY ??
          (commandOptions.apiKeyStdin
          ? (await Bun.stdin.text()).trim()
          : undefined)
        const result = await runSetup({
          entrypoint: input.runtime.entrypoint ??
            fileURLToPath(new URL("./entry.ts", import.meta.url)),
          configureOnly: name === "configure",
          apiUrl: commandOptions.apiUrl,
          appUrl: commandOptions.appUrl,
          apiKey,
          workspaceId: commandOptions.workspace,
          addWorkspace: commandOptions.addWorkspace,
          codexHome: commandOptions.codexHome,
          claudeHome: commandOptions.claudeHome,
          projectOrigins: commandOptions.project,
          backfillWindow: backfillWindowOption(commandOptions.backfill),
          noOpen: commandOptions.open === false,
          noDaemon: commandOptions.daemon === false,
          nonInteractive: commandOptions.nonInteractive || input.jsonRequested,
          output: input.display.stderr
        })
        if (input.jsonRequested) input.display.printJson(result)
        else input.display.setup(result, name === "configure")
        input.setExitCode(result.ready ? 0 : 1)
      })
  }

  addSetupCommand("setup")
  addSetupCommand("configure")

  program
    .command("preflight")
    .description("inspect the machine without making changes")
    .helpGroup("Diagnostics:")
    .option("--codex-home <path>", "override the Codex data directory")
    .option("--claude-home <path>", "override the Claude Code data directory")
    .allowExcessArguments(false)
    .action(async (commandOptions: SyncCommandOptions) => {
      const report = await inspectPreflight({
        paths: htPaths(),
        codexHome: commandOptions.codexHome,
        claudeHome: commandOptions.claudeHome
      })
      if (input.jsonRequested) input.display.printJson(report)
      else input.display.preflight(report)
      input.setExitCode(report.ready ? 0 : 1)
    })

  program
    .command("login")
    .description("log in to a Habitat workspace")
    .helpGroup("Workspace:")
    .option("--api-key-stdin", "read a Habitat API key from standard input")
    .option("--api-key <key>", "use a Habitat API key")
    .option("--api-url <url>", "override the Habitat API URL")
    .option("--app-url <url>", "override the Habitat app URL")
    .option("--no-open", "do not open a browser")
    .allowExcessArguments(false)
    .action(async (commandOptions: LoginCommandOptions) => {
      const apiUrl = habitatApiUrl(commandOptions.apiUrl)
      const apiKey = commandOptions.apiKey ?? process.env.HABITAT_API_KEY ??
        (commandOptions.apiKeyStdin
          ? (await Bun.stdin.text()).trim()
          : undefined)
      const authenticated = apiKey
        ? { apiKey, ...((await apiRequest(apiUrl, apiKey, "/v1/me") as MeEnvelope).data) }
        : await loginWithBrowser({
          apiUrl,
          appUrl: habitatAppUrl(commandOptions.appUrl, apiUrl),
          openBrowser: commandOptions.open === false ? async () => false : undefined,
          onVerification: ({ url, userCode, browserOpened }) => {
            input.display.loginVerification(browserOpened
              ? `Opened Habitat in your browser. Confirm code ${userCode}.`
              : `Open ${url} and confirm code ${userCode}.`)
            input.display.loginVerification("Waiting for authorization…")
          }
        })
      const config = await saveHabitatLogin({
        apiUrl,
        workspace: authenticated.workspace,
        principal: authenticated.principal
      }, authenticated.apiKey)
      await wakeDaemon()
      const result = {
        loggedIn: true,
        uploadEnabled: true,
        apiUrl,
        workspace: config.workspaces.find((workspace) =>
          workspace.workspace.id === config.activeWorkspaceId
        )?.workspace,
        principal: config.workspaces.find((workspace) =>
          workspace.workspace.id === config.activeWorkspaceId
        )?.principal
      }
      if (input.jsonRequested) input.display.printJson(result)
      else input.display.login(result)
      input.setExitCode(0)
    })

  program
    .command("logout")
    .description("remove a saved Habitat workspace login")
    .helpGroup("Workspace:")
    .addOption(new Option("--workspace <id>", "log out of a specific workspace")
      .conflicts("all"))
    .option("--all", "log out of every saved workspace")
    .allowExcessArguments(false)
    .action(async (commandOptions: LogoutCommandOptions) => {
      const result = await logoutHabitat({
        workspaceId: commandOptions.workspace,
        all: commandOptions.all
      })
      await wakeDaemon()
      if (input.jsonRequested) input.display.printJson({
        loggedOut: true,
        localCredentialsRemoved: true,
        ...result
      })
      else input.display.logout(result)
      input.setExitCode(0)
    })

  program
    .command("install")
    .description("install provider hooks and the background daemon")
    .helpGroup("Lifecycle:")
    .addOption(new Option("--harness <harness>", "select provider hooks")
      .choices(["all", "codex", "claude"]))
    .option("--codex-home <path>", "override the Codex data directory")
    .option("--claude-home <path>", "override the Claude Code data directory")
    .option("--no-daemon", "do not activate the background daemon")
    .allowExcessArguments(false)
    .action(async (commandOptions: InstallCommandOptions) => {
      const result = await installHT({
        entrypoint: input.runtime.entrypoint ??
          fileURLToPath(new URL("./entry.ts", import.meta.url)),
        paths: htPaths(),
        codexHome: commandOptions.codexHome,
        claudeHome: commandOptions.claudeHome,
        activateDaemon: commandOptions.daemon !== false,
        providers: harnessOption(commandOptions.harness)
      })
      if (input.jsonRequested) {
        input.display.printJson({ installed: true, localCaptureEnabled: true, ...result })
      } else {
        input.display.install(result)
      }
      input.setExitCode(
        result.daemon.active || result.daemon.manager === "manual" ? 0 : 1
      )
    })

  const config = program
    .command("config")
    .description("inspect HT configuration")
    .helpGroup("Configuration:")
    .allowExcessArguments(false)

  config
    .command("path")
    .description("print the user configuration path")
    .allowExcessArguments(false)
    .action(() => {
      const path = userConfigPath()
      if (input.jsonRequested) input.display.printJson({ path })
      else input.display.configPath(path)
      input.setExitCode(0)
    })

  config
    .command("check")
    .description("validate the HT configuration")
    .allowExcessArguments(false)
    .action(async () => {
      const summary = { valid: true, ...uploadPolicySummary(await readProjectUploadPolicy()) }
      if (input.jsonRequested) input.display.printJson(summary)
      else input.display.configCheck(summary)
      input.setExitCode(0)
    })

  program
    .command("uninstall")
    .description("remove HT hooks, launcher, and background service")
    .helpGroup("Lifecycle:")
    .allowExcessArguments(false)
    .action(async () => {
      await uninstallHT()
      if (input.jsonRequested) input.display.printJson({
        uninstalled: true,
        binaryRemoved: true,
        localDataPreserved: true,
        credentialsPreserved: true
      })
      else input.display.uninstall()
      input.setExitCode(0)
    })

  const addSyncCommand = (name: "sync" | "upload"): void => {
    program
      .command(name)
      .description(name === "sync"
        ? "synchronize local provider sessions"
        : "synchronize sessions to Habitat")
      .helpGroup("Sessions:")
      .addOption(new Option("--provider <provider>", "select a provider")
        .choices(["codex", "claude"]))
      .option("--codex-home <path>", "override the Codex data directory")
      .option("--claude-home <path>", "override the Claude Code data directory")
      .allowExcessArguments(false)
      .action(async (commandOptions: SyncCommandOptions) => {
        const result = await synchronize(commandOptions, name === "upload")
        if (input.jsonRequested) input.display.printJson(result)
        else input.display.sync(result, name === "upload")
        input.setExitCode(0)
      })
  }

  addSyncCommand("sync")
  addSyncCommand("upload")

  program
    .command("backfill")
    .description("scan and upload historical sessions")
    .helpGroup("Sessions:")
    .addOption(new Option("--provider <provider>", "select a provider")
      .choices(["codex", "claude"]))
    .option("--workspace <id>", "select a workspace")
    .option("--codex-home <path>", "override the Codex data directory")
    .option("--claude-home <path>", "override the Claude Code data directory")
    .option("--dry-run", "preview eligible sessions without uploading")
    .option("--retry-quarantined", "retry permanently failed uploads")
    .allowExcessArguments(false)
    .action(async (commandOptions: BackfillCommandOptions) => {
      const result = await backfill(commandOptions)
      if (input.jsonRequested) input.display.printJson(result)
      else input.display.backfill(result)
      input.setExitCode(0)
    })

  const sessions = program
    .command("sessions")
    .description("inspect locally captured sessions")
    .helpGroup("Sessions:")
    .allowExcessArguments(false)

  sessions
    .command("list")
    .description("list captured sessions")
    .addOption(new Option("--provider <provider>", "select a provider")
      .choices(["codex", "claude"]))
    .option("--limit <count>", "limit the number of sessions", "20")
    .option("--codex-home <path>", "override the Codex data directory")
    .option("--claude-home <path>", "override the Claude Code data directory")
    .allowExcessArguments(false)
    .action(async (commandOptions: SessionListCommandOptions) => {
      const provider = providerOption(commandOptions.provider)
      const limit = positiveInteger(commandOptions.limit, 20)
      await synchronize(commandOptions)
      const store = new LocalSessionStore(htPaths().database)
      try {
        const data = store.snapshots(provider).slice(0, limit).map(sessionSummary)
        if (input.jsonRequested) input.display.printJson({ data })
        else input.display.sessionsList(data)
      } finally {
        store.close()
      }
      input.setExitCode(0)
    })

  sessions
    .command("show")
    .description("show a captured session")
    .argument("<id>", "session ID or unique prefix")
    .option("--include-content", "include event text and attributes")
    .allowExcessArguments(false)
    .action((id: string, commandOptions: SessionShowCommandOptions) => {
      const store = new LocalSessionStore(htPaths().database)
      try {
        const batch = findSession(store.snapshots(), id)
        const data = {
          ...sessionSummary(batch),
          events: batch.events.map((event) => commandOptions.includeContent ? event : {
            ...event,
            text: null,
            attributes: {},
          })
        }
        if (input.jsonRequested) input.display.printJson({ data })
        else input.display.sessionShow(data, commandOptions.includeContent ?? false)
      } finally {
        store.close()
      }
      input.setExitCode(0)
    })

  sessions
    .command("search")
    .description("search captured session content")
    .argument("<text>", "search text")
    .addOption(new Option("--provider <provider>", "select a provider")
      .choices(["codex", "claude"]))
    .option("--limit <count>", "limit the number of matches", "20")
    .allowExcessArguments(false)
    .action((text: string, commandOptions: SessionSearchCommandOptions) => {
      const term = text.trim()
      if (!term) throw new Error("sessions search requires search text.")
      const provider = providerOption(commandOptions.provider)
      const limit = positiveInteger(commandOptions.limit, 20)
      const store = new LocalSessionStore(htPaths().database)
      try {
        const query = term.toLocaleLowerCase()
        const matches = store.snapshots(provider).flatMap((batch) => {
          const content = searchableContent(batch)
          if (!content.toLocaleLowerCase().includes(query)) return []
          return [{ ...sessionSummary(batch), match: snippet(content, query) }]
        }).slice(0, limit)
        if (input.jsonRequested) input.display.printJson({ data: matches })
        else input.display.sessionSearch(matches, term)
      } finally {
        store.close()
      }
      input.setExitCode(0)
    })

  program
    .command("status")
    .description("show ingestion and delivery health")
    .helpGroup("Diagnostics:")
    .option("--verbose", "show local and per-workspace details")
    .allowExcessArguments(false)
    .action(async (commandOptions: StatusCommandOptions) => {
      const current = await status()
      if (input.jsonRequested) input.display.printJson(current)
      else input.display.status(current, commandOptions.verbose ?? false)
      input.setExitCode(current.overall === "needs-attention" ? 1 : 0)
    })

  program
    .command("doctor")
    .description("run diagnostics and print repair guidance")
    .helpGroup("Diagnostics:")
    .allowExcessArguments(false)
    .action(async () => {
      const [preflight, current] = await Promise.all([
        inspectPreflight({ paths: htPaths() }),
        status()
      ])
      const daemon = object(current.daemon)
      const exporter = object(current.exporter)
      const delivery = object(exporter.delivery)
      const checks = {
        platform: preflight.platform.supported,
        stateDirectory: preflight.installation.stateDirectoryWritable,
        providerDetected: preflight.providers.codex.detected ||
          preflight.providers.claude.detected,
        daemon: daemon.reachable === true,
        uploadPolicy: object(current.uploadPolicy).valid === true,
        habitat: exporter.enabled !== true || exporter.connected === true,
        delivery: typeof delivery.quarantined !== "number" ||
          delivery.quarantined === 0
      }
      const healthy = Object.values(checks).every(Boolean)
      const result = {
        healthy,
        checks,
        preflight,
        status: current,
        hints: [
          ...(!checks.providerDetected
            ? ["Start at least one Codex or Claude Code session, then run `ht sync`."]
            : []),
          ...(!checks.daemon ? ["Run `ht install` to repair or activate the background service."] : []),
          ...(!checks.uploadPolicy ? ["Run `ht config check` and correct the reported policy error."] : []),
          ...(!checks.habitat ? ["Run `ht login` again, then resume with `ht backfill`."] : []),
          ...(!checks.delivery
            ? ["Inspect the delivery issues above, then run `ht backfill --retry-quarantined` after remediation."]
            : [])
        ]
      }
      if (input.jsonRequested) input.display.printJson(result)
      else input.display.doctor(result)
      input.setExitCode(healthy ? 0 : 1)
    })

  const daemon = new Command("daemon")
    .description("run the HT background daemon")
    .allowExcessArguments(false)
    .action(async () => {
      await runDaemon()
      input.setExitCode(0)
    })
  program.addCommand(daemon, { hidden: true })

  const hook = new Command("hook")
    .description("notify HT of a provider stop hook")
    .argument("<provider>", "codex or claude")
    .allowExcessArguments(false)
    .action(async (providerValue: string) => {
      const provider = providerOption(providerValue)
      if (!provider) throw new Error("hook requires codex or claude.")
      await notifyDaemon(provider)
      input.setExitCode(0)
    })
  program.addCommand(hook, { hidden: true })

  return program
}

const collectOption = (
  value: string,
  previous: readonly string[] | undefined
): string[] => [...(previous ?? []), value]

interface MeEnvelope {
  data: {
    workspace: { id: string; slug: string; name: string }
    principal: { id: string; kind: string; name: string }
  }
}

const synchronize = async (
  options: SyncCommandOptions,
  requireExporter = false
): Promise<Record<string, unknown>> => {
  const provider = providerOption(options.provider)
  let exporterConfigurationError: string | null = null
  let destinations: CollectorDestination[] = []
  try {
    destinations = await configuredDestinations(htPaths(), options.codexHome)
  } catch (error) {
    exporterConfigurationError = appendError(exporterConfigurationError, error)
  }
  if (requireExporter && destinations.length === 0) {
    throw new Error(exporterConfigurationError ??
      "Habitat setup is required. Run `ht setup` or set HABITAT_API_URL and HABITAT_API_KEY.")
  }
  const customHomes = options.codexHome !== undefined ||
    options.claudeHome !== undefined
  if (!customHomes) {
    const daemonResult = await syncThroughDaemon(provider, htPaths())
    if (daemonResult) {
      return {
        ...daemonResult,
        ...(exporterConfigurationError ? { exporterConfigurationError } : {})
      }
    }
  }
  const paths = htPaths()
  const collector = new SessionCollector({
    statePath: paths.database,
    destinations,
    codexHome: options.codexHome,
    claudeHome: options.claudeHome
  })
  try {
    return {
      ...await collector.runOnce(provider),
      ...(exporterConfigurationError ? { exporterConfigurationError } : {})
    }
  } finally {
    collector.close()
  }
}

const backfill = async (
  options: BackfillCommandOptions
): Promise<Record<string, unknown>> => {
  const provider = providerOption(options.provider)
  const dryRun = options.dryRun ?? false
  const retryQuarantined = options.retryQuarantined ?? false
  const paths = htPaths()
  const config = await expandGitWorktreeRoutes(
    await readConfig(paths),
    { codexHome: options.codexHome }
  )
  const workspaceId = options.workspace ?? config.activeWorkspaceId ?? undefined
  const connection = await resolveHabitatExport(paths, workspaceId)
  if (!connection) {
    throw new CliError(
      "AUTH_REQUIRED",
      "Habitat login is required before a backfill can be prepared.",
      "Run `ht setup`, then retry `ht backfill --dry-run`."
    )
  }
  const uploadPolicy = projectUploadPolicy(config)
  const cutoff = backfillCutoff(config.backfillWindow)
  const destination: CollectorDestination = {
    exporter: new HabitatExporter({
      apiUrl: connection.apiUrl,
      apiKey: connection.apiKey,
      destinationId: connection.destinationId
    }),
    allows: (snapshot, projectOrigin) => {
      const routed = connection.config
        ? projectWorkspaceId(
            config,
            projectOrigin,
            snapshot.source.provider
          ) === connection.config.workspace.id
        : true
      return routed && allowsProjectUpload(uploadPolicy, snapshot, projectOrigin) &&
        (cutoff === null || new Date(snapshot.session.updatedAt).getTime() >= cutoff)
    }
  }
  const collector = new SessionCollector({
    statePath: paths.database,
    destinations: [destination],
    codexHome: options.codexHome,
    claudeHome: options.claudeHome
  })
  try {
    const scan = await collector.scan(provider)
    const preview = collector.backfillPreview(provider)
    if (dryRun) {
      return {
        dryRun: true,
        scan,
        preview,
        next: preview.eligible > 0
          ? "Run `ht backfill` to upload the eligible sessions."
          : "No eligible sessions need backfilling."
      }
    }

    const queued = collector.prepareBackfill(provider)
    collector.pipeline.retryDestination(
      connection.destinationId,
      retryQuarantined
    )
    const flush = {
      attempted: 0,
      exported: 0,
      failed: 0,
      quarantined: 0,
      withheld: 0
    }
    for (let cycle = 0; cycle < 10_000; cycle += 1) {
      const result = await collector.flush(4, provider, { paceMs: 250 })
      flush.attempted += result.attempted
      flush.exported += result.exported
      flush.failed += result.failed
      flush.quarantined += result.quarantined
      flush.withheld = result.withheld
      if (result.attempted === 0) break
    }
    const delivery = collector.pipeline.stats(connection.destinationId)
    return {
      dryRun: false,
      resumable: true,
      retriedQuarantined: retryQuarantined,
      scan,
      preview,
      queued,
      flush,
      delivery,
      complete: delivery.pending === 0 && delivery.quarantined === 0,
      next: delivery.pending > 0
        ? "Some sessions remain queued or withheld. Re-run `ht backfill` after resolving the reported condition."
        : delivery.quarantined > 0
          ? "Some sessions were quarantined. Run `ht doctor` for diagnostics."
          : "Historical sessions are up to date."
    }
  } finally {
    collector.close()
  }
}

const status = async (): Promise<Record<string, unknown>> => {
  const paths = htPaths()
  const store = new LocalSessionStore(paths.database)
  const pipeline = new IncrementalPipelineStore(store.database)
  const local = {
    home: paths.home,
    deviceId: store.deviceId(),
    sessions: store.snapshotCount(),
    pipeline: pipeline.stats(),
    harnesses: pipeline.harnesses(true)
  }
  store.close()

  const port = await daemonPort(paths)
  const daemonState = await readDaemonStatus(paths)
  let policy: Record<string, unknown>
  try {
    policy = { valid: true, ...uploadPolicySummary(await readProjectUploadPolicy()) }
  } catch (error) {
    policy = {
      valid: false,
      configPath: userConfigPath(),
      error: error instanceof Error ? error.message : String(error)
    }
  }
  let reachable = false
  try {
    const response = await fetch(`http://127.0.0.1:${port}/healthz`, {
      signal: AbortSignal.timeout(1_000)
    })
    const health = response.ok
      ? await response.json() as { deviceId?: unknown }
      : null
    reachable = health?.deviceId === local.deviceId
  } catch {
    // Reported below.
  }

  try {
    const [config, connections] = await Promise.all([
      readConfig(paths),
      resolveHabitatExports(paths)
    ])
    const workspaceStatus = await Promise.all(connections.map(async (connection) => {
      const pipelineState = (() => {
        const deliveryStore = new LocalSessionStore(paths.database)
        const deliveryPipeline = new IncrementalPipelineStore(deliveryStore.database)
        try {
          return {
            delivery: {
              ...deliveryPipeline.stats(connection.destinationId),
              issues: deliveryStore.deliveryIssues(connection.destinationId)
            },
            backfill: deliveryPipeline.backfillStatus(connection.destinationId)
          }
        } finally {
          deliveryStore.close()
        }
      })()
      try {
        const me = await apiRequest(connection.apiUrl, connection.apiKey, "/v1/me")
        return {
          id: connection.config?.workspace.id ?? connection.destinationId,
          name: connection.config?.workspace.name ?? "Environment workspace",
          active: connection.config?.workspace.id === config.activeWorkspaceId,
          connected: true,
          destination: connection.apiUrl,
          destinationId: connection.destinationId,
          projects: connection.config
            ? projectsForWorkspace(config, connection.config.workspace.id)
            : [],
          delivery: pipelineState.delivery,
          backfill: pipelineState.backfill,
          remote: me
        }
      } catch (error) {
        return {
          id: connection.config?.workspace.id ?? connection.destinationId,
          name: connection.config?.workspace.name ?? "Environment workspace",
          active: connection.config?.workspace.id === config.activeWorkspaceId,
          connected: false,
          destination: connection.apiUrl,
          destinationId: connection.destinationId,
          projects: connection.config
            ? projectsForWorkspace(config, connection.config.workspace.id)
            : [],
          delivery: pipelineState.delivery,
          backfill: pipelineState.backfill,
          error: error instanceof Error ? error.message : String(error)
        }
      }
    }))
    const connected = workspaceStatus.length > 0 &&
      workspaceStatus.every((workspace) => workspace.connected)
    const delivery = workspaceStatus.reduce((totals, workspace) => ({
      pending: totals.pending + workspace.delivery.pending,
      retrying: totals.retrying + workspace.delivery.retrying,
      nextRetryAt: earliestTimestamp(totals.nextRetryAt, workspace.delivery.nextRetryAt),
      quarantined: totals.quarantined + workspace.delivery.quarantined,
      delivered: totals.delivered + workspace.delivery.delivered
    }), { pending: 0, retrying: 0, nextRetryAt: null as string | null, quarantined: 0, delivered: 0 })
    return finalizeStatus({
      configured: workspaceStatus.length > 0 && config.projects.length > 0,
      local,
      daemon: { reachable, port, ...object(daemonState) },
      uploadPolicy: policy,
      activeWorkspaceId: config.activeWorkspaceId,
      backfillWindow: config.backfillWindow,
      harnesses: local.harnesses.map(harnessStatus),
      workspaces: workspaceStatus,
      exporter: {
        kind: "habitat",
        enabled: workspaceStatus.length > 0,
        connected,
        delivery,
        hint: workspaceStatus.length === 0 ? "Run `ht setup`." : undefined
      }
    })
  } catch (error) {
    return finalizeStatus({
      configured: false,
      local,
      daemon: { reachable, port, ...object(daemonState) },
      uploadPolicy: policy,
      harnesses: local.harnesses.map(harnessStatus),
      workspaces: [],
      exporter: {
        enabled: false,
        connected: false,
        error: error instanceof Error ? error.message : String(error),
        hint: "Run `ht setup` again."
      }
    })
  }
}

const sessionSummary = (batch: SessionSnapshot): Record<string, unknown> => {
  const usage = batch.events.reduce((total, event) => ({
    inputTokens: total.inputTokens + (event.usage?.inputTokens ?? 0),
    outputTokens: total.outputTokens + (event.usage?.outputTokens ?? 0),
    cachedInputTokens: total.cachedInputTokens + (event.usage?.cachedInputTokens ?? 0)
  }), { inputTokens: 0, outputTokens: 0, cachedInputTokens: 0 })
  return {
    id: batch.session.id,
    nativeSessionId: batch.source.nativeSessionId,
    provider: batch.source.provider,
    kind: batch.session.kind,
    title: batch.session.title,
    project: batch.session.project,
    model: batch.session.model,
    status: batch.session.status,
    startedAt: batch.session.startedAt,
    updatedAt: batch.session.updatedAt,
    events: batch.events.length,
    toolCalls: batch.events.filter((event) => event.kind === "tool_call").length,
    usage
  }
}

const findSession = (batches: SessionSnapshot[], id: string): SessionSnapshot => {
  const matches = batches.filter((batch) =>
    batch.session.id === id || batch.source.nativeSessionId === id ||
    batch.session.id.startsWith(id) || batch.source.nativeSessionId.startsWith(id)
  )
  if (matches.length === 0) throw new Error(`Session not found: ${id}`)
  if (matches.length > 1) throw new Error(`Session ID is ambiguous: ${id}`)
  return matches[0]!
}

const searchableContent = (batch: SessionSnapshot): string => [
  batch.session.title,
  batch.session.project,
  batch.session.model,
  batch.source.provider,
  ...batch.events.flatMap((event) => [event.text, event.toolName])
].filter((value): value is string => typeof value === "string").join("\n")

const snippet = (content: string, query: string): string => {
  const normalized = content.toLocaleLowerCase()
  const index = normalized.indexOf(query)
  const start = Math.max(0, index - 80)
  const end = Math.min(content.length, index + query.length + 120)
  return `${start > 0 ? "…" : ""}${content.slice(start, end).replace(/\s+/g, " ")}${end < content.length ? "…" : ""}`
}

const apiRequest = async (
  apiUrl: string,
  apiKey: string,
  path: string,
  options: { readonly method?: string; readonly body?: unknown } = {}
): Promise<unknown> => {
  const response = await fetch(new URL(path, `${apiUrl}/`), {
    method: options.method ?? "GET",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...(options.body === undefined ? {} : { "Content-Type": "application/json" })
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    signal: AbortSignal.timeout(15_000)
  })
  const text = await response.text()
  let body: unknown
  try {
    body = JSON.parse(text) as unknown
  } catch {
    body = text
  }
  if (!response.ok) {
    const error = object(object(body).error)
    throw new Error(typeof error.message === "string" ? error.message : `HTTP ${response.status}`)
  }
  return body
}

const providerOption = (value: string | undefined): Provider | undefined => {
  if (value === undefined || value === "all") return undefined
  if (value === "codex" || value === "claude") return value
  throw new Error("Provider must be codex or claude.")
}

const harnessOption = (value: string | undefined): Provider[] | undefined => {
  if (value === undefined) return undefined
  if (value === "all") return ["codex", "claude"]
  if (value === "codex" || value === "claude") return [value]
  throw new Error("--harness must be all, codex, or claude.")
}

const positiveInteger = (value: string | undefined, fallback: number): number => {
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 1) throw new Error("Expected a positive integer.")
  return parsed
}

const backfillWindowOption = (
  value: string | undefined
): "30d" | "90d" | "all" | undefined => {
  if (value === undefined) return undefined
  if (value === "30d" || value === "90d" || value === "all") return value
  throw new Error("--backfill must be 30d, 90d, or all.")
}

const configuredDestinations = async (
  paths: ReturnType<typeof htPaths>,
  codexHome?: string
): Promise<CollectorDestination[]> => {
  const [savedConfig, connections] = await Promise.all([
    readConfig(paths),
    resolveHabitatExports(paths)
  ])
  const config = await expandGitWorktreeRoutes(savedConfig, { codexHome })
  const uploadPolicy = projectUploadPolicy(config)
  return connections.map((connection) => ({
    exporter: new HabitatExporter({
      apiUrl: connection.apiUrl,
      apiKey: connection.apiKey,
      destinationId: connection.destinationId
    }),
    allows: (snapshot, projectOrigin) => {
      const routed = connection.config
        ? projectWorkspaceId(
            config,
            projectOrigin,
            snapshot.source.provider
          ) === connection.config.workspace.id
        : true
      const cutoff = connection.config ? backfillCutoff(config.backfillWindow) : null
      return routed && allowsProjectUpload(uploadPolicy, snapshot, projectOrigin) &&
        (cutoff === null || new Date(snapshot.session.updatedAt).getTime() >= cutoff)
    }
  }))
}

const backfillCutoff = (window: "30d" | "90d" | "all"): number | null => {
  if (window === "all") return null
  return Date.now() - (window === "90d" ? 90 : 30) * 24 * 60 * 60 * 1_000
}

const object = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}

const finalizeStatus = (
  report: Record<string, unknown>
): Record<string, unknown> => ({
  overall: statusOverall(report),
  ...report
})

const harnessStatus = (
  harness: ReturnType<IncrementalPipelineStore["harnesses"]>[number]
): Record<string, unknown> => ({
  provider: harness.provider,
  selected: harness.selected,
  state: harness.lastHookReceivedAt
    ? "active"
    : harness.provider === "codex"
      ? "verification-required"
      : "waiting-for-first-session",
  configuredAt: harness.configuredAt,
  reviewedAt: harness.reviewedAt,
  lastHookReceivedAt: harness.lastHookReceivedAt,
  lastEvent: harness.lastEvent,
  lastSessionId: harness.lastSessionId
})

const statusOverall = (
  report: Record<string, unknown>
): "healthy" | "syncing" | "needs-attention" => {
  const daemon = object(report.daemon)
  const exporter = object(report.exporter)
  const delivery = object(exporter.delivery)
  const localPipeline = object(object(report.local).pipeline)
  const harnesses = Array.isArray(report.harnesses)
    ? report.harnesses.map(object)
    : []
  const critical = report.configured !== true ||
    daemon.reachable !== true ||
    daemon.state === "degraded" ||
    object(report.uploadPolicy).valid !== true ||
    exporter.enabled !== true ||
    exporter.connected !== true ||
    harnesses.some((harness) => harness.state === "verification-required") ||
    number(delivery.quarantined) > 0
  if (critical) return "needs-attention"
  return number(delivery.pending) > 0 ||
    number(delivery.retrying) > 0 ||
    number(localPipeline.requests) > 0 ||
    number(localPipeline.backfills) > 0
    ? "syncing"
    : "healthy"
}

const earliestTimestamp = (first: string | null, second: string | null): string | null => {
  if (!first) return second
  if (!second) return first
  return new Date(first).getTime() <= new Date(second).getTime() ? first : second
}

const number = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0

const appendError = (current: string | null, error: unknown): string => {
  const next = error instanceof Error ? error.message : String(error)
  return current ? `${current}; ${next}` : next
}

class CliError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly hint?: string
  ) {
    super(message)
    this.name = "CliError"
  }
}

const commandError = (
  error: unknown
): { code: string; message: string; hint?: string } => {
  if (error instanceof CliError) {
    return {
      code: error.code,
      message: error.message,
      ...(error.hint ? { hint: error.hint } : {})
    }
  }
  if (error instanceof CommanderError) {
    const raw = error.message.replace(/^error:\s*/i, "")
    const unknownOption = /^unknown option '([^']+)'/i.exec(raw)
    const message = unknownOption
      ? `Unknown option ${unknownOption[1]}.`
      : raw.length > 0
        ? `${raw[0]!.toUpperCase()}${raw.slice(1)}`
        : "Invalid command arguments."
    return {
      code: "INVALID_ARGUMENT",
      message,
      hint: "Run `ht --help` to see supported commands and options."
    }
  }
  const message = error instanceof Error ? error.message : String(error)
  if (/permission|EACCES|EPERM/i.test(message)) {
    return {
      code: "PERMISSION_REQUIRED",
      message,
      hint: "Review the requested file or service permission, then retry."
    }
  }
  if (/not logged in|credentials are missing|login is required/i.test(message)) {
    return {
      code: "AUTH_REQUIRED",
      message,
      hint: "Run `ht login` and approve this device in your browser."
    }
  }
  return { code: "COMMAND_FAILED", message }
}
