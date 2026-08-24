import { chmod, copyFile, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { homedir } from "node:os"
import { basename, delimiter, dirname, join, resolve } from "node:path"

import { htPaths, type HTPaths } from "./config.ts"
import type { Provider } from "./domain.ts"
import { terminateOwnedSubprocess } from "./owned-subprocess.ts"

interface HookHandler { type?: unknown; command?: unknown; [key: string]: unknown }
interface HookGroup { matcher?: unknown; hooks?: unknown; [key: string]: unknown }
type JsonObject = Record<string, unknown>

export interface InstallResult {
  readonly launcher: string
  readonly providers: readonly Provider[]
  readonly codexHooks: string
  readonly claudeSettings: string
  readonly daemon: { readonly manager: "launchd" | "systemd" | "manual"; readonly active: boolean; readonly path: string | null }
  readonly notices: readonly string[]
}

export interface DaemonRuntime {
  readonly platform: NodeJS.Platform
  readonly home: string
  readonly uid: number
  readonly command: (args: readonly string[]) => Promise<boolean>
}

const daemonRuntime = (): DaemonRuntime => ({
  platform: process.platform,
  home: homedir(),
  uid: process.getuid?.() ?? 0,
  command
})

export const pauseHTDaemon = async (
  paths: HTPaths = htPaths(),
  runtime?: DaemonRuntime
): Promise<boolean> => {
  // Isolated/test HT homes must never manipulate the user's real service.
  if (!runtime && paths.home !== htPaths().home) return false
  const service = runtime ?? daemonRuntime()
  if (service.platform === "darwin") {
    const path = launchAgentPath(service.home)
    const domain = launchAgentDomain(service.uid)
    return service.command(["launchctl", "bootout", domain, path])
  }
  if (service.platform === "linux") {
    return service.command(["systemctl", "--user", "stop", "ht-collector.service"])
  }
  return false
}

export const resumeHTDaemon = async (
  paths: HTPaths = htPaths(),
  runtime?: DaemonRuntime
): Promise<boolean> => {
  if (!runtime && paths.home !== htPaths().home) return false
  const service = runtime ?? daemonRuntime()
  if (service.platform === "darwin") {
    return service.command([
      "launchctl",
      "bootstrap",
      launchAgentDomain(service.uid),
      launchAgentPath(service.home)
    ])
  }
  if (service.platform === "linux") {
    return service.command(["systemctl", "--user", "start", "ht-collector.service"])
  }
  return false
}

export const restartHTDaemon = async (
  paths: HTPaths = htPaths(),
  runtime?: DaemonRuntime
): Promise<boolean> => {
  if (!runtime && paths.home !== htPaths().home) return false
  const service = runtime ?? daemonRuntime()
  if (service.platform === "darwin") {
    return service.command([
      "launchctl",
      "kickstart",
      "-k",
      `${launchAgentDomain(service.uid)}/dev.ht.collector`
    ])
  }
  if (service.platform === "linux") {
    return service.command(["systemctl", "--user", "restart", "ht-collector.service"])
  }
  return false
}

export const installHT = async (options: {
  readonly entrypoint: string
  readonly paths?: HTPaths
  readonly codexHome?: string
  readonly claudeHome?: string
  readonly activateDaemon?: boolean
  readonly providers?: readonly Provider[]
  readonly daemonRuntime?: DaemonRuntime
}): Promise<InstallResult> => {
  const paths = options.paths ?? htPaths()
  const providers = options.providers ?? ["codex", "claude"]
  const selected = new Set(providers)
  const codexHome = options.codexHome ?? process.env.HT_CODEX_HOME ?? process.env.CODEX_HOME ?? join(homedir(), ".codex")
  const claudeHome = options.claudeHome ?? process.env.HT_CLAUDE_HOME ?? process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), ".claude")
  await mkdir(paths.bin, { recursive: true, mode: 0o700 })
  await mkdir(paths.logs, { recursive: true, mode: 0o700 })
  await rm(join(paths.bin, "codex-hook-review"), { force: true })
  await rm(join(paths.home, "codex-hook-review.terminal"), { force: true })
  await installLauncher(paths.launcher, options.entrypoint)

  const codexHooks = join(codexHome, "hooks.json")
  const claudeSettings = join(claudeHome, "settings.json")
  if (selected.has("codex")) {
    await mergeProviderHooks(codexHooks, "codex", paths.launcher, ["Stop", "SessionEnd"])
  } else {
    await removeProviderHooks(codexHooks, paths.launcher)
  }
  if (selected.has("claude")) {
    await mergeProviderHooks(claudeSettings, "claude", paths.launcher, ["Stop", "SessionEnd"])
  } else {
    await removeProviderHooks(claudeSettings, paths.launcher)
  }

  const daemon = options.activateDaemon === false
    ? { manager: "manual" as const, active: false, path: null }
    : await installDaemon(paths, options.daemonRuntime)
  const launcher = resolve(paths.launcher)
  const resolvedCommand = Bun.which("ht")
  const binOnPath = (process.env.PATH ?? "")
    .split(delimiter)
    .filter(Boolean)
    .some((entry) => resolve(entry) === resolve(paths.bin))
  const notices = [
    ...(selected.has("codex")
      ? ["Codex requires one-time hook review before automatic ingestion can begin."]
      : []),
    ...(!binOnPath
      ? [`Add Habitat to PATH: export PATH="${paths.bin}:$PATH"`]
      : []),
    ...(resolvedCommand && resolve(resolvedCommand) !== launcher
      ? [
          `Another ht command currently resolves first at ${resolvedCommand}. ` +
            `Use ${paths.launcher} or put ${paths.bin} before it in PATH.`
        ]
      : []),
    ...(options.activateDaemon === false
      ? ["Background service installation was skipped; run `ht daemon` manually when needed."]
      : daemon.active
        ? []
        : ["The background service could not be activated automatically; run `ht daemon` manually."])
  ]
  return {
    launcher: paths.launcher,
    providers: [...selected],
    codexHooks,
    claudeSettings,
    daemon,
    notices
  }
}

export const uninstallHTHooks = async (options: {
  readonly paths?: HTPaths
  readonly codexHome?: string
  readonly claudeHome?: string
} = {}): Promise<void> => {
  const paths = options.paths ?? htPaths()
  const codexHome = options.codexHome ?? process.env.HT_CODEX_HOME ?? process.env.CODEX_HOME ?? join(homedir(), ".codex")
  const claudeHome = options.claudeHome ?? process.env.HT_CLAUDE_HOME ?? process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), ".claude")
  await removeProviderHooks(join(codexHome, "hooks.json"), paths.launcher)
  await removeProviderHooks(join(claudeHome, "settings.json"), paths.launcher)
  await stopDaemon(paths)
}

export const uninstallHT = async (options: {
  readonly paths?: HTPaths
  readonly codexHome?: string
  readonly claudeHome?: string
} = {}): Promise<void> => {
  const paths = options.paths ?? htPaths()
  await uninstallHTHooks(options)
  await rm(paths.launcher, { force: true })
  await rm(join(paths.bin, "codex-hook-review"), { force: true })
  await rm(join(paths.home, "codex-hook-review.terminal"), { force: true })
  await rm(join(paths.home, "libexec", "ht"), { force: true })
}

const installLauncher = async (launcher: string, entrypoint: string): Promise<void> => {
  const compiled = basename(process.execPath) !== "bun"
  const command = compiled
    ? `exec ${shellQuote(resolve(process.execPath))} "$@"`
    : `exec ${shellQuote(resolve(process.execPath))} ${shellQuote(resolve(entrypoint))} "$@"`
  await writeFile(launcher, `#!/bin/sh\n${command}\n`, { mode: 0o755 })
  await chmod(launcher, 0o755)
}

const mergeProviderHooks = async (
  path: string,
  provider: "codex" | "claude",
  launcher: string,
  events: readonly string[]
): Promise<void> => {
  const root = await readJson(path)
  await backup(path)
  const hooks = object(root.hooks)
  for (const event of events) {
    const existing = Array.isArray(hooks[event]) ? hooks[event] as HookGroup[] : []
    const cleaned = removeHTHandlers(existing, launcher)
    cleaned.push({
      hooks: [{
        type: "command",
        command: `${shellQuote(launcher)} hook ${provider}`,
        timeout: 5,
        ...(provider === "codex" ? { statusMessage: "Saving session to HT" } : {})
      }]
    })
    hooks[event] = cleaned
  }
  root.hooks = hooks
  await atomicJson(path, root)
}

const removeProviderHooks = async (path: string, launcher: string): Promise<void> => {
  let root: JsonObject
  try {
    root = await readJson(path, false)
  } catch (error) {
    if (isMissing(error)) return
    throw error
  }
  const hooks = object(root.hooks)
  for (const [event, groups] of Object.entries(hooks)) {
    if (!Array.isArray(groups)) continue
    hooks[event] = removeHTHandlers(groups as HookGroup[], launcher)
  }
  root.hooks = hooks
  await atomicJson(path, root)
}

const removeHTHandlers = (groups: HookGroup[], launcher: string): HookGroup[] =>
  groups.flatMap((group) => {
    const handlers = Array.isArray(group.hooks) ? group.hooks as HookHandler[] : []
    const remaining = handlers.filter((handler) =>
      typeof handler.command !== "string" || !handler.command.includes(launcher)
    )
    return remaining.length > 0 ? [{ ...group, hooks: remaining }] : []
  })

const installDaemon = async (
  paths: HTPaths,
  runtime?: DaemonRuntime
): Promise<InstallResult["daemon"]> => {
  // A caller using an isolated HT home must explicitly provide an isolated
  // service runtime. This prevents tests and advanced callers from registering
  // the real user's background service by accident.
  if (!runtime && paths.home !== htPaths().home) {
    return { manager: "manual", active: false, path: null }
  }
  const service = runtime ?? daemonRuntime()
  if (service.platform === "darwin") {
    const path = launchAgentPath(service.home)
    await mkdir(dirname(path), { recursive: true })
    const plist = launchAgentPlist(paths)
    const domain = launchAgentDomain(service.uid)
    const target = `${domain}/dev.ht.collector`
    const [matches, loaded] = await Promise.all([
      fileMatches(path, plist),
      service.command(["launchctl", "print", target])
    ])
    if (matches && loaded) {
      return { manager: "launchd", active: true, path }
    }

    if (!matches) {
      if (loaded && !await service.command(["launchctl", "bootout", domain, path])) {
        return { manager: "launchd", active: false, path }
      }
      await writeFile(path, plist, { mode: 0o600 })
    }
    const active = await service.command(["launchctl", "bootstrap", domain, path])
    return { manager: "launchd", active, path }
  }
  if (service.platform === "linux") {
    const path = join(service.home, ".config", "systemd", "user", "ht-collector.service")
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, `[Unit]
Description=HT local session collector
After=network-online.target

[Service]
ExecStart=${paths.launcher} daemon
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
`, { mode: 0o600 })
    const reloaded = await service.command(["systemctl", "--user", "daemon-reload"])
    const active = reloaded && await service.command(["systemctl", "--user", "enable", "--now", "ht-collector.service"])
    return { manager: "systemd", active, path }
  }
  return { manager: "manual", active: false, path: null }
}

const launchAgentPlist = (paths: HTPaths): string => `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>dev.ht.collector</string>
  <key>ProgramArguments</key>
  <array><string>${xml(paths.launcher)}</string><string>daemon</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>${xml(join(paths.logs, "collector.log"))}</string>
  <key>StandardErrorPath</key><string>${xml(join(paths.logs, "collector.error.log"))}</string>
</dict>
</plist>
`

const launchAgentPath = (home: string): string =>
  join(home, "Library", "LaunchAgents", "dev.ht.collector.plist")

const launchAgentDomain = (uid: number): string => `gui/${uid}`

const fileMatches = async (path: string, desired: string): Promise<boolean> => {
  try {
    return await readFile(path, "utf8") === desired
  } catch (error) {
    if (isMissing(error)) return false
    throw error
  }
}

const stopDaemon = async (paths: HTPaths): Promise<void> => {
  // Tests and advanced callers can manage isolated HT homes. They must not
  // stop the real user-level service that belongs to the configured default home.
  if (paths.home !== htPaths().home) return
  if (process.platform === "darwin") {
    const path = join(homedir(), "Library", "LaunchAgents", "dev.ht.collector.plist")
    const domain = `gui/${process.getuid?.() ?? 0}`
    await command(["launchctl", "bootout", domain, path], true)
    await rm(path, { force: true })
  } else if (process.platform === "linux") {
    const path = join(homedir(), ".config", "systemd", "user", "ht-collector.service")
    await command(["systemctl", "--user", "disable", "--now", "ht-collector.service"], true)
    await rm(path, { force: true })
    await command(["systemctl", "--user", "daemon-reload"], true)
  }
  void paths
}

const readJson = async (path: string, missingIsEmpty = true): Promise<JsonObject> => {
  try {
    return object(JSON.parse(await readFile(path, "utf8")) as unknown)
  } catch (error) {
    if (missingIsEmpty && isMissing(error)) return {}
    throw error
  }
}

const backup = async (path: string): Promise<void> => {
  try {
    await copyFile(path, `${path}.ht-backup`)
  } catch (error) {
    if (!isMissing(error)) throw error
  }
}

const atomicJson = async (path: string, value: JsonObject): Promise<void> => {
  await mkdir(dirname(path), { recursive: true })
  const temporary = `${path}.${process.pid}.tmp`
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 })
  await rename(temporary, path)
}

const object = (value: unknown): JsonObject =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as JsonObject
    : {}

const command = async (
  args: readonly string[],
  ignoreFailure = false
): Promise<boolean> => {
  let child: ReturnType<typeof Bun.spawn> | null = null
  try {
    child = Bun.spawn([...args], { stdout: "ignore", stderr: "ignore" })
    const ok = await child.exited === 0
    return ok || ignoreFailure
  } catch {
    return ignoreFailure
  } finally {
    if (child?.exitCode === null) {
      await terminateOwnedSubprocess(child)
    }
  }
}

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`
const xml = (value: string): string => value
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&apos;")

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
