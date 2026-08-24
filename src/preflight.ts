import { constants } from "node:fs"
import { access, stat } from "node:fs/promises"
import { homedir } from "node:os"
import { dirname } from "node:path"

import { htPaths, type HTPaths } from "./config.ts"
import { defaultClaudeHome } from "./providers/claude.ts"
import { defaultCodexHome } from "./providers/codex.ts"

export interface PreflightReport {
  readonly ready: boolean
  readonly platform: {
    readonly os: NodeJS.Platform
    readonly architecture: string
    readonly supported: boolean
    readonly serviceManager: "launchd" | "systemd" | "manual"
  }
  readonly installation: {
    readonly installed: boolean
    readonly launcher: string
    readonly stateDirectory: string
    readonly stateDirectoryWritable: boolean
  }
  readonly providers: {
    readonly codex: { readonly detected: boolean; readonly path: string }
    readonly claude: { readonly detected: boolean; readonly path: string }
  }
  readonly notices: readonly string[]
}

export const inspectPreflight = async (options: {
  readonly paths?: HTPaths
  readonly codexHome?: string
  readonly claudeHome?: string
} = {}): Promise<PreflightReport> => {
  const paths = options.paths ?? htPaths()
  const codexHome = options.codexHome ?? defaultCodexHome()
  const claudeHome = options.claudeHome ?? defaultClaudeHome()
  const supportedOs = process.platform === "darwin" || process.platform === "linux"
  const supportedArchitecture = ["arm64", "x64"].includes(process.arch)
  const [installed, stateDirectoryWritable, codexDetected, claudeDetected] =
    await Promise.all([
      isFile(paths.launcher),
      canWrite(paths.home),
      isDirectory(codexHome),
      isDirectory(claudeHome)
    ])
  const notices = [
    ...(!supportedOs
      ? [`HT does not yet provide a background-service installer for ${process.platform}.`]
      : []),
    ...(!supportedArchitecture
      ? [`HT does not publish a binary for the ${process.arch} architecture.`]
      : []),
    ...(!stateDirectoryWritable
      ? [`HT cannot write its state directory at ${paths.home}.`]
      : []),
    ...(!codexDetected && !claudeDetected
      ? ["No Codex or Claude Code session directory was detected yet."]
      : [])
  ]
  return {
    ready: supportedOs && supportedArchitecture && stateDirectoryWritable,
    platform: {
      os: process.platform,
      architecture: process.arch,
      supported: supportedOs && supportedArchitecture,
      serviceManager: process.platform === "darwin"
        ? "launchd"
        : process.platform === "linux" ? "systemd" : "manual"
    },
    installation: {
      installed,
      launcher: paths.launcher,
      stateDirectory: paths.home,
      stateDirectoryWritable
    },
    providers: {
      codex: { detected: codexDetected, path: codexHome },
      claude: { detected: claudeDetected, path: claudeHome }
    },
    notices
  }
}

const isFile = async (path: string): Promise<boolean> => {
  try {
    return (await stat(path)).isFile()
  } catch {
    return false
  }
}

const isDirectory = async (path: string): Promise<boolean> => {
  try {
    return (await stat(path)).isDirectory()
  } catch {
    return false
  }
}

const canWrite = async (path: string): Promise<boolean> => {
  let candidate = path
  while (candidate !== dirname(candidate)) {
    try {
      if (!(await stat(candidate)).isDirectory()) return false
      await access(candidate, constants.W_OK)
      return true
    } catch (error) {
      if (!isMissing(error)) return false
      candidate = dirname(candidate)
    }
  }
  try {
    await access(homedir(), constants.W_OK)
    return true
  } catch {
    return false
  }
}

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
