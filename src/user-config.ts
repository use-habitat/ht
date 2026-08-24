import { basename, join } from "node:path"
import { stat } from "node:fs/promises"

import {
  configPath,
  configPaths,
  readConfig,
  type ConfigPathOptions,
  type HTConfig
} from "./config.ts"
import type { SessionSnapshot } from "./snapshot.ts"

export interface ProjectUploadPolicy {
  readonly configPath: string
  readonly configured: boolean
  readonly include: readonly string[] | null
  readonly exclude: readonly string[]
  readonly home: string
}

export type UserConfigPathOptions = ConfigPathOptions

export const userConfigPath = configPath

export const projectUploadPolicy = (
  config: HTConfig,
  options: UserConfigPathOptions = {},
  configured = true
): ProjectUploadPolicy => ({
  configPath: configPath(options),
  configured,
  include: config.upload.projectOrigins.include ?? null,
  exclude: config.upload.projectOrigins.exclude ?? [],
  home: options.home ?? process.env.HOME ?? "."
})

export const readProjectUploadPolicy = async (
  options: UserConfigPathOptions = {}
): Promise<ProjectUploadPolicy> => {
  const paths = configPaths(options)
  const config = await readConfig(paths)
  return projectUploadPolicy(config, options, await isFile(paths.config))
}

export const allowsProjectUpload = (
  policy: ProjectUploadPolicy,
  snapshot: SessionSnapshot,
  projectOrigin: string | null
): boolean => {
  const origin = projectOrigin ? normalizedPath(projectOrigin) : null
  const project = snapshot.session.project ?? (origin ? basename(origin) : null)
  const matches = (pattern: string): boolean => {
    const expanded = expandedPattern(pattern, policy.home)
    return expanded.includes("/")
      ? origin !== null && globMatches(expanded, origin)
      : project !== null && globMatches(expanded, project)
  }
  if (policy.exclude.some(matches)) return false
  return policy.include === null || policy.include.some(matches)
}

export const uploadPolicySummary = (policy: ProjectUploadPolicy): Record<string, unknown> => ({
  configPath: policy.configPath,
  configured: policy.configured,
  projectOrigins: {
    include: policy.include,
    exclude: policy.exclude,
    default: policy.include === null ? "include" : "exclude"
  }
})

const expandedPattern = (pattern: string, home: string): string => {
  const expanded = pattern === "~"
    ? home
    : pattern.startsWith("~/") ? join(home, pattern.slice(2)) : pattern
  return normalizedPath(expanded)
}

const normalizedPath = (value: string): string => {
  const normalized = value.replaceAll("\\", "/")
  return normalized.length > 1 ? normalized.replace(/\/+$/, "") : normalized
}

const globMatches = (pattern: string, value: string): boolean =>
  new RegExp(`^${globExpression(pattern)}$`).test(value)

const globExpression = (pattern: string): string => {
  let expression = ""
  for (let index = 0; index < pattern.length; index += 1) {
    const character = pattern[index]!
    if (character === "*") {
      if (pattern[index + 1] === "*") {
        index += 1
        if (pattern[index + 1] === "/") {
          index += 1
          expression += "(?:.*/)?"
        } else {
          expression += ".*"
        }
      } else {
        expression += "[^/]*"
      }
    } else if (character === "?") {
      expression += "[^/]"
    } else {
      expression += escapeRegex(character)
    }
  }
  return expression
}

const escapeRegex = (value: string): string =>
  /[\\^$.*+?()[\]{}|]/.test(value) ? `\\${value}` : value

const isFile = async (path: string): Promise<boolean> => {
  try {
    return (await stat(path)).isFile()
  } catch {
    return false
  }
}
