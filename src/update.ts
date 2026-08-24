import { createHash } from "node:crypto"
import {
  access,
  chmod,
  mkdir,
  mkdtemp,
  rename,
  rm,
  writeFile
} from "node:fs/promises"
import { dirname, join } from "node:path"

import { htPaths, type HTPaths } from "./config.ts"
import {
  pauseHTDaemon,
  resumeHTDaemon,
  type DaemonRuntime
} from "./install.ts"

const defaultReleaseBaseUrl = "https://app.use-habitat.com/cli"
const releaseVersionPattern = /^v[0-9][0-9A-Za-z.-]*$/

export interface UpdateResult {
  readonly updated: boolean
  readonly currentVersion: string
  readonly version: string
  readonly binary: string
  readonly checksumVerified: boolean
  readonly daemonRestarted: boolean | null
}

export interface UpdateOptions {
  readonly currentVersion: string
  readonly requestedVersion?: string
  readonly force?: boolean
  readonly paths?: HTPaths
  readonly releaseBaseUrl?: string
  readonly platform?: NodeJS.Platform
  readonly architecture?: string
  readonly fetch?: typeof globalThis.fetch
  readonly restartDaemon?: boolean
  readonly daemonRuntime?: DaemonRuntime
}

export const updateHT = async (
  options: UpdateOptions
): Promise<UpdateResult> => {
  const paths = options.paths ?? htPaths()
  const binary = join(paths.home, "libexec", "ht")
  try {
    await access(binary)
  } catch {
    throw new Error(
      `HT's managed installation was not found at ${binary}. ` +
      "Install it first with `curl -fsSL https://app.use-habitat.com/install.sh | sh`."
    )
  }

  const releaseBaseUrl = releaseBase(options.releaseBaseUrl ??
    process.env.HT_RELEASE_BASE_URL ??
    defaultReleaseBaseUrl)
  const fetcher = options.fetch ?? globalThis.fetch
  const currentVersion = releaseVersion(options.currentVersion)
  const requestedVersion = options.requestedVersion
    ? releaseVersion(options.requestedVersion)
    : await downloadText(new URL("latest", releaseBaseUrl), fetcher)
  const nextVersion = releaseVersion(requestedVersion.trim())

  if (nextVersion === currentVersion && options.force !== true) {
    return {
      updated: false,
      currentVersion,
      version: currentVersion,
      binary,
      checksumVerified: false,
      daemonRestarted: null
    }
  }

  const asset = releaseAsset(
    options.platform ?? process.platform,
    options.architecture ?? process.arch
  )
  const versionUrl = new URL(`${nextVersion}/`, releaseBaseUrl)
  const [download, checksums] = await Promise.all([
    downloadBytes(new URL(asset, versionUrl), fetcher),
    downloadText(new URL("SHA256SUMS", versionUrl), fetcher)
  ])
  const expected = expectedChecksum(checksums, asset)
  const actual = createHash("sha256").update(download).digest("hex")
  if (expected.toLocaleLowerCase() !== actual) {
    throw new Error(
      `Checksum verification failed for ${asset}; the installed HT binary was not changed.`
    )
  }

  await mkdir(dirname(binary), { recursive: true, mode: 0o700 })
  const temporaryDirectory = await mkdtemp(join(dirname(binary), ".ht-update-"))
  const temporaryBinary = join(temporaryDirectory, "ht")
  let daemonPaused = false
  let daemonRestarted: boolean | null = null
  try {
    await writeFile(temporaryBinary, download, { mode: 0o755 })
    await chmod(temporaryBinary, 0o755)
    if (options.restartDaemon !== false) {
      daemonPaused = await pauseHTDaemon(paths, options.daemonRuntime)
    }
    try {
      await rename(temporaryBinary, binary)
    } finally {
      if (daemonPaused) {
        daemonRestarted = await resumeHTDaemon(paths, options.daemonRuntime)
      }
    }
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true })
  }

  return {
    updated: true,
    currentVersion,
    version: nextVersion,
    binary,
    checksumVerified: true,
    daemonRestarted
  }
}

const releaseBase = (value: string): URL => {
  let url: URL
  try {
    url = new URL(value.endsWith("/") ? value : `${value}/`)
  } catch {
    throw new Error("HT_RELEASE_BASE_URL must be an http(s) URL.")
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw new Error("HT_RELEASE_BASE_URL must be an http(s) URL.")
  }
  return url
}

const releaseVersion = (value: string): string => {
  const version = value.startsWith("v") ? value : `v${value}`
  if (!releaseVersionPattern.test(version)) {
    throw new Error(
      `${JSON.stringify(value)} is not an HT release version such as v0.4.0.`
    )
  }
  return version
}

const releaseAsset = (
  platform: NodeJS.Platform,
  architecture: string
): string => {
  const operatingSystem = platform === "darwin"
    ? "darwin"
    : platform === "linux"
      ? "linux"
      : null
  if (!operatingSystem) {
    throw new Error("HT updates currently support macOS and Linux.")
  }
  const architectureName = architecture === "arm64" ||
      architecture === "aarch64"
    ? "arm64"
    : architecture === "x64" ||
        architecture === "x86_64" ||
        architecture === "amd64"
      ? "x64"
      : null
  if (!architectureName) {
    throw new Error(`Unsupported CPU architecture: ${architecture}.`)
  }
  return `ht-${operatingSystem}-${architectureName}`
}

const downloadText = async (
  url: URL,
  fetcher: typeof globalThis.fetch
): Promise<string> => {
  const response = await fetcher(url)
  if (!response.ok) {
    throw new Error(
      `HT release download failed (${response.status}) for ${url.toString()}.`
    )
  }
  return response.text()
}

const downloadBytes = async (
  url: URL,
  fetcher: typeof globalThis.fetch
): Promise<Uint8Array> => {
  const response = await fetcher(url)
  if (!response.ok) {
    throw new Error(
      `HT release download failed (${response.status}) for ${url.toString()}.`
    )
  }
  return new Uint8Array(await response.arrayBuffer())
}

const expectedChecksum = (value: string, asset: string): string => {
  for (const line of value.split(/\r?\n/)) {
    const fields = line.trim().split(/\s+/)
    const checksum = fields[0]
    const filename = fields.at(-1)?.replace(/^\*/, "")
    if (filename === asset && checksum && /^[0-9a-f]{64}$/i.test(checksum)) {
      return checksum
    }
  }
  throw new Error(`The release checksum does not contain ${asset}.`)
}
