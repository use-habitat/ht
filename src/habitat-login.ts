import { hostname } from "node:os"

import { terminateOwnedSubprocess } from "./owned-subprocess.ts"

export const defaultHabitatApiUrl = "https://habitat-sessions-api.habitat-e3b.workers.dev"
export const defaultHabitatAppUrl = "https://app.use-habitat.com"

export interface HabitatLoginResult {
  readonly apiKey: string
  readonly workspace: { readonly id: string; readonly slug: string; readonly name: string }
  readonly principal: { readonly id: string; readonly kind: string; readonly name: string }
}

export const loginWithBrowser = async (options: {
  readonly apiUrl: string
  readonly appUrl: string
  readonly deviceName?: string
  readonly openBrowser?: (url: string) => Promise<boolean>
  readonly fetch?: typeof globalThis.fetch
  readonly delay?: (milliseconds: number) => Promise<void>
  readonly now?: () => number
  readonly onVerification?: (value: {
    readonly url: string
    readonly userCode: string
    readonly browserOpened: boolean
  }) => void
}): Promise<HabitatLoginResult> => {
  const fetcher = options.fetch ?? globalThis.fetch
  const started = await request(fetcher, options.apiUrl, "/v1/cli/login", {
    deviceName: options.deviceName ?? hostname()
  }) as {
    data?: {
      deviceCode?: unknown
      userCode?: unknown
      expiresIn?: unknown
      interval?: unknown
    }
  }
  const deviceCode = string(started.data?.deviceCode, "Habitat did not return a device code.")
  const userCode = string(started.data?.userCode, "Habitat did not return a verification code.")
  const expiresIn = positiveNumber(started.data?.expiresIn, 600)
  const interval = positiveNumber(started.data?.interval, 2)
  const verificationUrl = new URL("/cli/authorize", trailingSlash(options.appUrl))
  verificationUrl.searchParams.set("code", userCode)

  const browserOpened = await (options.openBrowser ?? openBrowser)(verificationUrl.toString())
  options.onVerification?.({ url: verificationUrl.toString(), userCode, browserOpened })

  const now = options.now ?? Date.now
  const delay = options.delay ?? ((milliseconds: number) =>
    new Promise<void>((resolve) => setTimeout(resolve, milliseconds)))
  const deadline = now() + expiresIn * 1_000
  while (now() < deadline) {
    await delay(interval * 1_000)
    const response = await fetcher(new URL("/v1/cli/login/token", trailingSlash(options.apiUrl)), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ deviceCode }),
      signal: AbortSignal.timeout(15_000)
    })
    const body = await response.json().catch(() => null) as {
      data?: {
        status?: unknown
        apiKey?: unknown
        workspace?: HabitatLoginResult["workspace"]
        principal?: HabitatLoginResult["principal"]
      }
      error?: { message?: unknown }
    } | null
    if (response.status === 202 && body?.data?.status === "pending") continue
    if (!response.ok) throw new Error(errorMessage(body, response.status))
    if (
      body?.data?.status !== "authorized" ||
      !body.data.workspace ||
      !body.data.principal
    ) {
      throw new Error("Habitat returned an invalid login response.")
    }
    return {
      apiKey: string(body.data.apiKey, "Habitat did not return an API key."),
      workspace: body.data.workspace,
      principal: body.data.principal
    }
  }
  throw new Error("Habitat login timed out. Run `ht login` to try again.")
}

export const habitatApiUrl = (value?: string): string =>
  secureUrl(value ?? process.env.HABITAT_API_URL ?? defaultHabitatApiUrl, "API")

export const habitatAppUrl = (value: string | undefined, apiUrl: string): string => {
  const inferred = isLocalUrl(apiUrl) ? "http://localhost:3000" : defaultHabitatAppUrl
  return secureUrl(value ?? process.env.HABITAT_APP_URL ?? inferred, "app")
}

const request = async (
  fetcher: typeof globalThis.fetch,
  apiUrl: string,
  path: string,
  body: unknown
): Promise<unknown> => {
  const response = await fetcher(new URL(path, trailingSlash(apiUrl)), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000)
  })
  const value = await response.json().catch(() => null) as {
    error?: { message?: unknown }
  } | null
  if (!response.ok) throw new Error(errorMessage(value, response.status))
  return value
}

const openBrowser = async (url: string): Promise<boolean> => {
  const command = process.platform === "darwin"
    ? ["open", url]
    : process.platform === "linux"
      ? ["xdg-open", url]
      : null
  if (!command) return false
  let child: ReturnType<typeof Bun.spawn> | null = null
  try {
    child = Bun.spawn(command, { stdout: "ignore", stderr: "ignore" })
    return await child.exited === 0
  } catch {
    return false
  } finally {
    if (child?.exitCode === null) {
      await terminateOwnedSubprocess(child)
    }
  }
}

const secureUrl = (value: string, label: string): string => {
  const url = new URL(value)
  if (url.protocol !== "https:" && !isLocalUrl(url.toString())) {
    throw new Error(`Hosted Habitat ${label} URLs must use HTTPS.`)
  }
  return url.toString().replace(/\/$/, "")
}

const isLocalUrl = (value: string): boolean => {
  const url = new URL(value)
  return url.hostname === "127.0.0.1" || url.hostname === "localhost"
}

const trailingSlash = (value: string): string => value.endsWith("/") ? value : `${value}/`

const string = (value: unknown, message: string): string => {
  if (typeof value !== "string" || !value) throw new Error(message)
  return value
}

const positiveNumber = (value: unknown, fallback: number): number =>
  typeof value === "number" && Number.isFinite(value) && value > 0 ? value : fallback

const errorMessage = (
  body: { error?: { message?: unknown } } | null,
  status: number
): string => typeof body?.error?.message === "string"
  ? body.error.message
  : `Habitat login failed (HTTP ${status}).`
