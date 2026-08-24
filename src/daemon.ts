import { unwatchFile, watchFile } from "node:fs"

import type { Provider } from "./domain.ts"
import { SessionCollector, type CollectorDestination } from "./collector.ts"
import {
  daemonPort,
  expandGitWorktreeRoutes,
  projectWorkspaceId,
  readConfig,
  resolveHabitatExports,
  htPaths,
  writeDaemonStatus
} from "./config.ts"
import { HabitatExporter } from "./exporters/habitat.ts"
import { IncrementalPipelineStore } from "./pipeline-store.ts"
import { LocalSessionStore } from "./store.ts"
import { allowsProjectUpload, projectUploadPolicy } from "./user-config.ts"
import { cliVersion } from "./version.ts"

export interface DaemonCycleResult {
  readonly requests: {
    readonly attempted: number
    readonly processed: number
    readonly failed: number
  }
  readonly backfills: {
    readonly attempted: number
    readonly processed: number
    readonly failed: number
  }
  readonly scan: Awaited<ReturnType<SessionCollector["scan"]>> | null
  readonly flush: Awaited<ReturnType<SessionCollector["flush"]>>
}

const daemonFlushLimit = 4
const daemonUploadPaceMs = 250
const minimumDaemonRetryDelayMs = 750
const configReloadDelayMs = 50

export interface ConfigWatcher {
  close(): void
}

export const runDaemon = async (): Promise<void> => {
  const paths = htPaths()
  let port = await daemonPort(paths)
  const identityStore = new LocalSessionStore(paths.database)
  const deviceId = identityStore.deviceId()
  identityStore.close()
  let active: Promise<DaemonCycleResult> | null = null
  let stopped = false
  let fullSyncRequested = false
  let wakeRequested = false
  let retryTimer: ReturnType<typeof setTimeout> | null = null
  let configWatcher: ConfigWatcher | null = null

  const runOnce = async (fullSync: boolean, provider?: Provider): Promise<DaemonCycleResult> => {
    let destinations: CollectorDestination[] = []
    let configuredProviders: Provider[] | null = null
    let exportError: string | null = null
    try {
      const [connections, savedConfig] = await Promise.all([
        resolveHabitatExports(paths),
        readConfig(paths)
      ])
      const config = await expandGitWorktreeRoutes(savedConfig)
      const uploadPolicy = projectUploadPolicy(config)
      configuredProviders = [...new Set(
        config.projects.flatMap((project) => project.providers)
      )]
      destinations = connections.map((connection) => ({
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
          const cutoff = backfillCutoff(config.backfillWindow)
          return routed &&
            allowsProjectUpload(uploadPolicy, snapshot, projectOrigin) && (
            cutoff === null ||
            new Date(snapshot.session.updatedAt).getTime() >= cutoff
          )
        }
      }))
    } catch (error) {
      exportError = error instanceof Error ? error.message : String(error)
    }

    const collector = new SessionCollector({
      statePath: paths.database,
      destinations,
      autoPrepareDestinations: false
    })
    const startedAt = new Date().toISOString()
    try {
      if (configuredProviders) {
        collector.pipeline.configureHarnesses(configuredProviders)
      }
      const requests = await drainPipelineRequests(collector)
      const durableBackfills = await drainBackfillRequests(collector, destinations)
      const scan = fullSync
        ? await collector.scan(provider)
        : durableBackfills.scan
      if (fullSync) {
        for (const destination of destinations) {
          collector.pipeline.prepareDestination(
            destination.exporter.destinationId,
            {
              provider,
              allows: destination.allows
            }
          )
        }
      }
      const flush = await collector.flush(daemonFlushLimit, provider, {
        paceMs: daemonUploadPaceMs
      })
      const result: DaemonCycleResult = {
        requests,
        backfills: durableBackfills.backfills,
        scan,
        flush
      }
      await writeDaemonStatus({
        pid: process.pid,
        state: exportError || requests.failed > 0 ||
            durableBackfills.backfills.failed > 0 || flush.failed > 0
          ? "degraded"
          : "running",
        strategy: "hook-targeted",
        sourcePolling: false,
        startedAt,
        completedAt: new Date().toISOString(),
        workspaces: destinations.map((destination) =>
          destination.exporter.destinationId
        ),
        exporterError: exportError,
        result,
        pipeline: collector.pipeline.stats()
      }, paths)
      return result
    } finally {
      collector.close()
    }
  }

  const scheduleRetry = (nextAttemptAt: number | null): void => {
    if (retryTimer || stopped) return
    const delay = nextAttemptAt === null
      ? 30_000
      : Math.max(minimumDaemonRetryDelayMs, nextAttemptAt - Date.now())
    retryTimer = setTimeout(() => {
      retryTimer = null
      void cycle(false)
    }, delay)
  }

  const cycle = (
    requestFullSync = false,
    provider?: Provider
  ): Promise<DaemonCycleResult> => {
    if (requestFullSync) fullSyncRequested = true
    wakeRequested = true
    if (active) return active
    active = (async () => {
      let latest: DaemonCycleResult | null = null
      do {
        const fullSync = fullSyncRequested
        fullSyncRequested = false
        wakeRequested = false
        try {
          latest = await runOnce(fullSync, provider)
        } catch (error) {
          await writeDaemonStatus({
            pid: process.pid,
            state: "degraded",
            strategy: "hook-targeted",
            sourcePolling: false,
            failedAt: new Date().toISOString(),
            error: error instanceof Error ? error.message : String(error)
          }, paths)
        }
      } while ((fullSyncRequested || wakeRequested) && !stopped)
      if (!latest) {
        throw new Error("Collector stopped before synchronization completed.")
      }
      const retryStore = new LocalSessionStore(paths.database)
      const retryPipeline = new IncrementalPipelineStore(retryStore.database)
      const retryState = retryPipeline.stats()
      const nextAttemptAt = retryPipeline.nextAttemptAt()
      retryStore.close()
      if (
        retryState.pending > 0 || retryState.requests > 0 ||
        retryState.backfills > 0
      ) scheduleRetry(nextAttemptAt)
      return latest
    })().finally(() => {
      active = null
      if ((fullSyncRequested || wakeRequested) && !stopped) {
        void cycle(false)
      }
    })
    return active
  }

  const handleRequest = async (request: Request): Promise<Response> => {
    const url = new URL(request.url)
    if (request.method === "GET" && url.pathname === "/healthz") {
      return Response.json({
        status: "ok",
        version: cliVersion,
        pid: process.pid,
        deviceId,
        strategy: "hook-targeted",
        sourcePolling: false
      })
    }
    if (request.method === "POST" && url.pathname === "/wake") {
      void cycle(false)
      return Response.json({ accepted: true }, { status: 202 })
    }
    if (request.method === "POST" && url.pathname === "/sync") {
      const provider = providerValue(url.searchParams.get("provider"))
      if (url.searchParams.get("background") === "true") {
        void cycle(true, provider)
        return Response.json({ accepted: true }, { status: 202 })
      }
      return Response.json(await cycle(true, provider))
    }
    return Response.json({ error: "not-found" }, { status: 404 })
  }
  let server = Bun.serve({
    hostname: "127.0.0.1",
    port,
    fetch: handleRequest
  })
  let resolveShutdown: () => void = () => {}
  const shutdownRequested = new Promise<void>((resolve) => {
    resolveShutdown = resolve
  })
  const shutdown = (): void => {
    if (stopped) return
    stopped = true
    resolveShutdown()
  }
  process.once("SIGINT", shutdown)
  process.once("SIGTERM", shutdown)
  try {
    await writeDaemonStatus({
      pid: process.pid,
      state: "starting",
      strategy: "hook-targeted",
      sourcePolling: false,
      localUrl: server.url.toString(),
      startedAt: new Date().toISOString()
    }, paths)
    configWatcher = watchConfig(paths.config, () => {
      void (async () => {
        try {
          const nextPort = await daemonPort(paths)
          if (nextPort !== port) {
            const nextServer = Bun.serve({
              hostname: "127.0.0.1",
              port: nextPort,
              fetch: handleRequest
            })
            server.stop(true)
            server = nextServer
            port = nextPort
          }
        } catch (error) {
          await writeDaemonStatus({
            pid: process.pid,
            state: "degraded",
            strategy: "hook-targeted",
            sourcePolling: false,
            failedAt: new Date().toISOString(),
            error: error instanceof Error ? error.message : String(error)
          }, paths)
        } finally {
          void cycle(true)
        }
      })()
    })
    void cycle(true)
    await shutdownRequested
  } finally {
    stopped = true
    configWatcher?.close()
    if (retryTimer) {
      clearTimeout(retryTimer)
      retryTimer = null
    }
    process.off("SIGINT", shutdown)
    process.off("SIGTERM", shutdown)
    server.stop(true)
    await Promise.resolve(active).catch(() => undefined)
  }
  await writeDaemonStatus({
    pid: process.pid,
    state: "stopped",
    strategy: "hook-targeted",
    sourcePolling: false,
    stoppedAt: new Date().toISOString()
  }, paths)
}

export const watchConfig = (
  path: string,
  onChange: () => void,
  reloadDelayMs = configReloadDelayMs
): ConfigWatcher => {
  let reloadTimer: ReturnType<typeof setTimeout> | null = null
  watchFile(path, { interval: Math.max(10, reloadDelayMs) }, (current, previous) => {
    if (
      current.mtimeMs === previous.mtimeMs &&
      current.ctimeMs === previous.ctimeMs &&
      current.size === previous.size
    ) return
    if (reloadTimer) clearTimeout(reloadTimer)
    reloadTimer = setTimeout(() => {
      reloadTimer = null
      onChange()
    }, reloadDelayMs)
  })
  return {
    close: () => {
      unwatchFile(path)
      if (reloadTimer) clearTimeout(reloadTimer)
    }
  }
}

export const drainPipelineRequests = async (
  collector: SessionCollector
): Promise<DaemonCycleResult["requests"]> => {
  let attempted = 0
  let processed = 0
  let failed = 0
  for (const request of collector.pipeline.dueRequests()) {
    attempted += 1
    try {
      await collector.processPath(request.provider, request.transcriptPath, {
        projectOrigin: request.projectOrigin,
        completed: request.reason === "SessionEnd"
      })
      collector.pipeline.acknowledgeRequest(request)
      processed += 1
    } catch (error) {
      if (isMissing(error)) {
        // Provider cleanup can win the race with this asynchronous hook request.
        // Retrying the same vanished path can never succeed; explicit sync remains
        // the recovery path for transcripts that providers archive elsewhere.
        collector.pipeline.acknowledgeRequest(request)
        processed += 1
      } else {
        collector.pipeline.failRequest(
          request,
          error instanceof Error ? error.message : String(error)
        )
        failed += 1
      }
    }
  }
  return { attempted, processed, failed }
}

export const drainBackfillRequests = async (
  collector: SessionCollector,
  destinations: readonly CollectorDestination[]
): Promise<{
  readonly backfills: DaemonCycleResult["backfills"]
  readonly scan: Awaited<ReturnType<SessionCollector["scan"]>> | null
}> => {
  const requests = collector.pipeline.dueBackfills()
  if (requests.length === 0) {
    return {
      backfills: { attempted: 0, processed: 0, failed: 0 },
      scan: null
    }
  }

  const providers = new Set(requests.map((request) => request.provider))
  const provider = providers.size === 1
    ? requests[0]?.provider ?? undefined
    : undefined
  let scan: Awaited<ReturnType<SessionCollector["scan"]>> | null = null
  try {
    scan = await collector.scan(provider)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    for (const request of requests) collector.pipeline.failBackfill(request, message)
    return {
      backfills: { attempted: requests.length, processed: 0, failed: requests.length },
      scan: null
    }
  }

  let processed = 0
  let failed = 0
  for (const request of requests) {
    try {
      const destination = destinations.find((candidate) =>
        candidate.exporter.destinationId === request.destinationId
      )
      if (!destination) {
        throw new Error(`Backfill destination ${request.destinationId} is unavailable.`)
      }
      collector.pipeline.prepareDestination(request.destinationId, {
        ...(request.provider ? { provider: request.provider } : {}),
        ...(destination.allows ? { allows: destination.allows } : {})
      })
      collector.pipeline.acknowledgeBackfill(request)
      processed += 1
    } catch (error) {
      collector.pipeline.failBackfill(
        request,
        error instanceof Error ? error.message : String(error)
      )
      failed += 1
    }
  }
  return {
    backfills: { attempted: requests.length, processed, failed },
    scan
  }
}

export const notifyDaemon = async (
  provider: Provider,
  paths = htPaths()
): Promise<boolean> => {
  const input = !process.stdin.isTTY
    ? await Bun.stdin.text().catch(() => "")
    : ""
  const payload = hookPayload(input)
  if (!payload.transcriptPath) return false
  const store = new LocalSessionStore(paths.database)
  try {
    const pipeline = new IncrementalPipelineStore(store.database)
    pipeline.recordHookObservation(
      provider,
      payload.reason,
      payload.nativeSessionId
    )
    pipeline.enqueueRequest({
      provider,
      transcriptPath: payload.transcriptPath,
      nativeSessionId: payload.nativeSessionId,
      projectOrigin: payload.projectOrigin,
      reason: payload.reason
    })
  } finally {
    store.close()
  }
  await wakeDaemon(undefined, paths)
  return true
}

export const wakeDaemon = async (
  _provider?: Provider,
  paths = htPaths()
): Promise<boolean> => {
  try {
    const port = await daemonPort(paths)
    if (!await daemonMatches(port, localDeviceId(paths))) return false
    const response = await fetch(`http://127.0.0.1:${port}/wake`, {
      method: "POST",
      signal: AbortSignal.timeout(1_000)
    })
    return response.ok
  } catch {
    return false
  }
}

export const daemonVersionMatches = async (
  paths = htPaths()
): Promise<boolean> => {
  try {
    const port = await daemonPort(paths)
    const response = await fetch(`http://127.0.0.1:${port}/healthz`, {
      signal: AbortSignal.timeout(1_000)
    })
    if (!response.ok) return false
    const health = await response.json() as {
      readonly deviceId?: unknown
      readonly version?: unknown
    }
    return health.deviceId === localDeviceId(paths) && health.version === cliVersion
  } catch {
    return false
  }
}

export const syncThroughDaemon = async (
  provider?: Provider,
  paths = htPaths()
): Promise<DaemonCycleResult | null> => {
  try {
    const port = await daemonPort(paths)
    if (!await daemonMatches(port, localDeviceId(paths))) return null
    const query = provider ? `?provider=${provider}` : ""
    const response = await fetch(`http://127.0.0.1:${port}/sync${query}`, {
      method: "POST",
      signal: AbortSignal.timeout(5 * 60_000)
    })
    if (!response.ok) return null
    return await response.json() as DaemonCycleResult
  } catch {
    return null
  }
}

export const requestDaemonSync = async (
  provider?: Provider,
  paths = htPaths()
): Promise<boolean> => {
  const port = await daemonPort(paths)
  const query = new URLSearchParams({ background: "true" })
  if (provider) query.set("provider", provider)
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      if (await daemonMatches(port, localDeviceId(paths))) {
        const response = await fetch(
          `http://127.0.0.1:${port}/sync?${query.toString()}`,
          {
            method: "POST",
            signal: AbortSignal.timeout(1_000)
          }
        )
        if (response.ok) return true
      }
    } catch {
      // A newly installed service may need a moment to bind its local port.
    }
    if (attempt < 4) await delay(100)
  }
  return false
}

const hookPayload = (input: string): {
  transcriptPath: string | null
  nativeSessionId: string | null
  projectOrigin: string | null
  reason: string
} => {
  let value: Record<string, unknown> = {}
  try {
    const candidate = JSON.parse(input) as unknown
    if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
      value = candidate as Record<string, unknown>
    }
  } catch {
    // An empty or malformed hook payload is reported by returning no path.
  }
  return {
    transcriptPath: firstString(
      value.transcript_path,
      value.transcriptPath,
      value.session_path,
      value.path
    ),
    nativeSessionId: firstString(value.session_id, value.sessionId),
    projectOrigin: firstString(value.cwd, value.project_dir, value.projectPath),
    reason: firstString(value.hook_event_name, value.event_name, value.event) ?? "hook"
  }
}

const firstString = (...values: unknown[]): string | null =>
  values.find((value): value is string =>
    typeof value === "string" && value.trim().length > 0
  )?.trim() ?? null

const providerValue = (value: string | null): Provider | undefined =>
  value === "codex" || value === "claude" ? value : undefined

const backfillCutoff = (window: "30d" | "90d" | "all"): number | null => {
  if (window === "all") return null
  const days = window === "90d" ? 90 : 30
  return Date.now() - days * 24 * 60 * 60 * 1_000
}

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds))

const localDeviceId = (paths: ReturnType<typeof htPaths>): string => {
  const store = new LocalSessionStore(paths.database)
  try {
    return store.deviceId()
  } finally {
    store.close()
  }
}

const daemonMatches = async (port: number, deviceId: string): Promise<boolean> => {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/healthz`, {
      signal: AbortSignal.timeout(1_000)
    })
    if (!response.ok) return false
    const health = await response.json() as { deviceId?: unknown }
    return health.deviceId === deviceId
  } catch {
    return false
  }
}

const isMissing = (error: unknown): boolean =>
  typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
