import type { Writable } from "node:stream"

import {
  intro as clackIntro,
  log,
  note,
  outro as clackOutro,
  spinner as clackSpinner,
  type SpinnerResult
} from "@clack/prompts"

import type { HabitatLogoutResult } from "../config.ts"
import type { InstallResult } from "../install.ts"
import type { PreflightReport } from "../preflight.ts"
import type { SetupResult } from "../setup.ts"
import type { UpdateResult } from "../update.ts"

type JsonObject = Record<string, unknown>

export interface CliFailure {
  readonly code: string
  readonly message: string
  readonly hint?: string
}

export class CliDisplay {
  constructor(
    readonly jsonRequested: boolean,
    readonly stdout: Writable = process.stdout,
    readonly stderr: Writable = process.stderr
  ) {}

  writeOut = (value: string): void => {
    this.stdout.write(value)
  }

  writeErr = (value: string): void => {
    this.stderr.write(value)
  }

  printJson(value: unknown): void {
    this.writeOut(`${JSON.stringify(value, null, 2)}\n`)
  }

  intro(message: string): void {
    if (!this.jsonRequested) clackIntro(message, { output: this.stdout })
  }

  error(failure: CliFailure): void {
    log.error(`[${failure.code}] ${failure.message}`, { output: this.stderr })
    if (failure.hint) log.info(failure.hint, { output: this.stderr })
  }

  commanderError(message: string): void {
    const normalized = message.replace(/^error:\s*/i, "").trim()
    log.error(normalized || "Invalid command arguments.", { output: this.stderr })
  }

  version(version: string): void {
    log.info(`ht ${version}`, { output: this.stdout })
  }

  updateStarted(): SpinnerResult | null {
    if (this.jsonRequested) return null
    const spinner = clackSpinner({ output: this.stderr })
    spinner.start("Checking for HT updates")
    return spinner
  }

  update(result: UpdateResult, spinner: SpinnerResult | null): void {
    if (!result.updated) {
      spinner?.stop(`HT ${result.version} is already up to date`)
      if (!spinner) log.success(`HT ${result.version} is already up to date.`, { output: this.stdout })
      return
    }
    spinner?.stop(`Updated HT ${result.currentVersion} → ${result.version}`)
    if (!spinner) {
      log.success(`Updated HT ${result.currentVersion} → ${result.version}.`, {
        output: this.stdout
      })
    }
    note(`Binary    ${result.binary}\nChecksum  verified`, "Release", {
      output: this.stdout
    })
    if (result.daemonRestarted === true) {
      log.success("Background daemon restarted.", { output: this.stdout })
    } else if (result.daemonRestarted === false) {
      log.warn("The binary was updated, but the background daemon could not restart.", {
        output: this.stdout
      })
      log.info("Run `ht install` to repair the background service.", {
        output: this.stdout
      })
    }
  }

  setup(result: SetupResult, configuredOnly: boolean): void {
    const needsAttention = result.backfill.failed > 0 ||
      result.backfill.quarantined > 0 ||
      result.actionsRequired.length > 0
    const message = needsAttention
      ? result.actionsRequired.length > 0 &&
          result.backfill.failed === 0 &&
          result.backfill.quarantined === 0
        ? "Habitat setup needs one final action."
        : "Habitat setup needs attention."
      : configuredOnly
        ? "Habitat configuration updated."
        : "Habitat is ready."
    if (needsAttention) log.warn(message, { output: this.stdout })
    else log.success(message, { output: this.stdout })

    note([
      row("Config", result.configPath),
      row("Workspace", `${result.workspace.name} (${result.workspace.slug})`),
      row("Sessions", result.workspaceUrl),
      row("Harnesses", result.harnesses.map((harness) =>
        harness.provider === "codex" ? "Codex" : "Claude Code"
      ).join(", ")),
      row("Projects", String(result.projects.length)),
      row("Backfill", backfillLabel(result.backfillWindow))
    ].join("\n"), "Configuration", { output: this.stdout })

    if (result.backfill.scheduled) {
      log.info(result.backfill.notified
        ? "Historical sessions are reconciling in the background."
        : "Historical ingestion is saved in the config and will reconcile when the background service starts.", {
        output: this.stdout
      })
    } else if (result.backfill.quarantined > 0) {
      log.error(`${result.backfill.quarantined} upload(s) failed permanently and were quarantined.`, {
        output: this.stdout
      })
      log.info("Run `ht doctor`; after correcting the error, run `ht backfill --retry-quarantined`.", {
        output: this.stdout
      })
    } else if (result.backfill.failed > 0 || result.backfill.retrying > 0) {
      log.warn(`${Math.max(result.backfill.failed, result.backfill.retrying)} upload(s) failed and will retry.`, {
        output: this.stdout
      })
      log.info("Run `ht status` to inspect retry progress.", { output: this.stdout })
    } else if (result.backfill.remaining > 0) {
      log.info(`${result.backfill.remaining} batch(es) remain queued; run \`ht status\` for details.`, {
        output: this.stdout
      })
    } else {
      log.warn("Historical backfill was not scheduled because the background service is unavailable.", {
        output: this.stdout
      })
    }
    if (result.harnesses.some((harness) =>
      harness.provider === "codex" && harness.state === "active"
    )) {
      log.success("Live ingestion: Codex verified.", { output: this.stdout })
    }
    for (const action of result.actionsRequired) {
      log.warn(action, { output: this.stdout })
    }
    for (const noticeMessage of result.installation?.notices ?? []) {
      if (!noticeMessage.startsWith("Codex requires")) {
        log.info(noticeMessage, { output: this.stdout })
      }
    }
    clackOutro("Run `ht status` any time to inspect ingestion.", {
      output: this.stdout
    })
  }

  preflight(report: PreflightReport): void {
    if (report.ready) log.success("This machine is ready for HT.", { output: this.stdout })
    else log.warn("This machine needs attention before HT setup.", { output: this.stdout })
    note([
      row("Platform", `${report.platform.os} ${report.platform.architecture}`),
      row("Service", report.platform.serviceManager),
      row("Installed", yesNo(report.installation.installed)),
      row("State directory", report.installation.stateDirectory),
      row("State writable", yesNo(report.installation.stateDirectoryWritable)),
      row("Codex", detected(report.providers.codex.detected, report.providers.codex.path)),
      row("Claude Code", detected(report.providers.claude.detected, report.providers.claude.path))
    ].join("\n"), "Preflight", { output: this.stdout })
    for (const message of report.notices) log.warn(message, { output: this.stdout })
  }

  login(input: {
    readonly apiUrl: string
    readonly workspace?: { readonly name?: string; readonly slug?: string }
    readonly principal?: { readonly name?: string }
  }): void {
    log.success("Logged in to Habitat.", { output: this.stdout })
    note([
      row("Workspace", input.workspace?.name ?? input.workspace?.slug ?? "unknown"),
      row("Device", input.principal?.name ?? "unknown"),
      row("API", input.apiUrl)
    ].join("\n"), "Account", { output: this.stdout })
  }

  loginVerification(message: string): void {
    log.info(message, { output: this.stderr })
  }

  logout(result: HabitatLogoutResult): void {
    const names = result.workspaces.map((workspace) => workspace.name).join(", ")
    log.success(`Logged out${names ? ` of ${names}` : ""}.`, { output: this.stdout })
    log.info(`${result.remainingWorkspaces} workspace${result.remainingWorkspaces === 1 ? "" : "s"} remain configured.`, {
      output: this.stdout
    })
  }

  install(result: InstallResult): void {
    const daemonReady = result.daemon.active || result.daemon.manager === "manual"
    if (daemonReady) log.success("HT capture is installed.", { output: this.stdout })
    else log.warn("HT capture was installed, but the background daemon is inactive.", {
      output: this.stdout
    })
    note([
      row("Launcher", result.launcher),
      row("Providers", result.providers.join(", ") || "none"),
      row("Daemon", `${result.daemon.manager} · ${result.daemon.active ? "active" : "inactive"}`),
      row("Codex hooks", result.codexHooks),
      row("Claude settings", result.claudeSettings)
    ].join("\n"), "Installation", { output: this.stdout })
    for (const message of result.notices) log.warn(message, { output: this.stdout })
  }

  configPath(path: string): void {
    note(path, "HT configuration", { output: this.stdout })
  }

  configCheck(summary: JsonObject): void {
    const projectOrigins = object(summary.projectOrigins)
    log.success("The HT configuration is valid.", { output: this.stdout })
    note([
      row("Path", text(summary.configPath, "unknown")),
      row("Configured", yesNo(summary.configured === true)),
      row("Default", text(projectOrigins.default, "include")),
      row("Include", list(projectOrigins.include, "all projects")),
      row("Exclude", list(projectOrigins.exclude, "none"))
    ].join("\n"), "Configuration", { output: this.stdout })
  }

  uninstall(): void {
    log.success("HT hooks, launcher, and background service were removed.", {
      output: this.stdout
    })
    log.info("Local session data and Habitat credentials were preserved.", {
      output: this.stdout
    })
  }

  sync(result: JsonObject, upload: boolean): void {
    const scan = object(result.scan)
    const flush = object(result.flush)
    const requests = object(result.requests)
    const failed = count(flush.failed) + count(requests.failed)
    if (failed > 0) log.warn("Synchronization completed with failures.", { output: this.stdout })
    else log.success(upload ? "Sessions synchronized with Habitat." : "Local sessions synchronized.", {
      output: this.stdout
    })
    note([
      row("Scanned", nullableCount(scan.scanned)),
      row("Captured", nullableCount(scan.captured)),
      row("Hooks processed", nullableCount(requests.processed)),
      row("Uploaded", String(count(flush.exported))),
      row("Withheld", String(count(flush.withheld))),
      row("Failed", String(failed)),
      row("Remaining", String(count(flush.remaining)))
    ].join("\n"), "Synchronization", { output: this.stdout })
    if (typeof result.exporterConfigurationError === "string") {
      log.warn(result.exporterConfigurationError, { output: this.stdout })
    }
  }

  backfill(result: JsonObject): void {
    const preview = object(result.preview)
    if (result.dryRun === true) {
      log.success("Backfill preview complete.", { output: this.stdout })
      note([
        row("Discovered", String(count(preview.discovered))),
        row("Eligible", String(count(preview.eligible))),
        row("Withheld", String(count(preview.withheld))),
        row("Delivered", String(count(preview.alreadyDelivered))),
        row("Pending", String(count(preview.pending))),
        row("Quarantined", String(count(preview.quarantined)))
      ].join("\n"), "Preview", { output: this.stdout })
    } else {
      const flush = object(result.flush)
      const delivery = object(result.delivery)
      if (result.complete === true) log.success("Historical sessions are up to date.", { output: this.stdout })
      else log.warn("Backfill completed with sessions still pending.", { output: this.stdout })
      note([
        row("Eligible", String(count(preview.eligible))),
        row("Queued", String(count(result.queued))),
        row("Uploaded", String(count(flush.exported))),
        row("Failed", String(count(flush.failed))),
        row("Pending", String(count(delivery.pending))),
        row("Quarantined", String(count(delivery.quarantined)))
      ].join("\n"), "Backfill", { output: this.stdout })
    }
    if (typeof result.next === "string") log.info(result.next, { output: this.stdout })
  }

  sessionsList(sessions: readonly JsonObject[]): void {
    if (sessions.length === 0) {
      log.info("No captured sessions found.", { output: this.stdout })
      return
    }
    log.success(`${sessions.length} captured session${sessions.length === 1 ? "" : "s"}.`, {
      output: this.stdout
    })
    for (const session of sessions) this.sessionSummary(session)
  }

  sessionShow(session: JsonObject, includeContent: boolean): void {
    this.sessionSummary(session)
    const events = Array.isArray(session.events) ? session.events.map(object) : []
    if (events.length === 0) return
    log.info(`${events.length} event${events.length === 1 ? "" : "s"}`, { output: this.stdout })
    for (const event of events) {
      const heading = [text(event.kind, "event"), text(event.toolName, "")]
        .filter(Boolean)
        .join(" · ")
      const content = includeContent && typeof event.text === "string" && event.text.length > 0
        ? event.text
        : text(event.occurredAt, "")
      log.message(content ? `${heading}\n${content}` : heading, { output: this.stdout })
    }
  }

  sessionSearch(matches: readonly JsonObject[], query: string): void {
    if (matches.length === 0) {
      log.info(`No sessions matched “${query}”.`, { output: this.stdout })
      return
    }
    log.success(`${matches.length} session${matches.length === 1 ? "" : "s"} matched “${query}”.`, {
      output: this.stdout
    })
    for (const match of matches) {
      this.sessionSummary(match, text(match.match, ""))
    }
  }

  status(report: JsonObject, verbose: boolean): void {
    const overall = report.overall
    const local = object(report.local)
    const pipeline = object(local.pipeline)
    const daemon = object(report.daemon)
    const exporter = object(report.exporter)
    const delivery = object(exporter.delivery)
    const workspaces = Array.isArray(report.workspaces)
      ? report.workspaces.map(object)
      : []
    const projectCount = workspaces.reduce(
      (total, workspace) => total + (Array.isArray(workspace.projects) ? workspace.projects.length : 0),
      0
    )
    const pending = count(delivery.pending)
    const retrying = count(delivery.retrying)
    const quarantined = count(delivery.quarantined)
    const nextRetryAt = text(delivery.nextRetryAt, "")
    const queued = Math.max(0, pending - retrying)
    const queue = pending === 0 && retrying === 0 && quarantined === 0
      ? "Empty"
      : [
          ...(queued > 0 ? [`${queued} pending`] : []),
          ...(retrying > 0 ? [
            `${retrying} retrying${nextRetryAt ? ` · next ${relativeUntil(nextRetryAt)}` : ""}`
          ] : []),
          ...(quarantined > 0 ? [`${quarantined} failed`] : [])
        ].join(" · ")

    clackIntro("Habitat status", { output: this.stdout })
    if (overall === "healthy") log.success("Everything is working.", { output: this.stdout })
    else if (overall === "syncing") log.warn("Ingestion is syncing.", { output: this.stdout })
    else log.error("Ingestion needs attention.", { output: this.stdout })

    const overview = [
      row("Daemon", daemon.reachable === true
        ? `Running · ${daemon.strategy === "hook-targeted" ? "hook-triggered" : text(daemon.strategy, "active")}`
        : "Not reachable"),
      row("Last cycle", relativeTime(daemon.completedAt ?? daemon.failedAt))
    ]
    const harnesses = Array.isArray(report.harnesses) ? report.harnesses.map(object) : []
    for (const harness of harnesses) {
      const provider = harness.provider === "codex" ? "Codex" : "Claude Code"
      const state = harness.state === "active"
        ? `Active · last hook ${relativeTime(harness.lastHookReceivedAt)}`
        : harness.state === "verification-required"
          ? "Verification required"
          : "Ready · waiting for first session"
      overview.push(row(provider, state))
    }
    overview.push(
      row("Projects", `${projectCount} selected`),
      row("Sessions", `${count(local.sessions)} discovered`),
      row("Uploads", `${count(delivery.delivered)} delivered`),
      row("Queue", queue),
      row("Backfill", `${backfillLabel(report.backfillWindow)}${
        count(pipeline.backfills) > 0
          ? ` · ${count(pipeline.backfills)} scheduled`
          : ""
      }`)
    )
    note(overview.join("\n"), "Overview", { output: this.stdout })

    for (const workspace of workspaces) {
      const workspaceDelivery = object(workspace.delivery)
      const workspaceBackfill = object(workspace.backfill)
      const projects = Array.isArray(workspace.projects) ? workspace.projects.length : 0
      const workspacePending = Math.max(
        0,
        count(workspaceDelivery.pending) - count(workspaceDelivery.retrying)
      )
      const details = [
        row("Status", workspace.connected === true ? "Connected" : "Disconnected"),
        row("Projects", String(projects)),
        row("Backfill", backfillStatusLabel(workspaceBackfill, workspaceDelivery)),
        row("Delivered", String(count(workspaceDelivery.delivered))),
        row("Pending", String(workspacePending)),
        row("Retrying", String(count(workspaceDelivery.retrying))),
        row("Failed", String(count(workspaceDelivery.quarantined)))
      ]
      if (verbose) {
        details.push(
          row("API", text(workspace.destination, "unknown")),
          row("Destination", text(workspace.destinationId, "unknown"))
        )
      }
      note(
        details.join("\n"),
        `${workspace.connected === true ? "✓" : "✗"} ${text(workspace.name, "Workspace")}` +
          `${workspace.active === true ? " (active)" : ""}`,
        { output: this.stdout }
      )
      if (verbose) this.deliveryIssues(workspaceDelivery)
    }

    const issues = statusIssues(report)
    for (const issue of issues) log.warn(issue, { output: this.stdout })
    if (verbose) {
      note([
        row("State directory", text(local.home, "unknown")),
        row("Device", text(local.deviceId, "unknown")),
        row("Daemon port", String(daemon.port ?? "unknown")),
        row("Sources", String(count(pipeline.sources))),
        row("Finalized", String(count(pipeline.finalizedSources))),
        row("Hook requests", String(count(pipeline.requests))),
        row("Backfill jobs", String(count(pipeline.backfills)))
      ].join("\n"), "Local details", { output: this.stdout })
    }
    clackOutro(
      issues.length > 0
        ? report.configured !== true
          ? "Run `ht setup` to finish configuration."
          : quarantined > 0
            ? "Run `ht doctor`, then retry quarantined backfill after remediation."
            : "Run `ht doctor` for repair guidance."
        : "All systems operational.",
      { output: this.stdout }
    )
  }

  doctor(input: {
    readonly healthy: boolean
    readonly checks: JsonObject
    readonly preflight: PreflightReport
    readonly status: JsonObject
    readonly hints: readonly string[]
  }): void {
    clackIntro("HT doctor", { output: this.stdout })
    if (input.healthy) log.success("All diagnostics passed.", { output: this.stdout })
    else log.error("One or more diagnostics need attention.", { output: this.stdout })
    note(Object.entries(input.checks).map(([name, passed]) =>
      `${passed === true ? "✓" : "✗"} ${humanize(name)}`
    ).join("\n"), "Checks", { output: this.stdout })
    for (const message of [...input.preflight.notices, ...statusIssues(input.status)]) {
      log.warn(message, { output: this.stdout })
    }
    for (const hint of input.hints) log.info(hint, { output: this.stdout })
    clackOutro(input.healthy ? "HT is healthy." : "Apply the guidance above, then run `ht doctor` again.", {
      output: this.stdout
    })
  }

  private sessionSummary(session: JsonObject, match = ""): void {
    const usage = object(session.usage)
    note([
      row("ID", text(session.id, "unknown")),
      row("Provider", text(session.provider, "unknown")),
      row("Project", text(session.project, "unknown")),
      row("Status", text(session.status, "unknown")),
      row("Updated", relativeTime(session.updatedAt)),
      row("Events", String(Array.isArray(session.events)
        ? session.events.length
        : count(session.events))),
      row("Tool calls", String(count(session.toolCalls))),
      row("Tokens", `${count(usage.inputTokens)} in · ${count(usage.outputTokens)} out`),
      ...(match ? [row("Match", match)] : [])
    ].join("\n"), text(session.title, "Untitled session"), { output: this.stdout })
  }

  private deliveryIssues(delivery: JsonObject): void {
    const issues = Array.isArray(delivery.issues) ? delivery.issues.map(object) : []
    const grouped = new Map<string, { state: string; error: string; count: number; next: string | null }>()
    for (const issue of issues) {
      const state = text(issue.state, "issue")
      const error = text(issue.lastError, "Unknown delivery error")
      const key = `${state}\n${error}`
      const current = grouped.get(key)
      grouped.set(key, {
        state,
        error,
        count: (current?.count ?? 0) + 1,
        next: earliestTimestamp(
          current?.next ?? null,
          typeof issue.nextRetryAt === "string" ? issue.nextRetryAt : null
        )
      })
    }
    for (const issue of grouped.values()) {
      log.warn(
        `${issue.state}${issue.count > 1 ? ` (${issue.count})` : ""}: ${issue.error}` +
          `${issue.next ? ` · next ${relativeUntil(issue.next)}` : ""}`,
        { output: this.stdout }
      )
    }
  }
}

const row = (label: string, value: string): string => `${label.padEnd(17)}${value}`

const object = (value: unknown): JsonObject =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as JsonObject
    : {}

const count = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0

const nullableCount = (value: unknown): string =>
  typeof value === "number" && Number.isFinite(value) ? String(value) : "not requested"

const text = (value: unknown, fallback: string): string =>
  typeof value === "string" && value.length > 0 ? value : fallback

const yesNo = (value: boolean): string => value ? "yes" : "no"

const detected = (value: boolean, path: string): string =>
  value ? `detected · ${path}` : `not detected · ${path}`

const list = (value: unknown, fallback: string): string =>
  Array.isArray(value) && value.length > 0 ? value.map(String).join(", ") : fallback

const backfillLabel = (value: unknown): string => {
  if (value === "30d") return "Last 30 days"
  if (value === "90d") return "Last 90 days"
  if (value === "all") return "All time"
  return "Not configured"
}

const backfillStatusLabel = (backfill: JsonObject, delivery: JsonObject): string => {
  const state = text(backfill.state, "not-scheduled")
  if (state === "pending") return "Scheduled"
  if (state === "retrying") {
    const next = text(backfill.nextRetryAt, "")
    return `Retrying${next ? ` · next ${relativeUntil(next)}` : ""}`
  }
  if (state === "complete") {
    if (count(delivery.pending) > 0 || count(delivery.retrying) > 0) return "Uploading"
    const completedAt = text(backfill.completedAt, "")
    return `Complete${completedAt ? ` · ${relativeTime(completedAt)}` : ""}`
  }
  return "Not scheduled"
}

const relativeTime = (value: unknown): string => {
  if (typeof value !== "string") return "Never"
  const elapsed = Date.now() - new Date(value).getTime()
  if (!Number.isFinite(elapsed)) return value
  if (elapsed < 0) return "Just now"
  const seconds = Math.floor(elapsed / 1_000)
  if (seconds < 5) return "Just now"
  if (seconds < 60) return `${seconds} seconds ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? "" : "s"} ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`
  const days = Math.floor(hours / 24)
  return `${days} day${days === 1 ? "" : "s"} ago`
}

const relativeUntil = (value: string): string => {
  const milliseconds = new Date(value).getTime() - Date.now()
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "now"
  const seconds = Math.ceil(milliseconds / 1_000)
  if (seconds < 60) return `in ${seconds} seconds`
  const minutes = Math.ceil(seconds / 60)
  if (minutes < 60) return `in ${minutes} minute${minutes === 1 ? "" : "s"}`
  const hours = Math.ceil(minutes / 60)
  return `in ${hours} hour${hours === 1 ? "" : "s"}`
}

const earliestTimestamp = (first: string | null, second: string | null): string | null => {
  if (!first) return second
  if (!second) return first
  return new Date(first).getTime() <= new Date(second).getTime() ? first : second
}

const statusIssues = (report: JsonObject): string[] => {
  const daemon = object(report.daemon)
  const exporter = object(report.exporter)
  const delivery = object(exporter.delivery)
  const workspaces = Array.isArray(report.workspaces) ? report.workspaces.map(object) : []
  const harnesses = Array.isArray(report.harnesses) ? report.harnesses.map(object) : []
  return [
    ...(report.configured === true ? [] : ["No project is configured for ingestion."]),
    ...(daemon.reachable === true ? [] : ["The background daemon is not reachable."]),
    ...(daemon.state === "degraded"
      ? [`The daemon is degraded${
          typeof (daemon.error ?? daemon.exporterError) === "string"
            ? `: ${String(daemon.error ?? daemon.exporterError)}`
            : "."
        }`]
      : []),
    ...harnesses
      .filter((harness) => harness.state === "verification-required")
      .map(() =>
        "Codex has not delivered a verification hook. Run `ht setup` to reopen the review, or open Codex and run `/hooks`."
      ),
    ...(object(report.uploadPolicy).valid === true
      ? []
      : [`The upload policy is invalid: ${text(object(report.uploadPolicy).error, "check the configuration")}`]),
    ...workspaces
      .filter((workspace) => workspace.connected !== true)
      .map((workspace) =>
        `${text(workspace.name, "A workspace")} is disconnected: ` +
          text(workspace.error, "authentication or network check failed")
      ),
    ...(count(delivery.quarantined) > 0
      ? [`${count(delivery.quarantined)} upload${count(delivery.quarantined) === 1 ? "" : "s"} permanently failed and are quarantined.`]
      : [])
  ]
}

const humanize = (value: string): string => value
  .replace(/([a-z])([A-Z])/g, "$1 $2")
  .replace(/^./, (character) => character.toUpperCase())
