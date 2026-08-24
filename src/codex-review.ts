import { existsSync } from "node:fs"
import { homedir } from "node:os"
import { join } from "node:path"

import { terminateOwnedSubprocess } from "./owned-subprocess.ts"

export interface CodexReviewResult {
  readonly opened: boolean
  readonly verified: boolean
  readonly reason:
    | "verified"
    | "opening-disabled"
    | "unsupported-platform"
    | "codex-not-found"
    | "launch-failed"
    | "exited-before-verification"
  readonly executable: string | null
}

interface InteractiveReviewInput {
  readonly command: readonly string[]
  readonly isVerified: () => boolean
  readonly spawnChild?: SpawnInteractiveChild
}

interface InteractiveChild {
  readonly exited: Promise<number>
  readonly exitCode: number | null
  kill(signal?: number | NodeJS.Signals): void
}

type SpawnInteractiveChild = (
  command: readonly string[],
  terminal: Bun.Terminal
) => InteractiveChild

interface RunCodexReviewOptions {
  readonly projectOrigin: string
  readonly platform?: NodeJS.Platform
  readonly isVerified: () => boolean
  readonly resolveExecutable?: () => string | null
  readonly runInteractive?: (
    input: InteractiveReviewInput
  ) => Promise<{ readonly opened: boolean; readonly verified: boolean }>
  readonly spawnChild?: SpawnInteractiveChild
}

const verificationPrompt =
  "Reply exactly with this sentence: Habitat hook verification complete. When /exit appears, press Enter again to return to Habitat."

export const runCodexReview = async (
  options: RunCodexReviewOptions
): Promise<CodexReviewResult> => {
  const platform = options.platform ?? process.platform
  if (platform !== "darwin" && platform !== "linux") {
    return {
      opened: false,
      verified: false,
      reason: "unsupported-platform",
      executable: null
    }
  }
  const executable = (options.resolveExecutable ?? resolveCodexExecutable)()
  if (!executable) {
    return {
      opened: false,
      verified: false,
      reason: "codex-not-found",
      executable: null
    }
  }

  try {
    const review = await (options.runInteractive ?? runInteractiveReview)({
      command: codexReviewCommand(executable, options.projectOrigin),
      isVerified: options.isVerified,
      spawnChild: options.spawnChild
    })
    return {
      ...review,
      reason: review.verified
        ? "verified"
        : review.opened
          ? "exited-before-verification"
          : "launch-failed",
      executable
    }
  } catch {
    return {
      opened: false,
      verified: false,
      reason: "launch-failed",
      executable
    }
  }
}

export const codexReviewCommand = (
  executable: string,
  projectOrigin: string
): readonly string[] => [
  executable,
  "-C",
  projectOrigin,
  verificationPrompt
]

export const resolveCodexExecutable = (options: {
  readonly which?: (command: string) => string | null
  readonly exists?: (path: string) => boolean
  readonly platform?: NodeJS.Platform
  readonly home?: string
  readonly environmentCandidate?: string
} = {}): string | null => {
  const which = options.which ?? ((command: string) => Bun.which(command))
  const exists = options.exists ?? existsSync
  const platform = options.platform ?? process.platform
  const home = options.home ?? homedir()
  const environmentCandidate = options.environmentCandidate ??
    process.env.HT_CODEX_CLI
  const candidates = [
    environmentCandidate,
    which("codex"),
    ...(platform === "darwin"
      ? [
          "/Applications/ChatGPT.app/Contents/Resources/codex",
          join(home, "Applications", "ChatGPT.app", "Contents", "Resources", "codex"),
          "/Applications/Codex.app/Contents/Resources/codex",
          join(home, "Applications", "Codex.app", "Contents", "Resources", "codex")
        ]
      : [])
  ]
  return candidates.find((candidate): candidate is string =>
    typeof candidate === "string" &&
    candidate.length > 0 &&
    exists(candidate)
  ) ?? null
}

const runInteractiveReview = async (
  input: InteractiveReviewInput
): Promise<{ readonly opened: boolean; readonly verified: boolean }> => {
  const output = process.stdout
  const keyboard = process.stdin
  const columns = output.columns ?? 120
  const rows = output.rows ?? 32
  let opened = false
  let exited = false
  let child: InteractiveChild | null = null
  const terminal = new Bun.Terminal({
    cols: columns,
    rows,
    data: (_terminal, data) => {
      output.write(data)
    }
  })
  const previousRaw = keyboard.isRaw
  const forwardInput = (data: Buffer): void => {
    terminal.write(data)
  }
  const resize = (): void => {
    terminal.resize(output.columns ?? 120, output.rows ?? 32)
  }

  try {
    if (keyboard.isTTY) {
      keyboard.setRawMode?.(true)
      keyboard.resume()
      keyboard.on("data", forwardInput)
    }
    output.on("resize", resize)
    const spawned = input.spawnChild
      ? input.spawnChild(input.command, terminal)
      : Bun.spawn([...input.command], { terminal })
    child = spawned
    opened = true
    void spawned.exited.then(() => {
      exited = true
    }, () => {
      exited = true
    })

    while (!exited) {
      if (input.isVerified()) {
        await delay(500)
        terminal.write("/exit\r")
        await Promise.race([
          spawned.exited,
          delay(5_000).then(() => {
            if (!exited) spawned.kill()
          })
        ])
        return { opened: true, verified: true }
      }
      await delay(250)
    }
    await spawned.exited
    return { opened: true, verified: input.isVerified() }
  } finally {
    if (child?.exitCode === null) {
      await terminateOwnedSubprocess(child)
    }
    output.off("resize", resize)
    keyboard.off("data", forwardInput)
    if (keyboard.isTTY) {
      keyboard.setRawMode?.(previousRaw === true)
      keyboard.pause()
    }
    terminal.close()
  }
}

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds))
