import { afterEach, describe, expect, test } from "bun:test"
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile
} from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

import { runCodexReview } from "../src/codex-review.ts"
import { htPaths } from "../src/config.ts"
import {
  installHT,
  restartHTDaemon,
  type DaemonRuntime
} from "../src/install.ts"
import { updateHT } from "../src/update.ts"

const temporary: string[] = []

afterEach(async () => {
  await Promise.all(temporary.splice(0).map((path) =>
    rm(path, { recursive: true, force: true })
  ))
})

describe("HT service lifecycle", () => {
  test("restarts a loaded daemon so it runs the installed CLI version", async () => {
    const directory = await temporaryDirectory("ht-daemon-restart-")
    const calls: string[][] = []
    const runtime: DaemonRuntime = {
      platform: "darwin",
      home: join(directory, "user"),
      uid: 500,
      command: async (args) => {
        calls.push([...args])
        return true
      }
    }

    expect(await restartHTDaemon(htPaths(join(directory, "ht")), runtime)).toBe(true)
    expect(calls).toEqual([
      ["launchctl", "kickstart", "-k", "gui/500/dev.ht.collector"]
    ])
  })

  test("does not rewrite or re-register an unchanged loaded LaunchAgent", async () => {
    const directory = await temporaryDirectory("ht-launch-agent-")
    const paths = htPaths(join(directory, "ht"))
    const serviceHome = join(directory, "user")
    const calls: string[][] = []
    let loaded = false
    const runtime: DaemonRuntime = {
      platform: "darwin",
      home: serviceHome,
      uid: 501,
      command: async (args) => {
        calls.push([...args])
        if (args[1] === "print") return loaded
        if (args[1] === "bootout") loaded = false
        if (args[1] === "bootstrap") loaded = true
        return true
      }
    }
    const options = {
      entrypoint: join(import.meta.dir, "..", "src", "entry.ts"),
      paths,
      codexHome: join(directory, "codex"),
      claudeHome: join(directory, "claude"),
      providers: [],
      daemonRuntime: runtime
    } as const

    const first = await installHT(options)
    const plist = first.daemon.path!
    const contents = await readFile(plist, "utf8")
    const modifiedAt = (await stat(plist, { bigint: true })).mtimeNs
    expect(calls.map((call) => call.slice(0, 2))).toEqual([
      ["launchctl", "print"],
      ["launchctl", "bootstrap"]
    ])
    expect(calls.flat()).not.toContain("kickstart")

    calls.length = 0
    await Bun.sleep(5)
    const second = await installHT(options)

    expect(second.daemon).toEqual(first.daemon)
    expect(calls).toEqual([
      ["launchctl", "print", "gui/501/dev.ht.collector"]
    ])
    expect(await readFile(plist, "utf8")).toBe(contents)
    expect((await stat(plist, { bigint: true })).mtimeNs).toBe(modifiedAt)

    loaded = false
    calls.length = 0
    await Bun.sleep(5)
    await installHT(options)
    expect(calls.map((call) => call.slice(0, 2))).toEqual([
      ["launchctl", "print"],
      ["launchctl", "bootstrap"]
    ])
    expect((await stat(plist, { bigint: true })).mtimeNs).toBe(modifiedAt)
  })

  test("re-registers a loaded LaunchAgent only when its configuration changed", async () => {
    const directory = await temporaryDirectory("ht-launch-agent-change-")
    const paths = htPaths(join(directory, "ht"))
    const serviceHome = join(directory, "user")
    const calls: string[][] = []
    let loaded = false
    const runtime: DaemonRuntime = {
      platform: "darwin",
      home: serviceHome,
      uid: 502,
      command: async (args) => {
        calls.push([...args])
        if (args[1] === "print") return loaded
        if (args[1] === "bootout") loaded = false
        if (args[1] === "bootstrap") loaded = true
        return true
      }
    }
    const options = {
      entrypoint: join(import.meta.dir, "..", "src", "entry.ts"),
      paths,
      codexHome: join(directory, "codex"),
      claudeHome: join(directory, "claude"),
      providers: [],
      daemonRuntime: runtime
    } as const
    const first = await installHT(options)
    await writeFile(first.daemon.path!, "stale configuration\n")
    calls.length = 0

    await installHT(options)

    expect(calls.map((call) => call.slice(0, 2))).toEqual([
      ["launchctl", "print"],
      ["launchctl", "bootout"],
      ["launchctl", "bootstrap"]
    ])
    expect(await readFile(first.daemon.path!, "utf8"))
      .toContain(`<string>${paths.launcher}</string><string>daemon</string>`)
  })

  test("resumes a paused daemon when update activation fails", async () => {
    const directory = await temporaryDirectory("ht-update-cleanup-")
    const paths = htPaths(directory)
    await mkdir(join(directory, "libexec", "ht"), { recursive: true })
    const binary = new TextEncoder().encode("replacement binary\n")
    const checksum = new Bun.CryptoHasher("sha256")
      .update(binary)
      .digest("hex")
    const calls: string[][] = []
    const runtime: DaemonRuntime = {
      platform: "darwin",
      home: join(directory, "user"),
      uid: 503,
      command: async (args) => {
        calls.push([...args])
        return true
      }
    }

    await expect(updateHT({
      currentVersion: "0.4.3",
      requestedVersion: "v9.8.7",
      paths,
      platform: "darwin",
      architecture: "arm64",
      daemonRuntime: runtime,
      fetch: (async (input) => String(input).endsWith("SHA256SUMS")
        ? new Response(`${checksum}  ht-darwin-arm64\n`)
        : new Response(binary)) as typeof fetch
    })).rejects.toThrow()

    expect(calls.map((call) => call.slice(0, 2))).toEqual([
      ["launchctl", "bootout"],
      ["launchctl", "bootstrap"]
    ])
  })

  test("terminates an owned interactive child when review fails", async () => {
    let exitCode: number | null = null
    let resolveExit: (code: number) => void = () => {}
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve
    })
    let kills = 0

    const result = await runCodexReview({
      projectOrigin: "/work/project",
      platform: "darwin",
      resolveExecutable: () => "/usr/bin/true",
      isVerified: () => {
        throw new Error("verification failed")
      },
      spawnChild: () => ({
        exited,
        get exitCode() {
          return exitCode
        },
        kill() {
          kills += 1
          exitCode = 143
          resolveExit(exitCode)
        }
      })
    })

    expect(result.reason).toBe("launch-failed")
    expect(kills).toBe(1)
    expect(await exited).toBe(143)
  })
})

const temporaryDirectory = async (prefix: string): Promise<string> => {
  const directory = await mkdtemp(join(tmpdir(), prefix))
  temporary.push(directory)
  return directory
}
