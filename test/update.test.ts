import { afterEach, describe, expect, test } from "bun:test"
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile
} from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Writable } from "node:stream"

import { runCli } from "../src/cli.ts"
import { htPaths } from "../src/config.ts"
import { updateHT, type UpdateOptions } from "../src/update.ts"

const temporary: string[] = []

afterEach(async () => {
  await Promise.all(
    temporary.splice(0).map((path) =>
      rm(path, { recursive: true, force: true })
    )
  )
})

describe("ht update", () => {
  test("downloads, verifies, and activates the latest platform release", async () => {
    const home = await installedHome("old ht\n")
    const asset = platformAsset()
    const nextBinary = new TextEncoder().encode("new ht\n")
    const checksum = new Bun.CryptoHasher("sha256")
      .update(nextBinary)
      .digest("hex")
    const requests: string[] = []
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const pathname = new URL(request.url).pathname
        requests.push(pathname)
        if (pathname === "/cli/latest") return new Response("v9.8.7\n")
        if (pathname === `/cli/v9.8.7/${asset}`) {
          return new Response(nextBinary)
        }
        if (pathname === "/cli/v9.8.7/SHA256SUMS") {
          return new Response(`${checksum}  ${asset}\n`)
        }
        return new Response("missing", { status: 404 })
      }
    })

    try {
      const result = await updateHT({
        currentVersion: "0.4.3",
        paths: htPaths(home),
        releaseBaseUrl: new URL("cli/", server.url).toString(),
        restartDaemon: false
      })

      expect(result).toEqual({
        updated: true,
        currentVersion: "v0.4.3",
        version: "v9.8.7",
        binary: join(home, "libexec", "ht"),
        checksumVerified: true,
        daemonRestarted: null
      })
      expect(await readFile(result.binary, "utf8")).toBe("new ht\n")
      expect(requests).toEqual([
        "/cli/latest",
        `/cli/v9.8.7/${asset}`,
        "/cli/v9.8.7/SHA256SUMS"
      ])
    } finally {
      server.stop(true)
    }
  })

  test("does not redownload an already-current release without --force", async () => {
    const home = await installedHome("current ht\n")
    const requests: string[] = []
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        requests.push(new URL(request.url).pathname)
        return new Response("v0.4.3\n")
      }
    })

    try {
      const result = await updateHT({
        currentVersion: "0.4.3",
        paths: htPaths(home),
        releaseBaseUrl: new URL("cli/", server.url).toString(),
        restartDaemon: false
      })

      expect(result).toMatchObject({
        updated: false,
        currentVersion: "v0.4.3",
        version: "v0.4.3",
        checksumVerified: false
      })
      expect(await readFile(result.binary, "utf8")).toBe("current ht\n")
      expect(requests).toEqual(["/cli/latest"])
    } finally {
      server.stop(true)
    }
  })

  test("preserves the installed binary when checksum verification fails", async () => {
    const home = await installedHome("trusted ht\n")
    const asset = platformAsset()
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const pathname = new URL(request.url).pathname
        if (pathname === "/cli/latest") return new Response("v9.8.7\n")
        if (pathname === `/cli/v9.8.7/${asset}`) {
          return new Response("untrusted ht\n")
        }
        if (pathname === "/cli/v9.8.7/SHA256SUMS") {
          return new Response(`${"0".repeat(64)}  ${asset}\n`)
        }
        return new Response("missing", { status: 404 })
      }
    })

    try {
      await expect(updateHT({
        currentVersion: "0.4.3",
        paths: htPaths(home),
        releaseBaseUrl: new URL("cli/", server.url).toString(),
        restartDaemon: false
      })).rejects.toThrow("Checksum verification failed")
      expect(await readFile(join(home, "libexec", "ht"), "utf8"))
        .toBe("trusted ht\n")
    } finally {
      server.stop(true)
    }
  })

  test("exposes pinned and forced updates through Commander", async () => {
    let received: UpdateOptions | undefined
    let output = ""
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        output += String(chunk)
        callback()
      }
    })
    expect(await runCli(
      ["update", "--to", "v9.8.7", "--force", "--json"],
      {
        stdout,
        update: async (options) => {
          received = options
          return {
            updated: true,
            currentVersion: "v0.4.7",
            version: "v9.8.7",
            binary: "/tmp/ht",
            checksumVerified: true,
            daemonRestarted: true
          }
        }
      }
    )).toBe(0)
    expect(received).toMatchObject({
      currentVersion: "0.4.7",
      requestedVersion: "v9.8.7",
      force: true
    })
    expect(JSON.parse(output)).toMatchObject({
      updated: true,
      version: "v9.8.7",
      checksumVerified: true,
      daemonRestarted: true
    })
  })
})

const installedHome = async (contents: string): Promise<string> => {
  const home = await mkdtemp(join(tmpdir(), "ht-update-"))
  temporary.push(home)
  await mkdir(join(home, "libexec"), { recursive: true })
  await writeFile(join(home, "libexec", "ht"), contents, { mode: 0o755 })
  return home
}

const platformAsset = (): string => {
  const operatingSystem = process.platform === "darwin" ? "darwin" : "linux"
  const architecture = process.arch === "arm64" ? "arm64" : "x64"
  return `ht-${operatingSystem}-${architecture}`
}
