import { afterEach, describe, expect, test } from "bun:test"
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const temporaryDirectories: string[] = []
const isolatedPath = "/usr/bin:/bin:/usr/sbin:/sbin"

const temporaryDirectory = (prefix: string): string => {
  const directory = mkdtempSync(join(tmpdir(), prefix))
  temporaryDirectories.push(directory)
  return directory
}

const commandOutput = async (command: string[], environment: Record<string, string>) => {
  const process = Bun.spawn(command, {
    env: environment,
    stdout: "pipe",
    stderr: "pipe"
  })
  const [status, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text()
  ])
  return { status, stdout, stderr }
}

afterEach(() => {
  while (temporaryDirectories.length > 0) {
    const directory = temporaryDirectories.pop()
    if (directory) rmSync(directory, { recursive: true, force: true })
  }
})

describe("public HT installer", () => {
  test("resolves latest, verifies its checksum, and installs a runnable launcher", async () => {
    const releaseDirectory = temporaryDirectory("habitat-cli-release-")
    const homeDirectory = temporaryDirectory("habitat-cli-home-")
    const installer = join(import.meta.dir, "..", "install.sh")
    const operatingSystem = process.platform === "darwin" ? "darwin" : "linux"
    const architecture = process.arch === "arm64" ? "arm64" : "x64"
    const asset = `ht-${operatingSystem}-${architecture}`
    const binary = join(releaseDirectory, asset)
    writeFileSync(binary, "#!/bin/sh\nprintf 'test ht %s\\n' \"$*\"\n")
    chmodSync(binary, 0o755)

    const checksum = new Bun.CryptoHasher("sha256").update(await Bun.file(binary).arrayBuffer()).digest("hex")
    const checksums = `${checksum}  ${asset}\n`
    const requests: string[] = []
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const pathname = new URL(request.url).pathname
        requests.push(pathname)
        if (pathname === "/cli/latest") return new Response("v9.8.7\n")
        if (pathname === `/cli/v9.8.7/${asset}`) return new Response(Bun.file(binary))
        if (pathname === "/cli/v9.8.7/SHA256SUMS") return new Response(checksums)
        return new Response("missing", { status: 404 })
      }
    })

    try {
      const installation = await commandOutput(["sh", installer], {
        HOME: homeDirectory,
        HT_HOME: join(homeDirectory, ".ht"),
        HT_RELEASE_BASE_URL: new URL("cli", server.url).toString().replace(/\/$/, ""),
        NO_PROXY: "127.0.0.1,localhost",
        no_proxy: "127.0.0.1,localhost",
        PATH: isolatedPath,
        SHELL: "/bin/zsh",
        TMPDIR: temporaryDirectory("habitat-cli-tmp-")
      })

      expect({
        status: installation.status,
        stderr: installation.stderr,
        requests
      }).toEqual({
        status: 0,
        stderr: "",
        requests: [
          "/cli/latest",
          `/cli/v9.8.7/${asset}`,
          "/cli/v9.8.7/SHA256SUMS"
        ]
      })
      expect(installation.stdout).toContain("HT v9.8.7 installed")
      const installed = await commandOutput([join(homeDirectory, ".ht/bin/ht"), "setup"], {
        PATH: isolatedPath
      })
      expect({ status: installed.status, stderr: installed.stderr }).toEqual({ status: 0, stderr: "" })
      expect(installed.stdout).toBe("test ht setup\n")
    } finally {
      server.stop(true)
    }
  })
})
