#!/usr/bin/env bun

import { fileURLToPath } from "node:url"

import { runCli } from "./cli.ts"

process.exitCode = await runCli(Bun.argv.slice(2), {
  entrypoint: fileURLToPath(import.meta.url)
})
