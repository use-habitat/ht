import { z } from "zod"

import { ingestBatchStructuralSchema } from "../src/ingest-batch.ts"

const schemaPath = new URL("../protocol/ingest-v2.schema.json", import.meta.url)
const generated = allowUnknownObjectProperties(z.toJSONSchema(ingestBatchStructuralSchema, {
  target: "draft-2020-12",
  io: "output"
})) as Record<string, unknown>
const schema = {
  $id: "https://raw.githubusercontent.com/use-habitat/ht/main/protocol/ingest-v2.schema.json",
  title: "Habitat ingestion batch v2",
  description: "The public wire contract produced by HT for Habitat-compatible ingestion APIs.",
  ...generated,
  "x-habitat-invariants": [
    "cursor.to must be greater than or equal to cursor.from",
    "baseline batches must start at cursor zero",
    "source.sourceSize must be greater than or equal to cursor.to",
    "final batches require a completed or archived session"
  ]
}
const rendered = `${JSON.stringify(schema, null, 2)}\n`

if (process.argv.includes("--check")) {
  const current = await Bun.file(schemaPath).text().catch(() => "")
  if (current !== rendered) {
    console.error("protocol/ingest-v2.schema.json is stale; run `bun run protocol:generate`.")
    process.exit(1)
  }
} else {
  await Bun.write(schemaPath, rendered)
  console.log("Generated protocol/ingest-v2.schema.json")
}

/**
 * Zod strips unknown object keys by default, while generated JSON Schema uses
 * `additionalProperties: false`. Remove that stricter representation so older
 * compatible consumers can accept optional fields added by a minor release.
 */
function allowUnknownObjectProperties(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(allowUnknownObjectProperties)
  if (!value || typeof value !== "object") return value
  return Object.fromEntries(
    Object.entries(value).flatMap(([key, child]) =>
      key === "additionalProperties" && child === false
        ? []
        : [[key, allowUnknownObjectProperties(child)]]
    )
  )
}
