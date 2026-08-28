import { describe, expect, test } from "bun:test"

import { ingestBatchSchema } from "../src/ingest-batch.ts"

interface ProtocolFixture {
  readonly file: string
  readonly valid: boolean
  readonly expectedIssue?: string
}

interface ProtocolManifest {
  readonly schemaVersion: number
  readonly fixtures: readonly ProtocolFixture[]
}

const protocolRoot = new URL("../protocol/", import.meta.url)
const manifest = await Bun.file(new URL("manifest.json", protocolRoot)).json() as ProtocolManifest

describe("public ingestion protocol", () => {
  test("publishes fixtures for schema v2", () => {
    expect(manifest.schemaVersion).toBe(2)
    expect(manifest.fixtures.some((fixture) => fixture.valid)).toBe(true)
    expect(manifest.fixtures.some((fixture) => !fixture.valid)).toBe(true)
  })

  for (const fixture of manifest.fixtures) {
    test(`${fixture.valid ? "accepts" : "rejects"} ${fixture.file}`, async () => {
      const input = await Bun.file(new URL(fixture.file, protocolRoot)).json()
      const result = ingestBatchSchema.safeParse(input)
      expect(result.success).toBe(fixture.valid)
      if (!result.success && fixture.expectedIssue) {
        expect(result.error.issues.some((issue) =>
          issue.message.toLowerCase().includes(fixture.expectedIssue!.toLowerCase())
        )).toBe(true)
      }
    })
  }
})
