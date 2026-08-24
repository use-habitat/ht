const rules: ReadonlyArray<{ readonly pattern: RegExp; readonly replacement: string }> = [
  {
    // PostgreSQL text and jsonb reject U+0000 even when it arrives as a valid JSON escape.
    pattern: /\u0000/g,
    replacement: "\uFFFD"
  },
  {
    pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
    replacement: "[REDACTED_PRIVATE_KEY]"
  },
  {
    pattern: /\b(?:ghp|github_pat|sk-proj|sk-ant|xox[baprs])-[-A-Za-z0-9_]{12,}\b/g,
    replacement: "[REDACTED_TOKEN]"
  },
  {
    pattern: /\bAKIA[0-9A-Z]{16}\b/g,
    replacement: "[REDACTED_AWS_ACCESS_KEY]"
  },
  {
    pattern: /(authorization\s*[:=]\s*bearer\s+)[^\s,;]+/gi,
    replacement: "$1[REDACTED_TOKEN]"
  },
  {
    pattern: /((?:password|passwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token)\s*[:=]\s*["']?)[^\s,"';}]+/gi,
    replacement: "$1[REDACTED_SECRET]"
  },
  {
    pattern: /(https?:\/\/[^\s:/]+:)[^@\s/]+(@)/g,
    replacement: "$1[REDACTED_PASSWORD]$2"
  }
]

export interface RedactionResult<T> {
  readonly value: T
  readonly replacements: number
}

export const redactValue = <T>(value: T): RedactionResult<T> => {
  let replacements = 0
  const visit = (candidate: unknown): unknown => {
    if (typeof candidate === "string") {
      let result = candidate
      for (const rule of rules) {
        result = result.replace(rule.pattern, (...args: unknown[]) => {
          replacements += 1
          const match = args[0]
          if (typeof match !== "string") return rule.replacement
          return match.replace(rule.pattern, rule.replacement)
        })
      }
      return result
    }
    if (Array.isArray(candidate)) return candidate.map(visit)
    if (typeof candidate === "object" && candidate !== null) {
      return Object.fromEntries(
        Object.entries(candidate).map(([key, entry]) => [key, visit(entry)])
      )
    }
    return candidate
  }
  return { value: visit(value) as T, replacements }
}
