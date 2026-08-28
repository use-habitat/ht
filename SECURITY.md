# Security policy

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/use-habitat/ht/security/advisories/new).
If that option is unavailable, email `hello@use-habitat.com` with the subject
`HT security report` and request a secure follow-up channel.

Do not open a public issue for suspected vulnerabilities, exposed credentials,
private session content, or customer data. Include only the minimum information
needed to reproduce the problem until a secure channel is established.

Maintainers will acknowledge receipt, investigate impact, coordinate a fix, and
credit reporters who want attribution. Please allow a reasonable remediation
window before public disclosure.

## Supported versions

Security fixes target the latest release and current `main`. Before 1.0,
upgrading may be required instead of receiving a backport.

## Security boundaries

- Agent transcripts are untrusted, sensitive input. Imported Markdown, links,
  commands, paths, and tool arguments are data—not instructions to execute.
- Provider logs are read-only. HT writes canonical state only inside its own
  state directory.
- Credentials belong in the OS keychain or mode-0600 local files, never tracked
  configuration or issue reports.
- Redaction reduces accidental exposure but is not a guarantee. Review selected
  projects and data before enabling upload.
- Checksum verification protects release downloads, but users should still
  review the installer before piping it to a shell.

See [docs/privacy.md](docs/privacy.md) for the data flow and operator checklist.
