# Unreleased

## Development

- Reject v9 config schemas in PB8 guard checks for both local source trees and
  selected Git refs before process stops or checkout. Preserve PB7's package-only
  check, PB8 return values and CLI markers. Share static schema/literal helpers
  with UTF-8 BOM support and a 256 KiB parsing limit.

- Add an unconnected, read-only Passivbot source classifier that distinguishes
  PB8 and PB9 by their config schema rather than their shared package version.
  Inspect Python declarations with AST and report Git identity and diagnostics
  without importing bot code or changing existing runtime behavior. Reject unset
  or relative source paths, isolate Git reads from inherited repository/config
  overrides, and withhold the commit when inspected metadata differs from HEAD
  beyond CRLF/LF line-ending conversion.

## Documentation

- Add a staged Passivbot v9 integration plan with an initial compatibility
  inventory, independent Codex/Claude review workflow, and milestone acceptance
  criteria for development on main.
