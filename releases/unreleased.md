# Unreleased

## Development

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
