# Unreleased

## Development

- Add opt-in SHA-256 revisions to PB9 config reads and guarded updates to the
  shared save entry point. Recheck exact source bytes under a reentrant,
  descriptor-bound directory lock before private atomic replacement, reject
  stale/deleted targets, and keep native validation outside the transaction.
  Default saves remain create-only; no revision is stored in config metadata.
  The lock coordinates PBGui writers; noncooperating same-UID file editors
  remain outside that guarantee. UI/API binding is still pending.

- Add validated, create-only PB9 config saving through the shared save entry
  point. Publish a complete private file atomically below an owned 0700 root,
  preserve authored sparse source and GUI metadata, bound formatted output,
  and reject existing names.
  Concurrent creators cannot overwrite one another. Default saves stay
  create-only; UI binding follows separately. Publication acknowledgement failures have a
  distinct error code so a committed file is never blindly removed or retried.

- Add an explicit, read-only PB9 file context to the shared config loader.
  Read only bounded regular JSON files beneath an approved directory using
  no-follow descriptors, reject duplicate keys/nonfinite numbers, and validate
  the authored source with its isolated PB9 runtime. Retain sparse source and
  GUI metadata without the PB7 pipeline. No saving or UI/API binding is added.

- Add an in-memory PB9 document boundary that validates an independent authored
  snapshot while keeping root GUI metadata outside the native helper. Return
  sparse config, unchanged GUI metadata and inspected source identity separately;
  reject non-JSON or oversized documents before runtime inspection. Existing
  PB7/PB8 file loaders and all persistence paths remain unchanged.
  The document cap counts ASCII-escaped JSON bytes; the helper's independent
  request cap also includes its runtime envelope and may reject near-limit
  native payloads after inspection, before launching a process.

- Add native in-memory PB9 validation through the isolated helper. Gate supported
  inline inputs before native imports, reject external coin/config references,
  GUI/internal metadata and symbolmap-dependent coin sources, and return only
  acceptance plus source identity. Keep the caller's sparse config untouched;
  no migration, file loading/saving, API binding or runtime compilation is added.

- Add an isolated PB9 authored-template adapter using a one-request helper in
  the selected environment. Bound execution time and protocol output, strip
  ambient Python/provider environment settings, verify source-module and schema
  identity, and retain sparse JSON-native template structure. The initial Linux
  transport cleans up its process group, including descendants within that group;
  other platforms fail explicitly before startup. No API/UI binding,
  config preparation, migration, saving or bot start is introduced.

- Protect existing PB9 path reservations when saving PB7/PB8 paths from Welcome.
  Validate only actual changes under the INI transaction lock, reject conflicts
  with HTTP 409 and stable error-code headers, and retain raw interpreter
  identity when legacy normalization follows a symlink. Unrelated settings and
  installations without PB9 keep their existing behavior.

- Add a validated PB9 path writer using the existing locked, atomic INI
  transaction. Save both canonical paths together while preserving interpreter
  symlinks, reject invalid layouts without publication, and support explicitly
  clearing both keys. Register next-operation apply metadata without exposing
  PB9 in Welcome setup or adding an API route.

- Add an unconnected, read-only PB9 layout detector with a lazy INI wrapper for
  pb9dir/pb9venv. Check source and environment separation, static Python version,
  CLI and Rust artifact presence without starting the runtime. Report detection
  separately from readiness; missing Git identity is a warning and Rust stamp
  presence is reported without requiring or verifying it.

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
