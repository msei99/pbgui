# Logging Migration Implementation Plan

**Status: completed.** The implementation, migration, policy guardrails, and
documentation described below were completed in the unreleased change set.

The completed follow-up also adds automatic process-safe startup cleanup,
configurable managed rotation scopes, and canonical `data/logs` locations for
all PBGui-owned transcripts. PB7/native bot logs remain externally owned.

## Goal

Finish PBGui's logging migration without collapsing intentionally separate
daemon, data-pipeline, subprocess, or worker-protocol output into one file.

The completed implementation must provide:

- one canonical project-relative log root,
- centralized secret redaction,
- safe concurrent append, rotation, purge, and settings updates,
- explicit Tier-1/Tier-2/Tier-3 ownership,
- structured operational context,
- no browser session token in the Logging Monitor page,
- documented exceptions for machine protocols and human-facing CLI output,
- regression tests that prevent alternative loggers from returning.

## Non-Goals

- Do not group daemon logs into `PBGui.log`.
- Do not group data-pipeline logs into `PBGui.log`.
- Do not replace machine-readable subprocess stdout/stderr protocols with log
  lines.
- Do not replace user-visible VPS/job transcripts with application logs.
- Do not move or rewrite existing historical log files automatically.
- Do not introduce an external logging service or browser dependency.
- Do not claim central redaction makes it acceptable for callers to log known
  secrets; callers must still avoid secrets at the source.

## Current Risks

The implementation order is driven by these confirmed issues:

1. `logging_helpers.py` derives logs and `pbgui.ini` from `Path.cwd()`.
2. Messages, exceptions, URLs, and nested metadata have no secret redaction.
3. Logging append, rotation, purge, and INI updates are not coordinated across
   threads or processes.
4. `api/logging.py` injects the HttpOnly browser session token into HTML.
5. Grouped log rotation settings are saved by physical stem but read by
   logical service name.
6. Direct appenders and an unused alternative logger remain.
7. Tier ownership and allowed stdout/file-transcript exceptions are not
   enforceable by tests.

## Target Model

### Canonical paths

Define in `logging_helpers.py`:

```text
PBGDIR = directory containing logging_helpers.py
PBGGUI_INI = PBGDIR / "pbgui.ini"
LOG_ROOT = PBGDIR / "data" / "logs"
```

All default helper output and rotation configuration must use these paths.
Explicit `logfile=` remains supported for legitimate daemon, task, test, and
remote paths.

Starting PBGui from a different working directory must not change either the
log destination or rotation settings source.

### Log tiers

| Tier | Ownership | Destination |
| --- | --- | --- |
| Tier 1 | Independent daemon/service | Dedicated service log |
| Tier 2 | Data pipeline/downloader/job family | Dedicated pipeline log or documented transcript |
| Tier 3 | API/UI helper without independent lifecycle | `PBGui.log` through `LOG_GROUPS` |

Maintain explicit sets for tests and documentation:

- Tier-3 grouped services
- dedicated daemon/pipeline services
- allowed standalone/embedded logger implementations
- allowed CLI or worker-protocol files
- allowed transcript writers

The first review must classify at least:

```text
ApiLogging, ApiKeys, BalanceCalc, CoinDataUI, Dashboard, Services,
V7Instances, MarketDataAPI, PB7OhlcvAPI, PBV7UI
```

These are expected to be Tier 3 unless a concrete independent lifecycle or
pipeline requirement justifies a dedicated log. `OptimizeQueueAPI` remains
dedicated by design. PBRun, PBData, PBCoinData, PBCluster, PBApiServer,
PBMonitorAgent, VPSMonitor, and exchange data pipelines remain dedicated.

### Context contract

Preserve the current human-readable line format and JSON metadata suffix. Use
reserved metadata fields instead of adding incompatible positional arguments:

```json
{
  "host": "optional VPS or local host identity",
  "instance": "optional bot/config identity",
  "operation": "required stable action name for operational events",
  "request_id": "required for request-scoped API error/warning events"
}
```

Rules:

- `operation` is required for state-changing actions, retries, failures, and
  security-relevant events.
- `request_id` is required when an API request context exists.
- `host` is required for remote/VPS operations.
- `instance` or the existing `user` field is required for bot/user-specific
  operations.
- Fields that do not apply are omitted, not filled with placeholders.
- Tracebacks use `meta={"traceback": traceback.format_exc(), ...}`.

Do not attempt to update every informational line in one pass. Apply the
contract first to error, warning, state-change, remote-operation, and security
events, then enforce it for new code.

## Phase 1: Central Helper Foundation

Primary files:

- `logging_helpers.py`
- `tests/test_logging_helpers.py`

### 1.1 Canonical path handling

Replace cwd-relative defaults for:

- log files,
- rotation settings reads,
- rotation settings writes.

Tests must set cwd to an unrelated temporary directory and verify that helper
defaults still resolve through monkeypatched canonical project paths. Tests
must never write the real repository `pbgui.ini` or runtime logs.

### 1.2 Central redaction

Add one bounded, reusable redaction layer applied before formatting to:

- message text,
- `code`,
- user and tags,
- exception/traceback text,
- metadata keys and values,
- nested dictionaries, lists, tuples, and sets,
- URLs and query strings.

Sensitive key matching must be case-insensitive and cover at least:

```text
password, passwd, api_key, apikey, api_secret, secret, token,
session, cookie, authorization, bearer, private_key, passphrase
```

Text redaction must cover at least:

- `Authorization: Bearer ...`,
- cookie/session assignments,
- common `key=value` and JSON-like secret fields,
- sensitive URL query parameters,
- PEM private-key blocks.

Use a stable replacement such as `[REDACTED]`. Bound recursion depth,
collection size, and rendered value length so malicious metadata cannot cause
unbounded work. Handle recursive objects safely.

Non-serializable metadata must produce a deterministic sanitized fallback
instead of silently disappearing. Redaction failures must fail closed for the
affected value.

### 1.3 Concurrent file operations

Use one lock identity per physical log path. The lock must cover the full:

```text
size check -> rotation -> append
```

transaction. Purge must use the same lock. Requirements:

- in-process thread lock,
- POSIX advisory lock for cross-process writers,
- safe fallback on platforms without `fcntl`,
- no recursive logging while a log lock is held,
- bounded failure handling that falls back to stderr without exposing the
  unredacted original message.

Rotation must preserve configured generation count deterministically under
concurrent writers. The implementation must not describe a multi-step
rotation as atomic.

### 1.4 Safe settings updates

Read and write only canonical `PBGDIR/pbgui.ini`. Protect the full INI
read-modify-write with the project's reentrant cross-process configuration
lock and atomic replacement. Preserve unrelated sections and concurrent
changes.

Normalize rotation overrides by physical log stem. A grouped `VPSManager`
write and a UI override for `PBGui.log` must resolve to the same `PBGui`
rotation rule.

## Phase 2: Logging Monitor Security and Correctness

Primary files:

- `api/logging.py`
- `frontend/logging_monitor.html`
- logging API tests
- `api/serial.txt`

Before editing route handlers, run API route impact analysis for all modified
Logging Monitor endpoints.

### 2.1 Remove session-token injection

Remove `%%TOKEN%%` replacement and all JavaScript token handling. Browser
requests must rely solely on the same-origin HttpOnly session cookie. Confirm
that rendered HTML contains no token and that unauthenticated endpoints remain
rejected.

### 2.2 Correct rotation presentation

- Resolve settings by physical log stem.
- Enumerate all configured rotated generations without stopping at the first
  missing generation.
- Keep stable ordering.
- Ensure grouped services show the effective `PBGui` rule.

### 2.3 Correct purge behavior

- Accept only a base `.log` filename.
- Reject `.log.1`, `.old`, traversal, control characters, and separators.
- Resolve below `LOG_ROOT` after validation.
- Use the effective configured max size and backup count.
- Serialize purge with active writers and rotation.
- Preserve expected HTTP status codes and log failures before returning an
  error.

## Phase 3: Runtime Migration and Classification

Run GitNexus impact analysis before changing every function, method, or class
listed below.

### 3.1 Clear direct-logger violations

- Replace the direct `PBV7UI.log` append in `PBApiServer.py` with
  `human_log`; classify `PBV7UI` as Tier 3.
- Remove `Log.py` after a final graph and text reference check confirms no
  external/project consumer.
- Convert `Exchange.save_income_other()` from concatenated invalid JSON under
  `data/logs` to a redacted structured diagnostic event, unless a real data
  consumer is discovered. If it is persisted data, move it to a locked atomic
  data contract instead of treating it as a log.
- Convert traceback text embedded in `api/services.py` messages to structured
  `meta["traceback"]`.
- Keep per-job and VPS task transcripts, but report transcript/alias write
  failures through the appropriate service logger without recursive failure.

### 3.2 Classify stdout and embedded scripts

Document and test legitimate stdout/stderr protocols instead of replacing
them mechanically:

- worker JSON/progress protocols,
- embedded remote probes,
- installer CLI,
- maintenance CLI,
- subprocess stdout/stderr capture.

Review `starter.py` as a human-facing service-control CLI. Either document its
stdout as intentional CLI output or add service logging for operational
events while preserving stdout compatibility. Do not duplicate every line in
both channels without a consumer need.

### 3.3 Remote VPS monitor

The embedded monitor cannot assume the local PBGui module tree. Preserve it as
a documented Tier-1 standalone implementation, but align it with the central
contract where feasible:

- canonical timestamp and level format,
- bounded rotation,
- redaction before write,
- deterministic remote log path,
- stderr only as a last-resort sanitized fallback.

Do not deploy the updated embedded script to any VPS without separate explicit
user approval.

### 3.4 Service constants and grouping

Add or normalize `SERVICE` constants in modules already using `human_log`,
including:

- `Database.py`
- `Exchange.py`
- `ParetoDataLoader.py`
- `Status.py`
- `vps_manager_core.py`
- `PBRun.py`
- `PBCoinData.py`
- `PBData.py`
- `market_data.py`
- `binance_best_1m.py`
- `hyperliquid_aws.py`
- `tradfi_sync.py`
- `api/live.py`

Replace repeated service literals only; do not change log ownership while
performing this mechanical cleanup.

Update `LOG_GROUPS` only after the Tier-3 review. Add a test asserting both
the grouped allowlist and the dedicated-service denylist.

## Phase 4: Operational Context Adoption

Prioritize call sites where context changes incident response quality:

1. authentication and API-key operations,
2. VPS and remote actions,
3. service start/stop/restart,
4. bot instance/process control,
5. market-data and detached job lifecycle,
6. database backup/restore and destructive operations.

For API requests, introduce or reuse a request ID at middleware/request scope
and return it in an appropriate response header. Pass it into warning/error
and state-change log metadata without exposing session identifiers.

Context adoption is complete when representative high-risk paths carry the
required fields and tests enforce the contract for new events. It is not
necessary to rewrite historical low-value informational messages solely to
add empty metadata.

## Phase 5: Regression Guardrails

Add offline tests for the following groups.

### Paths and settings

- default log path independent of cwd,
- canonical INI path independent of cwd,
- atomic settings update preserves unrelated values,
- grouped physical-stem rotation overrides.

### Redaction

- every sensitive key spelling,
- nested containers and mixed-case keys,
- message, code, user, tags, metadata, exception, and traceback channels,
- bearer headers, cookies, URL query strings, and PEM blocks,
- recursive and non-serializable metadata,
- Unicode and multiline values.

### Concurrency

- multiple threads append without lost or merged lines,
- multiple processes append without corruption,
- rotation under concurrent writers,
- purge under concurrent writers,
- settings updates do not lose unrelated changes.

### Ownership and migration

- every Tier-3 service maps to `PBGui`,
- every Tier-1/Tier-2 service remains dedicated,
- `Log.py` and unapproved logger setup do not return,
- no production `traceback.print_exc()`,
- no unapproved `logging.debug/info/warning/error/exception/critical`,
- no unapproved direct append under `data/logs`,
- only allowlisted CLI/protocol files use runtime `print()`.

Implement the audit test with AST/token-aware inspection where practical so
comments, strings, and embedded protocol source are not misclassified by a
simple regular expression.

### Logging Monitor

- cookie-only authentication,
- no session token in rendered HTML or JavaScript,
- traversal/control-character rejection,
- rotated filename rejection for purge,
- sparse generation listing,
- effective configured purge/rotation values,
- grouped log settings.

## Phase 6: Documentation and Cleanup

Update:

- `docs/help/31_logging.md`
- German logging help if present
- `AGENTS.md` only if the final contract needs clarification
- `releases/unreleased.md`

Remove stale lists of old PB6/Streamlit GUI helpers from the logging guide.
Document:

- tier ownership,
- canonical paths,
- context fields,
- redaction behavior and limitations,
- allowed protocol/transcript exceptions,
- rotation and purge semantics.

Every implementation batch touching API/startup/runtime code must increment
`api/serial.txt` after its final API edit.

## Verification Strategy

Run verification after each phase rather than waiting until the end:

1. focused helper tests,
2. focused Logging Monitor/API tests,
3. focused migration and audit tests,
4. concurrency stress tests in isolated temporary directories,
5. `python -m pytest tests/`,
6. `git diff --check`,
7. GitNexus `detect_changes(scope="all")` before any requested commit.

No test may read, rotate, purge, or write production `data/logs` or the real
`pbgui.ini`.

## Implementation Batches

Use these reviewable batches:

1. **Foundation:** canonical paths, redaction, locking, atomic settings, unit
   tests.
2. **Monitor:** cookie-only page, rotation/purge fixes, API tests, serial bump.
3. **Migration:** direct appenders, obsolete logger removal, service constants,
   Tier classification, serial bump where required.
4. **Context and guardrails:** request/operation context, AST audit tests,
   concurrency tests, documentation.

Do not commit between batches without explicit user approval.

## Acceptance Criteria

The roadmap item is complete when:

1. Default logs and rotation settings always resolve from `PBGDIR`.
2. Every central logging input channel is redacted and covered by tests.
3. Concurrent append, rotation, purge, and settings updates pass isolated
   thread/process tests without data loss or corruption.
4. The Logging Monitor uses only the HttpOnly cookie and renders no session
   token.
5. Grouped log rotation settings and purge behavior are correct.
6. No active alternative local logger or unjustified direct `data/logs`
   appender remains.
7. Tier-3 helpers are grouped and daemon/pipeline logs remain dedicated.
8. Runtime `print()` and transcript writers are limited to documented,
   test-allowlisted protocols and CLIs.
9. High-risk operational events carry applicable host, instance/user,
   operation, and request context.
10. Logging help matches the implemented ownership and security model.
11. Focused tests and the complete offline test suite pass.

All acceptance criteria are covered by the central helper, Logging Monitor,
runtime migration, operational-context, and policy-audit test suites. The
final offline verification completed with 1,269 tests passed and 39 explicit
live/runtime tests skipped. Focused logging/path/migration verification
completed with 233 tests passed. Python compilation, shell syntax validation,
and `git diff --check` also succeeded.
