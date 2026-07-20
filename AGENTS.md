# PBGui — Agent Instructions

## Before Finishing ANY Task

1. **Changelog** — Add entry to `releases/unreleased.md`. Never skip.
2. **serial.txt** — If any file under `api/`, `PBApiServer.py`, or a module imported at API startup changed: increment `api/serial.txt` by 1 before finishing. This is mandatory for every final change set that touches API startup/runtime code so the UI can show the restart-required button. If you make more API/startup edits after an earlier serial bump in the same session, bump `api/serial.txt` again. Never tell the user to restart first because you forgot this bump.
3. **Commit** — Always ask before committing or pushing. Never commit without explicit user confirmation.

## Project Architecture

### Stack
- **Backend**: FastAPI. All new features as FastAPI routes.
- **Frontend**: Vanilla JS + HTML (no React, Vue, jQuery). Served via FastAPI `/app/` static mount.
- **CSS**: Custom dark theme, CSS variables per page (`:root`), no Tailwind/Bootstrap.
- **Python**: 3.12 default, 3.10 for PB6 legacy.

### Directory Layout
| Path | Purpose |
|------|---------|
| `api/` | FastAPI route modules (auth, dashboard, vps, logging, market_data, etc.) |
| `frontend/` | Vanilla JS HTML pages |
| `frontend/js/` | Shared JS modules: `log_viewer_panel.js`, `pbgui_nav.js`, `pbgui_dialogs.js` |
| `master/` | Async backend daemons: SSH pool, monitoring, log streaming, file sync |
| `tests/` | Pytest tests |
| `docs/help/` | English guides |
| `docs/help_de/` | German guides |
| `data/` | Runtime data: `logs/`, `instances/`, `run_v7/`, `bt_v7/`, `opt_v7/`, `dashboards/` |

### Data Flow
```
FastAPI router → reads DB / memory → returns JSON
Vanilla JS fetch() → same-origin HttpOnly session cookie → updates DOM
External API client → Authorization: Bearer token → JSON API
WebSocket /ws/* → push every N seconds → JS updates charts/tables
SSE /api/live/stream → delta applies on top of DB snapshot
```

## Code Conventions

### General
- **GUI language is English throughout** — only guides are offered in DE as well.
- Snake_case for variables/functions, PascalCase for classes, UPPER_CASE for constants.
- Private methods: `_leading_underscore`.
- `SERVICE = "ModuleName"` constant at top of every module for logging.

### FastAPI Patterns
- Router variable always named `router = APIRouter()`.
- Auth via `require_auth` / `optional_auth`: browsers use the HttpOnly `pbgui_session` cookie; external API clients use an `Authorization: Bearer` header. Never accept session tokens in query parameters.
- HTML pages use `%%API_BASE%%` and `%%WS_BASE%%` placeholders. Never expose a browser session token through HTML, JavaScript, URLs, logs, SSE, WebSocket, or proxy query strings.
- Browser WebSockets must call `authenticate_websocket()` so the HttpOnly cookie is validated, the socket is registered, and logout/expiry closes it with code `4001`.
- Name validation: reject `/`, `\\`, `\x00`, `.`, `..`.

### Frontend Patterns
- HTML page template:
  ```html
  <nav id="topnav"></nav>
  <div id="page-body">...</div>
  <script>
    window.API_BASE = %%API_BASE%%;
    window.WS_BASE = %%WS_BASE%%;
    window.PBGUI_NAV_CONFIG = { subtitle: '...', current: 'page_key' };
  </script>
  <script src="/app/pbgui_nav.js"></script>
  ```
- Log viewing: always use shared `LogViewerPanel` from `frontend/js/log_viewer_panel.js`.
- Navigation: `pbgui_nav.js` via `PBGUI_NAV_CONFIG`; browser authentication comes from the same-origin cookie.
- Guide coverage: every productive page registered in `FASTAPI_PAGES` must have a `GUIDE_TOPICS` entry and matching EN/DE Markdown topics. Review the relevant guides with every productive UI change and keep the parity/mapping tests passing.
- Cache busting: use `?v=N` on JS/CSS asset URLs, increment when file changes.
- Never add external CDN or web-hosted frontend dependencies (`https://...` scripts, stylesheets, fonts, maps, or other runtime assets). All browser assets must be served locally from `/app/` (for example under `frontend/vendor/`) so PBGui works offline and does not leak requests to third parties.
- Selection UI standard, full table rows: use clickable rows with `.selected`/`.is-selected`, no visible checkbox column for bulk row selection. Selected rows use `background: rgba(77,166,255,.12)` on cells and `border-left: 3px solid var(--accent)` on the first cell. Table headers use the standard sticky dark header style (`background: var(--bg2)`, `border-bottom: 2px solid var(--border)`) so the first data row is clearly separated. Support click and click-drag range selection when multiple rows can be selected.
- Selection UI standard, compact multi-column item grids: use button-like item rows such as Market Data coin pickers (`.coin-picker-row.coin-picker-button.selected`). Keep the compact rounded field look with the selected state provided by `.coin-picker-row.selected` (`border-color: rgba(99,179,237,.24)`, `background: rgba(99,179,237,.12)`). Do not add the full-table left accent bar here. Support click, keyboard toggle, and click-drag selection for large lists.
- **Confirm dialogs**: never use native `window.confirm()` / `window.alert()`. Use the shared modal system for security decisions and irreversible or high-consequence actions. Do not add confirmation fatigue to routine stop, restart, cancel, or cleanup actions whose intent and effect are already clear.

### Logging
- 3-tier model:
  - Tier 1: Daemon logs (own file, e.g. `PBRun.log`) — NOT in `LOG_GROUPS`.
  - Tier 2: Data pipeline logs (own file) — NOT in `LOG_GROUPS`.
  - Tier 3: UI helpers → `PBGui.log` — MUST be added to `LOG_GROUPS` in `logging_helpers.py`.
- Import: `from logging_helpers import human_log as _log`, then `_log('ServiceName', msg, level='...')`.
- Never use `print()` or `logging.xxx()` in GUI modules.
- Never use `traceback.print_exc()` — use `meta={'traceback': traceback.format_exc()}`.
- All logs go to `data/logs/`.

### Passivbot Configs
- Load/save via `pb7_config.py` (`load_pb7_config` / `save_pb7_config`) — never raw `json.load`/`json.dump`.
- Exception: override configs (`{SYMBOL.json}`) are sparse diffs — raw JSON is fine.
- Symbol resolution: always use `data/coindata/{exchange}/mapping.json` — never ad-hoc CCXT fetches.
- USDT linear perps: filter `quote == "USDT"` and `swap == True`.

### File Operations
- Atomic writes: temp file + `os.replace()` for configs, PID files, JSON data files, secrets.
- Not needed for: log files, one-time creation.
- Check `Path.exists()` before read, handle parse errors explicitly.
- JSON with `indent=4`.
- Use `pathlib.Path` over string paths.

### Error Handling
- FastAPI: `raise HTTPException(status_code=..., detail=...)`.
- Re-raise expected `HTTPException` values unchanged before broad exception handling so statuses such as restart-blocking HTTP 409 are preserved.
- Always log exceptions before showing popup.
- Retry only for transient errors (Network, Timeout).
- Never blanket catch without logging.

## Audit Non-Regression Rules

### Security Boundaries
- Never place passwords, private keys, API keys, session tokens, or other secrets in PBGui-controlled URLs/query strings, rendered HTML, browser storage, command lines, or logs. If a third-party provider requires a server-side query token, keep that URL out of browsers and logs. Send unsaved secrets to PBGui in authenticated request bodies and redact diagnostic payloads.
- Never read or print an entire secret-bearing file such as `pbgui.ini`, `.env`, credential stores, auth state, private keys, or token files during diagnostics. Query only explicitly allowlisted non-secret keys with narrowly scoped tools, and ensure surrounding lines or unrelated values cannot appear in tool output.
- Render untrusted values with `textContent` or context-correct escaping. Do not interpolate user-controlled values into `innerHTML`, inline JavaScript handlers, shell commands, or selectors.
- Validate every filesystem or remote identifier at the boundary. Resolve paths below an approved root, reject traversal and control characters, and revalidate persisted names before reads, writes, deletes, SSH actions, or process control.
- Build subprocess and SSH commands from validated argv elements. Never concatenate untrusted values into a shell command and never disable SSH host-key verification.
- Sensitive files and directories use the helpers in `secure_files.py`: files are owner-only (`0600`), directories are owner-only (`0700`), and writes are atomic.

### State And Concurrency
- Read-modify-write operations on shared JSON, INI, dashboard, API-key, queue, task, and market-data state require the appropriate reentrant cross-process lock plus atomic replacement. A temporary filename alone does not prevent lost updates.
- On failed batch persistence, restore unsaved buffered data without overwriting values that arrived while the write was in progress.
- Repeated browser requests and replaceable WebSockets must use generations/request IDs so stale responses, callbacks, reconnects, and timers cannot overwrite or revive newer state.
- Shared clients, streams, and caches need explicit ownership plus bounded TTL/size. Release them when the last consumer leaves; do not retain dead remote-log streams or exchange clients indefinitely.

### Background Lifecycle
- Every API-owned task, watcher, thread, executor job, subprocess, stream, and cached client must be registered with a clear owner and have deterministic shutdown: signal/cancel, await/join, close pipes/clients, and remove registry entries.
- Startup must clean up resources created before a partial failure. Shutdown must be idempotent and use `gather(..., return_exceptions=True)` or equivalent so one cleanup failure does not skip the rest.
- Distinguish API-owned controllers from independent persistent jobs. API restart may stop its controllers and reconstruct them from disk, but must not kill Passivbot backtest/optimize processes, detached DB Sync jobs, or detached OHLCV preloads.
- Operations that cannot safely survive or be reconstructed across an API restart must register a restart blocker and return HTTP 409 with a useful reason.

## Important Constraints

### Do NOT Modify Without Approval
- PB7/passivbot code (`pb7/` or upstream repo).
- PB6 legacy modules: `Multi.py`, `Backtest.py`, `Optimize.py`, `BacktestMulti.py`, `OptimizeMulti.py`, `Instance.py`.
- Do not deploy/copy files to any bot/VPS host or remote server without explicit permission.

### General Rules
- Never make large unsolicited design changes. Propose first, wait for "Yes".
- Never extend scope — stop when the original request is satisfied. Ask if something extra seems useful.
- Never silently do more than requested.
- Always ask before committing or pushing.
- Never build modal windows/dialogs that close when the user clicks outside them; require an explicit button/action to close.

### Remote Bot Operations
- Never update, pull, deploy, copy files, stash changes, restart services, or otherwise modify any bot/VPS host without explicit confirmation for that exact remote action.
- A generic release, commit, tag, or push approval does not imply permission to update any bot/VPS host.
- Read-only inspection of remote hosts is allowed when needed for debugging, but any command that changes files, git state, services, processes, or runtime state requires a separate question first.

### Release Workflow
- Steps in `RELEASING.md`.
- Per release: bump `pbgui_purefunc.py` `PBGUI_VERSION`, move `releases/unreleased.md` notes into a dedicated `releases/vX.YY.md` file, keep `CHANGELOG.md` index updated.
- Per release: bump `api/serial.txt` so the UI makes the restart requirement visible for already running processes. Treat the serial bump as a normal release-prep step; do not rely on remembering it ad hoc.
- A new minor release such as `v1.93` requires the complete offline test suite. Small `v1.92.x` patch releases need only focused tests for the changed areas unless broad shared runtime, security, or persistence behavior changed.
- Commit: `Release vX.YY`, tag `vX.YY`, push branch + tags.

## Testing
- Never run verification steps or tests that can delete, mutate, or overwrite real user/runtime data unless the user explicitly asked for that destructive action. Prefer dry runs, read-only inspection, mocks, or isolated test data.
- Default run: `python -m pytest tests/`. It must remain offline and must not read or write production runtime data.
- Current public market data is tested only with `-m live_exchange --run-live`; generated mappings, caches, and reports still belong under Pytest temporary directories.
- PB7 integration requires `-m external_pb7 --run-external-pb7`; local PBGui runtime reads require `-m local_runtime --run-local-runtime` and remain read-only.
- Keep `pytest.ini`, `tests/conftest.py`, test dependencies, and active tests versioned. Never hide the test tree with `.git/info/exclude` or project ignore rules.
- Pytest 7.0+, discovery: `test_*.py`, `Test*` classes, `test_*` functions.
- Use `@pytest.mark.parametrize` for multiple cases.
- Module/class/function docstrings required.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **pbgui** (16058 symbols, 49461 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/pbgui/context` | Codebase overview, check index freshness |
| `gitnexus://repo/pbgui/clusters` | All functional areas |
| `gitnexus://repo/pbgui/processes` | All execution flows |
| `gitnexus://repo/pbgui/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
