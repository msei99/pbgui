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
Vanilla JS fetch() → GET /api/... with Bearer token → updates DOM
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
- Auth via `require_auth` / `optional_auth` dependencies (Bearer token or `?token=` query param).
- HTML pages: `%%TOKEN%%`, `%%API_BASE%%`, `%%WS_BASE%%` placeholders replaced server-side.
- WebSocket validation: check `token` query param, close 4001 on invalid.
- Name validation: reject `/`, `\\`, `\x00`, `.`, `..`.

### Frontend Patterns
- HTML page template:
  ```html
  <nav id="topnav"></nav>
  <div id="page-body">...</div>
  <script>
    window.TOKEN = %%TOKEN%%;
    window.API_BASE = %%API_BASE%%;
    window.WS_BASE = %%WS_BASE%%;
    window.PBGUI_NAV_CONFIG = { subtitle: '...', current: 'page_key' };
  </script>
  <script src="/app/pbgui_nav.js"></script>
  ```
- Log viewing: always use shared `LogViewerPanel` from `frontend/js/log_viewer_panel.js`.
- Navigation: `pbgui_nav.js` via `PBGUI_NAV_CONFIG` + `window.TOKEN`.
- Cache busting: use `?v=N` on JS/CSS asset URLs, increment when file changes.
- Selection UI standard, full table rows: use clickable rows with `.selected`/`.is-selected`, no visible checkbox column for bulk row selection. Selected rows use `background: rgba(77,166,255,.12)` on cells and `border-left: 3px solid var(--accent)` on the first cell. Table headers use the standard sticky dark header style (`background: var(--bg2)`, `border-bottom: 2px solid var(--border)`) so the first data row is clearly separated. Support click and click-drag range selection when multiple rows can be selected.
- Selection UI standard, compact multi-column item grids: use button-like item rows such as Market Data coin pickers (`.coin-picker-row.coin-picker-button.selected`). Keep the compact rounded field look with the selected state provided by `.coin-picker-row.selected` (`border-color: rgba(99,179,237,.24)`, `background: rgba(99,179,237,.12)`). Do not add the full-table left accent bar here. Support click, keyboard toggle, and click-drag selection for large lists.
- **Confirm dialogs**: never use native `window.confirm()` / `window.alert()` for security prompts or destructive actions. Use the page's shared modal system (e.g., `openConfirmModal(title, message, onConfirm)`) so the UI stays visually consistent and testable.

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
- Always log exceptions before showing popup.
- Retry only for transient errors (Network, Timeout).
- Never blanket catch without logging.

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
- Commit: `Release vX.YY`, tag `vX.YY`, push branch + tags.

## Testing
- Never run verification steps or tests that can delete, mutate, or overwrite real user/runtime data unless the user explicitly asked for that destructive action. Prefer dry runs, read-only inspection, mocks, or isolated test data.
- Run: `/home/mani/software/venv_pbgui/bin/python -m pytest tests/`
- Pytest 7.0+, discovery: `test_*.py`, `Test*` classes, `test_*` functions.
- Use `@pytest.mark.parametrize` for multiple cases.
- Module/class/function docstrings required.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **pbgui** (11134 symbols, 31998 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.

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
