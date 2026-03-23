# Copilot instructions (pbgui)

## Release workflow

- Release steps are in `RELEASING.md`.
- For each release `vX.YY`:
  - `README.md`: bump `# vX.YY` and add a new top entry under `# Changelog`.
  - `pbgui_func.py`: bump the `About` string.
  - Commit: `Release vX.YY`, tag `vX.YY`, push branch + tags.

## Project conventions

- Streamlit/UI helpers live in `pbgui_func.py`. Pure helpers (no Streamlit) live in `pbgui_purefunc.py`.
- Exchange operations (CCXT, market fetch, symbol info) belong in `Exchange.py`.
- Database operations belong in `Database.py`.
- Never open a dialog inside another dialog: within `@st.dialog` do not call `error_popup/info_popup/result_popup`; show errors inline (e.g. `st.error`).
- Use `:material/...:` icon buttons for compact inline navigation (e.g. `:material/arrow_left:` / `:material/arrow_right:` for month navigation). Prefer material icons for concise controls consistent with existing UI.
- When showing/exporting configs as JSON, use real JSON serialization (`json.dumps`) so `null/true/false` are preserved (avoid Python `None/True/False` formatting).

## Chat trigger phrases

- If the user writes `Bitte in Todo festhalten:`, store the content in `docs/roadmap/TODO.md` (do not store it in repository root files).
- If the user writes `Merke Dir:`, store the content as a persistent instruction in this file (`.github/copilot-instructions.md`) under `## Persistent Notes`.

## Persistent Notes

- Future `Merke Dir:` entries must be appended here as short bullet points.
- Before writing any `Merke Dir:` entry to the file, show the planned text as a proposal and wait for user confirmation.
- Always write `Merke Dir:` entries in English, even if the user submits them in German.
- **NEVER make large unsolicited design changes**: no module rewrites, file deletions, architecture overhauls, or replacing/deleting existing code without explicit user confirmation. Always propose first, ask, wait for "Yes" — then implement.
- **Never extend the scope of a task on your own**: if you think something additional should be done, ask first — never silently do more than was requested.
- **Stop immediately when the original request is satisfied**: do not write additional scripts, run extra tests, or take further actions beyond exactly what was asked. If something seems useful, ask first — never just do it.
- **Always ask before committing and pushing**: never commit or push code without explicit user confirmation first.
- **Never deploy/copy files to manibot01 (or any remote server) without explicit user permission**: do not use `rsync`, `scp`, `ssh` file writes, or any other method to push changes to remote servers unless the user explicitly asks for it in the current request.
- **Symbol resolution always uses the mapping file** (`data/coindata/{exchange}/mapping.json`) — never ad-hoc CCXT market fetches or hardcoded prefix lists. For USDT linear perps filter on `quote == "USDT"` and `swap == True`. See `bybit_best_1m.py` (`_get_bybit_usdt_symbol`) and `binance_best_1m.py` (`_get_binance_symbol`) as reference implementations.
- GUI language is English throughout; only guides/tutorials are offered in both English and German.
- **Logging architecture (3-tier model)**:
  - **Tier 1 — Daemon logs** (each own file): services with their own long-running daemon loop. Do NOT add to `LOG_GROUPS` — they get `data/logs/{service}.log` automatically.
  - **Tier 2 — Data pipeline logs** (each own file): data pipeline components (database, exchange, market data, sync). Do NOT add to `LOG_GROUPS`.
  - **Tier 3 — `PBGui.log`**: UI helper classes without their own daemon loop. MUST be added to `LOG_GROUPS` in `logging_helpers.py` (single source of truth for routing).
  - Routing is handled via the `LOG_GROUPS` dict in `logging_helpers.py` — **never** hardcode the `logfile=` parameter at call sites.
  - The service tag (e.g. `[VPSManager]`) is always embedded in each log entry → grep works even in a shared file.
  - All logs go to `data/logs/` — never to any other directory.
  - **No direct `print()` or `logging.xxx`** in GUI modules — exclusively use `_log('ServiceName', msg, level='...')` via `from logging_helpers import human_log as _log`.
- **Migrate everything to FastAPI**: whenever code needs to be rewritten, always prefer a pure FastAPI + Vanilla JS approach (REST endpoints + WebSocket, no JS frameworks). We are gradually migrating away from Streamlit — no new Streamlit polling fragments, no `run_every`, no `st.components.v1.html()` iframes. Use `st.html(unsafe_allow_javascript=True)` only as the thin embedding shim; all logic, state, and updates go through FastAPI. Frontend is always plain Vanilla JS — no React, no Vue, no jQuery.
- **st.html vs iframe embedding rule**: Use `st.html(unsafe_allow_javascript=True)` when the component needs dynamic height (collapsible sections, expandable content). Use `st.components.v1.iframe()` when the component has a fixed/known height (tables, dashboards). **Critical**: `st.html` passes through DOMPurify with `SAFE_FOR_XML=true` (Streamlit 1.54+), which checks `/<[/\w!]/.test(scriptElement.innerHTML)` and **removes the entire `<script>` element** if it matches. Therefore: all `<` and `>` inside JS string literals in `st.html` components MUST be escaped as `\x3C` / `\x3E`. Comparison operators must be spaced (`a < b`, not `a<b`). iframe-loaded components (e.g. `jobs_monitor.html`) are NOT affected — they run as standalone pages via FastAPI without DOMPurify. When migrating to pure FastAPI, bulk-replace `\x3C`→`<` and `\x3E`→`>`.
- **st.html JS validation rule**: When creating or editing `st.html` components with `<script>` blocks, always validate the JS first by extracting the script content and running `node --check` on it. Common pitfall: `\x3C`/`\x3E` hex escapes are only valid inside JS **string literals** — using them as comparison operators (e.g. `v \x3C 100`) causes a silent `SyntaxError` that kills the entire script block without any visible error. Operators must use real `<` `>` (with spaces for DOMPurify safety).
- **Changelog discipline**: After every feature or bugfix, immediately add an entry to the `# Changelog` section in `README.md` under the current unreleased version. Never finish a task without updating the changelog. Do NOT use a separate `CHANGELOG.md` file.

## Repo boundaries (important)

- Do not modify PB7/passivbot code (e.g. workspace folder `pb7/` or the upstream `passivbot` repo) unless the user explicitly asks/approves first.

## Guides & tutorials (pattern)

- New user-facing guides/tutorials MUST be Markdown files in:
  - `docs/help/` (EN)
  - `docs/help_de/` (DE)
- The first Markdown line MUST be a `# Title` heading; the central Help index uses it as the label.

- Any page that introduces a new guide/tutorial MUST add a persistent `📖 Guide` button in the page header (top-right), opening an in-page `@st.dialog("Help & Tutorials")` modal (Language toggle + Topic select).

- Header placement MUST follow the existing pattern (copy it):
  - Reference implementations:
    - `navi/v7_strategy_explorer.py`
    - `navi/system_api_keys.py`
  - Use the same layout:
    - `c_title, c_help = st.columns([0.95, 0.05], vertical_alignment="center")`
    - Title/header in `c_title`, `st.button("📖 Guide", ...)` in `c_help`

- Topic preselection MUST be done via substring match (e.g. default_topic contains "PBData").

- DO NOT place the Guide button inside the body, expanders, or between inputs.

- If a Help modal already exists on the page/module, reuse that behavior (do not invent a new modal flow).
  - Prefer opening the modal (don’t navigate away) so user context is preserved.

- **Guide sync is mandatory for user-facing changes**:
  - If a page’s visible behavior, labels, workflow, defaults, or warnings change, update the matching guide(s) in the same task.
  - For pages with EN+DE guides, update both `docs/help/...` and `docs/help_de/...` together.
  - This explicitly includes `navi/system_api_keys.py` and `20_api_keys.md` when API key or TradFi provider UX changes.
  - Before finishing, verify guide parity by checking changed UI strings/options against guide text.
  - Do not leave placeholder-only translations when a full EN guide exists; provide a real DE update.

## Backups before mass edits

- **ALWAYS** create backup before using sed/awk/Python scripts for mass edits (>10 locations).
- Pattern: `cp file.py file.py.backup` before edits, `rm file.py.backup` after verification.
- Or use git: `git add file.py && git commit -m "WIP: before mass edit"` then `git restore file.py` if needed.
- Especially critical for: `sed -i`, regex replacements, multi-file changes.

## Commit messages

- Keep commit messages short and user-focused.
- Describe WHAT was added/changed, not HOW or debugging details.
- Example: "Added step parameters to optimizer" ✅
- Not: "Fixed bugs in st.number_input, corrected parentheses, added WIDGET_STEP..." ❌

## Passivbot documentation

- Official docs: https://github.com/enarjord/passivbot/tree/master
- Configuration reference: https://github.com/enarjord/passivbot/blob/master/docs/configuration.md
- Use original help texts from `docs/configuration.md` when adding parameters to GUI

## Python versions

- Default: PBGui + PB7 use Python 3.12.
- PB6 stays on Python 3.10.

## Logging

- **ALL logs** MUST go to `data/logs/` directory.
- **NEVER** create log files in data directories (`coindata/`, `caches/`, etc.).
- **Never** use the standard Python `logging` module or `print()` in GUI modules.
- Exclusively use `_log('ServiceName', msg, level='...')` via `from logging_helpers import human_log as _log`.
- Log file routing is determined by `LOG_GROUPS` in `logging_helpers.py` (see 3-tier model in `## Persistent Notes`).
- Always pass tracebacks via `meta={'traceback': traceback.format_exc()}` — never use `traceback.print_exc()`.

## Module responsibilities

- `pbgui_func.py`: Streamlit/UI helpers
- `pbgui_purefunc.py`: Pure helpers (no Streamlit)
- `Exchange.py`: Exchange operations (CCXT, market fetch, symbol info)
- `Database.py`: Database operations
- `PBCoinData.py`: CMC API, symbol lists, dynamic ignore
- `Config.py`: V6/V7 configurations, BalanceCalculator
- `PBRun.py`: Live instances, bot status

## Data organization

- `coindata/`: CMC data, exchange mappings
- `data/`: User data, instances, configs
- `data/logs/`: All logs (pattern: `{ModuleName}.log`)

## Legacy modules (PB6)

**Do not modify** unless absolutely necessary. Ask user for approval first.

- `Multi.py`, `Backtest.py`, `BacktestMulti.py`, `Optimize.py`, `OptimizeMulti.py`
- `Instance.py`
- `navi/v6_multi_backtest.py`, `navi/v6_multi_optimize.py`, `navi/v6_multi_run.py`
- `navi/v6_single_backtest.py`, `navi/v6_single_optimize.py`, `navi/v6_single_run.py`
- `navi/v6_spot_view.py`

## Session State patterns

- Prefix with module/page name (e.g. `edit_multi_`, `bc_`, `v7_`)
- Clean up state in navigation callbacks
- Never modify session state directly in loops

## Error handling

- `error_popup()`: only for critical user-blocking errors
- `st.error()`: inline for non-critical validation
- `st.warning()`: for hints/warnings
- Always log exceptions before showing popup
- Rate limiting: use `sleep()` between API calls
- Retry only for transient errors (Network, Timeout)
- Never blanket catch without logging

## File operations

- Atomic writes (write to temp file, then rename) are required for any file that could be read by another process during writing, or where a partial write would leave the file in a corrupt state (e.g. config files, PID files, JSON data files). Not needed for log files or one-time creation.
- When reading files, always sanitize content (e.g. `.strip()`) and handle parse errors (`ValueError`, `JSONDecodeError`, etc.) explicitly — never assume well-formed input.
- Check `Path.exists()` before read
- JSON with `indent=4` for readability
- Never overwrite on fetch/parse failure
- Use `pathlib.Path` instead of string concatenation

## Naming conventions

- `snake_case` for variables/functions
- `PascalCase` for classes
- Private methods: `_leading_underscore`
- Constants: `UPPER_CASE`

## Testing

- **ALWAYS** use `venv_pbgui` virtual environment for running PBGui tests.
- Test command: `/home/mani/software/venv_pbgui/bin/python -m pytest tests/`
- Test structure follows Passivbot conventions:
  - Tests organized in subdirectories: `market_data/`, `config/`, `ui/`
  - Test files: `test_*.py`, functions: `test_*()`
  - Use test classes to group related tests
  - Use `@pytest.mark.parametrize` for multiple similar cases
  - Document with docstrings: module, class, and function level
- Write clear, focused tests with descriptive names and assertion messages.
- See `tests/README.md` for complete testing documentation.
