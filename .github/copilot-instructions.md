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
- Pattern: `data/logs/{ModuleName}.log` (e.g. `data/logs/PBCoinData.log`, `data/logs/PBData.log`).
- **NEVER** create log files in data directories (`coindata/`, `caches/`, etc.).
- Use standard Python logging module with appropriate log levels.

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

- Atomic writes: write to temp file, then rename
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
