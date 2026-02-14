# Copilot instructions (pbgui)

## Release workflow

- Release steps are in `RELEASING.md`.
- For each release `vX.YY`:
  - `README.md`: bump `# vX.YY` and add a new top entry under `# Changelog`.
  - `pbgui_func.py`: bump the `About` string.
  - Commit: `Release vX.YY`, tag `vX.YY`, push branch + tags.

## Project conventions

- Streamlit/UI helpers live in `pbgui_func.py`. Pure helpers (no Streamlit) live in `pbgui_purefunc.py`.
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
