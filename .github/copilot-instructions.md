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
- When showing/exporting configs as JSON, use real JSON serialization (`json.dumps`) so `null/true/false` are preserved (avoid Python `None/True/False` formatting).

## Guides & tutorials (pattern)

- New user-facing guides/tutorials must be Markdown files in:
  - `docs/help/` (EN)
  - `docs/help_de/` (DE)
- The first Markdown line must be a `# Title` heading; the central Help index uses it as the label.
- Page UX standard: add a `📖 Guide` button which opens an in-page `@st.dialog("Help & Tutorials")` modal (Language toggle + Topic select), preselected via a substring match on the topic.
  - Use the same modal behavior as the API-Keys editor in `navi/system_api_keys.py`.
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
