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

## Python versions

- Default: PBGui + PB7 use Python 3.12.
- PB6 stays on Python 3.10.
