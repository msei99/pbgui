# Releasing pbgui

## Checklist

### Mandatory validation gate

- Do not release when any required test fails, hangs, is interrupted, or cannot run because of an environment or sandbox restriction.
- Resolve the cause and successfully complete the required checks before committing the release, creating its tag, pushing, or publishing it. If a sandbox blocks a check, rerun it in an authorized environment where it can complete.
- Do not exclude or skip a failing or blocked required test merely to obtain a green result. Existing, justified test skip conditions remain valid.
- Documenting a failed or incomplete check in release notes does not permit a release. If validation remains unresolved, stop the release and report the blocker.
- Follow the mandatory `check_test_suite` verdict when choosing focused versus full offline validation, and record every completed full-suite run with `record_test_suite_run`.

### 1) Bump version strings

- `pbgui_purefunc.py`: update `PBGUI_VERSION = "vX.YY"`
- `api/serial.txt`: increment so running clients show the restart requirement

### 2) Add changelog entry

- During development, add entries to `releases/unreleased.md`.
- When releasing, move those notes into `releases/vX.YY.md`.
- Update `CHANGELOG.md` so the newest released version is linked near the top.

### 3) Commit + tag + push

```bash
git add CHANGELOG.md releases/ pbgui_purefunc.py api/serial.txt README.md
git commit -m "Release vX.YY"
git tag vX.YY
git push
git push --tags
```

### 4) (Optional) GitHub Release

- Create a GitHub Release from tag `vX.YY`.
