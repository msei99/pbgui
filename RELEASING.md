# Releasing pbgui

## Checklist

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
