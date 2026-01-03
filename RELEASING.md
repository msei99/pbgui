# Releasing pbgui

## Checklist

### 1) Bump version strings

- `README.md`: update the header `# vX.YY`
- `pbgui_func.py`: update the `About` string to `vX.YY`

### 2) Add changelog entry

- `README.md`: under `# Changelog`, add a new top entry:
  - `## vX.YY (DD-MM-YYYY)`
  - Put newest versions at the top.

### 3) Commit + tag + push

```bash
git add README.md pbgui_func.py
git commit -m "Release vX.YY"
git tag vX.YY
git push
git push --tags
```

### 4) (Optional) GitHub Release

- Create a GitHub Release from tag `vX.YY`.
