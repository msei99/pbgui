# Releasing pbgui

## Version bump (required)

1) Update version in `README.md`
- Update the header `# vX.YY`.

2) Extend the changelog in `README.md`
- Add a new top entry under `# Changelog`:
  - `## vX.YY (DD-MM-YYYY)`
  - Keep newest versions at the top.

3) Update the UI version string
- Update the `About` string in `pbgui_func.py`.

## Publish (optional)

- Commit message suggestion: `Release vX.YY`.
- (Optional) Create a git tag: `git tag vX.YY` and push: `git push && git push --tags`.
- (Optional) Create a GitHub release from the tag.
