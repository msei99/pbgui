# Node/npm dependency audit — 2026-10-30

The required project-wide stack is [Python, FastAPI and browser Vanilla JS](architecture.md). This audit distinguishes actual runtime use from legacy developer tooling and string/metadata matches. It does not authorize exceptions to that stack.

## Application and installation

- Claude: replace the Node worker, npm manifests, lockfile and Zod dependency with the official Python Agent SDK. Its native Claude Code executable is bundled with the Python wheel. Use the existing PBGui Python environment and requirements workflow; preserve private account profiles and PBGui tool approval boundaries.
- The tracked application/install/update sources contain no other direct Node/npm execution found by this audit. The normal installer and update playbooks already install `requirements.txt` with Python/pip.
- `ai_chat.py` reads OpenCode catalog fields named `npm` to select the provider's HTTP protocol. These strings do not execute npm or install JavaScript packages.
- `ai_capabilities.py` excludes `node_modules` from accessible paths. Retain that security filter for inspected third-party repositories.
- Browser HTML/CSS/Vanilla JS and the locally served Plotly asset execute in the browser and do not require Node.js or npm.
- Historical release notes describe their original versions; the current Guide and unreleased notes describe the Python migration.

## Development/test migration

- The former Node subprocess checks execute their existing Vanilla JS assertions in an isolated native Chromium browser. Python supplies only frontend/Guide source fixtures; the browser cannot read local files or private runtime state. The six CommonJS fixture files are now browser `.js` fixtures, and package/runtime imports have been replaced by explicit browser assertion/source helpers.
- All 64 formerly Playwright-dependent Python test modules use `tests/browser_driver.py`, a direct Python-to-Chromium CDP connection. Playwright and its bundled Node driver have been uninstalled from the existing PBGui environment. Native browser installation is explicit and no driver download occurs during tests.
- Repository-local GitNexus configuration, generated index/runner and generated skills have been removed. `tools/code_impact.py` provides a conservative Python AST definition/call/import inventory; dynamic and browser callback relationships require source review and focused tests. Global editor tools used by other repositories are outside PBGui and were not uninstalled.
- `tests/test_python_browser_tooling.py` guards against new package manifests, JavaScript server runtimes and Playwright imports/commands. It also verifies that failing JavaScript assertions remain failures, undeclared filesystem reads are denied and isolated browser processes/profiles are cleaned up.

DOM nodes and cluster nodes are ordinary application concepts, not runtime dependencies. Security filters for third-party `node_modules` and historical release descriptions remain deliberately intact.

## Validation

- The focused JavaScript assertion group completed with **2,273 passed** using the Python driver.
- The affected UI/tooling inventory contains 993 cases. Seven read-only production-log checks remain disabled because they require the explicit `local_runtime` opt-in. The other cases were exercised with isolated synthetic data; this is not a full-suite result.
- The final UI, renderer, tooling and Guide gate completed with **201 passed**. Coin Data now measures card constraints before opening and populating the card; all 12 native selections stayed below the unchanged 20 ms budget (maximum 19.4 ms), with no long tasks. Orders preserves the exact fractional right offset during continuing follow, while retaining manual follow resumption, resize and density behavior. Its native scenario pans outside the actual label-dependent follow margin and verifies fractional-offset preservation; the exact range assertions remain intact.
- The dependency guard confirms that the repository has no package manifests, CommonJS runtime fixtures or direct Node/npm/Playwright execution dependencies. `pip check` reports no broken requirements. Obsolete temporary test profiles were cleaned up; the user's browser profile and production data were not used.
