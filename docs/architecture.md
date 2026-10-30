# PBGui architecture

## Required technology stack

- Application server, AI integrations, installers, update scripts, build helpers and project-owned verification tooling use Python and Python packages. HTTP routes use FastAPI.
- Browser pages use locally served HTML, CSS and Vanilla JavaScript. Do not introduce React, Vue, Angular, TypeScript build pipelines or other frontend frameworks.
- Do not introduce Node.js, npm, npx, Yarn, pnpm, Bun, npm packages or a JavaScript server/worker runtime as a project dependency. This rule applies to the entire project, including development and test tooling, not only AI providers.
- Install compatible Python dependencies in the existing PBGui virtual environment through the versioned requirements files. Do not introduce an additional environment for one provider. Update shared Python packages together when compatibility requires it and verify the affected API, authentication and validation boundaries.
- Existing native dependencies used by Python packages or provider clients remain explicit dependencies: for example, the official Claude Python Agent SDK bundles a native Claude Code client. This does not require Node.js or npm. Native clients must retain the same ownership, credential isolation, tool authorization and deterministic shutdown boundaries.

## Existing boundaries

- FastAPI serves the existing Vanilla JS frontend; JavaScript executes in the browser.
- PB7/PB8 bot code and external provider implementations remain separate from PBGui. Do not modify their implementation or deploy changes to remote hosts without the required authorization.
- No package installation or update runs implicitly during an AI request, login or API startup. Dependency changes belong to the existing installation/update workflow.

## Verification and change analysis

Project tests are Python. Browser behavior and Vanilla JS assertions execute in
an isolated native Chromium browser controlled directly by Python over CDP.
`tests/browser_driver.py` uses the Python `websockets` package; no automation
server, Node driver or package manager is required. Test browser profiles are
temporary and all unhandled page requests are blocked. Browser installation is
explicit; tests never download a driver or reuse the user's profile.

`tools/code_impact.py` inventories Python definitions, possible callers and
imports without importing application modules. It reports dynamic-call limits
explicitly. Browser JavaScript and callback relationships still require source
review and focused behavioral checks. There is no project-owned JavaScript code
indexer or generated index bootstrap.

See the [Node/npm removal audit](node_dependency_audit.md) and
[browser test setup](../tests/README.md).
