# PBGui architecture

## Required technology stack

- Application server, AI integrations, installers, update scripts, build helpers and project-owned verification tooling use Python and Python packages. HTTP routes use FastAPI.
- Browser pages use locally served HTML, CSS and Vanilla JavaScript. Do not introduce React, Vue, Angular, TypeScript build pipelines or other frontend frameworks.
- Do not introduce Node.js, npm, npx, Yarn, pnpm, Bun, npm packages or a JavaScript server/worker runtime as a project dependency. This rule applies to the entire project, including development and test tooling, not only AI providers.
- Install compatible Python dependencies in the existing PBGui virtual environment through the versioned requirements files. Do not introduce an additional environment for one provider. Update shared Python packages together when compatibility requires it and verify the affected API, authentication and validation boundaries.
- Existing native dependencies used by Python packages or provider clients remain explicit dependencies: for example, the official Claude Python Agent SDK bundles a native Claude Code client. This does not require Node.js or npm. Native clients must retain the same ownership, credential isolation, tool authorization and deterministic shutdown boundaries.
- Optimizer Suite reuse restores result-owned Training/Holdout provenance and verifies it against the native training scenarios. Individual validation backtests do not define the original training plan. AI save proposals may copy an explicitly identified original run's windows through the native backend while retaining the selected strategy; arbitrary model-authored GUI metadata remains blocked and the copied plan requires the normal user review.

## Existing boundaries

- FastAPI serves the existing Vanilla JS frontend; JavaScript executes in the browser.
- PB7/PB8 bot code and external provider implementations remain separate from PBGui. Do not modify their implementation or deploy changes to remote hosts without the required authorization.
- No package installation or update runs implicitly during an AI request, login or API startup. Dependency changes belong to the existing installation/update workflow.
- AI Loop strategy experiments cannot change worker execution settings. The reviewed `optimize.gpu` block, CPU allocation and legacy population alias are immutable provider-independent inputs, including Auto/null values. Only PBGui/worker hardware selection and explicitly reviewed user changes own runtime tuning; continuations inherit these settings from the source run, not its AI-generated optimizer variants.
- Normal AI-requested Vast optimizers use the native shared-worker preparation, GPU sizing, rental and cleanup path with a durable single-job authorization. Review binds the source config, credential generation, saved rental limits and per-job convergence settings. Prepared jobs and workers cannot claim work outside that authorization; replay recovers the same input/worker identities. An explicit single-job approval is independent of an already paused shared queue and does not resume unrelated jobs. A persisted pause generation binds review, rental preflight and dispatch; later Pause/End commands retain precedence, including when the queue was already paused. Legacy authorizations retain their original pause behavior. This does not create an AI Loop or change global Auto rent/convergence settings. Local optimizer queue tools require an explicit local target. Manual row/detail Start uses the same native single-job authorization without an AI conversation: private bounded user-bound reviews freeze the existing input and cost/control snapshot, and deterministic worker identities make confirmation retries idempotent. An unpaid reservation must still own its exact input before it can resume.
- Loop display snapshots and configuration comparisons use at most two API-owned spawned Python workers, admitting at most eight concurrent requests. Owner checks, archive integrity and the existing display projection remain mandatory; these readers do not start controllers, call AI providers or modify runs. The API coalesces overlapping reads, bounds admission/cache state and joins its report workers at shutdown without stopping independent native jobs.

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

### Vast worker startup readiness

Vast `running` and the onstart host-key log are not guard-ready signals.
Before the health preflight, the controller now publishes and launches the same
locally versioned deadline guard through the authenticated `root` SSH transport.
It uses `/work/pbgui` consistently with the pinned image (no home fallback),
and copies the pinned image worker only if absent. Container credentials are
resolved inside the container from the SSH environment or PID 1 environment;
no account credential is sent over SSH or included in commands/logs. Existing
guard records and amendment state are never replaced by publication. A shared
process lock serializes onstart and SSH guards; publication has its own lock,
waits up to ten seconds for the initial record, and fails closed on errors.
The subsequent health preflight retains all identity/revision checks. Before `worker_ready`, the controller retries only this exact
missing-file preflight error and the categorized SSH authentication failure on
its existing five-second control-aware cycle, bounded by 300 seconds from the
persisted `setup_started_at` (and the unchanged rental deadline). Restarting a
controller does not reset that window. Expiry is fatal; malformed guard records,
CUDA/runtime failures and guard identity/deadline mismatches are not relaxed.
Successful health must still match the pinned PB8 revision, provider instance,
lease/job identity and authorized deadline, and meet the CPU allocation check.
