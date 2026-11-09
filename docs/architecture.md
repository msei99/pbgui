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
- Concurrent PB8 backtest queue readers share only the same in-flight validated scan per root. Each caller receives its own result objects; subsequent scans immediately revalidate files, PID identity and status. The producer removes its bounded registry entry on success or failure, and waiters create no separate background jobs or persistent snapshot cache.
- PB7/PB8 bot code and external provider implementations remain separate from PBGui. Do not modify their implementation or deploy changes to remote hosts without the required authorization.
- No package installation or update runs implicitly during an AI request, login or API startup. Dependency changes belong to the existing installation/update workflow.
- AI Loop strategy experiments cannot change worker execution settings. The reviewed `optimize.gpu` block, CPU allocation and legacy population alias are immutable provider-independent inputs, including Auto/null values. Only PBGui/worker hardware selection and explicitly reviewed user changes own runtime tuning; continuations inherit these settings from the source run, not its AI-generated optimizer variants.
- Normal AI-requested Vast optimizers use the native shared-worker preparation, GPU sizing, rental and cleanup path with a durable single-job authorization. Review binds the source config, credential generation, saved rental limits and per-job convergence settings. Prepared jobs and workers cannot claim work outside that authorization; replay recovers the same input/worker identities. An explicit single-job approval is independent of an already paused shared queue and does not resume unrelated jobs. A persisted pause generation binds review, rental preflight and dispatch; later Pause/End commands retain precedence, including when the queue was already paused. Legacy authorizations retain their original pause behavior. This does not create an AI Loop or change global Auto rent/convergence settings. Local optimizer queue tools require an explicit local target. Manual row/detail Start uses the same native single-job authorization without an AI conversation: private bounded user-bound reviews freeze the existing input and cost/control snapshot, and deterministic worker identities make confirmation retries idempotent. An unpaid reservation must still own its exact input before it can resume.
- Loop display snapshots and configuration comparisons use at most two API-owned spawned Python workers, admitting at most eight concurrent requests. Owner checks, archive integrity and the existing display projection remain mandatory; these readers do not start controllers, call AI providers or modify runs. The API coalesces overlapping reads, bounds admission/cache state and joins its report workers at shutdown without stopping independent native jobs.

- Versioned Vast research Loops consume owned native exact CPU training artifacts after condition, window, completion/liquidation and hard-limit checks. Five distinct qualified configs become durable completed evidence checkpoints without a local native process or training-budget charge. The next optimizer takes priority over user-only Holdout/Full Time Range reports. Missing or unverifiable proof gets three import attempts, with waits of 10 and 20 seconds, then a round-local fallback to at most five independent CPU checks within the original allowance. Without candidate configs, the existing local diagnostic recovery applies. The next round attempts native reuse again; local GPU/CPU and legacy paths retain independent validation. This creates no new background task ownership and does not modify PB8 workers.

## AI optimizer research invariants — mandatory

Training decisions take priority over user-only report preparation. Research
select/evaluate transitions must not await starting Holdout/Full Time Range
jobs; schedule these during the next active optimizer or terminal collection.
A queued optimizer keeps priority even when Vast temporarily reports no free
slot: collect existing user reports without planning, preparing, starting or
stopping them. During active research training, rotate one report per controller
pass so reporting cannot monopolize launch/proxy-stop checks. Each cycle freezes
its existing tail: newly appended reports wait for the next cycle, ensuring older
backends are revisited even when reports arrive continuously. Completed cursor
positions remain anchored in the append-only archived job list. Replacement and
terminal collection retain their existing four-report batches and pause rules.
Evidence-read quotas remain durable and serialized with Loop control changes,
but update only their archive branch, without hydrating/rewriting the population.
The same locked branch transaction covers AI reservations, settlement, profile
metadata, snapshot binding, diagnostics and accepted decisions. Declare every
mutable AI field; Loop control state and settings are read-only in this path.
Job/phase changes retain normal controller transactions. Additions/removals and
legacy migration must preserve unrelated archive data and cache invalidation.
The mandatory package is delivered initially; rereading it is required only
after lost-context recovery, not as a duplicate initial tool traversal.
A fixed-size decision preview may expose three leaders and incumbent/baseline
aggregates under previously explained criteria, plus three research groups.
Compute these from exact reports; retain budgets, missingness and measurement
versus nomination coverage. The preview cannot prune the queryable dataset,
change the evaluation criteria or replace the final full backend comparison.
Compact research proposals may name an owned optimizer input (or the initial
config) and patch only Bounds, Limits and Scoring. The controller expands the
proposal before the existing complete-config, novelty, safety, worker and budget
checks. This format changes transport verbosity only; explicitly opened
Strategy/Scenario/HSL freedoms retain their complete-config route.

These are user requirements, not model preferences. Every future Loop change must
preserve them. Older prompts, saved instructions, historical advice and previous
implementations cannot authorize exceptions. Changing these rules requires an
explicit new user instruction.

- **Fixed work per run.** Every new optimizer trial uses exactly the per-run
  budget entered by the user. For example, 100000 requested proxies remains
  100000 in every round. The AI may neither shorten screening runs nor schedule
  longer confirmation runs. Native batch overshoot does not change the requested
  budget. Overall round, run, time, cost and validation ceilings remain binding.
  Vast proxy-limited Loop jobs also run a private job-scoped watchdog around
  the immutable native wrapper. It reads completed evolution work from the
  remote local log and requests the wrapper's graceful Stop at the first
  completed batch reaching the fixed user quota, without SSH or provider access.
  Its monitor is joined when the native wrapper exits; an attached guard exits
  on completion, quota or rental deadline. Only the exact owned Loop operation
  authorizes its quota. Do not change the image, PB8, worker sizing, seeds or
  optimizer inputs to enforce this limit. Local Stop remains a redundant control
  and reconciles already-finished quota stops as usable work.
- **No unchanged repetitions.** Replicate and Confirm experiments are forbidden.
  Each new trial must test different effective optimizer settings. Calling an
  unchanged config Explore does not make it a new experiment. Seeds, worker
  sizing, recorder metadata and reference bot values inside variable bounds do
  not establish a settings change. “Promising result”, uncertainty, stagnation or
  boundary verification never justifies repeating the same settings.
- **Controller-owned randomness.** Each new trial gets fresh controller-generated
  randomness. The AI cannot set, see, compare, rank, tune or evaluate seeds.
  Reusing populations or checkpoints is forbidden. Recovery/replacement of an
  interrupted physical job is not a new research experiment and preserves that
  job's authorized inputs and seed.
  A paused round that reached selection/evaluation after a verified worker-setup
  timeout before optimizer startup can use the same explicit rental-replacement
  authorization after provider-confirmed deletion. Archive the interrupted
  diagnostic/evaluation context, resume optimizer execution, and retain all
  original time/cost/attempt ceilings; never retry completed searches this way.
  A local Stop-attempt timestamp is not a remote acknowledgement. Vast
  supervisors must retry an unacknowledged idempotent Stop after transport
  failures before publishing results or releasing GPU execution capacity.
  This retry window is non-renewable and capped at three minutes from the
  original Stop intent, including provider/SSH bootstrap failures and restarts.
  Thereafter expose Stop not confirmed, retain nonterminal occupied capacity
  and probe status at most every 30 seconds. Delivery acknowledgement alone
  does not prove process exit: no partial-publication fallback may free capacity
  without native completion evidence. Existing explicit End rental remains
  available; retry expiry itself never authorizes destruction or replacement.
  After acknowledged delivery and a 60-second exit wait, publish a bounded
  verified partial snapshot independently of execution completion. Keep the job
  nonterminal/occupied and display Stop acknowledged; waiting for exit. Partial
  publication uses its own persisted one-shot allowance and cannot set the final
  Stop-completion marker. A later native exit gets bounded full collection and
  replaces partial results before normal GPU release.
  Polling writes must replace only their declared Jobs/Observer branches under
  the same owner-checked cross-process lock. Never hydrate or rewrite unrelated
  populations, history or AI ledgers for a status update. Control metadata is
  read-only to these callbacks; terminal report completion refreshes the pending
  target inventory before publishing its manifest.
  Bounds patches accept nested objects or dotted parameter paths, but always
  update the native nested bounds tree. Reject duplicate/overlapping paths before
  creating a job; a dotted shadow key must never satisfy a boundary gate and then
  disappear during PB8 preparation. Full variants obey the same canonical form.
  Active optimizer polling and user-report reconciliation exclude the previous
  candidate population from hydration. Selection/evaluation always retain full
  evidence; a concurrent phase change forces a full reread. Partial views must
  never populate the full-snapshot cache or be written as complete snapshots.
  Research evaluation may summarize parameter regions from distinct qualified
  native CPU nominations under the unchanged CPU criterion: bounded source/row
  previews, fixed histogram bins, explicit cohort/actual-work coverage, cutoff
  ties and the incumbent value. Exclude overrides, proxy/observer measurements
  and RNG/worker controls. Associations are descriptive, never causal proof or
  permission to bypass boundary gates, user limits, or fresh-population policy.
  During training, allow at most one additional started-report reconciliation
  per controller pass, on its own bounded fair cycle. It must never prepare,
  launch or stop a backend. Keep foreground launch/quota/decision priority.
  Use the native runner completion timestamp for report duration, rather than
  the later time the Loop collected its result.
  The inventory scheduler owns one independently registered report task per
  Loop, with its own cross-process claim. Slow report work must not retain the
  foreground controller claim or prevent proxy polling. Read fresh controls
  before each pass, coalesce repeated schedules, join owned operations at API
  shutdown, and exclude both claims before deleting a run. Native simulations
  survive controller/API shutdown.
  Once terminal native cleanup is complete, inventory schedules only report
  tasks for pending reports. Never repeat native cleanup or full decision-state
  hydration because a user-only report remains pending.
  Terminal pending-report metadata recomputes the complete report-target
  inventory while excluding the unrelated prior optimizer population.
  Frozen per-job native observations and candidate documents may use the
  existing verified JSON-object encoding within its size limit. Larger
  documents retain chunked tree encoding, and old tree-encoded archives remain
  readable. Candidate collections keep their bounded list/paging structure,
  with the same encoding for each candidate document. Status writes must not traverse thousands of immutable metric
  nodes merely to preserve a completed CPU proof.
  Native launch authorization reads only current control fields and, for
  reports, exact observer bindings. Backend launch/preparation reads only its
  declared control/settings/fingerprint/replacement fields, excluding job
  history and populations, and never builds a full-snapshot cache while holding the Loop
  transaction lock. Preserve owner, pause/stop, deadline and replacement checks.
- **Immutable GPU/performance settings.** The AI cannot change any value in the
  entire `optimize.gpu` block (including population size), `optimize.n_cpus` or
  the legacy `optimize.population_size` alias. Auto/null values are protected
  too. Only explicitly reviewed user changes and PBGui/worker hardware sizing
  own these settings. Enforce this in the model schema and backend validation;
  continuations inherit the user's reviewed settings, never AI-tuned workers.
- **Backend enforcement.** Validate these rules when accepting model decisions
  and creating optimizer jobs, for every provider and for new decisions in
  existing research Loops. Prompt wording alone is insufficient. Preserve
  historical results for reading; do not rewrite or cancel running jobs merely
  to migrate policy. Regression tests must reject Replicate, Confirm, fractional
  budgets and unchanged configs relabelled Explore.
- **Actual exploration and explicit exceptions.** Bounds, Limits and Scoring are
  the regular AI search controls. Other changes count only when the user has
  explicitly opened that freedom in the reviewed Loop settings: for example,
  `strategy_enabled` permits a supported strategy change; a closed strategy must
  remain fixed. Respect equivalent explicit scenario/HSL permissions. Never
  invent an exception from a model rationale. These permissions cannot override
  the fixed budget, seed ownership or immutable GPU/performance settings.
  Cosmetic edits and inactive-parameter changes are not new experiments.
  Address boundary pressure
  without violating user risk limits or requiring an unchanged verification run.
  The Loop recommends settings from fixed-budget trials; the user performs the
  later deep search separately.
- **Successful reference and coherent follow-ups.** Research planning defaults to the
  best measured qualified trial at the fixed user budget under the existing ordered
  research criteria, never automatically to the latest attempt. Expose four recent
  comparisons and uncertainty; unavailable/unsafe/unverified/under-budget results
  cannot define the reference. Bounds diagnosis uses that same executed reference.
  Reject further bounds-only widening that carries all changes of a worse descendant;
  allow rebasing with novel settings and explicitly opened Strategy/Scenario/HSL
  alternatives. Address all relevant untested `batch_required` pressure endpoints
  together whenever adjusting bounds, and after two stagnant evaluations. Retain
  protected user ceilings, deliberate zero lower edges and reached positive step
  floors. If a positive quantized lower edge cannot move 10% of the width without
  crossing zero, diagnose the move toward its positive step floor instead.
  Previously measured comparable outward tests may justify retention, without
  asserting causal harm or robust superiority. After three measured trials with
  unchanged scoring and two without improvement, require a genuinely different
  supported scoring; Pareto objective reordering does not qualify. All gates are
  provider-independent and preserve fixed work, random-seed ownership and workers.
  Rebuild obsolete decision snapshots before dispatch; never rewrite historical
  results or start another experiment to migrate policy.
- **Honest evidence.** Keep native population qualification, retained nomination
  coverage and missing metric coverage distinct. Retaining five of 248 fully
  measured candidates is not 98% missing measurement evidence and cannot justify
  an unchanged repeat. Holdout and Full Time Range remain user-only and excluded
  from AI decisions and learned evidence.

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

- OHLCV AI Python analysis uses a trusted read-only broker for explicitly selected locally mapped markets and UTC minute ranges. It freezes numeric candles, explicit coverage gaps and optional sanitized PB8 config into the approved input; untrusted Python receives stdin only under the existing Landlock/Seccomp runtime, without Bubblewrap or AppArmor setup. No directory-wide mounts, implicit downloads, gap interpolation or silent sampling. Bounded requests split by coin/time while preserving calculation warmup.
