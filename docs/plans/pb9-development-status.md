# PB9 integration — optional experimental feature

**Integration target: `main`; preparation does not publish or update hosts.**

**Status: UNFINISHED / NOT FULLY TESTED — NOT A STABLE RELEASE**

Status recorded on 22 November 2026.

The PB9 integration is intended for controlled testing in the existing Cluster.
It is not a production-readiness claim. PB9 integration is incomplete, and the
full remote installation/update workflow has not been tested end to end on a
real master or VPS. No host is updated merely by publishing this branch.

## Included in this snapshot

- PB9 Run/VPS placement, signed operation family, complete-bundle materialization, runner gate, native monitoring and remote log/restart paths reuse PB8 components. Protocol, API and browser paths are tested with isolated fixtures. An actual remote live-trading migration has NOT been performed or accepted; this remains unfinished development. The local API was activated at serial 3794; this is not a remote deployment acceptance test.

- Existing PB9 Run, Backtest, Optimize and Pareto integration, using shared PB8
  components where supported. This is not a claim of complete PB8 feature parity.
- Explicit PB9 Install/Update actions in the standalone local Master Installer
  and VPS Manager, reusing the PB8 binary build and update workflow. PB9 uses
  separate sibling source and environment directories (`pb9`, `venv_pb9`).
- Config schema checks before checkout, slot-separation checks, failed-update
  launch guards, optional PB9 telemetry and default-path uninstall handling.
- PB9 Vast native CLI/cohort calibration and the worker pinned to public
  official Passivbot v9 source `bf7329b039595e65acdf5c14475078912aeede96` (including merged PR #2305).
  Immutable image:
  `ghcr.io/msei99/pbgui-pb9-worker@sha256:7ee6a7bf0f6bb0c235e87ab2988efd1d2f0529f4348f1d9c20a13d8384395bcb`.
- Shared Optimize config-list loading improvements.

## Checks completed, with their limits

- Focused offline backend and browser regression tests for the changed areas:
  **1,151 passed**, including PB7/PB8 compatibility. The full offline suite has not been run for
  this development snapshot.
- Ansible syntax checks for the PB9 local-master and VPS playbooks. Syntax checks
  do not execute the remote workflow.
- An actual PB9 source-bound wheel build, installation and native extension
  verification in an isolated temporary environment. This did not replace an
  existing user installation or perform a complete fresh-host install.
- Worker native build, CLI/cohort probes and offline BTC/ETH/SOL/HYPE market-age
  tests. The exact public manifest and image digest were verified anonymously.
- Local PBGui API activation at serial 3772 and real browser checks of local
  Update PB9, remote Install PB9 and the native worker revision. No installation
  or rental was triggered during those browser checks.

Historical GPU throughput measurements used earlier source/image revisions.
They do not validate physical GPU execution or performance of the latest pinned
worker. See the separate throughput investigation and worker receipts for those
revision-specific limitations.

## Latest Results integration check (22 November 2026)

- Local API serial **3794**. In the real PBGui browser, **All** completed with
  **5,435/5,435** results. A native PB9 row opened a new Run draft from both PB9
  and All views; its host assignment remained **disabled**, and it was not saved.
  Native balance/equity, PnL, drawdown and exposure charts and the PBGui HYPE
  price overlay were inspected. The original Run tab was restored afterwards.
- **125** focused reader/index/PB8 backend tests and **210** native PB9/archive
  tests passed. **85** frontend-logic/browser regressions passed. The result and
  archive workflow subset had 11 passes and one schedule-dialog timeout;
  the failed case passed in its isolated rerun. This timing failure is retained
  as a test limitation, not represented as an uninterrupted green batch.
- Fable reviewed the bounded reader/cache and UI changes in narrow, tool-free
  planning/review passes. This is not a review of every historical branch commit.
  Its symlink-key concern was checked with a real isolated reader and a symlink
  fixture. The path list is canonical before cache lookup. Sliding 60-second
  idle expiry is intentional for active long pagination; fixed owner/root/name
  binding, entry/path quotas and per-page file checks remain in force. Shared
  LRU eviction can still invalidate an older concurrent pagination token.
- The complete All list is a background load. Subsecond completion of thousands
  of results is **not** claimed; transient browser timeouts were observed and
  are covered by bounded same-page retries. Cross-tab transport performance is
  not exhaustively qualified. No full offline suite was run: the suite gate
  required focused checks for this task.

## Not tested end to end / still incomplete

- Follow-up: PR #2305 is merged in official v9 source `bf7329b03`. That revision
  has passed 380 focused native tests (2 skipped), isolated wheel/source
  verification, two bounded offline PBGui-source backtests and local PBGui
  config/metadata and editor checks. This does not qualify remote installation
  or physical GPU execution. The matching Vast worker above is now rebuilt,
  published and pinned to `bf7329b03`; its source/native/CLI/cohort and
  offline Hyperliquid probes passed. No physical GPU run was performed
  with this revision.
- A full additional PB9 install and update on a real remote master or VPS,
  including failure recovery and continued operation of existing PB7/PB8 bots.
- A physical GPU run with the latest pinned worker revision.
- Complete PB9 Run/VPS parity: deployment, process control, status/logs and
  same-account protection still need a systematic practical acceptance pass.
- PB9 AI suggestions and AI Loops integration.
- PB9 Strategy Explorer integration, intentionally deferred until last.

## Controlled test prerequisites

After publication to `main`, update PBGui on the controlling master and **all
active Cluster state replicas** to the PB9-aware revision and restart their
updated PBGui processes. Wait for automatic Cluster handshakes to confirm
`pb9_instances_v1` support everywhere before publishing any PB9 config, including
one assigned to the local master. Existing PB7/PB8 runtimes remain independently
configured; PB9 installation is a separate explicit action only on selected PB9
execution hosts. No additional Cluster or local-only execution mode is introduced.

The rollout guard remains enabled. Older PBGui versions reject unknown PB9
operations in the shared signed operation stream; silently filtering these
operations is not supported. Once PB9 operations are published, do not downgrade
a replica to a version that cannot read them. Keep a known compatible revision
for rollback. Every remote update or restart requires host-specific authorization;
merging or publishing code does not authorize those actions.

Fresh installer defaults remain PB7/PB8. No PB9 source checkout, virtual
environment, config assignment or trading process is created merely by updating
PBGui. Main integration does not certify the outstanding real-host/GPU checks
listed above and is not a new stable-release qualification.

The normal PB9 installer follows the official Passivbot `v9` branch. The Vast
worker is pinned separately to the PR #2305 commit above; cloud preparation
rejects a local source revision that differs from the worker. An installer update
can therefore replace a temporary PR checkout with the official branch head.
Check the resulting source identity before cloud testing. This snapshot does
not silently deploy the PR commit to other hosts.

Keep a known working PBGui revision and backups of the selected host's settings
and configurations before a controlled remote test. Do not treat passing unit
tests, a successful local API restart or an available Install button as proof
that remote deployment or live trading has been accepted.
