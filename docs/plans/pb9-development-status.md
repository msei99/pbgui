# PB9 integration — unfinished development snapshot

**Branch: `dev-pb9-integration`**

**Status: UNFINISHED / NOT FULLY TESTED — NOT A STABLE RELEASE**

Status recorded on 21 November 2026.

This branch makes the current PB9 integration available for controlled testing.
It is not a production-readiness claim. PB9 integration is incomplete, and the
full remote installation/update workflow has not been tested end to end on a
real master or VPS. No host is updated merely by publishing this branch.

## Included in this snapshot

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

Update the controlling master and chosen test host to this same development
branch before attempting the new PB9 installation action. This step requires
explicit host-specific authorization. Fresh installer defaults remain PB7/PB8;
PB9 installation is a separate explicit action.

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
