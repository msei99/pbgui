# PB9 parity audit — 2026-11-16

This is a verification ledger, not a claim that PB9 has complete PB8 parity.
Production trading and paid cloud execution are not part of the checks below.
PB7/PB8 bot source and VPS hosts were not modified.

## Corrections in this change set

| Area | Correction and evidence |
| --- | --- |
| Installation | Local source and environment are siblings of PBGui, at `/home/mani/software/pb9` and `/home/mani/software/venv_pb9`. The move preserved the existing installation. |
| Native data | Preserve native repo CWD in Backtest and Optimize. The downloaded native store was moved intact to `pb9/caches`; 49 catalog path pairs were rebased and checksums verified. PBGui raw candles were not copied. |
| Launcher | PB9 Backtest reuses the PB8 persistent launcher with an explicit native environment and exact owned-unit cancellation. Durable results remain authoritative when an old PID has been reused. |
| Readiness | Inspect the same native cache as execution, require native cached metadata offline, retain full acquisition warmup and ask the native source loader to admit the first requested day. File presence alone is insufficient. |
| Drafts | Shared guarded serialization preserves unsaved Backtest names, values and sections on reload. Credentials are rejected; clean source documents reload afresh; stale requests cannot replace a newer editor. |
| Results | Enable the existing Optimize-from-Result handoff for native results. Keep native validation and result ownership. |
| Source defaults | New native Optimize drafts show the PBGui source selected by the queue policy. An explicit native-source preference is preserved. |
| Storage | Account for PB9's checkout, native cache, configured environment, logs and Run configs. A configured PB9 environment cannot be offered as an unused environment. |

## Browser checks

The real authenticated Brave session was inspected through a separate agent tab;
the user's unsaved Optimize tab was not reloaded. No configuration was saved,
no trading instance was started, no candle preload was triggered and no cloud
resource was rented.

- Run: real PBv9 menu, Add Instance, shared live controls, native fields and Guide.
- Backtest: new draft, PBGui source, Additional Parameters, typed name/balance and
  reload recovery. Native Result-to-Optimize is also covered by an offline browser
  regression using the real shared controls.
- Optimize: supported exchange catalog, native policy/bounds, Additional Parameters,
  shared page geometry and five Vast settings views. Paid actions report the
  unpublished native worker explicitly.

## Remaining limitations

- Local PB9 test commit `e434e65` on `pbgui/external-source-prefix` supersedes the
  insufficient proof-reuse-only commit `4c20a1c`. Explicit external directories
  define each coin/source's available simulation window, including Hyperliquid
  offline. This does not assert market inception or relax native-store admission.
  It is a local test build, not an upstream release or a completed full optimization.
- Native offline operation remains dependent on valid cached market and coverage
  metadata. Internal gaps and incomplete offline tails remain subject to native
  checks; accepting a dataset's start does not make arbitrary missing data valid.
- A normal PB9 installer/update workflow and PB9 VPS deployment are not complete.
- Vast uses shared controls and runtime profiles, but the native image is not
  published as an immutable registry digest and no physical GPU rental test has
  been authorized or run. Local candidate evidence does not establish cloud parity.
- Native AI Loops are not adapted. Strategy Explorer remains deferred as requested.

## Validation status

The test-suite gate selected focused testing; no full suite was run.

- Broad audit: 121 explicitly selected files, 4,598 passed and eight UI failures.
  Five failures concerned old asset/binding assertions; three isolated-function
  fixtures lacked the shared draft helper or URL/history context after the change.
  These fixtures were corrected without removing the behavioral assertions.
- Latest native Optimize/default-source and shared PB8/PB9 OHLCV checks: 116 passed.
- Logging/guide checks: 46 passed with one missing expected-service registration;
  that registry expectation was corrected and its focused recheck passed.
- The strengthened common result-to-Optimize action passed for both generations.
- Final shared/native editor browser rerun: 272 passed, including all eight
  formerly failing cases. A separate latest-source check of the strengthened
  shared handoff passed. After the last serializer change, 22 focused checks
  passed, covering inline TOKEN coin overrides, recursive credential rejection
  and current shared asset contracts. Counts overlap; they are not unique totals.

Before the source-window correction, real offline HYPE Readiness reported native
source rejection for both Bybit and Hyperliquid. The exact acquisition warmup start
is `2024-12-05T07:59:00`; it is separate from the trading-age gate.
Historical synthetic smoke tests do not prove that this external-source workload
succeeds.

The actual native Optimize queue currently has `use_pbgui_market_data=False`.
An empty new-draft source therefore preserves the user's explicit own-data choice;
this preference was not changed by the audit. The new default-path behavior is
covered for unset, enabled and disabled settings using isolated fixtures.

API serial was incremented to 3736 and only the local API service was restarted.
The real browser subsequently restored an unsaved native Optimize draft and its
own-data choice. No PBGui commit or push was performed during that initial audit.

## Authorized local source-window test build

The user subsequently approved activating and committing the native correction.
The first cached-proof-only fix was insufficient for Hyperliquid and was replaced
by the explicit external-window policy in native commit `e434e65`. Native checks:
351 passed, two skipped; documentation routing passed. The real offline reader
accepted 665 Bybit and 893 Hyperliquid HYPE rows from the existing source files,
with unchanged source hashes and blocked network access. This is a bounded native
reader check, not a claim of a completed full Backtest or Optimize run.

PBGui Readiness now retains exact minute bounds instead of passing an end time
through the native day-only setter. It restores the manager range and temporary
helper context even on error. The native source identity and Rust pairing are
checked again after activation. Existing queue snapshots are not rewritten.

Final activation checks: PBGui accepted clean native commit `e434e65` and the
existing Rust/source pairing. The authenticated browser reported source ready
for both Bybit and Hyperliquid on the existing HYPE configuration. No config was
saved and no real job was enqueued during this check. Final focused PBGui
Readiness/guide checks: 99 passed. API serial is 3738; no PBGui commit or push
was performed for these additional Readiness corrections.

## Local native follow-up after Fable review

The local PB9 follow-up rejects existing unreadable/empty external daily files
instead of treating them as an ordinary late start. Healthy same-day alternatives
remain supported. Selected coin and eligible BTC daily-file metadata revisions now
invalidate automatic prepared-cache reuse, including combined exchange caches.
Source changes across preparation, publication and cache loading are rejected;
loaded materialized arrays are released when the final source check fails.
This uses filesystem revisions, not cryptographic candle-content hashes.

Insufficient post-warmup coverage is reported per coin. Documentation now limits
requested/actual range-manifest guarantees to offline snapshots. Native-store
listing proofs and the existing global tradability gate remain enforced.

Validation: 496 focused native tests passed, two skipped; AI documentation routing
reported zero errors and its existing documentation-size warning. The actual
offline source reader accepted 665 Bybit and 893 Hyperliquid HYPE rows, with network
access blocked and unchanged raw source hashes. No complete optimization was run.
The user approved local activation. Native commit `93ff594` contains this
follow-up. PBGui's clean-source identity check accepts the commit and verifies
the existing Rust/source pairing. Its isolated offline Readiness check reports
"Configured source available" for both Bybit and Hyperliquid HYPE. No push,
deployment, or existing queue-snapshot rewrite has been performed.

The user subsequently requested public PR publication. The fix was ported in a
separate worktree onto upstream v9 `f7832f56d`, retaining its new cache publication
lock and adding a source-change-during-lock-wait regression. Focused validation:
585 passed, two skipped; the actual offline reader and Rust pairing checks passed.
The PR commit is `1b85ea6cc`; [upstream PR #2268](https://github.com/enarjord/passivbot/pull/2268)
is open, ready for review, and mergeable at publication. CI is still running.
The active local PB9 installation remains on `93ff594`; no deploy or queue change
was performed as part of publication.

## Native Pareto-to-Backtest handoff

The shared queue client now keeps PB9 submissions, status and Open Queue in the
PB9 namespace. The native Pareto endpoint uses the installed Passivbot
`extract_result_config` reader before config handoff: `backtest.coins` and other
known result-only fields no longer enter strict authored-config validation.
Unknown authored fields remain rejected; sparse policy, dates and GUI provenance
are preserved. Validation: 324 helper/API tests and 16 browser/guide tests passed.
All 38 existing candidates passed the real isolated conversion check; the native
Backtest validator accepted the converted config and source files were unchanged.

The user approved restarting only the local API. It restarted with serial 3739;
PBRun and the data-service process start times remained unchanged. In the
authenticated browser, the real PB9 Results → Paretos → View JSON path returned
schema `v9.0.0` and bot policy without `backtest.coins` or result metrics. No real
job was queued or started for this activation check. No commit or push was made.

## Native optimizer workload display

The local queue estimate was still using the PB8 snapshot loader for PB9 files.
The native adapter now supplies its own gateway and existing native training data
plans to the shared candle-count routine and bounded snapshot cache. Cache entries
are isolated by estimator owner and native source revision. Training scenarios are
counted once; Holdouts, warmup and optimizer iteration/population factors are not
included. Unresolved selectors and dates remain unknown.

Validation: 62 focused workload, native API, retained Vast-performance and Guide
tests passed. A read-only probe of all three existing native snapshots produced
1,926,720 coin candles each, stable on repeat, with zero PB8-helper calls, no
estimation errors and unchanged snapshot hashes. The previously approved local
API restart activated serial 3740. The authenticated PB9 Queue displayed 1.93M
for all three existing entries. No jobs, queue settings or remote services were
modified, and no commit or push was performed.
