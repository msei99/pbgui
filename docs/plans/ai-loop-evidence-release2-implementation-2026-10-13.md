# Release 2 evidence prototype — implementation record

> Superseded model policy: the user removed manual per-model acceptance, profile activation and separate probe/test requests. See [the current policy](ai-loop-model-context-policy-2026-10-14.md). Earlier gate descriptions below are historical, not active requirements.

Status: local prototype with offline verification and the bounded real-provider acceptance described below. Not published, committed, deployed or activated by an API restart. No rental was changed. See the earlier test-boundary incident below.

**Update 2026-10-14:** [Bounded real-provider acceptance](ai-loop-evidence-release2-live-acceptance-2026-10-14.md) now passed for ChatGPT `gpt-6.1-sol`/high and one free OpenCode chat model. The previous statements below about outstanding live gates describe the earlier offline-only state. Other OpenCode models/protocols remain unaccepted; measured profiles have not been activated in production.

**Final preparation:** See the [release checklist](ai-loop-evidence-release2-release-checklist-2026-10-14.md) for exact selection scope, explicit owner-bound profile installation, document inclusion and retained limitations. The final focused group passed **944 tests**, and both isolated actual-browser capability checks passed. A stale read-only preview now stays provisional rather than preventing the actual start/resume context renewal. Serial is **3238**, Loop frontend asset version **55**. No profile activation, commit, release or restart has been performed.

## Implemented

- Fresh isolated decisions for both providers, using one shared mandatory/optional evidence contract. Historical observations and assessments are not automatically injected into model context.
- Exact current goal/safety values, missing/null/zero distinctions, metric-column tables, configuration deltas and lossless duplicate aliases. All original candidate records remain in the immutable snapshot.
- Separately budgeted compact findings, deterministic legacy status projection (including `rejected_exact`), stable selection and explicit omission counts. Overlong optional text is archived and referenced without an extra model round or pause.
- Owner-private snapshots and manifest-bound archive references with independent byte checksums. A compact archive index keeps older originals reachable after eviction from the mutable UI index. Existing originals are never overwritten when an old ID is retried.
- Bundled bounded JSON pages and exact string fragments. Decision/dialog/generation quotas, idempotent native call IDs, no-progress detection, reserved mandatory recovery capacity and complete required-page coverage after Codex compaction.
- OpenCode chat/completions, Responses and Messages tool dialogs, preserving native IDs and reasoning/signatures. Complete SSE assembly with meaningful-progress watchdogs, bounded frames, duplicate event handling and refusal to use partial results. Nonstreaming calls require a finite measured profile.
- Actual cumulative OpenCode wire-budget checks, initial optional-evidence reserve, durable per-call reservations and usage reconciliation. Positive confirmed token/USD limits are mandatory for paid Zen decisions; uncertain usage is retained across retries/resume.
- Recoverable controller pauses and API recovery with model/budget selection, checksum validation and an actual new-provider payload check before unpausing. Interrupted requests need explicit acknowledgement; unknown context or tool capability never silently falls back to the old format.
- Visible budget-derived native archive limits for new Loop-owned jobs. Removed the silent 512-artifact and three-source reader limits. Historical advice uses whole sources, or explicitly labelled native extrema/crowding thinning only when semantics are safely reconstructable; otherwise it stays optional and bootstrap can continue fresh.
- Provisional source/archive/cost previews, optional historical advice, visible recovery limits and persistent nonsecret drafts. Final bootstrap budgets are recalculated after interpretation. EN/DE help describes the same behavior.

## Profile and release boundary

Offline profiles in tests are synthetic fixtures, including deliberately enlarged contexts for old isolated policy tests. They do not create acceptance files in production or claim support for any model.

Production profiles require explicit successful tool, context and untruncated-output measurements. They bind owner (`profile_owner`), provider, model, account profile, reasoning, service tier, protocol and the deliberately maintained `ADAPTER_PROTOCOL_VERSION`. Ordinary source edits do not invalidate model gates. Incompatible wire/tool changes require an explicit protocol-version bump and new acceptance. ChatGPT context telemetry refreshes its verified context cache; observed larger windows never expand the gated capacity automatically. Expired telemetry can be renewed by one isolated tool-free capability check, recording usage and outcome; previews never trigger this check.

The current pager defaults are provisional: 5,000 proxy tokens per page, factor 1.25, 512 wrapper tokens, 64 calls per dialog and at most two dialogs. Recovery capacity is derived from the profile's recovery passes and reserves, not a fixed candidate count. Real acceptance may require smaller pages and repeated boundary tests.

Before release, perform separately authorized, bounded **real Codex and OpenCode gates** for the intended models/protocols. Verify cold context discovery (at least 256,000), actual runtime configuration, exact untruncated tool-page delivery, native follow-up IDs, compaction recovery where applicable, SSE or a finite nonstreaming latency profile, and reported usage. Only then record measurements with `ModelProfiles.accept`, passing the server-derived `profile_owner`. No profile is promoted from offline mocks.

A verified profile alone cannot guarantee 100 rounds under provider outages, expired authentication, exhausted subscription usage, missing data or rental/Loop deadlines. Current mandatory evidence exceeding the selected profile still causes a diagnosed, recoverable pause.

## Verification

- Mandatory test-suite gate: **focused**; full suite explicitly not run.
- Offline native-shaped Codex recovery, API resume, workflow and real browser navigation checks: **77 passed** (temporary fixtures and mocked endpoints, no screenshots).
- Final focused regression set: **918 passed**, 1 warning. Provider wire/hash checks: **418 passed**. Final compression/recovery checks: **77 passed**. These sets overlap and must not be added together.
- Python compilation, JavaScript syntax and `git diff --check` passed. Previous prototype API serial: **3233**; frontend asset version: **54**. No commit or API restart was performed.

## Claude review corrections

1. Snapshot reuse now verifies an input digest covering the supplied evidence, repair feedback, frozen instructions, settings and comparison context. JEV and controller repairs create new immutable snapshots and input-bound ledgers. Identical transport retries reuse their snapshot and ledger. Paid reservations and model-call caps remain cumulative across revisions of the same round/stage.
2. Expired ChatGPT context telemetry is renewed during start/resume or decision preflight with a bounded isolated tool-free subscription check. Missing or insufficient telemetry still pauses safely. Usage/outcome is persisted, concurrent preflights recheck after a shared instance lock, and the probe runtime always closes. No production probe was intentionally executed in this review.
3. Gate identity now uses an explicit adapter protocol version rather than hashing complete source files. No production gate is inferred or migrated from a source hash.
4. The first optimizer job uses the interpreted rubric when deriving its native archive budget.
5. Bootstrap source selection, initial dispatch and resume use the same complete native wire envelope and initial context reserve, including reasoning and streaming configuration.
6. SSE progress is determined from new deltas/items instead of serializing the accumulated state twice per event. Chat text/reasoning is accumulated in chunks. Keepalives and empty deltas remain non-progress.
7. Malformed complete tool arguments produce a charged native tool error with the original call ID and can be corrected in the same dialog. Incomplete streams still never execute tools. Messages replay uses a legal object input and marks the native tool result as an error.

Also corrected: terminal-loop expiry runs hourly from the registered controller scan; a retention storage failure cannot block ordinary controller scheduling. Interrupted/disputed proof remains protected. Native-source archives receive a lightweight skip-cache marker, exhausted OpenCode contract corrections use `evidence_recovery`, and appended instructions explicitly enforce HSL Off/On/Auto even with older frozen base prompts.

Review verification: the expanded focused set passed **939 tests**, with one existing Pydantic namespace warning. After the final Messages replay adjustment, the affected evidence/dialog/review subset passed **77 tests**; these sets overlap. Full-suite execution remains unnecessary under the focused gate. Python compilation and `git diff --check` passed. Final API serial: **3235**. Real Codex/OpenCode gates remain outstanding.

Test-boundary incident: an initial new regression used an instance transport override that the existing test wrapper deleted after its first call. A later test call reached the real Codex adapter and failed authentication. It did not establish any acceptance result. The wrapper now restores instance overrides correctly; the new regression module additionally fails immediately on any attempt to obtain a real chat service. Subsequent verification used fake transports and temporary stores.

## Context-probe callback follow-up (2026-10-14)

Claude identified a missed integration defect: the isolated probe enabled Loop mode without installing the callback invoked by `CodexRuntime.chat`. The probe now installs an explicit no-op callback because it owns no evidence coverage. The shared chat consumer obtains optional callbacks with `getattr` and invokes only callable handlers, for both thread compaction and ordinary turn events. Normal evidence decisions retain their existing coverage handler.

The new regression executes the real `start_thread`, `chat` and `_read_stdout` paths with native-shaped JSON-RPC events. Only the RPC/subprocess boundary is replaced. It covers thread compaction without a turn ID, reasoning, context telemetry, a completed answer and successful cache renewal. A second case removes the callback to verify safe optional dispatch. Both assert that no real subprocess starts, the probe closes, and a subsequent preflight uses the renewed cache.

Focused verification: **159 passed**, one existing Pydantic namespace warning; Python compilation and `git diff --check` passed. API serial: **3236**. No real provider gate, restart, commit or release was performed. The two nonblocking retention deviations remain open: the expiry age currently starts at snapshot creation rather than loop completion, and deleted-loop snapshots are not swept. Real Codex/OpenCode acceptance remains required before publication.

## Read-only calibration against the saved problem case

The additional production-data read was explicitly approved separately. No production state was written. Aggregate measurements: [JSON](ai-loop-evidence-release2-prototype-measurements-2026-10-13.json). These are the implemented projection of the current saved record, not a new claim of exact reconstruction of the original historical provider request. The earlier SHA-verified reconstruction remains in the original measurement report.

| Phase | Mandatory proxy tokens | Bounded pages |
|---|---:|---:|
| Interpret | 17,781 | 5 |
| Bootstrap before optional-source budgeting | 71,669 | 23 |
| Bootstrap after optional-source budgeting | 19,473 | 6 |
| Select | 22,578 | 7 |
| Evaluate | 45,441 | 13 |

The findings package contains 16 of 31 eligible findings within its own 6,000-token budget; the remaining 15 originals are optional. Bootstrap safely defers the complete 26-candidate historical source because its native semantics cannot be reconstructed and continues fresh. The measured largest delivered page is 4,997 proxy tokens under the provisional 5,000 profile.

Selection stress envelopes remain context-bound: 100 / 512 / 1,000 distinct candidates require 35,005 / 105,947 / 189,973 mandatory proxy tokens. Larger pages alone cannot solve this. The current rubric and conservative 256k/32k-output profile derive a visible native archive limit of **68** for one new own job, with capacity for all **6** native objective extrema. This value belongs to this configuration/profile, not a fixed global limit. Actual complete provider payloads are still checked before sending.

No real provider context, truncation, billing or streaming behavior has been established by these local measurements. Both real acceptance gates remain required.
