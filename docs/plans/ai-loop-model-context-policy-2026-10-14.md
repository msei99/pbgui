# Automatic AI Loop model context policy — 2026-10-14

User requirement: use available models with at least 256k context. No model allowlist, per-model activation or separate synthetic live tests/probes.

OpenCode's context comes from the provider catalog. A normal ChatGPT decision reports its actual context through native token-usage telemetry. An uncached ChatGPT selection begins with a conservative 256k planning budget, then uses the observed window. The planning floor is not presented as a verified capability. Missing/damaged optional caches do not block use or demand approval. Runtime context below 256k, or unavailable context without metadata/cache, prevents committing a winner.

The read-only evidence contract, independent exact validation, immutable snapshots, complete recovery after compaction and provider cost ledger remain. Native tools are used when supported; otherwise models can request the same bounded reader by returning a JSON object in a normal assistant response. The controller sends the exact reader result as the next ordinary user message. No shell, external tool or write action becomes available.

Transport adapts during actual requests. Explicit unsupported-tool errors switch the decision to the JSON reader; explicit streaming rejection switches to nonstreaming. Transport hints expire after 24 hours independently per feature, and normal context observations do not renew them. Expiry retries the preferred transport only during an ordinary decision request. Switching to JSON preserves both contract-recovery dialogs and all per-call spending/read counters. Uncertain reservations remain reserved. Streaming connection/first-progress/inactivity limits are 30/300/300 seconds. Nonstreaming has a finite 600-second per-call limit. Healthy streaming remains bounded by the authorized Loop deadline without a fixed short total-turn limit.

Earlier model reports are historical measurements, not a list of permitted models. Changing model, account profile or reasoning level needs no acceptance request. Existing paid-provider spend authorization is independent of model acceptance and remains unchanged.

Only offline code regressions are used for this change. No live calibration, model acceptance, production Loop, GPU job or rental is started. Commit, publication and API restart still need their own authorization.

## Verification

- Focused Loop, AI chat, frontend logic and help coverage: 960 passed.
- Final adapter/review/RPC checks after the JSON-response fallback: 82 passed. All three OpenCode protocols reuse a complete JSON response when streaming was requested; no second request or probe is sent, response ownership is released and transport telemetry is cached.
- Isolated browser checks: 2 passed (3 deselected); provider requests and runtime state were mocked.
- Python compilation, JavaScript syntax and `git diff --check` passed. No full-suite or live-model run was added.
- API serial at release preparation: 3245. Loop frontend asset version: 56. Validation did not restart the running API.

## Review follow-up

The user-facing EN/DE help and release notes describe current behavior only. The unused pager-profile validator is removed. Missing model-context settings in decision preparation and archive allocation use `context_budget`.

Transport fallback timestamps are independent and expire after 24 hours. Missing, invalid or stale timestamps are ignored; context-cache writes cannot extend transport fallback lifetime. Native-tool negotiation rebuilds the ordinary JSON-reader request without consuming either contract-recovery dialog or resetting spending/read counters. Complete nonstreaming replies also remove streaming parameters from subsequent requests in the same dialog.

Focused evidence/dialog/review/help checks: 110 passed. Final dialog/review run after the follow-up-request fix and pause-reason regressions: 80 passed. Python compilation and `git diff --check` passed. No live provider request, API restart, production Loop or rental action was performed.

## v2.08.12 release validation

978 focused Loop/chat/frontend/help/release-discovery tests and two isolated browser checks passed. The test-suite gate required focused tests; no full suite or new live-model test was run. Complete graph review: 426 changed symbols, 21 affected execution flows, no partial/truncated result. User authorization covers the commit and publication; no API restart or remote bot deployment is included.
