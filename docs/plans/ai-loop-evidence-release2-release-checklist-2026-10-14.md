# AI Loop evidence — current release checklist, 2026-10-14

The user's latest instruction supersedes earlier per-model acceptance and profile-installation rules: available models with at least 256,000 context tokens must work without a manual allowlist, separate model-test requests or activation files.

## Current behavior

- OpenCode context comes from provider metadata. ChatGPT context is learned from the ordinary decision's native telemetry and cached automatically. Until first telemetry, the request uses a conservative 256k planning floor, explicitly provisional rather than a claimed context size. A winner is not committed if the required context cannot be established.
- Models without native tools use JSON reader requests in the normal dialog, with the same immutable evidence manifest and quotas. Explicit unsupported-tool/streaming responses negotiate the actual decision transport, not a separate probe. Optional runtime caches are not approvals.
- Normal streaming has finite connection/first-progress/inactivity protection; nonstreaming uses a finite per-call limit. Existing authorized Zen spend limits remain.
- Older acceptance reports and measurements are historical records. Their model names, reasoning levels and protocols do not restrict selection. No profile activation step is required.

## Release steps

1. Obtain explicit commit/release approval; follow `RELEASING.md` for version, notes, changelog index, serial, graph change check, commit/tag/push and resolved GitHub issues.
2. Include related `docs/plans/ai-loop-evidence-release2-*` documents and the current automatic-context policy as provenance. Keep `docs/plans/ai-tasks-2026-10-11.md` out; it is unrelated and untouched.
3. After an independently authorized API restart, a user-requested Loop can use an available model from 256k. Do not create acceptance profiles or run separate model calibration/acceptance requests.
4. Observe ordinary Loop progress, context telemetry, read counters and complete required-page recovery. Do not bypass missing exact safety evidence or hard context/time/budget limits.

## Known limitations

- Context below 256k is unsupported. An OpenCode model with no usable provider context metadata cannot be claimed to meet the minimum; report that specific reason instead of requiring model approval.
- Snapshot expiry still counts from creation rather than Loop end; snapshots of deleted Loops are not swept.
- Context size cannot guarantee a model follows the configuration/decision contract; ordinary bounded correction/error handling remains necessary.
- No commit, release, API restart or production Loop was performed during this change.
