# Vast UI review: issues 326–340

Review against the integrated PB8 Optimize UI (2026-09-14). The old standalone Vast page redirects to Optimize; its hidden legacy dropdown and nested log viewer are not the public job workflow.

| Issue | Finding and resolution |
| --- | --- |
| #326 | Confirmed. Worker actions share an in-flight lock with queue start. Pending labels and disabled controls survive refreshes; failures restore usable controls. |
| #327 | Confirmed recovery gap, but ordinary Save was never blocked. Add Retry validation for request/config collection failures. Queueing remains blocked until validation succeeds; stale replies cannot change a newer draft's state. |
| #328 | Confirmed defensive handling gap. Null/missing preference values become empty fields, never literal `null` or `undefined` search filters. |
| #329 | Confirmed. Shared PBGui confirmation plus per-job lock covering both confirmation and deletion. Redrawn rows remain disabled; requeue and deletion cannot overlap. A missing confirmation service fails closed. |
| #330 | Confirmed. Non-JSON error responses retain their HTTP status; malformed successful responses report an invalid response. HTML response bodies are not rendered. |
| #331 | Confirmed. Requeue immediately displays Preparing and disables the row action across refreshes. |
| #332 | Not reproducible through the visible integrated workflow. The cited selector/close button belong to the hidden legacy viewer. Queue → Open log selects the job, opens the shared modal, and explicitly reveals its controls. Closing/switching to a local log must hide cloud controls. Covered by browser regression tests rather than unconditionally revealing the container. |
| #333 | Confirmed. Selecting an offer reveals the explanation that the GPU type must be saved and the offer is not reserved. |
| #334 | Confirmed. Changing Show incompatible hosts submits a fresh preview with form validation. Older responses cannot restore stale offers or release a newer search's busy state. |
| #335 | Confirmed. Domain errors from authenticated Vast routes carry an explicit response header. Provider 401/403 remains recoverable and displays the provider error. Actual PBGui authentication loss clears unsaved credentials, stops requests/polling, and visibly requests sign-in/reload. |
| #336 | Confirmed. Failed billing refresh immediately renders Unavailable, or preserves the previous amount as last retrieved. Error details remain in the tooltip; amounts are only displayed for the matching lease. |
| #337 | Confirmed. Unavailable supervision is explained in Settings and the open cloud log; queue start also exposes the prerequisite explanation. |
| #338 | Report mistakes the hidden backing element for the display. Last activity already renders its text; rental cards expose price and deadline. Tests verify visible activity and rental details; no duplicate banner added. |
| #339 | Report targets the hidden legacy selector. Jobs are selected using the unified queue's Open log action. Tests switch jobs and verify the selected job's telemetry/log without exposing another selector. |
| #340 | Report targets the hidden legacy raw state/viewer. The shared Optimize dashboard already presents progress/errors, and its shared LogViewerPanel opens the selected optimizer/provider log. Tests cover these visible paths; no second raw-state/log panel added. |

All browser/network checks use mock HTTP responses and isolated state. No real rentals or runtime data are changed. GitHub issues are not closed or commented on by this local review.

GitNexus was rebuilt successfully for this checkout at HEAD `e8d70da3`. The subsequent impact analysis reports MEDIUM for request/render/refresh and HIGH for `_error` (16 direct route callers). Callback/test entry points without resolved callers were checked via their event registrations and Pytest discovery. The full offline Vast tests cover the shared API error boundary.

## Follow-up: remove legacy controls

At the user’s request, the hidden legacy job selector, raw job/worker blocks, nested log viewer and its buttons/listeners/CSS were removed. Job selection now uses `selectedJobId`; `workerActivity()` supplies the dashboard directly. Only the shared Optimize log viewer remains. The findings above record the original review; these obsolete elements no longer exist in the final implementation.
