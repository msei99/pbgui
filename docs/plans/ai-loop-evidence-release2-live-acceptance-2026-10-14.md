# AI Loop evidence — bounded live acceptance, 2026-10-14

> Superseded model policy: the user removed manual per-model acceptance, profile activation and separate probe/test requests. See [the current policy](ai-loop-model-context-policy-2026-10-14.md). Earlier gate descriptions below are historical, not active requirements.

The user authorized short real-provider tests. Both executed gates passed using synthetic evidence and temporary Loop/profile stores. No optimizer jobs, GPU rentals, production Loops, API restart, commit or release were performed. Production acceptance profiles were not activated.

| Gate | Actual model and selection | Result |
|---|---|---|
| ChatGPT cold context | `gpt-6.1-sol`, default account profile, high effort, default service tier | Passed; native telemetry reported 475,000 tokens, without a prior Loop context cache |
| ChatGPT evidence tools | Same selection | Passed; a 4,994-proxy-token page and its continuation were delivered through native tool calls; exact first/middle/last synthetic strings matched |
| ChatGPT compaction recovery | Same selection | Passed after one retry; native compaction completed, the complete required package was read again, and exact evidence proof matched |
| ChatGPT ordinary decision | Same selection | Production `LoopAI.decide` selected the sole synthetic candidate; no jobs were created |
| OpenCode evidence tools and SSE | `opencode-go`, `longcat-2.5-preview-free`, chat/completions, default profile, no reasoning override | Passed; explicit tool metadata, 1,000,000-token catalog context, native tool-result follow-ups, complete multi-page reads and exact proof |
| OpenCode ordinary decision | Same selection | Production decision path passed with per-call accounting and no pending reservation or jobs |

The OpenCode catalog advertised zero input/output prices and `free=true`; the gate checked these before sending. No paid OpenCode model was tested. Streaming calls in the tool test took approximately 4.3–8.4 seconds to complete; the trial used finite 30-second connection and 90-second first-progress/inactivity limits. These are conservative trial limits, not a claim about all workloads or provider latency guarantees.

The first ChatGPT compaction attempt returned a temporary server error. The bounded retry waited for the native compaction turn to complete before starting the recovery turn, then succeeded in approximately 35 seconds including the normal decision check. The successful recovery read the required package and both optional evidence pages; no simulated compaction or provider answer was injected.

The real catalogs exposed an implementation defect: `_go_catalog_metadata` computed explicit `loop_tool_support`, but `_go_models` omitted it from its public projection. This now passes through strictly as `is True`; absent/false capability remains unsupported. Focused offline catalog/chat tests: **109 passed**. API serial: **3237**. Source compilation and whitespace checks passed.

The initial broader command was rejected by automatic approval review before execution because it selected paid models without a confirmed cost limit and could persist production acceptance profiles. The executed alternative tested only the configured ChatGPT model and one zero-price OpenCode model, with temporary profiles. Paid OpenCode models and the Responses/Messages transports remain unaccepted for live use. The tested model/profile/reasoning selections must not be generalized to other selections.

Measured profile values and individual call usage are recorded in the [aggregate JSON](ai-loop-evidence-release2-live-acceptance-2026-10-14.json). Those measurements are reviewable results; they do not silently activate production support. The two previously noted snapshot-retention deviations remain open.
