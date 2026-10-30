# AI Setup and Chat

Claude Loop decisions use the native SDK's structured output with a JSON schema for the current stage. The SDK validates the result and requests corrections when needed; PBGui accepts only its structured object, never final free text. Missing or invalid structured output pauses the Loop with **Claude did not return the required structured decision.** Evidence, configuration and budget checks still apply independently. Contract failures retain bounded rejection diagnostics without storing the model response or prompt. This does not start a replacement run or change rental limits.

After you confirm an action, its review card disappears immediately while **Applying approved action...** shows that execution is pending. Completion or failure appears when the server replies. Cancelling the confirmation keeps the pending review available.

### Verified Vast host decisions

For an explicit host block/unblock request, the assistant prepares a proposal for the exact open cloud log or an owner-verified Loop job. The review names the job, lease and physical Machine ID. **Approve** applies an explicit Boolean rather than toggling a button. Existing rentals and optimizers continue; only future rental eligibility changes. The completion reports historical execution separately from freshly read exclusions. A changed host decision or target invalidates an older proposal. Unknown legacy targets and storage errors are reported without claiming success. After a lost response, verify local status instead of repeating the write. See the [Vast guide](48_vast_gpu.md#verified-host-exclusions-in-ai-chat).

If the approval response is lost, the drawer automatically reads that exact owned host proposal instead of sending another approval. You can also ask the assistant to verify the same proposal; `get_vast_host_action_status` is read-only and bound to the current conversation. An unfinished proposal is not proof of execution. A committed host action remains historically successful even when later journal/history persistence needs recovery. Failed actions remain visibly failed even if the assistant's explanatory continuation is already running.

### Replace GPU and continue loop

For an owned PB8 Vast Loop, the native `propose_ai_loop_gpu_replacement` capability prepares a review of one `loop_id` and current `worker_id`. Approval publishes durable replacement intent; it does not create a new Loop or change objectives, frozen input, baseline or incumbent. The review identifies the rental, chain-local excluded machines, in-flight work loss, charged attempts, remaining budget and original deadline. Capacity/limit waits and provider failures remain visible in the same Loop. Acknowledgement recovery after an API restart reuses already committed intent without renting twice; interruption before intent was committed requires a fresh review. This is separate from replacing an entire Loop on an existing rental. See the [PB8 Optimize guide](43_pbv8_optimize.md#replacing-a-gpu-within-the-same-ai-loop).


## AI Setup

### Claude Agent — Claude subscription

Claude can also drive **AI Loops** using the same selected model and effort. Loop decisions use an isolated SDK dialog with the read-only `loop_evidence` tool and the SDK format tool for submitting the final structured decision. Normal chat actions and approval tools are unavailable there. Queuing, starting and rental limits still require the existing reviewed user approval. Model context must be at least 256,000 tokens; PBGui reads the actual native metadata instead of assuming a model's capacity. Stop/shutdown cancels the owned process and preserves uncertain requests for the normal recovery workflow.

Open **Information → AI Setup → Claude Agent** and choose **Sign in with Claude**. Open the native Anthropic sign-in link when it appears and sign in with your own Claude account. If Anthropic displays a one-time code, paste it into **Sign-in code** and choose **Complete sign-in**. The code is forwarded to the pending native login and is never saved. Each PBGui user has a separate native profile; existing terminal logins and legacy API-key files are not imported.

The official Python Agent SDK and its bundled native Claude Code client are installed with PBGui's normal Python requirements in the existing PBGui environment. Node.js, npm and a separate provider environment are not required. Existing native account profiles are preserved. Native account availability determines the model choices. There is no API-key or model fallback. PBGui does not install packages during login or chat.

The model lists show the concrete names and versions reported by the SDK, including Opus and Fable when advertised. Duplicate aliases such as **Default (recommended)** are combined with their concrete model; they do not hide that model's name. Availability can differ from the Claude website. Fable may draw on separate usage credits depending on your plan; selecting it through the SDK does not show Anthropic's interactive credit-consent prompt. Check your Anthropic usage settings before testing or using it.

Choose **Test model → Test connection** for a fixed synthetic request without conversation data or PBGui tools. Then choose Claude Agent and a model in **Models and tests** and select **Use for new chats**. The drawer offers the same PBGui capabilities and reviewed action proposals as other text providers. Streaming, Stop, saved history and provider/model restoration use the shared chat workflow. Claude currently accepts text only. Subscription usage limits are unavailable; PBGui does not estimate remaining allowances.

**Disconnect** stops Claude turns for your account and signs out through the native client. Saved conversations remain readable. You are responsible for account eligibility, provider terms and usage settings, including any extra-usage billing enabled with Anthropic. A successful connection does not guarantee unlimited or free usage.

### Other connections and shared controls

The **ChatGPT**, **OpenCode** and **OpenRouter / Jev** connection cards each offer **Test connection** with a visible test-model picker and an inline result. ChatGPT uses the profile selected in that card; OpenCode lists Zen and Go separately. These buttons make the same isolated fixed-input request as **Test model**, using default reasoning/speed, without changing your default model or any conversation. Tests may consume provider credits; Jev retains its configured cost guard. Provider errors are shown without falling back to another model/provider.

Short explanations appear as tooltips on dotted-underlined field labels: hover, focus with the keyboard or tap to read them; Escape closes them. For connection tests, use the **Test model** label above the selector. Action buttons do not open help. Longer instructions are in Guide. There are no inline help expanders.

Open **Information → AI Setup** to configure ChatGPT profiles, OpenCode and OpenRouter credentials, model defaults and GitHub issue publication. The previous full-page chat URL now opens this setup page. Conversations remain in the global **AI** drawer, available on every page; **Open AI Chat** opens it from Setup.

Use **Models and tests** to select a provider/profile, model, reasoning effort and advertised speed. **Use for new chats** saves the owner-specific default without changing existing conversations or running AI Loops. **Test model** sends one fixed synthetic request using the chosen selection. It creates no conversation, sends no page context, source or diagnostics and exposes no PBGui tools. Tests consume provider credits; Jev uses its native decision endpoint and the saved preflight cost limit. The reply and elapsed time appear on this page.

The sidebar switches between setup areas. Browser reload restores the selected area, profile, model, effort and speed. Connection/usage updates run automatically while visible and preserve in-progress edits. Keys and tokens use password fields, are cleared after submission or leaving the page, and never enter navigation state or chat messages.

## Images in chat

Paste a screenshot into the message field (Ctrl+V / Cmd+V), or drag PNG, JPEG or WebP files onto the chat. There is no upload button. Up to five images of 4 MiB each can be attached per message. Check the previews and remove unwanted images before Send. Images alone can also be sent.

Sending shares the images with the selected AI provider so it can inspect the actual screenshot. Use ChatGPT or an image-capable OpenCode model; Jev does not accept images. Unsupported models fail explicitly instead of continuing without the images. Images stay in the private, owner-scoped chat history across navigation and API restarts, with a 20 MiB image limit per conversation. Deleting or automatically pruning a conversation removes its stored images. Unsent images stay only in the current browser tab. They are not automatically published on GitHub.

## Debugging and GitHub issues

In **AI Setup → GitHub issues**, **Check permissions** rechecks the saved token/account, effective `public_repo` permission (also included in `repo`), and whether the public target repository accepts issues. It uses GET requests only and creates no test issue, repository or attachment. Missing scope headers remain unverified; expired/revoked tokens and missing scopes are reported explicitly. This is a point-in-time permission check, not a guarantee of a future write or attachment upload. Scope checks use GitHub's documented [OAuth scope response headers](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/scopes-for-oauth-apps).

Describe an error or feature request directly in AI Chat, in any language. A concrete product wish such as “This period selector should also offer Year and All time”, together with the page context or a screenshot, is enough for the AI to prepare an English feature-request review; you do not have to mention GitHub. Preparing the review publishes nothing. Only Publish issue sends it to GitHub. Existing supported configuration actions still use their normal tools, and diagnosis-only questions do not automatically produce issues. For example: “Investigate why the service status is wrong and create a GitHub issue” or “Create a feature request for yearly and all-time dashboard ranges.” No visit to Debug export is required.

With a tool-capable model, the assistant can inspect bounded, redacted local diagnostic records/logs and installed PBGui source. Relevant evidence is sent to the selected AI provider. It distinguishes observed behavior, likely causes, missing evidence and proposed fixes; these read tools do not execute tests or change the application. Ask for diagnosis alone if you do not want an issue.

Open **Information → AI Setup → GitHub issues** and connect your account once using the password field. Never paste the personal access token into a chat message. The same encrypted, owner-specific GitHub connection is used by the support dialog. The token needs `public_repo`; it stays on the PBGui server and is never sent to the model.

The assistant prepares a clean English title and body. Click **Review changes** to inspect the exact public issue, then **Publish issue** to create it in `msei99/pbgui`. Nothing is published just by describing the problem or generating a proposal. If you request diagnostic attachments, they are encrypted for PBGui support and hosted as release assets in your public `pbgui-support-assets` repository; this is shown in the review. The chat workflow currently prepares text and optional server diagnostics; use the support dialog when selecting screenshot attachments.

After publication, the chat provides the issue link. If the response is uncertain, ask the assistant to check the publication receipt and resume that same review; it checks for the existing issue instead of blindly creating a duplicate. API-interrupted approvals are marked interrupted and are never automatically replayed. GitHub connection status updates automatically when returning to AI Setup or the browser tab.

New AI Loop requests create fresh definitions by default. Chat tools bind the original user message verbatim to a new reviewed definition. If you explicitly ask to start a saved definition unchanged, the chat can instead prepare a queue/start proposal with `reuse_existing=true` and its exact name/revision. This preserves the saved goals, presets, targets, training/holdout windows and Loop limits without saving or creating a definition. Review the displayed saved goals, current AI selection and rental settings before approving. The full definition is checked again at approval; already-used revisions remain single-use. New objectives still require a fresh definition or a reconciliation save proposal with complete fresh settings. Current PBGui default instructions are used; old custom Loop instruction versions are not inherited. A changed user request invalidates the start proposal. Only the selected optimizer bundle supplies starting data/configuration.

AI Loop reviews name each changed setting and show whether approval saves a definition or starts/replaces a run. Long text is expandable; **Save definition** starts no jobs or rentals.

For bot diagnostics, the AI can list retained current/historical log files across monitor-discovered VPS hosts and local runtime, then read actual server-side text without controlling the Log Viewer. Literal searches inspect the file beyond its visible tail. Results include file/host, returned timestamps and truncation/coverage limits; inaccessible files produce an explicit error. Deleted logs and unobserved hosts are outside the inventory. A tail, truncated results, or an empty remote search do not establish that no trades occurred throughout the bot's history. The AI must distinguish current blockers from proven historical causes.


Clarification comes before review. While a question is unanswered, the AI cannot create approval proposals. If a further clarification is needed after a proposal was prepared, the pending proposal is withdrawn; after your answer, the AI can prepare the complete proposal again.

If a tool fails while the model continues, Activity shows **AI continuing after [tool] error** and the safe reason. This does not mean the chat stopped: the model can correct its request and try again. Later success is shown separately. Safe validation reasons are also recorded in `PBGui.log`.

For optimizer training/holdout setup, the AI passes scenario-generation parameters separately to the save proposal. PBGui creates the training suite and protected holdout provenance, and includes the scenario changes in the approval preview. Arbitrary GUI metadata remains blocked. Saving still requires approval and does not start a Loop by itself.

The AI can read local candle inventory for selected coins and exchanges: actual stored start/end dates, dataset/timeframe, coverage and missing days from Market Data and PB8 caches. It uses this evidence when choosing training and holdout periods. This read starts no downloads; date boundaries alone do not prove uninterrupted coverage or the full history available remotely.

Opening or reopening the AI drawer restores the saved conversation and keeps its messages visible without sending a new question. Running requests continue updating automatically.

Activity and streamed ChatGPT assistant text appear chronologically inside the chat directly after your latest request while the AI works and collapses when the response finishes; expand **Activity** to inspect it again. The header's context ring shows the last reported token usage against the model's context window when supplied by ChatGPT. Hover over it for token counts. **—** means the provider has not supplied usage data; it is separate from your account's weekly usage limit. The ring updates automatically as usage is reported and may decrease after context compaction. After an API restart, it waits for fresh runtime telemetry.

Provider, ChatGPT profile, model, reasoning effort and speed are saved automatically per PBGui account. They survive page changes, browser reloads and API restarts. Restoring an older conversation keeps its transcript but does not replace your saved model selection. If the saved model becomes unavailable, select an available model explicitly; PBGui does not silently substitute another model. New AI Loop runs and **Continue** use this current selection; existing runs keep their recorded model.

If the selected provider is not connected, AI Chat shows a connection hint for AI Setup and does not request its models. On initial load, an available connected provider is preferred. Starting a new chat keeps the connection hint visible until a provider is connected.

Stop also works while a new conversation is still being created. The pending prompt is not sent when the delayed creation response arrives, and the composer becomes available again.

## Reading Run and Backtest inputs

The assistant can read saved PB7/PB8 Run configurations, saved Backtest configurations and the exact configuration used by a listed Backtest result, as well as the existing optimizer and result projections. This includes `live.approved_coins.long` / `.short`, ignored coins, trading parameters, backtest settings, available overrides and non-secret GUI metadata. It uses the selected page entity directly; you do not need to paste stored coin lists. Searches and pagination are available when the name is unknown. Reads do not save configurations, start bots or queue jobs.

Credential fields such as passwords, API keys, private keys, tokens, cookies and authorization data are removed recursively. Credential stores and arbitrary filesystem paths are not readable through these tools. A config read returns the saved version; unsaved editor changes may differ. For web research, the assistant reads the local inputs first and prepares the relevant public information for your normal inline prompt approval. Creating that preview does not query the running model or start research.

## Isolated web research

Ask for internet research in the normal chat input, in the global AI drawer. The assistant prepares a public-information-only prompt directly in the conversation. There is no separate research input or window. Review the exact prompt and expandable fixed instructions, then choose **Approve research** on that message. To revise it, ask in the normal chat; a new proposal replaces the pending approval. **Reject** dismisses a preview and **Cancel research** stops a running job.

The conversation's configured model, ChatGPT profile, effort and speed are pinned. The isolated workflow supports **ChatGPT, OpenCode Go and OpenCode Zen**. OpenRouter does not perform the web search itself; connected Jev can interpret the completed report as described below. For Go/Zen, the selected model prepares public search queries, PBGui retrieves evidence from the Exa search service also used by OpenCode, and the same model evaluates those results. The reviewed instructions disclose Exa before approval; model credentials are never sent to the search service. PBGui never switches providers or models silently. A run must actually use web search; unsearched answers are rejected. Go/Zen requests keep the exact selected model alias; differing upstream names are allowed for aliases, but a reported identity change between planning and answering stops the run. Provider limits/charges apply; there is no fixed USD budget.

Technical separation happens automatically in the background: a disposable process/thread with an empty temporary directory for ChatGPT, or disposable isolated HTTP clients for Go/Zen. Neither receives normal chat history, shell, file-change, browser, plugin or PBGui action tools. The model receives the reviewed public prompt, fixed instructions, current UTC date and retrieved public evidence. Exa receives only public search queries; it receives no model credentials or PBGui context. Results appear in the same chat as inert text and HTTPS source links. They initially remain outside the normal assistant's message history and UI-control context and cannot automatically trigger actions.

Prompts and results are saved separately with the conversation and restored on reopening or browser reload, without re-sending research. There is no fixed total runtime. ChatGPT research stops after 180 seconds without observable progress; search start/completion and non-empty reasoning/answer updates from the active turn reset that interval. Heartbeats and unrelated events do not. Go/Zen and typed Jev requests use a 180-second idle read timeout per HTTP request, with a separate 30-second connection limit; incoming response data resets the read timeout. A provider that sends no progress/data can still reach the idle limit while computing. Jev starts with its own waiting period, independent of research duration. Cancellation and provider disconnect still stop active jobs. ChatGPT research counts web calls for diagnostics but does not stop after eight calls; multi-asset research may need more. Go/Zen currently use a bounded plan of up to eight search queries. Provider charges and limits still apply. At most one job per owner and four globally may run. Deleting or rewinding the associated conversation cancels affected jobs and revokes approvals. Logging out of the selected ChatGPT profile cancels its research. Disconnecting the shared OpenCode connection cancels both Go and Zen jobs and revokes their previews. Search-service failures or empty source results produce an error, without a Python or provider fallback. Browser storage does not contain research prompts or answers.

Research previews have no time-based approval expiry. Saved previews and results remain available in their retained conversation after browser or API restarts. Approval rechecks the exact content, current chat model/settings and provider connection; changed content/settings require a new reviewed proposal. Jev still checks its approved/current cost ceiling and current pricing before sending decisions. Rejecting, replacing, rewinding or deleting a proposal, or disconnecting its provider, revokes approval. Interrupted jobs never restart automatically. Existing conversation/history retention limits still apply.

### Jev and further analysis

New combined research/Jev approvals include one final, tool-free analysis request to the same selected model. After both stages succeed, PBGui displays **Preparing final analysis…** and automatically produces the table or presentation requested in the approved public prompt. It receives the complete report and Jev answer, without ordinary chat history, page context, PBGui tools, Python or further web search. Provider usage applies to this additional request. The result is rendered safely in the same research card and restored with the conversation. A final-analysis failure keeps both original results and displays its own error; cancellation stops the owned request. Budget reapproval pauses before continuing. Existing completed reports never restart automatically. The final display does not import evidence into the action-capable chat; the rules for explicitly reading results below remain unchanged.

All productive Go/Zen requests use one shared calculation based on the selected model’s advertised output limit: ordinary chat, capability turns across Chat Completions/Responses/Messages, research reports and query planning with or without reasoning. No productive path retains a fixed 4,096-token ceiling. The separate connectivity probe still requests only eight tokens. If no output limit is advertised, the fallback is 32,768 tokens. Input, tools and protocol space are conservatively reserved when a context limit is known; explicit thinking budgets must leave room for output. Provider length limits, refusal/content filtering and other incomplete responses are reported separately. A length-limited report remains visible as incomplete and is never passed to Jev as a finished report; PBGui does not silently retry a billable request.

Assistant replies render Markdown tables with headers and readable cells, including unambiguous table headers accidentally attached to introductory prose in the AI drawer. Rendering uses locally bundled sanitization; scripts, images, forms and executable links are excluded. Copy retains the original text. Automatic updates preserve the current reading position; the chat follows new output only when already near the bottom, after all research cards have been restored. For follow-up analysis, `read_research_result` returns lossless pages (default: Jev answer when present). The assistant must follow `next_offset` until `complete=true` for every requested part before claiming full coin coverage; `part=answer` retrieves the research report. All original stored text remains available, and every read keeps the chat analysis-only.

A compact status bar stays visible while scrolling a long research card. It distinguishes the Jev connection check, active web research, active Jev analysis, completion and failure; an animation runs only while the recorded job is running. If Jev fails, the finished research report remains visible and the bar explicitly says **Research completed · Jev failed**. Before a combined research/Jev job starts web research, PBGui performs read-only checks of OpenRouter authentication, the key spending limit and current Jev pricing. No billable test decision or research text is sent by that check. This cannot guarantee a later decision succeeds: timeouts, HTTP failures and invalid JSON are reported separately, without automatic decision retries.

The running card distinguishes **Web research running… · Jev next** from **Jev analysis running…**. A Jev-only approval or higher-cost continuation uses the existing report and immediately shows the Jev phase.

For per-coin classification, each coin gets a separate typed question with described risk categories. There is no fixed coin-count limit: 150 questions are supported. PBGui keeps questions together where they fit and splits only when the estimated token budget is exceeded, with the complete report in every request and one aggregate cost check before any decisions. OpenRouter documents a 32,000-token context. PBGui estimates input tokens locally using the bundled cl100k_base BPE vocabulary, adds 25% reserve plus 512 framing tokens, and uses this estimate for both packing and total cost. This is not an exact Jev count: OpenRouter lists Jev’s tokenizer as “Other” and TypeSafe does not publish its tokenizer or a token-count endpoint. The provider remains authoritative; unusual text may tokenize differently. No input is sent elsewhere for counting and no vocabulary is downloaded at runtime. Separate application safety bounds are 1 MB per input, 1 MB of question data and 4 MB for the complete transfer plan. A report that cannot fit with even one question needs a shorter reviewed version; evidence is never silently truncated. The status distinguishes web research from the Jev analysis. Oversized evidence is rejected, never silently shortened.

You can request both steps in one normal chat message: “Research the coins in this config using public sources, then have Jev assess the evidence. Do not change anything in PBGui.” The inline **Approve research + Jev** card shows the public research prompt, typed Jev questions and maximum Jev cost. After approval, PBGui automatically sends the completed report to the connected Jev decision model. Jev receives only that report, the reviewed questions and fixed evidence instructions; it has no PBGui tools. Its answer appears alongside the research result. If the total estimate exceeds the approved/current ceiling, the card shows the old ceiling and new estimate and waits for explicit approval of the higher per-job ceiling. Rejecting sends no decisions. Approving rechecks current pricing and continues Jev on the existing report, without repeating web research. Global budget preferences are unchanged. Errors leave the completed report available; there is no action or Python fallback.

For an existing report, ask “Have Jev evaluate that research result.” The assistant prepares **Approve Jev analysis**, showing the exact saved report and questions before transfer. There is no copy-and-paste requirement. Existing Jev payload and typed-question limits apply; unsupported or oversized requests fail explicitly.

For further discussion, ask a follow-up in the normal chat. The assistant can list and read the saved report with its dedicated research tools. **Before returning any report or Jev answer to the assistant, the server permanently restricts this conversation to analysis.** Managed, sanitized read tools remain available. PBGui actions, changes, drafts, selections, Python execution, previous action approvals and local chat commands are blocked. The restriction survives restart, provider changes and rewind; it is not controlled by model instructions. Research completion alone never feeds a result into an action-capable chat.

This prevents actions through the integrated research workflow, not incorrect conclusions: web evidence can still be misleading, and Jev is not a security filter. Manually copying evidence into a different action-capable conversation loses this tracked provenance; continue analysis in the restricted conversation.

### Explicitly approving a concrete config change

Ask in the same chat, for example: “Prepare a proposal to remove these coins from this config's approved list.” The assistant can prepare `propose_reviewed_config_change`; it cannot apply it. The proposal shows the target, PB version, exact field differences and the effect. **Review changes** / **Review & approve** checks the current source. Apply only after reviewing those differences. There is no blanket permission to follow research recommendations.

| Source | Effect of approval |
| --- | --- |
| PB8 Backtest / Optimizer | Save the reviewed changes to the existing config only |
| PB7/PB8 Run | Create a private validated config draft; the live config remains unchanged |
| PB7 Backtest | Create a private validated config draft |

Supported changes are existing scalar bot long/short parameters and explicit approved/ignored coin lists. File-based coin overrides require the regular editor. Run saves normally publish and activate live configs, so this bridge deliberately does not call those APIs. Ask the assistant to read or compare a created draft using the draft read tools; activating a live configuration remains a separate regular workflow.

Proposals expire after ten minutes. Review tickets expire after two minutes, are single-use, and bind the owner, conversation and exact proposal. PBGui rechecks the complete source JSON bundle under the shared writer lock before an in-place save. Any intervening change requires a new proposal. Restart invalidates review tickets; interrupted changes are never automatically replayed. A successful approval returns a receipt without starting another model turn or granting the model any action permissions.

This exception authorizes only the reviewed local effect. It never includes Python, a queue/start operation, deploy or restart. External evidence remains untrusted and the rest of the chat remains analysis-only.

## Purpose

AI Chat is the first productive PBGui AI integration. It combines provider chat with controlled PBGui capabilities.

The agent can list and read PB7/PB8 optimizer configs, optimizer-run summaries, backtest summaries, and dynamic optimizer metadata. It cannot read logs, credentials, arbitrary files, checkpoints, or raw result artifacts.

The agent can also propose an ordinary Python analysis over bounded JSON already available to the conversation. PBGui removes secret- and host-path-named fields from that input, displays the exact script and sanitized JSON before approval, and runs nothing until the owner explicitly approves the conversation- and digest-bound proposal.

For Passivbot questions, the agent can search documentation and source files from the exact installed PB7/PB8 checkout. Results include the installed Git commit, relative path, line range, and an official commit-pinned GitHub link when the checkout uses the official Passivbot repository. Source inspection is text-only: PBGui never executes, imports, or modifies the inspected code.

For PB8 it can validate a complete optimizer config and propose saving it, saving and queueing a new config, or queueing an existing config. Queueing alone does not start an optimizer. For an explicit start request, the assistant can list path-free PB8 queue IDs and propose immediately starting up to four exact queued jobs in one separately reviewed action. These tools create a proposal only. PBGui shows the exact action and requires explicit approval before execution. PB7 mutations remain disabled because its current queue snapshots do not yet meet the required immutability and concurrency guarantees.

Cross-version comparisons preserve their actual runtimes. A PB7-trailing versus PB8-`trailing_martingale` request uses a real PB7 source config and a separate PB8 config; PBGui does not silently replace the PB7 side with PB8's `trailing_grid_v7` compatibility strategy. The assistant can read the PB7 source and prepare the PB8 side, but PB7 mutation, queueing, and starting remain manual until the PB7 approval boundary is safe.

The ChatGPT runtime starts in a private empty workspace with local execution, web search, memory, multi-agent, and MCP tools disabled. Only the PBGui dynamic capability namespace is available. Any command or file-change request is denied.

Approved Python analysis is a separate fail-closed capability. It runs through Bubblewrap in an empty temporary workspace with a read-only Python runtime and installed libraries such as NumPy/Pandas when available. It has an isolated network namespace, no host home, PBGui data, credentials, or other host files, a sanitized environment, JSON standard input, bounded standard output/error, resource limits, and a short timeout. PBGui never falls back to unsandboxed execution when Bubblewrap or resource limiting is unavailable.

## ChatGPT

ChatGPT uses the official Codex login included with PBGui.

1. Select **Browser login** when PBGui and your browser run on the same computer.
2. Open the displayed HTTPS address.
3. Complete the normal ChatGPT browser authorization.
4. Wait until PBGui reports that ChatGPT is connected.
5. Select an account-visible model and send a message.

For a remote or headless PBGui host, select **Device code** instead. Device login requires device-code authorization to be enabled in the ChatGPT security settings and asks you to enter the displayed one-time code.

PBGui does not use an OpenAI Platform API key for this connection. Available models and limits depend on the connected ChatGPT account.
The model picker loads every visible text model from all pages of Codex `model/list`; PBGui does not maintain a fixed ChatGPT model list. Availability still depends on the connected account.

When Codex advertises a speed tier, use **Speed** in AI Setup or the AI drawer. **Model default** follows the model catalog, **Standard** requests standard speed, and **Fast** requests the advertised faster tier for that turn. The choice is saved with the conversation and can be changed before another message. Fast uses more ChatGPT credits; PBGui only shows tiers advertised for the selected model.

## OpenCode Zen and Go

OpenCode Zen and OpenCode Go use the same OpenCode workspace key. Zen includes changing free and pay-go models; Go adds subscription models and included usage.

The OpenCode provider card includes **Get OpenCode Go**, which opens the subscription page through a PBGui referral link. This is an affiliate-style referral: under OpenCode's current program, the inviter and new subscriber may receive account credit. Subscription terms and referral rewards are controlled by OpenCode and may change.

1. Create or copy the key in the OpenCode account console.
2. Enter it in the OpenCode card and select **Connect**.
3. PBGui verifies the key and stores it in an owner-only server-side file.
4. Select an available model and send a message.

PBGui supports both catalogs across Responses, Chat Completions, and Messages endpoints. Available IDs come from the live Zen/Go catalogs; names, protocol, costs, and limits come from OpenCode's live model metadata. Free models are detected from zero cost, shown first, and labeled **Free**. New models appear automatically when they use a supported protocol, while removed models disappear. Contributor models that may use prompts and responses for training are marked explicitly.

PBGui checks free-model availability automatically in the background and shows the latest owner-specific status beside each model. Training-opt-in models are never probed automatically.

PBGui capability tools are supported across OpenCode Chat Completions, OpenAI Responses, and Anthropic Messages using each protocol's native tool-call contract. PBGui enables them only when the live model metadata advertises tool-call support. Tool-capable choices are labeled **PBGui tools**; models that explicitly lack it remain labeled **Chat only**.

Models with advertised reasoning variants show an **Effort** selector. **Standard** sends no override and keeps the provider default. All other choices come from the selected model in provider order, so names vary and may include values such as `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, `ultra`, or provider-specific names. PBGui does not add a fixed variant list.

Models labeled **Chat only** cannot inspect installed Passivbot documentation, source, or current PBGui data. They receive no capability rules or tool names and answer directly from general knowledge. Select a model labeled **PBGui tools** when the answer requires local or installed-runtime evidence. Responses and Messages models can now carry installed version, documentation, and source results back through their native function-result formats without exposing unrestricted filesystem access.

## OpenRouter and Jev

You can also stay in a normal ChatGPT or OpenCode conversation and ask, “Use Jev to analyze the Pareto candidates in this Optimize result. Prioritize low drawdown and stable returns; explain the ten candidates to backtest.” The assistant resolves the exact optimizer run and creates a **Jev analysis** proposal showing the exact outbound Jev requests, run, question, and USD budget. Review and approve it in PBGui. Only then does PBGui call Jev; the normal assistant receives Jev's result and explains it. The Jev decision may mark Pareto rows but does not queue backtests. The drawer shows when Jev is unavailable because this PBGui account has no connected OpenRouter key. When an OpenRouter key is connected, the assistant may also suggest a reviewable Jev proposal when you did not name Jev. Only your approval starts a billable Jev request. A stated maximum number of candidates is carried into the proposal; otherwise Jev’s own yes/no decisions determine which candidates are marked.

Set **Jev preflight budget per analysis (USD)** in the OpenRouter card under **Information → AI Setup → OpenRouter / Jev**. The default is $0.01. Before any Jev request, PBGui fetches the pinned model's current OpenRouter price and checks the entire serialized request using a conservative token bound. If pricing is unavailable, output tokens become billable, or the estimate exceeds the configured budget, PBGui sends no Jev request. This guard is an estimate, not a provider-reported charge; the OpenRouter usage display continues to show only provider-reported key values.


Connect an OpenRouter API key in the OpenRouter card under **Information → AI Setup → OpenRouter / Jev**. PBGui verifies it with OpenRouter and stores it in a separate owner-only server file. The key is cleared from the browser input and never returned by the AI API. OpenRouter bills Jev requests to that account.

The OpenRouter card and AI drawer show the API key’s daily, weekly, and monthly USD spend and its configured spending limit, as reported by OpenRouter. These are key-level values, not workspace totals. **View full usage on OpenRouter** opens the provider’s Activity page for requests, tokens, and complete account activity. PBGui updates the displayed values automatically while the page or drawer is open.

Select **OpenRouter** and **Jev 1.13 · Structured decisions** in the AI drawer. On a PB7 or PB8 Optimize page, open a completed result and leave **Include page context** enabled. Ask about the Pareto candidates in that result, for example, “Which ten Pareto candidates should I backtest first?” You can also name one exact result in the question if you are on another page. When more than one run matches, PBGui asks you to choose one.

TypeSafe describes [Jev](https://docs.typesafe.ai/concepts/system-one) as a System One model that evaluates supplied text or JSON and returns typed answers and probabilities rather than generated explanations. PBGui identifies the exact selected Optimize run even if several runs share a display name. PBGui reads every Pareto candidate and all numeric PB8 metric columns locally. It sends Jev small, bounded batches containing up to 24 distinct metric columns per candidate plus compact profiles calculated from the remaining columns. Every candidate is assessed, but Jev does not receive every raw metric value. PBGui packs candidates against the same estimated 32,000-token budget described above, with safety reserve; there is no 30-KB context cutoff. It does not impose a separate total-byte budget: before any billable Jev call, it checks the complete planned analysis against the configured USD limit using current OpenRouter pricing. A question that exceeds the USD limit or Jev context is rejected without sending candidate data. PBGui marks the candidates chosen by Jev's yes/no decisions, respecting a maximum if the question states one, and names the highest-priority candidate as its backtest champion. For PB7 it uses the metrics projected by its Pareto summary. A displayed priority probability is not a probability of profitable live trading or out-of-sample success. Jev does not queue backtests or change configs; use the existing Optimize controls to review and queue the marked candidates.


Jev can also make a typed choice from your own data. Put **Options:** on a separate line, then list 2–20 numbered alternatives with their relevant facts. For example:

```text
Which hosting plan better fits a low-cost, reliable backtest worker?
Options:
1. Plan A: 12 GB VRAM, $0.20/hour, 96% reliability
2. Plan B: 16 GB VRAM, $0.32/hour, 99% reliability
```

The answer shows Jev's chosen option, provider-reported confidence, and probabilities. The listed facts are supplied by you; PBGui does not invent missing details. To compare managed results, ask “Which recent PBGui backtest results best balance drawdown and return?” Jev reads up to ten recent results per PB7/PB8 version, including their projected metrics. These comparisons only read data and do not select or queue anything.

For a simple yes/no or ordered rating, use a text template:

```text
Data:
GPU is unavailable for two days; CPU backtests still run.
Yes/No: Are all backtests blocked?
```

For a rating, replace the last line with `Score: How severe is the disruption?` and add `Levels:` followed by 2–10 numbered descriptions from least to most severe.

For other typed decisions, send a `jev` JSON block containing your own `state` and `questions`. Jev supports `choice`, `score` (ordered levels), and `noul` (yes probability), including several questions in one request. Without `sources`, this sends only the data you put in the block. Example:

```jev
{"state":{"worker":"GPU is unavailable for two days; CPU backtests still run"},"questions":{"blocked":{"type":"noul","instructions":"Are backtests completely blocked?"},"impact":{"type":"score","instructions":"How severe is the disruption?","criteria":["No impact","Some work delayed","All validation stopped"]}}}
```

To use other PBGui data, name 1–4 supported read or analysis capabilities in `sources`. PBGui fetches the selected data, shows the **complete request that will go to OpenRouter** (including your state and the PBGui results), and sends it only after you select **Send to OpenRouter**. Cancel discards the preview. The request is limited to 24 KB. For example, compare recent PB8 backtest summaries:

```jev
{"state":{"goal":"Prefer lower drawdown over maximum profit"},"sources":[{"name":"recent","tool":"list_backtests","args":{"version":"v8","limit":5}}],"questions":{"low_risk":{"type":"noul","instructions":"Do the backtests in state.pbgui.recent support a low-risk validation candidate?"}}}
```

Available sources are the capabilities currently marked `read` or `analyze` in PBGui’s capability catalog, including optimizer runs and Pareto analyses, backtests, configs, dashboards, drafts, help, and installation summaries. Use their exact capability names and arguments. These queries do not queue jobs or change configurations. The earlier Optimize Pareto and recent-backtest prompts still work directly. Do not put credentials or secrets in your own `state`.

### Example questions

With a completed Optimize result open and **Include page context** enabled, you can copy one of these questions into the AI drawer:

- **Conservative:** “Assess the compact profiles derived from every Pareto candidate and all available metrics. Favor low worst drawdown and steady risk-adjusted return over maximum gain. Mark ten candidates for separate holdout backtests.”
- **Balanced:** “Which ten Pareto candidates should I validate next? Weigh drawdown, Sortino, and return together, and put the strongest overall backtest priority first.”
- **Across scenarios:** “If this result has metrics for individual scenarios, prefer candidates that perform consistently across them over candidates strong in only one window. Mark ten for validation backtests.”

Each question evaluates one selected Optimize run and marks up to ten Pareto rows. The answer ranks backtest priorities from training metrics; the separate holdout backtests determine how those candidates perform on unseen periods.

## Action approval

PBGui separates reversible browser actions from persistent actions. An explicit request to select Pareto candidates can directly mark the exact run-bound rows in the open Optimize page through a typed, owner-bound browser action. Backtest Compare actions can either replace the current selection with 2-20 exact managed results or add 1-20 newly requested results, such as a finished Holdout, to the existing selection; the browser deduplicates the combined set and still enforces the 20-result limit. Pages can also advertise reusable actions for their entities. For example, `show_log` maps selected or running Optimize and Backtest jobs or an active bot config to each page's existing log-viewer function instead of requiring a feature-specific model tool or Python analysis. The action may target another registered PBGui page: PBGui navigates there, automatically restores the current conversation, keeps the action pending while destination data loads, and acknowledges it only after the destination page validates the exact entity and invokes its registered callback. The shared bridge cannot execute model-generated JavaScript, arbitrary URLs, paths, or DOM selectors.

An automatic AI navigation is attempted only once. If the user subsequently leaves that destination manually before acknowledgement completes, PBGui cancels the stale pending navigation instead of pulling the browser back. Explicit action requests also reject unsupported future-tense promises: ChatGPT receives one bounded corrective continuation to produce actual tool evidence, an approval proposal, a precise blocker, or a focused clarification; otherwise PBGui reports that nothing executed.

PBGui also inventories currently visible non-sensitive controls such as buttons, same-origin links, text fields, checkboxes, and selects. Each receives an opaque short-lived ID plus its allowed `activate` or `set_value` operation. The assistant can therefore use ordinary PBGui controls, including closing a floating log window, without a feature-specific action. Password, file, credential, token, session, cookie, and other sensitive controls are omitted; field values are never copied into this inventory. Existing confirmation dialogs and proposal approval boundaries remain in force. A proposal review remains hidden while the model is working and appears only after the matching assistant answer is complete. After an approved action completes, PBGui automatically resumes the same assistant workflow so any remaining requested steps can produce their own review.

After the user confirms approval or rejection, the proposal card disappears immediately while PBGui executes the server-side decision. A visible applying/rejecting status replaces it; if the request fails, the card returns with its controls enabled so the action can be reviewed or retried safely.

The drawer keeps the confirmed visible message snapshot while a turn or approved-action continuation is busy. Polling updates messages, page-context chips, and usage values only when their displayed content changes. Large page context is sent only with the active provider request and is not retained inside durable user-message text, so history trimming cannot remove the visible question and proposal answer during a follow-up. A proposal being approved is hidden by its ID until the final server state arrives and cannot reappear as a second clickable Review card because of a stale poll.

**New** creates a separate conversation using the selected provider and model; previous conversations remain in History. On large pages, PBGui automatically limits the included control inventory by its UTF-8 size, including Unicode labels, so page context does not prevent creating or sending a chat.

Unambiguous reversible commands such as showing the only available log, closing a visible log window, or explicitly clicking one uniquely named visible control use a local browser fast path. PBGui performs the action immediately, records the user request and completion in the owner-bound conversation, and does not contact the selected AI provider. Ambiguous, analytical, or mutating requests continue through the normal model and approval flow.

The global drawer keeps its width, open/closed state, and pin mode in owner-scoped server preferences. The pin button switches between the default overlay and a side-by-side layout that shrinks PBGui so the drawer no longer covers the active page; mobile always uses the full-width overlay. The drawer reopens automatically after ordinary PBGui page navigation when it was open before navigation, while an explicit collapse remains closed. Width dragging uses a temporary browser-wide shield so Dashboard iframe widgets cannot steal mouse events, and a delayed initial preference response cannot reset a drag already in progress.

For an unclear request, the tool-capable assistant first checks available PBGui facts and then decides whether a missing user preference would materially change the result. If so, the model itself writes one focused question and 2–5 relevant clickable answers in the user’s language. PBGui adds **Write your own answer…** as the final option in the AI drawer. Clicking it focuses the message box so you can answer freely. The choices become clickable when the current model turn has finished; one click then starts the next turn. An accepted answer removes the pending question and continues the same conversation. The model decides whether missing preferences could change a recommendation and, if so, formulates its own clarification. It should explain the metrics and trade-offs it actually uses without silently imposing fixed weights. Zero strict matches never authorize automatic threshold relaxation: complete-run alternatives remain separate and require your choice before selection or queueing.

When the agent proposes a PB8 save/queue action, a Pareto candidate-by-exchange backtest matrix, Sweep Holdout/full-timerange validation, dashboard creation/editing, or Python analysis, PBGui displays an approval card in the global AI drawer. For Python, the card shows the exact code, sanitized input, input summary, and payload digest. After approval, PBGui persists the bounded analysis status, output, and diagnostics in the conversation before starting the optional model summary; the complete result remains available in Action History. One Pareto proposal may bind up to 1000 candidates and 1000 resulting backtest jobs. Backtest proposals show every candidate, validation mode, total job count, and whether queue autostart may begin immediately. Dashboard proposals may use a template or a free semantic layout with 1-10 rows, 1-2 columns, widget placement, users, periods, chart modes, widget options, heights, and Orders-to-Positions links. Existing dashboards can be read and changed cell by cell while preserving unrelated settings; approval fails if the dashboard changed after review. **Review changes** opens the exact review before the shared PBGui confirmation dialog; only that exact owner-, conversation-, and digest-bound payload can run. Rejecting or closing the dialog changes nothing. PBGui refreshes pending proposals from the server whenever a conversation is restored and after every approval or rejection, so an expired or already resolved card is not treated as current. A timeout in the optional model follow-up after successful approval is shown as a non-error completion note and never removes the analysis result, changes, or rolls back the executed action.

PBGui retries one transient timeout in the optional post-approval summary. The approved action itself is never repeated; if the retry also times out, the persisted result remains visible and the conversation receives the non-error completion note.

After an approved Pareto backtest matrix is queued from Optimize or AI Chat, PBGui remembers the exact queue IDs in the current browser tab and navigates directly to the PB8 Backtest Queue. The corresponding rows stay selected while configured timeframe, Holdout, and full-range jobs run. As soon as the complete group reaches a terminal state, PBGui resolves the successful result batches and opens the existing Results Compare chart automatically; failed or stopped jobs are reported and skipped. The handoff survives page reloads for up to seven days and never guesses results by model-generated paths.

Pending reviews remain available for seven days and survive API restarts. Approval still revalidates the current config digest, so a stale proposal cannot overwrite a config that changed after the preview was created.

Python stdout is returned as strict JSON when possible and otherwise as bounded text. Bounded stderr, exit status, timeout status, and truncation are shown with the result. For custom optimizer-wide calculations, the agent can bind Python directly to an optimizer-run resource: PBGui resolves every Pareto candidate and its sanitized metrics into sandbox stdin without sending the full dataset through model tool arguments. The review shows the exact code plus resource, candidate count, byte size, and dataset digest. Ordinary weighted min/max ranking can use the native complete-run ranking tool, which also scans every candidate and avoids conclusions from a truncated 200-row preview.

Approved optimizer-dataset Python uses unprivileged Landlock filesystem isolation plus a kernel seccomp filter, so it requires neither UID mapping nor an isolated loopback interface. It can read only the Python runtime and approved script, cannot write files, cannot access the network, and cannot inspect or signal sibling processes. With explicit approval, workspace Python can additionally receive masked read-only mounts at `/workspace/pbgui_data`, `/workspace/pb7`, and `/workspace/pb8`; this stronger path requires Bubblewrap namespace support and fails closed when the host denies it. Credential/API-key/token/password/session/cookie/SSH/private-key/certificate paths, `.env`, Git metadata, virtual environments, and every symbolic link are always masked. Proposal decisions and results use the existing owner-only durable action history. Python analysis is disposable rather than a restart blocker: graceful shutdown cancels and reaps it, while an ungracefully interrupted run is recorded as interrupted and is never replayed.

The key is cleared from the browser after the connection attempt. PBGui never displays a stored key.

## Conversations

PBGui automatically deletes conversations that have had no activity for 30 days when history is opened or a new chat is created. Each owner can keep up to 100 conversations; creating another chat removes the oldest inactive one. A running response is never removed. If the selected conversation has expired, PBGui selects the nearest available chat and explains the change. **Delete** still removes a selected chat immediately.

Conversations and completed messages are stored in owner-only server-side history. The global drawer provides the conversation list on every page. Selecting a history item restores its messages, last-used provider, model, reasoning effort, running state, error, and pending proposals. Sending a follow-up keeps that confirmed transcript visible while the new pending prompt is added. Provider, model, and effort can be changed freely between turns without creating a new conversation; when a new stateful provider thread is required, PBGui supplies a bounded transcript so the selected model can continue from the existing context. **New chat** prepares another conversation; at capacity the retention rule may remove the oldest inactive one. **Delete**, **Rewind**, and proposal approval use the shared PBGui dialog, which the global drawer loads on every page where it opens. Delete removes only the selected conversation. The global AI button in the top navigation opens a collapsible right-side drawer on every authenticated top-level PBGui page; AI Setup provides provider configuration and isolated model tests.

On desktop, drag the drawer's left edge to resize it from its compact usable width up to the complete browser viewport. PBGui stores the width as an owner-only server-side preference and restores it on other pages and later sessions. A smaller browser window temporarily clamps the saved width to the available viewport. Mobile layout remains full-width regardless of the saved desktop width.

Chat turns are owned by the API rather than the browser request. Navigating to another PBGui page, leaving AI Setup, switching conversations, starting a new chat, or collapsing the drawer does not stop the turn. The drawer reconnects through the persistent conversation snapshot; only **Stop** cancels active work. If the API restarts, unfinished work is marked interrupted and is never replayed automatically.

Every message offers **Copy**. User messages also offer **Rewind**, which persistently removes that message and all following responses, rejects pending proposals for the removed branch, resets provider context, preserves the provider/model currently selected in the drawer, and restores the prompt to the composer for editing or resending.

Proposal reviews use a red/green field diff instead of raw JSON. Removed values are prefixed with `-`, new values with `+`, and the changed config path is shown above them. Review and raw JSON panes can be resized vertically; Reject and Review & approve remain visible in a sticky footer.

If a turn fails while its prompt is still available in the current browser page, **Retry** submits that exact prompt again. PBGui deliberately does not store failed prompts separately, so a page restored later shows the error but does not guess an earlier prompt or offer an unsafe retry that might duplicate a completed request.

The drawer can include a small structured page context: page key, matching help topic, current section, explicitly registered resource references, and an optional focused field. When the Run editor's Passivbot log panel is open, up to 120 currently visible lines are included as a bounded, credential-redacted excerpt so routine bot-status questions do not require a Python proposal. Optimize Pareto context identifies the currently open run by name, Pareto count and modified timestamp and includes up to the currently marked candidate names, so follow-up requests such as “backtest these three” preserve the visible selection. Context chips show what will be attached before a new conversation is created. Context and log excerpts are untrusted data and never grant additional access. Productive pages register selected configs, dashboards, coins, exchanges, hosts, sections, and explicitly allowlisted non-sensitive focused fields through `PBGuiAI.registerPageContext()`.

PBGui does not scrape arbitrary page text, tables, forms, URLs, or browser storage. The shared context boundary rejects credential, password, token, API-key, private-key, session, cookie, SSH, secret, and generic log fields. The only log exception is the explicit bounded Passivbot excerpt above, which is redacted in both browser and API before being sent. API Keys and Logging expose only a non-secret user identity or section.

Starting a new user turn rejects any still-pending proposal from the earlier branch. Approve or reject a proposal before asking a different question; this prevents an old reviewed action from resuming against a newer conversation state. If an approved action succeeds but its automatic AI follow-up times out, PBGui reports the action as completed and the follow-up failure separately.

For contextual help, the agent can read or search the canonical English/German PBGui guides and then use the existing installed Passivbot documentation/source tools where implementation details are relevant.

While a response is running, the status line shows elapsed time and safe activity labels such as documentation search or source reading. Tool arguments, provider reasoning, prompts, and results are not exposed in that status. Select **Stop** to cancel the active provider request.

If a Stop request fails, AI Chat shows the error and restores the controls so you can retry cancellation.

Activity and retry controls appear at the bottom of the chat beside the composer. When an OpenAI Responses model supplies an explicit reasoning summary, PBGui stores and shows it in a collapsed **Reasoning summary** section. Hidden or encrypted chain-of-thought is never exposed.

Reasoning variants such as `high`, `xhigh`, `max`, or `ultra` can spend several minutes processing a tool result before any answer text appears. After a PBGui capability finishes, the status changes from the tool action to **model is processing results** so a slow reasoning phase is not mistaken for a stuck local search. PBGui stops a turn that does not complete within its bounded deadline and reports a normal timeout error.

## Privacy

Messages and enabled page context are sent to the selected external provider. Conversation history is stored privately by PBGui but prompts and responses are not written to operational logs. Review the provider's current privacy, retention, and subscription terms before sending sensitive information.

## Troubleshooting

- **Runtime missing:** install the PBGui dependencies that include `openai-codex-cli-bin` and restart the API service.
- **Browser login callback fails:** browser login requires the browser and PBGui API to run on the same computer; use Device code for a remote host.
- **Device login unavailable:** enable device-code login in the ChatGPT account security settings if required.
- **Authentication failed:** reconnect the provider and verify the subscription or key.
- **Usage limit reached:** wait for the provider limit to reset or select another connected provider.
- **Selected model is currently unavailable:** the model is advertised but has no healthy upstream capacity; select another model and retry later.
- **A tool-capable response takes several requests:** each capability result must be returned to the stateless model. PBGui bounds this to three capability rounds and then requests a final answer from the collected results.
- **Python analysis sandbox is unavailable:** install Bubblewrap and `prlimit` on the PBGui host. PBGui intentionally does not provide an unsandboxed fallback.
- **Python analysis timed out or output was truncated:** ask for a smaller input, a simpler calculation, or a more compact JSON result.
- **ChatGPT remains on model is processing results:** higher reasoning effort can legitimately take much longer after the local tool has completed. Select **Stop** if the result is no longer worth waiting for, or start a new chat with Standard or a lower model-supported effort.
- **No supported models:** refresh after connecting and confirm that the OpenCode account currently exposes models in the Zen or Go live catalog.

ChatGPT failed responses distinguish recognized usage/rate limits, model access, authentication, context limits, connection problems and server errors. Unknown failures are identified as such; they do not establish a Free-plan restriction. Provider messages are classified rather than displayed verbatim to avoid exposing sensitive content.

### Multiple ChatGPT subscriptions

Use **ChatGPT profile** to manage separate logins for your PBGui user. The existing login and older chats belong to **Default**. Enter a profile name and choose **Add profile**, then use Browser login or Device code to sign in to that subscription. You can rename, disconnect, or remove each profile separately. Check the displayed account and plan after login; the browser may still be signed into your previous OpenAI account.

Profile names must be unique within your PBGui user after trimming surrounding whitespace; names are case-sensitive. **Add profile** selects the new profile and clears **Profile name**. Automatic updates keep the field empty or preserve your newer draft. Selecting another profile fills its name for **Rename**. Existing profiles with duplicate names remain available and can be renamed or removed separately.

Selecting another profile in the chat drawer starts a new chat; changing profiles in AI Setup only changes the setup view. Existing chats retain their profile, including after an API restart or browser reload. Each ChatGPT conversation shows its profile name in the history. Removing a profile retains conversation history but does not redirect those chats to another subscription. An exhausted allowance never triggers an automatic account switch.

Account, plan, and available usage windows update automatically for the selected profile. Limits are shown only when OpenAI provides them. Credentials stay in isolated server-side profile directories. Only profile identifiers and navigation context appear in the browser URL. Up to 20 profiles can be managed per PBGui user.

The **Provider** menu lists each ChatGPT profile separately as **ChatGPT · profile name**. In AI Setup, selecting a profile updates its account settings and model catalog without creating a conversation.

The compact AI drawer also lists each ChatGPT profile in its Provider menu. Changing profiles starts a separate chat.

Usage limits show the percentage remaining and a bar for each reported window (monthly, weekly, daily or 5-hour), plus its reset date. The window duration is not available compute time.

If a login was interrupted or its link was lost after reloading, click Browser login or Device code again. PBGui cancels your previous pending attempt and displays a fresh link.

The compact AI drawer shows usage for the selected provider and ChatGPT profile, updating automatically every 30 seconds. OpenCode Go reports 5-hour, weekly and monthly usage with reset dates in both chat interfaces. Zen balance remains available in the OpenCode console.

Deleting a conversation clears its active response state. Reviewing one proposal leaves other pending proposals visible; cancelling review restores its card.

Starting a new chat clears the previous reasoning summary and activity history. If connecting OpenCode Go fails, the entered API key stays in the password field for correction and retry.

### AI Loop and Vast.ai configuration tools

The assistant can read your saved PB8 AI Loop definitions and prepare a new definition or edits as a reviewable save proposal. The starting snapshot comes from a saved PB8 optimizer config, including its strategy, exchanges, training scenarios, holdout and coin overrides. Loop goals, direction, proxy limit, execution target and run limits are editable. Saving a definition does not queue or start it; use the existing AI Loops Queue workflow with your current AI selection afterward. `max_runs` is the maximum number of optimizer attempts, not a guarantee of completed rounds.

The assistant can also read and propose changes to **Vast.ai GPU & Offers / Rental & Automation**. For example, “two RTX 3060” means GPU name `RTX 3060` and maximum rentals `2`, independently of Loop parallel variants. These settings are shared by all cloud jobs. Proposals show the exact changes, budget and rental count; enabling automatic rental can start paid rentals for waiting jobs after approval. Existing budget and duration are preserved unless explicitly changed. A settings change made while a proposal is open requires a fresh proposal.

No browser editor controls are needed for these tools. If an instruction has materially different meanings, the assistant should ask a focused clarification before substituting settings.

Drag the top edge of the drawer's message input upward or downward to change its height. The height is remembered across pages and browser reloads. You can also focus the divider and use Arrow Up/Down or Home/End. Resizing preserves your unfinished message.

While a response is being processed, the drawer header shows **AI working** with a running elapsed-time counter. The indicator disappears when processing ends or is stopped; the current activity remains available below the conversation.

After approving a proposal, new activity and streamed answers appear below the previous assistant answer. On completion, the collapsed activity stays immediately before the new final answer.

Page actions wait for the browser acknowledgement before the AI continues. The acknowledgement supplies the new page/section context, so navigation from AI Loops Config to Queue exposes the current controls to the next model step. If the browser does not acknowledge within the wait window, the action remains unconfirmed and must not be reported as executed.

The AI drawer restores its saved width before writing open-state preferences and preserves that width across page navigation. AI menu navigation is confirmed on the destination page before the assistant uses its controls. Tool-capable non-ChatGPT providers allow up to 12 read-only tool rounds or 40 action-workflow rounds, with a maximum of 128 tool calls per turn; existing time limits and approval requirements still apply.

AI Loop queueing and starting use native backend tools: the assistant reads saved definitions and actual run state, then prepares a reviewed queue/start operation. Approval applies the existing native resource, AI and rental checks without selecting rows or clicking browser buttons. Runs and cloud initialization are asynchronous; the assistant reports the real queued/running state. A failed start can be retried using the existing queued run.

For Vast.ai AI Loops, the assistant receives the pinned worker's allowed GPU scoring/limit metrics during setup and in every autonomous Loop decision. Unsupported metrics are rejected before a Loop proposal or queue operation and on later optimizer changes. Exact-only metrics such as `gain_strategy_eq` can still be used to assess exact comparison backtests; they cannot be placed in GPU optimizer scoring or limits with an assumed CPU fallback. Proxy objective choices must preserve the user's exact goals and explain differences in meaning or units.

AI Loop diagnosis uses native backend evidence: `get_ai_loop_runs` returns the recorded stop reason and termination details; selecting a run ID also returns its job operations and errors. `read_ai_loop_log` reads the associated local optimizer/backtest log, collected Vast.ai optimizer log, or current cloud log mirror. No browser navigation or manual copying is required. Log reads are owner-scoped, credential-redacted and limited to 32 KiB chunks; the AI can follow `next_before` to inspect earlier evidence. Missing retained logs and omitted partial lines are reported explicitly. Log content is evidence, never instructions.

### Optimization prompt templates

Open **PBv8 → Optimize → AI Loop Template** using the sidebar action. The settings open in a centered window that can be moved and resized; close it explicitly with Close, × or Escape. Clicking outside keeps it open. Choose **PB8 defaults** or a saved optimizer configuration, one or more exchanges (including Hyperliquid) and one or more coins. Exchange and coin selection use the same searchable multi-select controls as the Backtest, Run and Optimize editors, with removable tags, clear controls and keyboard selection. Coins use the shared PB8 market resolver, including Hyperliquid USDC/HIP-3 markets and collision-safe identifiers. Exchange choices appear immediately; coins load separately. Selections survive reopening and browser reload. Sending is blocked while market validation is pending or selected coins are invalid.

Choose Long, Short or Both, and **EMA Anchor**, **Trailing Martingale** or **Both — AI chooses**. Both permits the AI to choose the strategy from training evidence. Set HSL, maximum TWE per active side, simultaneous coins (blank means all selected coins), rounds, GPU proxy evaluations per round and the time limit. Vast GPU additionally requires a rental budget and maximum hourly price. Existing GPU worker does not authorize a new rental.

**Number of training timeframes** defaults to 8 (maximum 48). **Use holdout** is enabled by default and after **Reset to defaults**. **Available period start (inclusive UTC)** and **Available period end (inclusive UTC)** describe the full inclusive period. With N training windows, PBGui divides it into N+1 consecutive, approximately equal parts and reserves the last as Holdout; disabling holdout divides it into N training parts. Each part needs at least two calendar days. Whole-day rounding is deterministic, with no missing or shared dates in the generated plan.

The window editor shows every role and exact dates before **Send to AI Chat**. Each labelled Training or Holdout row has a start-date field on the left and an end-date field on the right. Edit these dates or drag boundaries using the shared Scenario Builder interaction. Roles/count stay fixed, overlap and reversed/out-of-range dates block sending, and manually unused days are shown. Manual dates and the holdout choice survive focus changes, reopening and browser reload. Changing the base period, count or holdout choice visibly recalculates the preview. **From starting configuration** loads the actual windows, also using inclusive UTC dates; incompatible source windows require correction or switching to **Manual date range**, rather than silent replacement.

The request includes the exact reviewed window contract. Follow-up chat messages without a new window block retain the last reviewed window plan; a newly submitted explicit plan replaces it. The reviewed plan is bound to its owner and conversation independently of the bounded transcript, so automatic removal of older messages and API restarts cannot discard it. Rewind does not release these constraints. If an older chat already lost its original plan before this update, send the reviewed dates again from AI Loop Template. After the current user request changes, saved Loop definitions require either a fresh reconciliation or an explicit unchanged queue/start reuse review. PBGui rejects optimizer save proposals and Loop definitions that differ from it. Only training windows enter optimizer scenarios and adaptive comparisons; holdout is stored separately. Historical optimizer advice is accepted only when original artifacts prove that every effective date lies in the reviewed Training windows; missing dates, shared Holdout boundary dates and manually unused days are excluded. Learned findings from other Loops are excluded because their full training lineage is not proven. Protected Holdout runs created before this evidence policy cannot reuse old decisions or snapshots: create a fresh reviewed Loop definition and run instead. Holdout and Full Time Range are always backtested as separate user-only background reports for every CPU-validated training candidate. The next optimization round does not wait for them. Reports for existing candidates finish even after stop or the training time limit. Prior Loops do not lock date windows. Holdout and full-period results never enter AI evidence, historical advice, adaptive search or winner selection. This controller rule also applies to restored Template Loops and cannot be disabled by AI settings. When Holdout is explicitly disabled, Full Time Range still runs.

**Automatic from selected coins** automatically runs the existing PB8 OHLCV start-date lookup after valid coin/exchange selection. Start is the first common usable date for every selected exchange/coin pair including warmup; end is the last complete UTC day, rather than a claim about locally cached candle coverage. This can query exchange inception data and update PB8's native first-timestamp cache; it does not download the full candle dataset, start an optimization or rent a worker. Progress is shown and sending waits for completion. Missing/unknown pair dates remain explicit failures; use **Manual date range** for manual dates. Editing either date switches to manual mode and cancels pending lookup. Changing selections, closing or leaving cancels obsolete jobs and ignores late replies. Manual dates and timeframe count survive reopening/reload. A saved source can instead retain its existing training windows through **From starting configuration**.

**Reset to defaults** clears the source, coins, dates, window preview, extra instructions and edited limits, then restores PB8 defaults, Binance, automatic coin-based dates, 8 training timeframes and enabled holdout. An existing tab can restore a different saved holdout choice; reset before checking defaults. Saved manual date ranges remain unchanged unless you reset or select automatic mode.

**Optimization goal** defaults to **AI choice**. The AI chooses and explains measurable evaluation criteria, GPU scoring metrics, simultaneous Pareto combinations, objective counts, reducers and experiment order. Gain, ADG and custom requests express user preferences rather than a fixed GPU formula. Explicit risk limits remain binding. Different scorings must be investigated over the experiment program; no switch or new optional filter is forced in every round. Controlled comparisons, bounds experiments and repeated seeds are allowed. A small training increase alone does not prove robust progress, and no arbitrary percentage threshold is imposed. At each adaptive evaluation, all available exact training candidates, baseline and incumbent are assessed under the same stated criteria; changing criteria does not itself establish improvement. The comparison and experiment knowledge are retained, including unsuccessful hypotheses and uncertainty. The original optimizing, metrics and bot manuals from the installed PB8 checkout are available through the Loop's paginated read-only evidence reader, with document paths and content hashes. If the essential manuals are missing, the decision pauses explicitly. Holdout and Full Time Range run independently in the background for the user and never guide AI adaptation.

AI Loop evidence limits return a normal tool notice when optional reads would consume mandatory recovery capacity. The AI can finish with existing evidence; missing mandatory pages must still be recovered before completion. Reaching this optional boundary alone no longer pauses the Loop or starts a second dialog.

AI Loop training selection can nominate CPU-criterion leaders from the complete native optimizer stream as well as Pareto candidates, within the recorded model capacity. It does not prescribe a GPU scoring formula or spend CPU validation budget automatically. Exact training reports identify their windows explicitly; report order is never a substitute for dates and labels.

**CPU training validation budget** is an independent total (Template default: 100; editable from 1 to 1000), not a calculation from rounds or a fixed candidate count. It counts the baseline and independent CPU training validation jobs; one suite job can contain several training reports. Holdout, Full Time Range and the optimizer’s internal exact evaluations are separate. The AI chooses how many distinct useful Pareto candidates to validate in each round and explains tradeoff coverage, duplicate avoidance and the budget left for later rounds. The old per-cycle candidate setting is ignored. Budget exhaustion remains a hard stop and can prevent completing all requested rounds; Continue retains the reviewed total rather than multiplying rounds by a candidate count.

**Maximum TWE per active side** is an upper safety ceiling. For a ceiling of 8, the AI may choose a positive interval ending at or below 8, or justify a fixed value for a controlled experiment. The Template does not prescribe `[8,8]` or a particular lower bound. The reviewed ceiling is preserved in Loop goals and checked against effective base settings, search bounds and runtime/scenario overrides before preparation and during later changes. GPU directions must also stay on or off throughout the search. The AI explains its initial interval and subsequent experiments.

GPU compatibility is checked before reviewing optimizer/Loop proposals, saving a Loop definition and queueing or starting a run, and checked again before execution. An active side must retain strictly positive exposure over its entire search range; a disabled side must stay disabled. For example, Long exposure bounds `[0, 8, 0.01]` are rejected with a field-specific error; `[0.01, 8, 0.01]` is a possible correction that keeps the maximum at 8. PBGui never applies that correction silently. Runtime overrides and every training scenario participate in the check. A rejected preparation keeps its original error; an attempt that never launched is not reported as a missing optimizer result.

**Send to AI Chat** sends the generated English request to a new chat using your selected AI provider/model and opens the shared AI drawer. It sends the text to that external provider and uses tokens. An existing message draft is preserved. Sending the request does not itself rent a worker or start a Loop. The generated request includes the technical round/proxy/time settings to be included in the subsequent reviewed Loop proposal. The normal proposal approval remains required. Template selections survive browser reload within the current tab; free-text objectives and additional instructions are not stored in browser storage. Reopening automatically reloads available configurations and markets. The open window and its geometry are restored after browser reload; it preserves your current Optimize view.
