# AI Chat

For bot diagnostics, the AI can list retained current/historical log files across monitor-discovered VPS hosts and local runtime, then read actual server-side text without controlling the Log Viewer. Literal searches inspect the file beyond its visible tail. Results include file/host, returned timestamps and truncation/coverage limits; inaccessible files produce an explicit error. Deleted logs and unobserved hosts are outside the inventory. A tail, truncated results, or an empty remote search do not establish that no trades occurred throughout the bot's history. The AI must distinguish current blockers from proven historical causes.


Clarification comes before review. While a question is unanswered, the AI cannot create approval proposals. If a further clarification is needed after a proposal was prepared, the pending proposal is withdrawn; after your answer, the AI can prepare the complete proposal again.

If a tool fails while the model continues, Activity shows **AI continuing after [tool] error** and the safe reason. This does not mean the chat stopped: the model can correct its request and try again. Later success is shown separately. Safe validation reasons are also recorded in `PBGui.log`.

For optimizer training/holdout setup, the AI passes scenario-generation parameters separately to the save proposal. PBGui creates the training suite and protected holdout provenance, and includes the scenario changes in the approval preview. Arbitrary GUI metadata remains blocked. Saving still requires approval and does not start a Loop by itself.

The AI can read local candle inventory for selected coins and exchanges: actual stored start/end dates, dataset/timeframe, coverage and missing days from Market Data and PB8 caches. It uses this evidence when choosing training and holdout periods. This read starts no downloads; date boundaries alone do not prove uninterrupted coverage or the full history available remotely.

Opening or reopening the AI drawer restores the saved conversation and keeps its messages visible without sending a new question. Running requests continue updating automatically.

Activity and streamed ChatGPT assistant text appear chronologically inside the chat directly after your latest request while the AI works and collapses when the response finishes; expand **Activity** to inspect it again. The header's context ring shows the last reported token usage against the model's context window for ChatGPT. Hover over it for token counts. **—** means the provider has not supplied usage data; it is separate from your account's weekly usage limit. The ring updates automatically as usage is reported and may decrease after context compaction. After an API restart, it waits for fresh runtime telemetry.

Provider, ChatGPT profile, model, reasoning effort and speed are saved automatically per PBGui account. They survive page changes, browser reloads and API restarts. Restoring an older conversation keeps its transcript but does not replace your saved model selection. If the saved model becomes unavailable, select an available model explicitly; PBGui does not silently substitute another model. New AI Loop runs and **Continue** use this current selection; existing runs keep their recorded model.

If the selected provider is not connected, AI Chat shows a connection hint in the sidebar workflow and does not request its models. On initial load, an available connected provider is preferred. Starting a new chat keeps the connection hint visible until a provider is connected.

Stop also works while a new conversation is still being created. The pending prompt is not sent when the delayed creation response arrives, and the composer becomes available again.

## Reading Run and Backtest inputs

The assistant can read saved PB7/PB8 Run configurations, saved Backtest configurations and the exact configuration used by a listed Backtest result, as well as the existing optimizer and result projections. This includes `live.approved_coins.long` / `.short`, ignored coins, trading parameters, backtest settings, available overrides and non-secret GUI metadata. It uses the selected page entity directly; you do not need to paste stored coin lists. Searches and pagination are available when the name is unknown. Reads do not save configurations, start bots or queue jobs.

Credential fields such as passwords, API keys, private keys, tokens, cookies and authorization data are removed recursively. Credential stores and arbitrary filesystem paths are not readable through these tools. A config read returns the saved version; unsaved editor changes may differ. For web research, the assistant reads the local inputs first and prepares the relevant public information for your normal inline prompt approval. Creating that preview does not query the running model or start research.

## Isolated web research

Ask for internet research in the normal chat input, in the AI drawer or Information → AI Chat. The assistant prepares a public-information-only prompt directly in the conversation. There is no separate research input or window. Review the exact prompt and expandable fixed instructions, then choose **Approve research** on that message. To revise it, ask in the normal chat; a new proposal replaces the pending approval. **Reject** dismisses a preview and **Cancel research** stops a running job.

The conversation's configured model, ChatGPT profile, effort and speed are pinned. The isolated workflow supports **ChatGPT, OpenCode Go and OpenCode Zen**. OpenRouter does not perform the web search itself; connected Jev can interpret the completed report as described below. For Go/Zen, the selected model prepares public search queries, PBGui retrieves evidence from the Exa search service also used by OpenCode, and the same model evaluates those results. The reviewed instructions disclose Exa before approval; model credentials are never sent to the search service. PBGui never switches providers or models silently. A run must actually use web search; unsearched answers are rejected. Go/Zen requests keep the exact selected model alias; differing upstream names are allowed for aliases, but a reported identity change between planning and answering stops the run. Provider limits/charges apply; there is no fixed USD budget.

Technical separation happens automatically in the background: a disposable process/thread with an empty temporary directory for ChatGPT, or disposable isolated HTTP clients for Go/Zen. Neither receives normal chat history, shell, file-change, browser, plugin or PBGui action tools. The model receives the reviewed public prompt, fixed instructions, current UTC date and retrieved public evidence. Exa receives only public search queries; it receives no model credentials or PBGui context. Results appear in the same chat as inert text and HTTPS source links. They initially remain outside the normal assistant's message history and UI-control context and cannot automatically trigger actions.

Prompts and results are saved separately with the conversation and restored on reopening or browser reload, without re-sending research. There is no fixed total runtime. ChatGPT research stops after 180 seconds without observable progress; search start/completion and non-empty reasoning/answer updates from the active turn reset that interval. Heartbeats and unrelated events do not. Go/Zen and typed Jev requests use a 180-second idle read timeout per HTTP request, with a separate 30-second connection limit; incoming response data resets the read timeout. A provider that sends no progress/data can still reach the idle limit while computing. Jev starts with its own waiting period, independent of research duration. Cancellation and provider disconnect still stop active jobs. ChatGPT research counts web calls for diagnostics but does not stop after eight calls; multi-asset research may need more. Go/Zen currently use a bounded plan of up to eight search queries. Provider charges and limits still apply. At most one job per owner and four globally may run. Deleting or rewinding the associated conversation cancels affected jobs and revokes approvals. Logging out of the selected ChatGPT profile cancels its research. Disconnecting the shared OpenCode connection cancels both Go and Zen jobs and revokes their previews. Search-service failures or empty source results produce an error, without a Python or provider fallback. Browser storage does not contain research prompts or answers.

Research previews have no time-based approval expiry. Saved previews and results remain available in their retained conversation after browser or API restarts. Approval rechecks the exact content, current chat model/settings and provider connection; changed content/settings require a new reviewed proposal. Jev still checks its approved/current cost ceiling and current pricing before sending decisions. Rejecting, replacing, rewinding or deleting a proposal, or disconnecting its provider, revokes approval. Interrupted jobs never restart automatically. Existing conversation/history retention limits still apply.

### Jev and further analysis

New combined research/Jev approvals include one final, tool-free analysis request to the same selected model. After both stages succeed, PBGui displays **Preparing final analysis…** and automatically produces the table or presentation requested in the approved public prompt. It receives the complete report and Jev answer, without ordinary chat history, page context, PBGui tools, Python or further web search. Provider usage applies to this additional request. The result is rendered safely in the same research card and restored with the conversation. A final-analysis failure keeps both original results and displays its own error; cancellation stops the owned request. Budget reapproval pauses before continuing. Existing completed reports never restart automatically. The final display does not import evidence into the action-capable chat; the rules for explicitly reading results below remain unchanged.

All productive Go/Zen requests use one shared calculation based on the selected model’s advertised output limit: ordinary chat, capability turns across Chat Completions/Responses/Messages, research reports and query planning with or without reasoning. No productive path retains a fixed 4,096-token ceiling. The separate connectivity probe still requests only eight tokens. If no output limit is advertised, the fallback is 32,768 tokens. Input, tools and protocol space are conservatively reserved when a context limit is known; explicit thinking budgets must leave room for output. Provider length limits, refusal/content filtering and other incomplete responses are reported separately. A length-limited report remains visible as incomplete and is never passed to Jev as a finished report; PBGui does not silently retry a billable request.

Assistant replies render Markdown tables with headers and readable cells, including unambiguous table headers accidentally attached to introductory prose in both the drawer and full chat. Rendering uses locally bundled sanitization; scripts, images, forms and executable links are excluded. Copy retains the original text. Automatic updates preserve the current reading position; the chat follows new output only when already near the bottom, after all research cards have been restored. For follow-up analysis, `read_research_result` returns lossless pages (default: Jev answer when present). The assistant must follow `next_offset` until `complete=true` for every requested part before claiming full coin coverage; `part=answer` retrieves the research report. All original stored text remains available, and every read keeps the chat analysis-only.

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

When Codex advertises a speed tier, use **Speed** in AI Chat or the AI drawer. **Model default** follows the model catalog, **Standard** requests standard speed, and **Fast** requests the advertised faster tier for that turn. The choice is saved with the conversation and can be changed before another message. Fast uses more ChatGPT credits; PBGui only shows tiers advertised for the selected model.

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

Set **Jev preflight budget per analysis (USD)** in the OpenRouter card on the full AI Chat page. The default is $0.01. Before any Jev request, PBGui fetches the pinned model's current OpenRouter price and checks the entire serialized request using a conservative token bound. If pricing is unavailable, output tokens become billable, or the estimate exceeds the configured budget, PBGui sends no Jev request. This guard is an estimate, not a provider-reported charge; the OpenRouter usage display continues to show only provider-reported key values.


Connect an OpenRouter API key in the OpenRouter card on the full AI Chat page. PBGui verifies it with OpenRouter and stores it in a separate owner-only server file. The key is cleared from the browser input and never returned by the AI API. OpenRouter bills Jev requests to that account.

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

Unambiguous reversible commands such as showing the only available log, closing a visible log window, or explicitly clicking one uniquely named visible control use a local browser fast path. PBGui performs the action immediately, records the user request and completion in the owner-bound conversation, and does not contact the selected AI provider. Ambiguous, analytical, or mutating requests continue through the normal model and approval flow.

The global drawer keeps its width, open/closed state, and pin mode in owner-scoped server preferences. The pin button switches between the default overlay and a side-by-side layout that shrinks PBGui so the drawer no longer covers the active page; mobile always uses the full-width overlay. The drawer reopens automatically after ordinary PBGui page navigation when it was open before navigation, while an explicit collapse remains closed. Width dragging uses a temporary browser-wide shield so Dashboard iframe widgets cannot steal mouse events, and a delayed initial preference response cannot reset a drag already in progress.

For an unclear request, the tool-capable assistant first checks available PBGui facts and then decides whether a missing user preference would materially change the result. If so, the model itself writes one focused question and 2–5 relevant clickable answers in the user’s language. PBGui adds **Write your own answer…** as the final option in the full chat and the drawer. Clicking it focuses the message box so you can answer freely. The choices become clickable when the current model turn has finished; one click then starts the next turn. An accepted answer removes the pending question and continues the same conversation. The model decides whether missing preferences could change a recommendation and, if so, formulates its own clarification. It should explain the metrics and trade-offs it actually uses without silently imposing fixed weights. Zero strict matches never authorize automatic threshold relaxation: complete-run alternatives remain separate and require your choice before selection or queueing.

When the agent proposes a PB8 save/queue action, a Pareto candidate-by-exchange backtest matrix, Sweep Holdout/full-timerange validation, dashboard creation/editing, or Python analysis, PBGui displays an approval card in both the full page and drawer. For Python, the card shows the exact code, sanitized input, input summary, and payload digest. After approval, PBGui persists the bounded analysis status, output, and diagnostics in the conversation before starting the optional model summary; the complete result remains available in Action History. One Pareto proposal may bind up to 1000 candidates and 1000 resulting backtest jobs. Backtest proposals show every candidate, validation mode, total job count, and whether queue autostart may begin immediately. Dashboard proposals may use a template or a free semantic layout with 1-10 rows, 1-2 columns, widget placement, users, periods, chart modes, widget options, heights, and Orders-to-Positions links. Existing dashboards can be read and changed cell by cell while preserving unrelated settings; approval fails if the dashboard changed after review. **Review & approve** opens the shared PBGui confirmation dialog; only that exact owner-, conversation-, and digest-bound payload can run. Rejecting or closing the dialog changes nothing. PBGui refreshes pending proposals from the server whenever a conversation is restored and after every approval or rejection, so an expired or already resolved card is not treated as current. A timeout in the optional model follow-up after successful approval is shown as a non-error completion note and never removes the analysis result, changes, or rolls back the executed action.

PBGui retries one transient timeout in the optional post-approval summary. The approved action itself is never repeated; if the retry also times out, the persisted result remains visible and the conversation receives the non-error completion note.

After an approved Pareto backtest matrix is queued from Optimize or AI Chat, PBGui remembers the exact queue IDs in the current browser tab and navigates directly to the PB8 Backtest Queue. The corresponding rows stay selected while configured timeframe, Holdout, and full-range jobs run. As soon as the complete group reaches a terminal state, PBGui resolves the successful result batches and opens the existing Results Compare chart automatically; failed or stopped jobs are reported and skipped. The handoff survives page reloads for up to seven days and never guesses results by model-generated paths.

Pending reviews remain available for seven days and survive API restarts. Approval still revalidates the current config digest, so a stale proposal cannot overwrite a config that changed after the preview was created.

Python stdout is returned as strict JSON when possible and otherwise as bounded text. Bounded stderr, exit status, timeout status, and truncation are shown with the result. For custom optimizer-wide calculations, the agent can bind Python directly to an optimizer-run resource: PBGui resolves every Pareto candidate and its sanitized metrics into sandbox stdin without sending the full dataset through model tool arguments. The review shows the exact code plus resource, candidate count, byte size, and dataset digest. Ordinary weighted min/max ranking can use the native complete-run ranking tool, which also scans every candidate and avoids conclusions from a truncated 200-row preview.

Approved optimizer-dataset Python uses unprivileged Landlock filesystem isolation plus a kernel seccomp filter, so it requires neither UID mapping nor an isolated loopback interface. It can read only the Python runtime and approved script, cannot write files, cannot access the network, and cannot inspect or signal sibling processes. With explicit approval, workspace Python can additionally receive masked read-only mounts at `/workspace/pbgui_data`, `/workspace/pb7`, and `/workspace/pb8`; this stronger path requires Bubblewrap namespace support and fails closed when the host denies it. Credential/API-key/token/password/session/cookie/SSH/private-key/certificate paths, `.env`, Git metadata, virtual environments, and every symbolic link are always masked. Proposal decisions and results use the existing owner-only durable action history. Python analysis is disposable rather than a restart blocker: graceful shutdown cancels and reaps it, while an ungracefully interrupted run is recorded as interrupted and is never replayed.

The key is cleared from the browser after the connection attempt. PBGui never displays a stored key.

## Conversations

PBGui automatically deletes conversations that have had no activity for 30 days when history is opened or a new chat is created. Each owner can keep up to 100 conversations; creating another chat removes the oldest inactive one. A running response is never removed. If the selected conversation has expired, PBGui selects the nearest available chat and explains the change. **Delete** still removes a selected chat immediately.

Conversations and completed messages are stored in owner-only server-side history. The full AI Chat page and global drawer use the same conversation list. Selecting a history item restores its messages, last-used provider, model, reasoning effort, running state, error, and pending proposals. Sending a follow-up keeps that confirmed transcript visible while the new pending prompt is added. Provider, model, and effort can be changed freely between turns without creating a new conversation; when a new stateful provider thread is required, PBGui supplies a bounded transcript so the selected model can continue from the existing context. **New chat** prepares another conversation; at capacity the retention rule may remove the oldest inactive one. **Delete**, **Rewind**, and proposal approval use the shared PBGui dialog, which the global drawer loads on every page where it opens. Delete removes only the selected conversation. The global AI button in the top navigation opens a collapsible right-side drawer on every authenticated top-level PBGui page; the full page remains available for provider setup and larger sessions.

On desktop, drag the drawer's left edge to resize it from its compact usable width up to the complete browser viewport. PBGui stores the width as an owner-only server-side preference and restores it on other pages and later sessions. A smaller browser window temporarily clamps the saved width to the available viewport. Mobile layout remains full-width regardless of the saved desktop width.

Turns started from either interface are owned by the API rather than the browser request. Navigating to another PBGui page, closing the full page, switching conversations, starting a new chat, or collapsing the drawer does not stop the turn. Both interfaces reconnect through the persistent conversation snapshot; only **Stop** cancels active work. If the API restarts, unfinished work is marked interrupted and is never replayed automatically.

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

Selecting another profile starts a new chat. Existing chats retain their profile, including after an API restart or browser reload. Each ChatGPT conversation shows its profile name in the history. Removing a profile retains conversation history but does not redirect those chats to another subscription. An exhausted allowance never triggers an automatic account switch.

Account, plan, and available usage windows update automatically for the selected profile. Limits are shown only when OpenAI provides them. Credentials stay in isolated server-side profile directories. Only profile identifiers and navigation context appear in the browser URL. Up to 20 profiles can be managed per PBGui user.

The top **Provider** menu lists each ChatGPT profile separately as **ChatGPT · profile name**. Selecting one also updates the sidebar profile and available models.

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
