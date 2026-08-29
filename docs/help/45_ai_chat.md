# AI Chat

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
The model picker is loaded dynamically from Codex `model/list`; PBGui does not maintain a fixed ChatGPT model list.

## OpenCode Zen and Go

OpenCode Zen and OpenCode Go use the same OpenCode workspace key. Zen includes changing free and pay-go models; Go adds subscription models and included usage.

The OpenCode provider card includes **Get OpenCode Go**, which opens the subscription page through a PBGui referral link. This is an affiliate-style referral: under OpenCode's current program, the inviter and new subscriber may receive account credit. Subscription terms and referral rewards are controlled by OpenCode and may change.

1. Create or copy the key in the OpenCode account console.
2. Enter it in the OpenCode card and select **Connect**.
3. PBGui verifies the key and stores it in an owner-only server-side file.
4. Select an available model and send a message.

PBGui supports both catalogs across Responses, Chat Completions, and Messages endpoints. Available IDs come from the live Zen/Go catalogs; names, protocol, costs, and limits come from OpenCode's live model metadata. Free models are detected from zero cost, shown first, and labeled **Free**. New models appear automatically when they use a supported protocol, while removed models disappear. Contributor models that may use prompts and responses for training are marked explicitly.

**Check free models** queues a serial background availability check. The latest owner-specific status is shown beside models without disabling manual retries. Training-opt-in models are never probed automatically.

PBGui capability tools are supported across OpenCode Chat Completions, OpenAI Responses, and Anthropic Messages using each protocol's native tool-call contract. PBGui enables them only when the live model metadata advertises tool-call support. Tool-capable choices are labeled **PBGui tools**; models that explicitly lack it remain labeled **Chat only**.

Models with advertised reasoning variants show an **Effort** selector. **Standard** sends no override and keeps the provider default. All other choices come from the selected model in provider order, so names vary and may include values such as `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, `ultra`, or provider-specific names. PBGui does not add a fixed variant list.

Models labeled **Chat only** cannot inspect installed Passivbot documentation, source, or current PBGui data. They receive no capability rules or tool names and answer directly from general knowledge. Select a model labeled **PBGui tools** when the answer requires local or installed-runtime evidence. Responses and Messages models can now carry installed version, documentation, and source results back through their native function-result formats without exposing unrestricted filesystem access.

## Action approval

PBGui separates reversible browser actions from persistent actions. An explicit request to select Pareto candidates can directly mark the exact run-bound rows in the open Optimize page through a typed, owner-bound browser action. Pages can also advertise reusable actions for their entities. For example, `show_log` maps selected or running Optimize and Backtest jobs or an active bot config to each page's existing log-viewer function instead of requiring a feature-specific model tool or Python analysis. The action may target another registered PBGui page: PBGui navigates there, automatically restores the current conversation, keeps the action pending while destination data loads, and acknowledges it only after the destination page validates the exact entity and invokes its registered callback. The shared bridge cannot execute model-generated JavaScript, arbitrary URLs, paths, or DOM selectors.

PBGui also inventories currently visible non-sensitive controls such as buttons, same-origin links, text fields, checkboxes, and selects. Each receives an opaque short-lived ID plus its allowed `activate` or `set_value` operation. The assistant can therefore use ordinary PBGui controls, including closing a floating log window, without a feature-specific action. Password, file, credential, token, session, cookie, and other sensitive controls are omitted; field values are never copied into this inventory. Existing confirmation dialogs and proposal approval boundaries remain in force. A proposal review remains hidden while the model is working and appears only after the matching assistant answer is complete. After an approved action completes, PBGui automatically resumes the same assistant workflow so any remaining requested steps can produce their own review.

After the user confirms approval or rejection, the proposal card disappears immediately while PBGui executes the server-side decision. A visible applying/rejecting status replaces it; if the request fails, the card returns with its controls enabled so the action can be reviewed or retried safely.

Unambiguous reversible commands such as showing the only available log, closing a visible log window, or explicitly clicking one uniquely named visible control use a local browser fast path. PBGui performs the action immediately, records the user request and completion in the owner-bound conversation, and does not contact the selected AI provider. Ambiguous, analytical, or mutating requests continue through the normal model and approval flow.

The global drawer keeps both its width and open/closed state in owner-scoped server preferences. It therefore reopens automatically after ordinary PBGui page navigation when it was open before navigation, while an explicit collapse remains closed. Width dragging uses a temporary browser-wide shield so Dashboard iframe widgets cannot steal mouse events, and a delayed initial preference response cannot reset a drag already in progress.

"Stable" has a canonical meaning for optimizer selection: a smooth rising strategy-equity curve with low choppiness and exponential-fit error, shallow drawdowns, short underwater/recovery periods, and strong Sharpe/Sortino. "Stable with good profit" uses the balanced preset with stability at 60% and profit at 40%, so it does not require a clarification. When a genuinely conflicting goal, missing hard limit, or unresolved resource does require clarification, PBGui presents 2-5 clickable quick replies instead of a questionnaire. Zero strict matches never authorize automatic threshold relaxation: complete-run alternatives are returned separately and require one of those confirmations before they may be selected or queued.

When the agent proposes a PB8 save/queue action, a Pareto candidate-by-exchange backtest matrix, dashboard creation/editing, or Python analysis, PBGui displays an approval card in both the full page and drawer. For Python, the card shows the exact code, sanitized input, input summary, and payload digest. Backtest proposals show every candidate, exchange, total job count, and whether queue autostart may begin immediately. Dashboard proposals may use a template or a free semantic layout with 1-10 rows, 1-2 columns, widget placement, users, periods, chart modes, widget options, heights, and Orders-to-Positions links. Existing dashboards can be read and changed cell by cell while preserving unrelated settings; approval fails if the dashboard changed after review. **Review & approve** opens the shared PBGui confirmation dialog; only that exact owner-, conversation-, and digest-bound payload can run. Rejecting or closing the dialog changes nothing. PBGui refreshes pending proposals from the server whenever a conversation is restored and after every approval or rejection, so an expired or already resolved card is not treated as current.

After an approved Pareto backtest matrix is queued from Optimize, PBGui navigates directly to the PB8 Backtest Queue so the newly created jobs and their current states are visible without another manual page change.

Pending reviews remain available for seven days and survive API restarts. Approval still revalidates the current config digest, so a stale proposal cannot overwrite a config that changed after the preview was created.

Python stdout is returned as strict JSON when possible and otherwise as bounded text. Bounded stderr, exit status, timeout status, and truncation are shown with the result. For custom optimizer-wide calculations, the agent can bind Python directly to an optimizer-run resource: PBGui resolves every Pareto candidate and its sanitized metrics into sandbox stdin without sending the full dataset through model tool arguments. The review shows the exact code plus resource, candidate count, byte size, and dataset digest. Ordinary weighted min/max ranking can use the native complete-run ranking tool, which also scans every candidate and avoids conclusions from a truncated 200-row preview.

With explicit approval, Python can also receive read-only mounts at `/workspace/pbgui_data`, `/workspace/pb7`, and `/workspace/pb8`. Normal files, datasets, databases, configs, optimizer/backtest artifacts, source, and files under `data/logs` are readable directly without copying them into model arguments. Credential/API-key/token/password/session/cookie/SSH/private-key/certificate paths, `.env`, Git metadata, virtual environments, and every symbolic link are always masked. The mounted roots cannot be written, and the sandbox still has no network access. Proposal decisions and results use the existing owner-only durable action history. Python analysis is disposable rather than a restart blocker: graceful shutdown cancels and reaps it, while an ungracefully interrupted run is recorded as interrupted and is never replayed.

The key is cleared from the browser after the connection attempt. PBGui never displays a stored key.

## Conversations

Conversations and completed messages are stored in owner-only server-side history. The full AI Chat page and global drawer use the same conversation list. Selecting a history item restores its messages, last-used provider, model, reasoning effort, running state, error, and pending proposals. Provider, model, and effort can be changed freely between turns without creating a new conversation; when a new stateful provider thread is required, PBGui supplies a bounded transcript so the selected model can continue from the existing context. **New chat** explicitly prepares another conversation without deleting older history. **Delete** uses an explicit PBGui dialog and removes only the selected conversation. The global AI button in the top navigation opens a collapsible right-side drawer on every authenticated top-level PBGui page; the full page remains available for provider setup and larger sessions.

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
