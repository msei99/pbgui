# Unreleased

- GPU & Offers now previews calculated Auto population, batch and work limit separately for every prepared queue job on the selected card. Each Auto-sized job can use a matching workload measurement or custom values; choices persist through manual rental and can be edited before the first job starts. Work limits are shown and edited in billions of candidate-bars while stored exactly; explicitly configured job sizing remains authoritative.

- Jev optimizer requests now pack candidates up to the model context-safe size instead of splitting at 16 KB and rejecting analyses above 100 KB. The 204-candidate run that previously failed now previews in four requests; shared instructions replace repeated per-candidate text to reduce transfer size without dropping candidate profiles, and the configured USD limit remains the pre-send cost gate. Agent guidance also tells the model not to treat a repeated open-ended question as a risk preference or substitute an analysis with invented weights.

- PB8 Optimize now labels staged-history screening as an optional search method rather than GPU sizing. Its three history and survivor fields stay hidden while disabled, and Vast.ai jobs hide the unsupported option unless an older config needs it switched off.

- The three optional Vast GPU sizing steppers now return from 1 to Auto with **−** and move from Auto to 1 with **+**. Other numeric steppers retain their previous behavior.

- GPU & Offers hides performance-test workload controls until **Performance test…** is chosen, leaving ordinary Rent uncluttered. In the PB8 editor, choosing Vast.ai now selects the GPU backend and exposes explicit population, batch and candidate-bar work-limit fields before queueing; queued snapshots are not changed by renting.

- PB8 Optimize groups the Vast.ai execution choice with GPU sizing and its validation instead of showing the choice and warnings at the top of the editor. The local GPU backend selector is hidden for Vast jobs because the rented worker supplies the GPU.

- PB8 Vast GPU sizing no longer fills the editor with static explanations, empty profile messages or red missing-field errors; measured profile and dispatch summaries appear only when data is available.

- Automatic Vast GPU population, batch size and candidate-bar limit remain optional when saving or queueing. With blank fields, PBGui selects the coordinated profile after measuring the rented GPU; manual values remain available as overrides. The editor no longer blocks auto-sized jobs.
- ChatGPT now presents model-authored clarification choices and the connected Jev proposal prominently in its PBGui tool catalog. If a response ends in an unanswered text question without a UI action, the model gets one presentation check to offer its own clickable choices or finish the answer; PBGui still does not choose whether to clarify or use Jev. The numeric ranking tool no longer requires a clarification for every qualitative goal.

- Existing ChatGPT conversations now refresh their Codex thread when PBGui agent guidance changes, carrying the bounded conversation history into the new thread and releasing the old one. Jev guidance favors a reviewable structured decision for qualitative Pareto backtest choices when the owner has an OpenRouter key, without hardcoded routing or invented numeric weights.

- AI Chat clarification choices now appear only after the current model turn finishes, so one click submits the answer in both the drawer and full chat. The drawer shows when Jev is unavailable without an owner-connected OpenRouter key; model guidance explains that requirement and highlights Jev for qualitative Pareto decisions before inventing ranking weights.

- PBGui AI now describes Jev using TypeSafe's System One decision-model contract and lets the model choose among Jev, native ranking, and Python analysis without a forced method. Jev proposal tools appear only for owners with a connected OpenRouter key, including after a connection change in an existing ChatGPT conversation. Approved Python analyses no longer receive Jev-specific continuation instructions; the AI Chat guides describe the current variable Jev selection.

- AI Chat now lets the model decide when a ranking needs clarification and whether an approval-bound Jev analysis would help. Jev honors a model-supplied maximum candidate count instead of always marking ten; without a cap, Jev’s own yes/no decisions determine the selection.

- Tool-capable AI chats now formulate clarifying questions and answer choices only when the model finds a material ambiguity. Full chat and drawer add a final free-text answer; accepted replies clear the pending question. Removed fixed PB7/PB8 comparison questions and optimizer ranking weights.

- Update the bundled Codex app-server to 0.157.1, load every page of its visible text model catalog, and expose advertised Fast speed tiers per conversation in AI Chat and the drawer. This includes current GPT-6 Astra, Sol, and Luna plus GPT-5.6 Sol models when available to the connected profile.

- Normal AI chats can request an approval-bound Jev analysis of an exact PB7/PB8 Optimize run with a full outbound payload preview and digest-bound approval. The owner can configure a USD budget per analysis; PBGui checks live OpenRouter model pricing and the complete request size before any billable Jev call, then returns the approved decision to the chat for explanation.

- Vast now removes verified-closed rental workers once no surviving job references them, both after queue-history deletion and during startup recovery. Hidden legacy job snapshots are purged once their retries are complete; unfinished retries remain protected. The current worker pointer and obsolete cloud logs are cleared with cleanup.

- Pareto questions for Jev scan all local metrics and send compact candidate profiles in context-safe batches. The complete planned analysis is checked against the configured USD budget and current OpenRouter price before any billable request; there is no separate 100 KB total-transfer cutoff.

- Audit unreleased code for obsolete test-era paths: remove the duplicate worker Dockerfile and unused calibration/progress branches, remove the superseded live first-candle fetch path, and update worker fixtures to the current official PB8 revision and metrics contract.

- Vast automatically purges legacy queue jobs already hidden by the old deletion bug during API startup, including their local snapshots, logs and performance history. Unfinished retry lineages are protected; cleanup runs in the existing background deletion worker.

- Fix Vast queue deletion during input preparation: atomically remove inactive job directories from queue discovery before deleting large snapshots, and release the queue lock during file cleanup. This prevents partially deleted status files from breaking automatic queue updates with HTTP 500. Interrupted marked deletions resume in the background at the next API startup, so leftover files cannot prevent the API from starting.

- Profit Sweep updates the selected account's policy status, journals, and transfer intents automatically, including when the page becomes visible again. Manual refresh buttons are removed, and background updates preserve policy drafts and navigation.

- Jev can use selected PBGui read-only data for typed decisions. Each sourced request shows the exact bounded OpenRouter payload for explicit approval; cancelled previews are discarded, and approval is bound to one owner, conversation, model, and message.

- Profit Sweep now synchronizes Bybit server time before private V5 transfer calls. Explicit Bybit timestamp rejections finish as failed transfers; older paused intents resolve after an exact transfer-ID history check confirms no matching transfer.

- The published PB8 GPU worker now checks out a fixed commit from the official enarjord/passivbot repository instead of layering PBGui's worker onto the older published PB8 image. Both worker wrappers verify that revision; new rentals and calibration tests use the publicly verified immutable image digest; already prepared rentals keep their recorded image and PB8 revision.

- Jev accepts explicit user data for Choice, Score, and Noul decisions through JSON or simple Data/Yes-No/Score text templates. Multiple typed JSON questions can share one state. These paths do not read PBGui resources.

- GPU performance testing now separates a fixed Small reference from deterministic Medium/Large EMA-anchor workloads with editable search bounds. An opt-in action on a prepared PB8 GPU Queue item measures a separate frozen-input copy without pretesting or changing normal jobs. Protocol-4 evidence requires a complete, one-batch generation across every scenario, and results show the measured population/batch and work cap. The published worker now enables these modes as well as the existing Small test.

- Jev can now choose among 2–20 user-supplied options and compare recent managed PB7/PB8 backtest results, alongside its existing Pareto selection. Choice answers show provider-reported probabilities and confidence.

- Switching the Guide between English and German keeps the selected topic and the corresponding reading position across the full guide and embedded help panels.

- The English and German AI Chat guides now include copyable Jev prompts for conservative, balanced, and cross-scenario Pareto backtest selection.

- PB8 Pareto backtest selections now submit one durable server-side queue batch. The API continues adding jobs after page navigation and restores progress when Optimize is reopened; repeated submissions reuse the batch and confirmed queue items.

- The AI drawer keeps unchanged messages, page-context chips, and usage values stable while polling a long Jev decision, avoiding repeated visual flicker.

- Jev provider rejections are reported without exposing raw provider response data.

- PB8 Vast GPU jobs can explicitly set population, dispatch batch and candidate-bar work limit in the editor. A one-click suggestion is available only for an exact measured workload and preliminary GPU power/VRAM match; the editor previews dispatch splitting without running a tuning pretest. Manual batch size is respected by the GPU tuning audit.

- Jev now identifies the exact Optimize run despite duplicate display names, scans all available PB8 numeric metrics locally, assesses compact profiles for every Pareto candidate, and marks its ten highest backtest priorities directly in the Pareto table.

- AI Chat automatically removes conversations inactive for 30 days when history is opened or a new chat is created, and evicts the oldest inactive chat above the 100-chat owner limit. Active turns are protected and expired selections move to the nearest available chat.

- AI Chat history now tolerates malformed Unicode in a legacy conversation, so one bad title or message no longer breaks the conversation list and appears as a generic provider error.

- AI Chat now shows OpenRouter-reported API-key spend and spending limits on the full page and in the drawer, with a link to OpenRouter Activity. New Jev chats no longer hit the previous 20-conversation cap.

- Documented controlled RTX 3060 (170 W) EMA-anchor GPU measurements across 11, 21, and 41 effective coins. For the 41-coin seven-scenario run, aligning population 11,648 to two full 5,824-candidate dispatches improved measured candidates/s by 23.27% versus population 12,288 with a small tail; no PBGui or PB8 tuning logic was changed.
- Documented a 41-coin RTX 3060 one-batch sweep at populations 5,632–7,168, including measured throughput, power and VRAM; clarified why PB8's 300-second exact-work heartbeat does not report within a single GPU batch.
- AI Chat adds an owner-scoped OpenRouter connection and Jev 1.13 decision model. Jev evaluates managed PB7/PB8 Optimize Pareto results and reports grounded backtest priorities.

- Fix Hyperliquid VPS limit sampling on hosts without `httpx` by using Python’s standard HTTP client; running bot counters now reach the overview once the updated agent runs.

- Hyperliquid request counters in API Keys, Hyperliquid Limits, and VPS Manager use apostrophes as thousands separators. The API-key editor places the limit card beside Futures Balance and offers confirmed credit purchase when a saved main account reaches its limit; vault accounts show the main-account restriction.

- Information now has a central Hyperliquid Limits page showing saved wallets, running PB7/PB8 bots, VPS hosts, counters, and active-bot history. The running bot's VPS samples each local wallet every five minutes instead of every Master polling all saved wallets; opening an idle Hyperliquid API key makes one current read without collecting background history.

- VPS Monitor can buy a user-selected number of Hyperliquid address request credits from a saved main account, showing the exact Perps USDC cost and requiring confirmation before the one-time action. It verifies and refreshes the limit after success, prevents parallel purchases, and blocks API restart during the action.

- VPS monitoring caches the legacy-cron migration count until the cron spool changes or one hour passes, reducing repeated `crontab -l` log entries. Cluster Sync reuses authenticated SSH transports for repeated peer commands. Daily VPS cleanup now removes timestamped PB7 run logs older than 30 days even when the legacy bot status file is absent, and reads that status before removing its directory.

- Validation: on a measured 170 W RTX 3060, the saved EMA-anchor workload selected 41 coins across seven scenarios. The identical first-scenario GPU proxy measured 6.41, 6.59, and 6.91 candidates/s at populations 4,096, 8,192, and 12,288 respectively; the fixed candidate-bar limit split every run into 728-candidate dispatches. These partial-suite measurements do not validate end-to-end throughput or change a production profile.

- VPS Monitor keeps fresh CPU, RAM, and swap samples when a bot metadata snapshot arrives, preventing false alert recovery and repeated alerts. Default PB7 bot CPU warning/error thresholds are now 15%/20%; the 500 MB RAM error threshold is unchanged.

- VPS Manager shows the Hyperliquid address request limit directly in each mapped PB7 or PB8 Hyperliquid bot row before CPU, with usage percent above the actual Used/Cap counters. Bots on the same wallet share values; clicking opens the persistent 24-hour history. Sampling remains once per wallet every five minutes.

- PBv8 Run now shows dedicated controls for the recently added HSL engine selector and signal-unavailability grace period when supported by the installed PB8 runtime.

- GPU & Offers now shows an at-a-glance status card for a waiting performance test, including its frozen search criteria and phase. Repeated status text is hidden, and saved rental details are collapsible.

- Waiting GPU performance tests retain their authorization through stale-offer HTTP 410 and other unconfirmed rental attempts; they retry only after Vast has verified that no instance was created, and stop after the first confirmed rental or user cancellation.
- Each isolated GPU calibration population now receives the staged PBGui market and first-candle caches in its own PB8 working directory, avoiding exchange-network fallback when local OHLCV data is complete.
- Published and pinned the calibration-v5 PB8 worker with per-case offline market caches by immutable digest; v4 and older rental images remain recoverable.
- GPU calibration verifies an unsplit batch at each population, stores the PB8-measured candidate-bar envelope with the accepted profile, and keeps that envelope fixed when production workloads add coins or scenarios.
- GPU calibration protocol 3 uses a fixed EMA-anchor workload and probes populations from 4,096 in 4,096-candidate steps. Completed PB8 generation profiles now report the actual rate window, GPU proxy time, and effective dispatch instead of relying on sparse generation log lines; legacy results remain separate.
- GPU offer search supports minimum advertised power and reliability. A separately authorized one-shot performance-test watch can wait for a matching offer, revalidate it immediately before rental, and verify the measured power limit after rental.
- Live local logs follow the newest tail of an atomically rewritten file without briefly replaying old lines.
