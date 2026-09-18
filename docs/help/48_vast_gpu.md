# Vast.ai GPU queue

If a rental control is unavailable, clicking it explains why; the cards do not show permanent startup notices.

Unsaved budget, transfer-reserve and deadline-minute edits survive automatic log-dashboard refreshes. Switching to another rental discards those edits. Changes take effect only after saving and worker acknowledgement.

During image download, the progress bar uses compressed layer sizes from the pinned worker manifest. “At least … GB available” counts completed downloads and cached layers; partial downloads are not estimated. The ready-layer counter tracks extraction separately. Unknown image versions fall back to layer counts. Update `vast_image_layers.py` when publishing a new worker image.

Cloud optimization is integrated into **PB8 Optimize**. There is no separate
Vast system page. Each installation uses its own Vast account and rental credit.

The log dashboard shows **Proxy / min**, **Exact / min**, **Proxy / exact** and **Exact / USD (estimate)**. Rates use counter differences over approximately the last 60 seconds of timestamped native GPU logs (at least 10 seconds are required). The interval and sample age are displayed; live rates disappear after 90 seconds without a new sample. Finished jobs show the last recorded interval, not a whole-run average. Proxy/exact is cumulative screening workload, not result quality or acceptance rate. Exact/USD uses the rental hourly price and excludes transfers, startup and idle time. Missing counters remain unavailable; population size is never used as a substitute. A bounded observation history is saved while PBGui polls jobs, so counter samples survive log-tail replacement and page/API reloads. No remote worker update is required.

## Performance History

History actions (refresh, open, compare and filter) are in the toolbar above the history table. The sidebar contains the five fixed Vast.ai navigation entries.

Open **Vast.ai → Performance History** in the existing sidebar. The API records local job snapshots approximately every 60 seconds, even when no browser is open. The private SQLite database is `data/vast/performance.sqlite3`; removing a job from the queue does not remove its history. Recording pauses while the API is stopped. On restart, available log-tail counters can be recovered, but unavailable earlier history cannot be reconstructed.

Select rows by click, keyboard (Space/Enter), or click-drag; select up to four. **View selected run** opens one run, including incomplete older records. **Same workload only** filters the entire retained history to the first selected workload. **Compare hardware** is enabled only for matching verified fingerprints; the API also enforces this rule. **Filter this page** searches the loaded page by config, GPU, machine, coin or exchange. Previous/Next page through older records. **Refresh history** retrieves new measurements.

The fingerprint covers the prepared config, data-file hashes and PB8 revision/worker image. It excludes job-specific paths, the local user name and allocated CPU worker counts. Seed, population, objectives, bounds, scenarios and data dates remain part of task identity. CPU allocation, GPU, memory and rental price are separate comparison axes. Missing or inconsistent frozen inputs are marked unverified, never silently grouped. A matching fingerprint does not certify identical cache state, scheduling or random outcomes.

Complexity fields distinguish configured coins from exported symbol datasets (including BTC references). Exported candles are counted from NumPy headers without loading the arrays; actual runtime-consumed candles remain unknown. Unreadable headers remain unknown rather than being estimated. Scenario count, date range, timeframes, population, parameter count and seed are retained with each run. No complete config, credentials or SSH details are stored in the history database.

Charts compare proxy/min and exact/min against minutes since the first recorded counter. Averages divide total measured counter increments by covered seconds, not the mean of instantaneous rates. Counter decreases break intervals. Repeated/stale observations do not create additional work; the latest native counter in each minute is retained. Utilization keeps its original measurement time and is unavailable when stale. Phases and genuine utilization observations are saved independently of counter arrival.

First-counter delay means the **first observed** counter relative to container setup (or job creation if setup is unknown), so late recording can overstate startup time. Exact/USD uses measured optimizer throughput and hourly rental price. Run cost estimates separately show container time and observed transfer volume; they are not provider invoices and do not allocate shared idle time between jobs. The comparable-host count includes retained runs with the same fingerprint, machine ID and measurable counter interval. No universal complexity score or automatic benchmark rentals are introduced.


### Config workload comparison

The PB8 queue and Performance History show **Est. coin candles / candidate**. This sums inclusive calendar days × 1,440 ÷ candle interval × distinct selected coins over active training scenarios. Scenario coin selections and exclusions are respected; the same coin on long and short counts once. Without Suite Mode, the base date range is used. Multiple exchanges, optimizer iterations and population size are not extra multipliers. Holdout validation, warm-up, missing data and actual runtime candle consumption are excluded. Unknown or dynamic inputs show **—**. This is a data-volume estimate for one full candidate, not a runtime prediction or the run's total calculation count. Queue values use the frozen snapshot. Older history can recover estimates only while its snapshot still exists.

Select two to four runs and choose **Compare configs** to describe different workloads. **Compare hardware** continues to require identical verified workload fingerprints. Both show proxy/min and exact/min separately. **Minutes / 1k exact** = 1,000 ÷ measured exact/min; **USD / 1k exact** = 1,000 ÷ measured exact/USD. These are measured-rate projections excluding startup, idle time and transfers, not total rental cost forecasts. Zero or unavailable throughput shows **—**. GPU, CPU allocation, worker version, strategy and search settings can also change throughput; config comparisons do not isolate hardware performance.

## Optimizer Settings

**Queue Settings** contains only local autostart, CPU count, CPU override and PBGui market-data selection.

The Queue sidebar has five separate Vast.ai buttons:

- **Account**: API key, connection test and balance. Save credentials with **Save key**.
- **GPU & Offers**: hardware requirements, maximum hourly price, offer preview and manual Rent. Save with **Save GPU requirements**.
- **Hosts**: known/preferred/working hosts and blocked machines, with an All/Preferred/Working/Blocked status filter. Host actions save immediately.
- **Rental & Automation**: maximum rental hours, budget target, idle cleanup and Stop on stagnation. Save with **Save rental & automation**. These are defaults for future rentals/jobs.
- **Performance History**: recorded runs and workload comparisons.

The first GPU & Offers visit opens Account when no key is configured. Each settings form saves only its own fields. Unsaved edits in another area and edits made while saving remain in the form. Manual Rent uses the saved rental policy. Control active rentals from Queue and the job log. **Guide** follows the current section; **Queue** in the left sidebar returns to jobs.

### Vast API key

1. Sign in to the [Vast console Keys page](https://console.vast.ai/manage-keys/). Under **API Keys**, click **+New**.
2. Name the key `PBGui`. Select scoped/custom permissions and enable the categories below.
3. Click **Create** and copy the key shown once.
4. In PBGui, open **PB8 Optimize → Account**. Paste it into **Vast API key**, click **Save key**, then **Test connection & refresh balance**.

| Permission | Used by PBGui |
| --- | --- |
| `user_read` | Account and available credit |
| `misc` | GPU offer search |
| `instance_read` | Instance status and logs |
| `instance_write` | Rent, start, stop, delete and attach the worker SSH key |
| `billing_read` | Provider-reported instance charges |

`billing_write`, `user_write`, machine and team permissions are not required for this workflow. A read-only key cannot rent or clean up instances. Enabling billing **read** does not authorize credit transfers.

The key stays in PBGui's local credential store with owner-only permissions; it is not copied to the rented worker or bot servers. No manually uploaded SSH key or GitHub registry credential is needed. PBGui manages its worker SSH identity. Fund the Vast account separately; PBGui does not add credit.

A successful balance test confirms account access, not every rental permission. If offer search, logs or startup return a permission error, check the corresponding category. Missing `billing_read` prevents the cost lookup; even with permission, charges can remain **Pending** until Vast posts them. To replace a key, save the replacement and test it before revoking the old key in Vast.

Sources: [Vast API key setup](https://docs.vast.ai/guides/reference/api-keys) and [permission reference](https://docs.vast.ai/api-reference/permissions).

**Create Vast.ai account**, directly below the account heading, opens the PBGui referral link. It supports PBGui through
the Vast referral program and is loaded only when clicked.

The versioned `ghcr.io/msei99/pbgui-pb8-worker` image is public. No GitHub account
or registry token is required. PBGui verifies anonymous access to the pinned
manifest before starting a paid rental. The image contains runtime software,
not user configurations, course data or credentials.

## GPU & Offers

For Cloud **Auto** CPU mode, **Min CPU cores** is the rental requirement. Set it to 16 or 32 here when that capacity is required. The CPU count in a local optimizer config does not override this minimum; the cloud execution copy uses the measured allocation, capped by the rented CPU quota. Legacy jobs with an explicit fixed CPU requirement still enforce that count.

Manual **Rent** uses the selected offer's GPU type, hardware, disk size, verification status and displayed price. Editing search filters after selecting a row does not replace that offer's specifications. Rental hours and budget still come from the rental controls; availability and limits are checked again before renting.

In the editor, choose only **Execution → Vast.ai GPU** or **Local**.
Under **Queue → GPU & Offers**, choose a GPU type and maximum
hourly price, minimum VRAM/RAM/CPU, disk size and verified-host preference.
**Preview available GPUs** is informational: clicking a row copies its type,
not its offer ID. Click **Save GPU requirements** to persist the requirements.
An empty type allows any GPU meeting the remaining requirements. Model search is
case-insensitive and accepts fragments: `3090` finds `RTX 3090`; spaces,
underscores and hyphens are interchangeable. Exact model names take precedence
when present in the provider model list. Stored fragments also work at queue start.

The preview shows CPU model/allocation, GPU memory bandwidth, maximum rental
duration and compatibility alongside price, transfer cost, network, location
and reliability. **Details** expands PCIe generation/lanes/bandwidth, disk model
and read speed without selecting a GPU type. Missing metadata is **Unknown**.
By default CUDA below 13 (including unknown CUDA) and known availability shorter
than the requested rental hours are excluded. **Show incompatible hosts** includes
them with a reason; click **Preview available GPUs** to apply changed filters.
Compatibility is a marketplace check, not a benchmark or a guarantee of runtime
performance. Availability is reported at search time and rechecked at start.
Field units follow the [Vast offer API](https://docs.vast.ai/api-reference/search/search-offers).

On the job row’s **Start** action, PBGui searches fresh offers and selects the lowest
hourly price among matching single-GPU hosts with CUDA 13 support. It raises the
minimum CPU allocation to cover waiting jobs. The selected type and limits are
never silently relaxed. If no offer matches, no rental starts; retry later or
change the requirements. A race where an offer disappears before provisioning
can still fail. Automatic replacement rentals require an enabled, explicitly started GPU pool.
Hourly estimates include disk; transfers cost extra. The worker checks its actual
CPU quota. Saved requirements apply to the next rental, not an existing worker.
Set **Min TFLOPS** to require the provider-advertised minimum GPU compute throughput;
`0` leaves compute throughput unrestricted. This requirement applies both to the
preview and to the final offer selection before a rental is created.

**Save & Queue** saves and immediately creates a cloud queue row with status
**Preparing input**. Its frozen input snapshot is then copied and compressed in
the background; no GPU can be rented until that preparation reaches **queued**.
The row shows copied input MB, completed file count and a progress bar; archive
compression is shown as a final indeterminate phase.
**Queue Selected** also respects the saved execution target. Local
configs continue through the existing local queue. Cloud jobs are shown in the
same **Queue** table as local jobs, identified by the **Execution** column.

The current profile supports fresh multi-coin `ema_anchor` and
`trailing_martingale` jobs, Binance, Bybit, Bitget, OKX, Hyperliquid and KuCoin with scenario-specific coins, exchanges, dates and supported GPU parameter overrides. HSL, per-coin override bundles,
BTC collateral and successive halving are rejected before rental. Pareto seeds
are not applied. The pinned GPU backend does not support `gain_strategy_eq`;
choose a supported objective such as `adg_strategy_eq` explicitly in the editor
if appropriate. PBGui does not silently change the optimization objective.
Local daily history for the selected coin and BTC reference is included to
preserve warmup; load missing data through Market Data first.

### Known, working and preferred hosts

The offer list identifies **Previously used** machines from actual local rental history. **Working** means at least one exact optimization result was recorded on that machine, or you explicitly chose **Mark working**. A rental stuck downloading an image is not automatically marked Working. This is historical evidence, not a guarantee about the next rental.

Use **Prefer host** in an offer's Details, in the rental's Host card or under **Hosts**. Preferred machines are shown and considered first, ordered by price within that group. PBGui searches those machines explicitly even when they are absent from the cheapest first page. Hardware requirements, maximum price, rental duration and budget checks still apply. If no preferred machine qualifies, ordinary matching offers remain available. Manual **Rent** still rents exactly the selected offer, and a block always overrides a preference.

Preferences and manual Working marks survive reloads and API restarts. Use **Remove preference** or **Clear working mark** to undo your marks; verified historical results remain visible. Old rentals without a saved Machine ID cannot be matched by GPU name. For a still-existing rental, a host action resolves the Machine ID from Vast; otherwise enter the known Machine ID manually. Future rental supervision records the machine identity automatically. These marks do not stop or change a current rental.

### Block unreliable hosts

Use **Block host** beside an offer, under Hosts, or in the optimizer log's **Host** card. PBGui saves the physical Vast **Machine ID**, so new offers from the same machine are excluded from previews and future manual or automatic rentals. Blocking does not end a current rental; use **End rental** separately when needed.

Open **Hosts** and select **Blocked** to review the IDs, add a known Vast Machine ID, or use **Unblock host**. Exclusions survive reloads and API restarts. Older rentals resolve their machine ID from the matching Vast instance when you click Block host. If that instance is no longer available, enter its Machine ID manually. When exclusions exist, offers without a valid Machine ID are also excluded because PBGui cannot verify that the host is allowed.

## GPU configuration validation

Choosing **Execution → Vast.ai GPU** starts an automatic configuration check
against the pinned worker image, independent of the local GPU/backend.
Changes trigger another check. Scoring/limit rows and mapped fields are marked
red; the validation panel lists field paths and reasons. New metric choices are
restricted to the cloud profile. Existing values are never automatically replaced.

**Save & Queue** is disabled while checks are pending or failing; ordinary saving
of otherwise valid configuration drafts remains available. Before queueing, PBGui
checks the current collected configuration again. The server uses the same checks
before any input export, including requests from Queue Selected or external clients.
Checks cover profile restrictions, metrics, goals, penalty values/ranges,
scenario references and reducers, GPU sizing and bounds. The rules are explicitly
bound to the image digest and PB8 revision; an unknown image cannot use old rules.
Market data availability/export and native runtime/device checks are additional
stages; passing the configuration check does not guarantee a successful GPU run.

## GPU pool and shared rentals

Set **Max concurrent GPUs** in **Rental & Automation** (default **1**, maximum **16**). To avoid starting every queued job manually, set **Auto rent & start** to **On** and save. The confirmation explicitly authorizes the per-rental and simultaneous budget targets. From then on, **Save & Queue** is enough: as soon as input preparation finishes, PBGui rents up to the saved limit and starts one optimizer on each GPU. Compatible idle GPUs are reused first. Provisioning and cleanup rentals count toward the limit until deletion is confirmed. **Auto rent & start** also works with a limit of 1.

Hours, budget and idle cleanup apply **per GPU rental**. The settings show the combined simultaneous budget targets. This is not a lifetime pool spending cap: while the pool is enabled and prepared jobs are waiting, it may rent replacement GPUs after earlier rentals end. Fresh offers must still satisfy your saved requirements and host exclusions. If none matches, the queue shows the reason and retries automatically.

**Pause queue** stops new dispatches and rentals; running jobs continue and idle cleanup still applies. Saving automatic settings while paused updates the limits without resuming the queue. Each active GPU is shown as a compact Queue card with its instance, job, phase and live upload or optimizer progress. Upload progress includes measured throughput and its saturation relative to the provider-advertised host download bandwidth when that value is available. Rental controls are per card. **Replace GPU** ends only that rental while keeping automatic scheduling enabled; PBGui waits for verified provider deletion before restoring pool capacity. Input that never produced optimizer results returns to the queue automatically. Existing optimizer results are preserved and require an explicit Requeue rather than being overwritten. Switching automation off by saving the setting pauses dispatch and prevents new rentals but does not terminate a running rental. Saving a lower GPU limit never terminates existing rentals. The scheduler is restored after an API restart. Each job's log, deadline and budget controls refer to its own GPU.

When a later job needs more transfer allowance, a worker with the current budget-control guard can automatically move unused rental time into transfer reserve within the same authorized budget. PBGui waits for the worker to confirm the shorter deadline before dispatching the job. A pending adjustment prevents idle cleanup. Older guards or insufficient remaining budget show a reason to adjust **Transfer reserve/Budget** manually; PBGui does not spend an unconfirmed allowance or increase the budget automatically.

In **Rental & Automation**, save maximum hours, budget target and idle cleanup independently of GPU requirements. The job row’s **Start** action uses these saved settings and finds a current matching offer. With the default limit of 1, PBGui
creates one instance, establishes verified SSH access, starts the first job,
imports its results and runs the next job on the same GPU. One optimizer runs
at a time on each GPU; its CPU worker count comes from its configuration.

Later compatible jobs also use this worker. Input data is cached by checksum,
so subsequent jobs transfer only missing or changed market files. Each job has
its own immutable input and output directories. Jobs needing more CPU allocation
or transfer reserve remain waiting with an explanation.

The deadline and budget apply to the entire rental. A new job never resets them.
In single-rental mode, an existing rental is never silently replaced. Spare budget beyond the maximum
rental duration is available for additional job transfers.

## Progress and controls

The worker panel shows rental state, hourly price, deadline and idle-deletion
time. Job details distinguish exact evaluations from GPU-screened candidates.
Paretos and logs are backed up roughly once per minute. **Open last downloaded
log** uses the shared log viewer. After completion, native results and preserved
sweep metadata are imported into PB8 Results/Paretos; the exact path is shown.

- **Stop & collect** stops the selected job and saves its results. With the queue
  running, the next job can then start on the same GPU.
- **Pause queue** prevents subsequent starts; the current job continues.
- **Start queue** on a reserved GPU card releases only that manual reservation for scheduling.
- **End rental** on a manual GPU card stops/collects its active job and deletes that worker.
- **Replace GPU** on an automatically managed card stops only that rental. Pool scheduling stays enabled and restores capacity after verified cleanup.
- **Resume supervision** recovers the existing worker after a controller/host
  interruption without creating a replacement rental.

When no eligible work remains, the worker is deleted immediately or after the
selected five-minute idle period. New eligible work cancels that countdown.
A paused queue also follows the idle cleanup policy. Confirm **deletion_verified**;
merely stopping a Vast instance can leave disk storage billable.

## Limits and recovery

Transient upload/SSH failures receive a 15-minute recovery window instead of a fixed two-attempt limit. PBGui retries after 15, 30, then at most 60 seconds and reuses partial files or verified chunks. New observed transfer progress renews that window; repeated retries without progress do not. The log shows the next retry and the original connection error. The separate 15-minute worker-setup timer stops applying once worker health is verified, so slow setup does not consume the upload's recovery time. Active transfers remain bounded by the rental deadline minus the three-minute collection reserve. Stop remains responsive during retry waits. Identity, integrity and disk-full errors still fail immediately; no deadline or budget is extended automatically.

A functioning local systemd user manager and OpenSSH are required. Separate
controller and rental-guard services survive browser and API restarts. A remote
guard attempts deletion at the fixed deadline independently. After a PBGui-host
reboot, use **Resume supervision** to restore transient local services.

Separate **Budget**, **Deadline** and **Transfer reserve** cards show the rental controls. Reserve inputs show four decimal places. The deadline adjustment field always defaults to 60 minutes. Adjustments accept whole minutes from 1 to 1,440, subject to the budget, 24-hour rental limit and cleanup margin. On an explicit deadline change, PBGui upgrades an older rental guard while the optimizer continues running. The new guard must first confirm the unchanged deadline; only then is the requested change sent. Failed handovers restore the old guard. Unconfirmed adjustments remain pending and do not extend the local deadline. API restart is blocked during the handover.

The active rental card lets you edit the **Budget target**. PBGui recalculates the
deletion deadline and sends the change to the worker guard for acknowledgement;
the change is pending until it is acknowledged. The same card shows the transfer
reserve retained for input and result transfers. Increase it there if a queued
job reports an insufficient transfer reserve; PBGui shortens the deadline within
the chosen budget. The budget is an estimate, not a provider-enforced spending cap. At the deadline,
deletion takes priority over unfinished collection; provider outages can delay
it. Current limits per job: 10 GiB input, two upload attempts and 2 GiB result
transfers. Raw verified backups remain under `data/vast/jobs/<job-id>/`.
Checkpoints stay in raw backups and are not installed for automatic local resume.
Partial backups contain Paretos and logs, not a complete optimizer binary.

This shared-queue workflow has offline and browser regression coverage. A paid
end-to-end acceptance run of the new workflow remains outstanding.

Validation accepts PB8 fixed bounds and `[minimum, maximum, step]`, including `0` or `null` for continuous step selection. Real errors offer alternatives with their tradeoffs, such as daily-growth ADG versus keeping the original objective with Local execution. Suggestions never modify the config. More than six issues are placed in an expandable list.

The validation panel uses the editor theme. **Use ADG** replaces only the affected scoring metric in the current draft, preserving its goal and scenario. **Use Local** changes the execution target. Both trigger validation updates; neither saves nor queues automatically. Finish an open inline scoring edit before applying ADG.

Execution help, worker revision and alternative explanations are shown on hover. Only actual errors and their action buttons appear inline; successful and pending checks add no information panel. During revalidation the existing error content stays in place, and unchanged results preserve the DOM, focus and expanded details.

With Vast execution selected, **Apply Training Scenarios** preserves existing scoring entries, including an explicit ADG replacement. Only an empty scoring list receives the cloud-compatible default objectives. The local preset remains unchanged.

Saving a PB8 optimizer config also saves the Scenario Generator template and inputs, even before Preview or Apply. Reopening restores these inputs. Older configs restore them from the applied template metadata when available. Changing generator inputs alone does not replace the applied scenarios.

Local and cloud jobs share the **Queue** table. **Execution** shows **Local** or **Vast.ai**. The cloud status **ready** is displayed as **queued**. Use the row’s **Open log** action to open job details and available logs, and **Close** to hide them. Use Rental & Automation, Account and GPU & Offers for their respective settings. **Start** leaves the queue visible. The row’s **Open log** action opens the same floating Optimize log window used by local jobs, with cloud progress/status and pause, stop, end-rental and recovery controls. During provisioning it shows status before the downloaded log is available. Explanations are available as hover help.

GPU validation offers **Replace metric** with supported scoring metrics, **Edit limit** or **Remove limit** for unsupported constraints, and **Choose coins** for invalid coin lists. These affect the draft only. Replacing a metric changes its meaning; removing a limit relaxes constraints. Explanations use dotted-underlined hover labels beside the actions, rather than tooltips on the buttons. **Use Local** is offered once as an alternative.

The pinned worker accepts 1–64 distinct approved coins across both sides. The previous one-coin restriction belonged to PBGui’s initial benchmark profile and has been removed. Multi-coin cloud execution has not yet been benchmarked here.

**Open log** appears when a downloaded log exists or the cloud job is provisioning/running. Waiting cloud jobs have a trash action instead of Stop. Deleting an inactive queue entry removes it from scheduling and the visible list while retaining stored artifacts and results. Active jobs must finish stopping/collection before removal.

The queue's **Exchange** column reflects the frozen cloud job snapshot. Older entries use their exported market-data manifest; later edits to the source config do not change this display.

Instance polling shares a short cache across controllers. HTTP 429 pauses provider requests automatically, respecting Retry-After. Cleanup verifies absence with fresh requests. Until Vast confirms an instance, the log shows provisioning (instance not confirmed). Rental failures retain their HTTP status and original creation error; an ambiguous creation is not automatically repeated.

Failed cloud jobs retain **Open log** even without a downloaded log file, so their saved failure remains accessible.

Without a downloaded optimizer log, the viewer opens **VastRunner.log**, the shared local supervision log containing provisioning and rate-limit failures. It may contain messages for other cloud jobs. Once available, the viewer switches to the job’s optimizer log.

**Requeue** on an inactive failed/cancelled cloud job prepares a replacement using the saved configuration and the previous evaluation/worker settings. The old entry is removed only after successful preparation; its artifacts remain stored. Requeue does not rent a GPU; Start controls execution.

Start reports a conflict while the previous rental is stopping or awaiting cleanup. Waiting jobs retain Open log while a shared rental remains unresolved, allowing access to supervision and cleanup controls.

The local rental supervisor also detects instances destroyed directly on Vast.ai. Two fresh successful absence checks at least ten seconds apart release the rental lock, end unfinished jobs on that rental and pause its queue; waiting jobs and local result snapshots are preserved. Provider errors reset confirmation. Stopped/offline instances still listed by Vast are not considered deleted. A changed ownership label requires inspection instead of assuming deletion. Newly created or ambiguous instances have a two-minute visibility grace period. No replacement rental is created by this check. The supervisor must be running; Resume supervision can restart it if it stopped.

For ambiguous creation, cleanup waits up to two minutes from the attempt, then requires two fresh successful checks with no matching instance. This can release Start before the rental deadline. Provider errors and rate limits do not count as absence.

Clicking Start immediately shows **Starting…** in the queue row while the request is pending. Cloud Start buttons are temporarily disabled to prevent duplicate requests.

The job log window offers **Requeue** for inactive failed/cancelled jobs and **Start** for ready jobs. Requeue keeps the window open on the replacement job; it does not start a rental.

**Stop & collect** is disabled before a job starts and after it finishes. It is available during provisioning, upload, execution, and collection.

During Requeue the queue displays only the preparing replacement. If preparation fails, the original remains available for retry.

During provisioning, PBGui retrieves the Vast image-pull log at most once per minute and shows it in the existing viewer. The optimizer log takes precedence when available. The file sidebar has a bounded width; activity/error text wraps.

Image loading is bounded by the rental deadline. The separate 15-minute worker setup timer starts only once Vast reports the container running. SSH identity is verified through a public host-key record in that rental's container log. The original spending/deletion deadline is never extended by setup or retries.

After a job failure, the queue pauses; the configured idle cleanup still applies. Once Vast confirms deletion, that instance cannot be resumed. Requeue prepares another attempt, and Start requires a new rental if the previous one has already been deleted.

If the extra provisioning log is not yet available, PBGui tries the container log. Empty or missing-file responses retain the last useful snapshot; without one, the viewer shows that it is waiting for provider logs.

The Optimize log dashboard shows the rented offer’s GPU, VRAM, CPU/RAM, memory/PCIe/disk/network bandwidth, hourly and transfer rates, host reliability, and budget/deadline. These are saved offer values, not live speed measurements or billed totals. Dotted labels explain each value on hover.

During image provisioning, the progress bar counts observed image layers: downloaded and ready (extracted or cached). Layers have different sizes, so this is not a byte percentage or time estimate. Worker setup follows image preparation; once optimization starts, the bar shows evaluations again.

Uploads report bytes consumed by SSH, percentage and average speed, followed by input verification. The cost card uses provider-reported instance charges, not a calculated job estimate.

Optimizer log snapshots refresh every 15 seconds during startup, including before
the first evaluation. Prepared offline bundles can include public market caches;
their original freshness is retained, and expired snapshots block startup.

**Vast instance charges** shows the provider's billing API amount for the entire
rented instance, including any other jobs sharing it. Hover for GPU, disk,
transfer amounts and retrieval time. Billing-read permission is required. The
query is cached for five minutes and does not block job-status polling. Pending
means no charge entry has been returned yet; it does not mean the rental is free.

During optimization, verified result snapshots are published approximately every minute after the first exact evaluation, within the transfer limit. Results and Pareto Explorer in the job log open this intermediate result. After cumulative intermediate result backups reach 1 GiB, PBGui stops downloading those full archives but continues enabled stagnation checks from a bounded metrics-only Pareto snapshot over the verified worker connection. Final collection updates the same result entry.

### Stop on stagnation

New PB8 configurations use **20,000 exact evaluations** when first switched to Vast, provided the iteration field has not been edited. Saved configurations and explicit iteration values are preserved. This is an upper limit; enabled stagnation detection may stop earlier, but does not guarantee it.

Rental & Automation offers an optional early stop (off by default). Minimum exact evaluations defaults to 512, patience to a further 512 exact evaluations, and improvement tolerance to 0.25%. Settings are frozen when each queued job starts; saving changes does not alter a running job.

After the minimum, PBGui measures the feasible exact Pareto front using normalized hypervolume with a fixed scale/reference established by the initial front. This supports one to three signed minimization objectives, including PB8 suite reductions. Relative improvements above the tolerance reset patience; smaller improvements accumulate against the last accepted baseline. Proxy counts and elapsed minutes do not consume patience. Missing, invalid, unsupported or entirely infeasible snapshots suspend detection and restart the observation window.

The job log shows the detection phase, evaluations since the last significant improvement, and patience used. Checks follow verified periodic result snapshots, so stopping can exceed the configured patience by one snapshot interval. On stagnation PBGui requests a graceful stop, collects the final native results, and records a successful stop as completed with a convergence reason. The shared queue then continues or applies its configured idle cleanup. The detector is a heuristic, not proof of optimality; rental deadlines and iteration limits still apply.

Final collection also checks the final Pareto snapshot, using the verified imported evaluation count. The dashboard identifies this as a final snapshot check, not a replay of every evaluation. It preserves the original stop reason (stagnation, requested stop or rental time limit); a retrospective threshold crossing does not rewrite that reason. Retry result import performs the same final check. If the final front cannot be assessed, the saved results remain available and the dashboard reports the check as unavailable.

The open Results/Paretos list refreshes when the next ten-second job poll discovers a newly published snapshot (normally about 60 seconds plus up to 10 seconds and transfer time). Elapsed measures optimizer runtime, excluding provisioning and upload; it freezes at the reported runtime when finished.

Live utilization is sampled approximately every 15 seconds over the existing SSH connection. CPU use is relative to effective container CPU capacity and needs two samples. RAM includes container cache; VRAM and GPU activity come from nvidia-smi. Values older than 45 seconds or unavailable readings are shown as a dash, not as zero.

New cloud jobs automatically use the measured CPU quota, rounded down to whole workers (minimum one), for both n_cpus and gpu.exact_workers. For example, a measured quota of 9.6 uses nine workers, even if the offer advertises ten cores. The source config remains intact; a separately verified execution copy is uploaded. Existing running pools are not resized. Local backtests from cloud results clear the container-only HLCV path and use local data, including for holdout dates.

Stop & collect first asks you to confirm the selected optimizer. After confirmation it immediately shows **Stopping…**, then **Stop requested** until the supervisor starts **Collecting results…**. The next eligible queued job starts after collection.

Each GPU card provides **Log** for the assigned job's detailed live view and **Results** for its linked optimizer result. Results stays disabled, with a hover explanation, until a verified result snapshot exists. **Replace GPU** requires confirmation because it stops the assigned optimizer, collects its current results, ends that rental and lets Auto rent & start restore pool capacity after verified cleanup.

Uploads retain checksum-verified 8 MiB chunks across SSH interruptions. Only missing chunks are sent again; the complete archive is verified before installation. Reconnecting is shown in upload progress.

The live log follows the newest line after long GPU messages finish wrapping. Scrolling upward intentionally pauses following; scrolling back to the bottom resumes it.

CPU checks outstanding counts submitted exact validations that PB8 has not yet incorporated: queued, running, and completed results waiting behind earlier submissions. PB8 does not expose these groups separately. The observation is taken from generation profiles; its age is shown and old values are labelled as the last sample. The top exact counter counts incorporated results.

Hover the dotted information labels in the job log for help. Stagnation 0 / 512 means the latest checked snapshot established or significantly improved the baseline; 512 more exact validations without sufficient improvement trigger stop and collection. Since last improvement uses the current exact count; the stagnation bar uses the last verified snapshot and may lag. The Stagnation tooltip shows the job’s minimum, patience and percentage threshold.

The log also shows the last checked exact result, the last significant improvement, and how many newer results await the next snapshot check. A repeatedly zero stagnation counter can indicate continued improvements, rather than a stalled checker.

Vast scoring and limits use the complete PB8 GPU metric contract, including supported aliases. The current worker and local PB8 have identical metric schema, registry and reduction-source hashes: 157 supported metric names and 460 accepted spellings including aliases. These are not 460 distinct metrics. CPU-only metrics remain unavailable. Broader metric support does not remove other GPU strategy or execution restrictions.

A new run shows a waiting message until its own provider or optimizer log is available. Earlier runs from the global runner log are not shown as its startup output.

### Recovering from UI request failures

- Worker actions show a pending label and remain locked until the request finishes. Requeue shows **Preparing…** in the queue; deleting a queue item asks for confirmation.
- If GPU validation cannot be completed, use **Retry validation**. Ordinary **Save** remains available, but **Save and Queue** requires successful validation.
- A rejected Vast key or missing provider permission is shown as a provider error. Correct the key/permissions and retry. **PBGui session expired** instead requires signing in again and reloading the page.
- **Vast instance charges** shows **Unavailable** after a failed billing request. A previously retrieved amount remains visible as **last retrieved**; hover the label for the failure and retrieval details.
- If local rental supervision is unavailable, Settings and the job log explain the systemd/OpenSSH prerequisite. Choose a job using **Open log** in the unified queue; that shared window contains its progress, errors, rental details and latest downloaded log.
- **Show incompatible hosts** refreshes the offer preview. Selecting an offer copies its GPU type into the requirements; save the requirements before starting. It does not reserve that offer.

Cloud export plans all active scenarios together: `coins`, `ignored_coins`, `exchanges`, dates and `coin_sources` are supported. It includes the shared market pool and BTC reference history and deduplicates files. Daily shards are limited to the combined base/scenario date range plus PB8's computed optimizer warmup, rounded outward to full days. Optimizer bounds, scenario parameter overrides and native warmup caps are respected; a zero cap means uncapped computed warmup, not all available history. If warmup cannot be determined, preparation stops rather than guessing. Requeue creates a fresh package from the saved configuration using these limits; existing packages are not rewritten. Additional scenario exchanges are included in public-market and first-candle metadata staging. The original config is unchanged; nested parameter overrides are flattened only in the frozen worker copy.

Missing mappings or local OHLCV files identify the scenario, coin and exchange; download the required history in Market Data before queueing. A coin need not be listed on every exchange. `coin_sources` must assign a coin consistently across the suite. Hyperliquid uses USDC; the other supported venues use USDT.

Empty or omitted scenario `exchanges` inherit the base exchanges. Omitted/null `coins` inherit the base selection, while `coins: []` explicitly selects no coins and is rejected with that explanation, matching PB8. Empty `overrides` and `coin_sources` are accepted. Each active scenario must contain 1–64 selected coins; the total suite may contain more. Parameter overrides must reference existing canonical PB8 paths supported by the pinned GPU worker. Per-coin override bundles, data-path overrides and HSL remain outside this cloud profile; errors identify the scenario and exact field.

The navigation Restart button also detects outdated local Vast run and guard supervisors, including older supervisors without a startup version. Restart replaces these local controllers and resumes their persisted jobs on the existing rental; it does not end the rental or request a new GPU.

During upload, **Transferring job metadata** reports compressed configuration/manifest bytes submitted to SSH. This is not confirmation that the worker has received them. Candle-file synchronization begins after the worker acknowledges metadata preparation.

**Stop & collect** also cancels an upload that is waiting for metadata acknowledgement. Transfer controllers continue checking stop requests while SSH or rsync is silent, including after its output closes but before its process exits. Before optimization starts there are no optimizer results to collect; the job becomes **Cancelled**. A paused rental remains subject to its existing idle cleanup and deadline.

Valid Pareto fronts with more than 1,000 results remain eligible for stagnation checks; all feasible points contribute to the exact hypervolume calculation.
