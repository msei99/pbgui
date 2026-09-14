# Vast.ai GPU queue

Cloud optimization is integrated into **PB8 Optimize**. There is no separate
Vast system page. Each installation uses its own Vast account and rental credit.

## Cloud setup in Optimizer Settings

The existing **Settings** sidebar button opens a full-width view beside the sidebar, with local execution settings, cloud account setup and GPU requirements. **Back to Queue** returns to the job list.

Open **Queue → Settings → Cloud setup**. Save your Vast API key and click **Test connection
& refresh balance**. The key is stored locally with owner-only permissions and
is never sent to bot servers. The balance uses available Vast rental credit.
Reading the account requires User Read; renting and cleanup need Instance
Read/Write and the SSH/bootstrap operations. PBGui does not top up the account.

**Create Vast.ai account** opens the PBGui referral link. It supports PBGui through
the Vast referral program and is loaded only when clicked.

The versioned `ghcr.io/msei99/pbgui-pb8-worker` image is public. No GitHub account
or registry token is required. PBGui verifies anonymous access to the pinned
manifest before starting a paid rental. The image contains runtime software,
not user configurations, course data or credentials.

## GPU requirements in Settings

In the editor, choose only **Execution → Vast.ai GPU** or **Local**.
Under **Queue → Settings → GPU requirements**, choose a GPU type and maximum
hourly price, minimum VRAM/RAM/CPU, disk size and verified-host preference.
**Preview available GPUs** is informational: clicking a row copies its type,
not its offer ID. Click **Save settings** to persist the requirements.
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
can still fail; PBGui does not create an unapproved replacement rental.
Hourly estimates include disk; transfers cost extra. The worker checks its actual
CPU quota. Saved requirements apply to the next rental, not an existing worker.

**Save & Queue** uses the current configuration and prepares a frozen input
snapshot. **Queue Selected** also respects the saved execution target. Local
configs continue through the existing local queue. Cloud jobs are shown in the
same **Queue** table as local jobs, identified by the **Execution** column.

The current profile supports fresh multi-coin `ema_anchor` and
`trailing_martingale` jobs, Binance/Bybit and date-only suites. HSL, overrides,
BTC collateral and successive halving are rejected before rental. Pareto seeds
are not applied. The pinned GPU backend does not support `gain_strategy_eq`;
choose a supported objective such as `adg_strategy_eq` explicitly in the editor
if appropriate. PBGui does not silently change the optimization objective.
Local daily history for the selected coin and BTC reference is included to
preserve warmup; load missing data through Market Data first.

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

## One rental for several jobs

In **Queue → Settings**, save maximum hours, budget target and idle cleanup with the GPU requirements. The job row’s **Start** action immediately rents using these saved settings, without another confirmation, and finds a current matching offer. PBGui
creates one instance, establishes verified SSH access, starts the first job,
imports its results and runs the next job on the same GPU. One optimizer runs
at a time; its CPU worker count comes from its configuration.

Later compatible jobs also use this worker. Input data is cached by checksum,
so subsequent jobs transfer only missing or changed market files. Each job has
its own immutable input and output directories. Jobs needing more CPU allocation
or transfer reserve remain waiting with an explanation.

The deadline and budget apply to the entire rental. A new job never resets them.
An existing rental is never silently replaced. Spare budget beyond the maximum
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
- **Start queue** resumes scheduling on the existing worker.
- **End rental** pauses cloud scheduling, stops/collects the active job and deletes
  the worker. Queued jobs remain available.
- **Resume supervision** recovers the existing worker after a controller/host
  interruption without creating a replacement rental.

When no eligible work remains, the worker is deleted immediately or after the
selected five-minute idle period. New eligible work cancels that countdown.
A paused queue also follows the idle cleanup policy. Confirm **deletion_verified**;
merely stopping a Vast instance can leave disk storage billable.

## Limits and recovery

A functioning local systemd user manager and OpenSSH are required. Separate
controller and rental-guard services survive browser and API restarts. A remote
guard attempts deletion at the fixed deadline independently. After a PBGui-host
reboot, use **Resume supervision** to restore transient local services.

The budget is an estimate, not a provider-enforced spending cap. At the deadline,
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

Local and cloud jobs share the **Queue** table. **Execution** shows **Local** or **Vast.ai**. The cloud status **ready** is displayed as **queued**. Use the row’s **Open log** action to open job details and available logs, and **Close** to hide them. Rental limits, account setup and GPU requirements are saved in Settings. **Start** leaves the queue visible. The row’s **Open log** action opens the same floating Optimize log window used by local jobs, with cloud progress/status and pause, stop, end-rental and recovery controls. During provisioning it shows status before the downloaded log is available. Explanations are available as hover help.

GPU validation offers **Replace metric** with supported scoring metrics, **Edit limit** or **Remove limit** for unsupported constraints, and **Choose coins** for invalid coin lists. These affect the draft only. Replacing a metric changes its meaning; removing a limit relaxes constraints. Explanations use dotted-underlined hover labels beside the actions, rather than tooltips on the buttons. **Use Local** is offered once as an alternative.

The pinned worker accepts 1–64 distinct approved coins across both sides. The previous one-coin restriction belonged to PBGui’s initial benchmark profile and has been removed. Multi-coin cloud execution has not yet been benchmarked here.

**Open log** appears when a downloaded log exists or the cloud job is provisioning/running. Waiting cloud jobs have a trash action instead of Stop. Deleting an inactive queue entry removes it from scheduling and the visible list while retaining stored artifacts and results. Active jobs must finish stopping/collection before removal.

The queue's **Exchange** column reflects the frozen cloud job snapshot. Older entries use their exported market-data manifest; later edits to the source config do not change this display.

Instance polling shares a short cache across controllers. HTTP 429 pauses provider requests automatically, respecting Retry-After. Cleanup verifies absence with fresh requests. Until Vast confirms an instance, the log shows provisioning (instance not confirmed). Rental failures retain their HTTP status and original creation error; an ambiguous creation is not automatically repeated.

Failed cloud jobs retain **Open log** even without a downloaded log file, so their saved failure remains accessible.

Without a downloaded optimizer log, the viewer opens **VastRunner.log**, the shared local supervision log containing provisioning and rate-limit failures. It may contain messages for other cloud jobs. Once available, the viewer switches to the job’s optimizer log.

**Requeue** on an inactive failed/cancelled cloud job prepares a replacement using the saved configuration and the previous evaluation/worker settings. The old entry is removed only after successful preparation; its artifacts remain stored. Requeue does not rent a GPU; Start controls execution.

Start reports a conflict while the previous rental is stopping or awaiting cleanup. Waiting jobs retain Open log while a shared rental remains unresolved, allowing access to supervision and cleanup controls.

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

During optimization, verified result snapshots are published approximately every minute after the first exact evaluation, within the transfer limit. Results and Pareto Explorer in the job log open this intermediate result. Final collection updates the same entry.

### Stop on stagnation

Setup offers an optional early stop (off by default). Minimum exact evaluations defaults to 512, patience to a further 512 exact evaluations, and improvement tolerance to 0.1%. Settings are frozen when each queued job starts; saving changes does not alter a running job.

After the minimum, PBGui measures the feasible exact Pareto front using normalized hypervolume with a fixed scale/reference established by the initial front. This supports one to three signed minimization objectives, including PB8 suite reductions. Relative improvements above the tolerance reset patience; smaller improvements accumulate against the last accepted baseline. Proxy counts and elapsed minutes do not consume patience. Missing, invalid, unsupported or entirely infeasible snapshots suspend detection and restart the observation window.

The job log shows the detection phase, evaluations since the last significant improvement, and patience used. Checks follow verified periodic result snapshots, so stopping can exceed the configured patience by one snapshot interval. On stagnation PBGui requests a graceful stop, collects the final native results, and records a successful stop as completed with a convergence reason. The shared queue then continues or applies its configured idle cleanup. The detector is a heuristic, not proof of optimality; rental deadlines and iteration limits still apply.

The open Results/Paretos list refreshes when the next ten-second job poll discovers a newly published snapshot (normally about 60 seconds plus up to 10 seconds and transfer time). Elapsed measures optimizer runtime, excluding provisioning and upload; it freezes at the reported runtime when finished.

Live utilization is sampled approximately every 15 seconds over the existing SSH connection. CPU use is relative to effective container CPU capacity and needs two samples. RAM includes container cache; VRAM and GPU activity come from nvidia-smi. Values older than 45 seconds or unavailable readings are shown as a dash, not as zero.

New cloud jobs automatically use the measured CPU quota, rounded down to whole workers (minimum one), for both n_cpus and gpu.exact_workers. For example, a measured quota of 9.6 uses nine workers, even if the offer advertises ten cores. The source config remains intact; a separately verified execution copy is uploaded. Existing running pools are not resized. Local backtests from cloud results clear the container-only HLCV path and use local data, including for holdout dates.

Stop & collect immediately shows **Stopping…**, then **Stop requested** until the supervisor starts **Collecting results…**. The next eligible queued job starts after collection.

Uploads retain checksum-verified 8 MiB chunks across SSH interruptions. Only missing chunks are sent again; the complete archive is verified before installation. Reconnecting is shown in upload progress.

The live log follows the newest line after long GPU messages finish wrapping. Scrolling upward intentionally pauses following; scrolling back to the bottom resumes it.

CPU checks outstanding counts submitted exact validations that PB8 has not yet incorporated: queued, running, and completed results waiting behind earlier submissions. PB8 does not expose these groups separately. The observation is taken from generation profiles; its age is shown and old values are labelled as the last sample. The top exact counter counts incorporated results.

Hover the dotted information labels in the job log for help. Stagnation 0 / 512 means the latest checked snapshot established or significantly improved the baseline; 512 more exact validations without sufficient improvement trigger stop and collection. Since last improvement uses the current exact count; the stagnation bar uses the last verified snapshot and may lag. The Stagnation tooltip shows the job’s minimum, patience and percentage threshold.

The log also shows the last checked exact result, the last significant improvement, and how many newer results await the next snapshot check. A repeatedly zero stagnation counter can indicate continued improvements, rather than a stalled checker.

Vast scoring and limits use the complete PB8 GPU metric contract, including supported aliases. The current worker and local PB8 have identical metric schema, registry and reduction-source hashes: 157 supported metric names and 460 accepted spellings including aliases. These are not 460 distinct metrics. CPU-only metrics remain unavailable. Broader metric support does not remove other GPU strategy or execution restrictions.

A new run shows a waiting message until its own provider or optimizer log is available. Earlier runs from the global runner log are not shown as its startup output.
