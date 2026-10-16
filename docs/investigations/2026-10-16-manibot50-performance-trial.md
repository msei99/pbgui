# manibot50 targeted PB8 performance trial

16 October 2026. Operator report; this does not activate patches on other hosts.

## Scope and authorization

The user requested installation and measurement of both prepared PB8 performance
fixes specifically on manibot50. They separately approved briefly pausing PBRun,
restarting only HYPErQuantum and hl_HYPErQuantum5, resuming PBRun, and restoring
original files if activation failed. Bot configurations, candle indices, account
credentials, host packages and other services are outside this change.

The runtime payload is the cumulative source patch documented in
`patches/pb8-live-history-performance.md`: parsed candle-row reuse/merge shortcut,
HSL current-endpoint reuse, and historical gap range indexing. Installation
replaces only `src/candlestick_manager.py`,
`passivbot-rust/src/hsl_evaluator.rs`, the active venv's native Rust extension,
and that extension's source-fingerprint stamp.

## Preparation and verification

- VPS source commit: `82e0edf67a1f037b783d139039021ade8f449478`.
  Both affected source files exactly matched their baseline SHA-256 hashes.
- The VPS's generated `Cargo.lock` differs from the initial local build, despite
  matching tracked source. It was preserved. A separate local build used that
  exact lock, with no compilation on the VPS.
- Trial source fingerprint:
  `cbd7ce2de14c797b2e6682e9913f1c1f3e7681e4cab9ff735c05f1bdf7a1a208`.
  Native artifact SHA-256:
  `1711e1c5322845af31c5b7883cef31563b883ce61073c1cb7b06cc39f131671c`.
- The rebuilt native module matched all **3,072 complete baseline HSL outputs**.
  **171 focused local tests passed** in 2.76 seconds. The staged artifact imported
  on the actual VPS and matched **256 synthetic reference outputs** there.
- Isolated synthetic candle/EMA tests on the VPS, with no exchange access,
  preserved outputs. With 16,884 irrelevant historical gaps, median process CPU
  time fell from 92.779 to 0.744 ms for a recent 500-candle read, and from 17.303
  to 0.218 ms for a warm projected EMA result. These are operation-level probes,
  separate from live measurement.
- Original files, permissions and hashes are retained below
  `/home/mani/software/pb8-performance-trials/20261016-live-performance/backup`.
  The sibling `stage` directory contains verified payloads and a manifest.

## Measurement discipline

An initial sampler was discarded. The accepted baseline began after all builds,
transfers and offline VPS probes completed. Builds were local, never on the VPS.
Accepted measurement windows run only a lightweight `/proc` sampler every five
seconds; no builds, transfers or additional diagnostic probes run concurrently
on manibot50. Host work excludes idle, I/O wait and steal; process CPU uses
utime/stime deltas. Values are measured interval averages, not lifetime `ps` CPU.
Memory figures are per-process RSS/swap and are not summed as unique residency.

Baseline: **06:43:43–06:46:43 UTC**, three minutes:

| Measurement | Before patch |
| --- | ---: |
| Host CPU work | 56.36% |
| Host idle | 41.84% |
| I/O wait | 1.81% |
| CPU steal | 0.00% |
| Bybit bot CPU | 13.67% |
| Hyperliquid bot CPU | 23.70% |
| Combined bot CPU | 37.37% |

The baseline also contained swap/page-fault bursts. It cannot by itself identify
all causes of host load; a sequential live before/after trial also changes process
age, cache warmth and market/network workload.

## Activation record

The first attempt requested graceful SIGINT shutdown at 06:47:39 UTC. Its
30-second stop limit expired before both bots exited, so no patch files were
installed and PBRun was resumed with original files. Bybit subsequently restarted
on the original code. The Hyperliquid process remained in shutdown. The approved retry allowed a longer SIGINT interval followed by the same
SIGTERM fallback used by PBRun; it did not escalate to SIGKILL. That retry
identified a second obstacle: the monitor-agent service auto-healer restarted
PBRun during the systemd stop, allowing a new bot process to appear. Again no
patch files were installed. The final activation pauses only the PBRun process
with SIGSTOP while its systemd unit remains active, then resumes it with SIGCONT.
This prevents the auto-healer from defeating the scoped pause without changing
monitoring settings or other services.

A subsequent wait check incorrectly counted zombie children as still running
while PBRun was paused, preventing activation again. This was an orchestration
error, not evidence of another performance defect. The check was corrected to
exclude exited/zombie processes while still verifying process identity.

Final activation succeeded at **06:55:08 UTC**. Both approved bots stopped
gracefully within approximately three seconds in the successful attempt; no
SIGTERM or SIGKILL was required in that attempt. PBRun resumed immediately.
Bybit relaunched as PID **240093**, Hyperliquid as PID **240111**, by 06:55:22.
The installed environment artifact, its embedded fingerprint, its stamp, the
complete Rust source fingerprint and its SHA-256 agree with the trial manifest.

The six-minute post-start sampler ran from **06:56:00 to 07:02:00 UTC**.
The first three minutes were warm-up and excluded; the accepted steady window
is **06:59:00–07:02:00 UTC**. No builds, transfers or extra VPS probes ran during
either accepted measurement. Both new process IDs remained unchanged throughout.

## Results

| Interval-average CPU work | Before, 3 minutes | After warm-up, 3 minutes |
| --- | ---: | ---: |
| Bybit HYPErQuantum | 13.67% | 10.79% |
| Hyperliquid hl_HYPErQuantum5 | 23.70% | 19.29% |
| Combined bots | 37.37% | 30.08% |
| Entire VPS | 56.36% | 50.12% |
| Host I/O wait | 1.81% | 0.31% |
| Host steal | 0.00% | 0.00% |

Combined bot CPU fell **19.5% relative**, or 7.29 percentage points. Host CPU
work fell **11.1% relative**, or 6.24 points. After-patch one-minute host work
ranged from 48.08% to 53.10%; the five-second peak was still **96.13%**. These
are measured improvements in this short trial, not removal of all high-load
spikes or a clean separation of patch effect from restart/cache/market effects.

Final per-process RSS/swap: Bybit approximately **256.2/5.2 MiB**, Hyperliquid
**241.8/2.3 MiB**. Available system memory was approximately **66.3 MiB**.
The baseline had substantial old swap backing and page-fault bursts; restart
changed those conditions. This memory change must not be attributed solely to
the patch. RAM headroom remains limited.

## Final runtime and health checks

- Both new processes map the current installed native module inode, with no
  deleted/old extension mapping. Source and module hashes still match the manifest.
- Only the two intended PB8 tracked source files are modified. `Cargo.lock` is
  unchanged. PBRun, monitor-agent, PBCluster and PBCoinData services are active.
- Both new main logs contained **zero ERROR/CRITICAL/traceback records** in the
  inspected bounded tails. Their stderr files did not grow after initial startup
  health capture; no new stderr tracebacks were found during measurement.
- Both new logs contained five HSL stale-input messages, so this does not
  certify absence of input-quality warnings. Latest extracted HSL summaries
  had `green=1`, `red=0`, `unavailable=0`, `estimated=1`, and no `stale_reason`.
  The latest visible summary timestamps were 07:05:06 UTC for Bybit and
  07:01:11 UTC for Hyperliquid; these are visible summaries, not a heartbeat
  guarantee for every internal evaluation. Estimated history remains in use.

The approved hotfix remains installed only on manibot50. No commit, push,
release, global deployment or bot-configuration edit was performed. Before a
future PB8 update, account for these two local source modifications rather than
silently overwriting them. Original runtime files and their hashes remain in
the backup directory for an explicitly requested rollback.

Local evidence: `/tmp/manibot50-performance-before-clean.jsonl`,
`/tmp/manibot50-performance-after.jsonl`,
`/tmp/manibot50-performance-comparison.json`,
`/tmp/manibot50-performance-activation-completed.jsonl`, and
`/tmp/manibot50-performance-health-final.json`. Remote activation/verification
records are retained in the trial directory beside the backup and stage.


## Subsequent publication

At the user's later request, the tested source changes were published in
[upstream PR #1994](https://github.com/enarjord/passivbot/pull/1994), commit
`fb8bf8c644df4d4901ac41f736b1957491393817`. This operator report and private VPS
evidence were excluded. PR publication did not change the installed hotfix or
any running process.
