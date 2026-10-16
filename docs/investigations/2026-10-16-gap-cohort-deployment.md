# PB8 timestamp-cohort deployment on manibot50

The user explicitly requested the PBGui release and direct installation of the tested PB8 follow-up on manibot50. Only `src/candlestick_manager.py` was replaced. PBGui on the VPS was not updated by this operation.

## Preconditions and activation

- PB8 HEAD remained `82e0edf67a1f037b783d139039021ade8f449478` with the already approved earlier performance overlays.
- Original candle source SHA256: `65d1a2348eabd2e8919917219d4b366e8fbb6eecce5744909a3927cae8052321`.
- Installed candle source SHA256: `8f7dc2162d433fe3328f76b714af8a2560d4932a4e2182b54df1f2b9f5ad57f1`.
- Native extension SHA256 remained `1711e1c5322845af31c5b7883cef31563b883ce61073c1cb7b06cc39f131671c`, matching the VPS Rust source/Cargo.lock fingerprint `cbd7ce2de14c797b2e6682e9913f1c1f3e7681e4cab9ff735c05f1bdf7a1a208`. No Rust build or native replacement occurred.
- A two-case synthetic gap/synthetic-exclusion import comparison passed before touching the active file. The source had already passed 838 local focused tests and five docs tests.
- Backup: `/home/mani/software/pb8-performance-trials/20261016-gap-timestamp-cohort/candlestick_manager.before.py`; staged source, log offsets and activation manifest are in the same owner-only directory.

PBRun PID 237972 was paused with SIGSTOP at 09:46:56 UTC while its systemd unit remained active. Only the two named bots received graceful shutdown signals. Both stopped by 09:47:04; the Python file was replaced atomically and PBRun resumed. Both replacement processes were observed at 09:47:06 and remained stable through the startup verification ending 09:47:37. No rollback was required.

| Bot | Previous PID | Replacement PID |
|---|---:|---:|
| HYPErQuantum | 240093 | 306970 |
| hl_HYPErQuantum5 | 240111 | 306977 |

## Health

At 09:48:28 UTC, both replacement processes were still running, the installed source hash matched, and PBRun was active. Since the recorded pre-activation log offsets:

| Bot | New ERROR/CRITICAL/traceback lines | HSL GREEN entries | HSL stale-input warnings |
|---|---:|---:|---:|
| HYPErQuantum | 0 | 7 | 3 |
| hl_HYPErQuantum5 | 0 | 3 | 1 |

The input-age warnings remain observable; GREEN entries and absent errors do not certify that every input is authoritative.

## Short CPU observation

No builds ran during either read-only sample. Each window was 60 seconds; the after sample began approximately 83 seconds after relaunch.

| Measurement | Before, 09:43:27–09:44:27 UTC | After, 09:48:27–09:49:27 UTC |
|---|---:|---:|
| Host CPU work | 50.68% | 47.94% |
| HYPErQuantum CPU | 11.32% | 11.37% |
| hl_HYPErQuantum5 CPU | 18.28% | 16.35% |
| Combined bot CPU | 29.60% | 27.72% |
| Host I/O wait | 1.29% | 0.30% |

These short windows include restart/cache/swap and market differences. They establish a healthy running installation, not an isolated causal CPU reduction. The two PBGui runtime improvements are released separately and are not included in this VPS comparison.

Local evidence: `/tmp/manibot50-cohort-activation.jsonl`, `/tmp/manibot50-cohort-before.jsonl`, `/tmp/manibot50-cohort-after.jsonl`. Remote `activation.json` records both process identities, file hashes and backup path. No bot configuration or other host was changed.
