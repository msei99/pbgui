# PB8 gap timestamp cohort follow-up

Base: `fb8bf8c644df4d4901ac41f736b1957491393817`, the published head of [PR #1994](https://github.com/enarjord/passivbot/pull/1994).

The patch is developed in `/tmp/pb8-gap-cohort-fix` on local branch `perf/gap-timestamp-cohort`. The PB8 branch is not committed, pushed, or part of the existing upstream PR. The Python-only fix was subsequently installed on manibot50 with explicit authorization; see [the deployment report](../docs/investigations/2026-10-16-gap-cohort-deployment.md). It leaves the original PB8 worker checkout untouched. It requires the earlier PR's candle/gap code; do not apply it directly to the older upstream base.

## Change

`_unverified_uncovered_gap_ranges` sorts/copies current real candles, excludes synthetic rows, and creates one contiguous int64 timestamp array for the synchronous call. All gap searches reuse that array instead of asking NumPy to repeatedly materialize the full strided timestamp field. No cross-call timestamp cache is introduced. Full missing-span widths, scalar search semantics, duplicate/off-grid handling and merge/provisional policies are unchanged.

## Validation

- 838 focused Python tests passed for candle/gap/disk reuse, sorted-copy, EMA timing, resolution boundaries and the existing live-performance binding tests.
- New differential coverage includes 2,000 independently generated states with unsorted/duplicate/off-grid timestamps, overlapping/legacy/malformed/reason-filtered metadata, large timestamps, reversed query bounds and synthetic exclusions.
- Six explicit full-outage cases, corrections/evictions/replacements and synthetic-set mutations compare against the exact frozen immediately preceding implementation.
- Sixteen full candle/EMA combinations compare cold and repeated warm reads, strict/provisional behavior, missing/synthetic/wide-gap coverage and fractional EMA spans. Candle arrays and close/volume/log-range EMA arrays match byte for byte, with nonempty-result assertions on covered histories.
- Work-count regression requires one contiguous timestamp vector shared by all 2,000 binary searches in a 1,000-gap cohort; no-unknown-gap reads still exit before allocating it.
- Five documentation tests passed. The AI documentation checker reported no errors and two existing documentation-size warnings.
- Native tests used the existing verified extension: SHA256 `fac37edb918337adf598238b36fd7b1e1ea8e6d89a2900a4d9f8696049872d77`, matching Rust source fingerprint `77d78087d87c4dac55a5590ac9385594b47105d57bac6f35e41515f22e6280a5`. This follow-up changes no Rust source.

## Paired local benchmark

`PYTHONPATH=src /home/mani/software/venv_pb8/bin/python tests/benchmark_gap_timestamp_cohort.py`

43,200 synthetic retained candles, 16,884 covered one-minute unknown-gap records, five alternating before/after CPU samples:

| Algorithm | Median CPU |
|---|---:|
| Frozen prior strided searches | 1,165.668 ms |
| Per-call contiguous timestamp index | 103.701 ms |

11.24x faster in this workload; identical empty missing-span results. The benchmark is included in the patch and uses temporary synthetic storage. It measures the gap-check operation, not total bot CPU. Raw HSL candle reads use `standardize=False` and bypass this specific standardization check; no direct HSL CPU saving is claimed.

Apply only in an approved checkout based on the stated head:

```bash
git apply --check /path/to/pb8-gap-timestamp-cohort.patch
git apply /path/to/pb8-gap-timestamp-cohort.patch
```

Further publication or deployment needs separate authorization.
