# ETH_EMA_Sweep convergence review — 2026-09-14

Read-only reconstruction of the transferred result `2026-09-14T05_57_22_combined_2455days_suite_1_coins_a6735b1b_vast_0d11bd1f3393`.

- Actual stop: rental deadline reserve (four minutes before 11:47 local), not convergence.
- Durable all_results.bin: 6,208 records; UI last reported 6,206.
- Decode the PB8 incremental overlay stream continuously, including deletion markers. The pinned worker emits differences after the first record, without a reset every 100 records.
- Reconstruct the feasible nondominated front from signed unpenalized objectives. Apply the persisted origin, scale and reference, 0.1% relative hypervolume tolerance and 512-evaluation patience.
- At record 4,821: reconstructed hypervolume 1.0206149352254041 exactly matches the saved convergence checkpoint; front size 463.
- Final reconstructed front: 543 points, matching the saved Pareto front. Both yield hypervolume 1.0231401642674254 (+0.2474223093% versus checkpoint).
- Continue the persisted baseline from the checkpoint, checking every record: significant improvement at 5,549 (+0.1344119812% versus previous baseline), then 5,709 (+0.1000135818%). Final age since significant improvement: 499/512, leaving 13 evaluations before the patience threshold if no further improvement occurred.
- Counterfactual: uninterrupted per-record checks would first have requested stopping at 5,223, before those later improvements. Patience is a heuristic, not proof that no better result can be found.
- Applying only the final snapshot through the current advance() implementation would reset the counter at 6,208. This differs from historical replay because improvements are attributed to the checked snapshot's count.

No job state, source results, rental or optimizer configuration was changed. No GPU was rented.
