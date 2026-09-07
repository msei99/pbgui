# Unreleased

- Fixed issue #224: exchange-key saves now require PB7 persistence, report publication/projection failures, synchronously project configured PB8 runtimes, and retain secret-free publication intent for serialized PBCluster recovery without reverting newer saves or breaking rename transactions.
- Hardened issue #224 recovery with before/after exchange-snapshot hashes: rejected or interrupted prewrite saves no longer republish stale PB7 credentials over newer desired state, while prior pending publications and committed post-replacement failures remain recoverable.
- Represented unavailable CoinMarketCap metrics as null, corrected empty copy-trading discovery, and added Coin Data quote selection.
- Fixed shared log viewers so replaced WebSockets cannot mutate current state, and bounded log snapshots, live buffers, requests, and rendered lines to the newest 50,000 entries.
- Serialized each Coin Data exchange refresh across processes and made JSON state publication durable and collision-free.
- Prevented duplicate resident task workers with process-lifetime ownership, identity-safe PID handling, and coordinated startup acknowledgement.
- Fixed stale API-key editor requests and made rename-plus-update a capability-gated, crash-recoverable locked transaction with credential-generation-safe expiry persistence.
- Fixed PB7/PB8 backup rendering when backup entries are present.
- Bounded logging rotation settings and made local log tails preserve partial lines across safe rotation-aware cursors.
- Serialized service lifecycle actions and added an identity-safe direct API restart PID handoff.
- Closed lifecycle review gaps in migration locking, task-worker ownership loss, active-log purging, and restart blocker inspection.
- Completed Coin Data row symbols and deterministic ordering, bounded ticker recovery, partial refresh reporting, and Hyperliquid HIP-3 fallback normalization.
- Bounded and coalesced Coin Data refresh jobs, invalidated credential expiry data after key replacement, and clarified durable Market Data Queue pauses.
- Kept API console writes synchronized with log purges, retained worker ownership until jobs quiesce, and preserved refresh diagnostics when page-state rebuilding fails.
- Audited issue #224: confirmed existing asynchronous PB8 credential projection and identified save/publication failure paths requiring follow-up; no runtime behavior changed.
