# Tutorial: Compare

Compare uses the same shared GUI but version-specific sources:

- PB7 backtest fills, **PBGui Simulation**, and **PB7 Backtest Engine**
- A **Stored PB8 Result** and a **Fresh PB8 Replay**

---

## 1) Start from a result (recommended)
1. Open PB7 or PB8 **Backtest Results**.
2. Select one result and click **Strategy Explorer**.

You can also open a PB8 candidate from **Pareto Explorer**. PB8 Run and PB8 Backtest provide direct config handoffs, but only a result handoff supplies stored-result provenance for **Stored PB8 Result vs Fresh PB8 Replay**.

To compare two PB8 Pareto candidates instead, select the first candidate and click **Pin Explorer Baseline**. Then select a different candidate from the same result and click **Strategy Explorer**. No result path is needed; the owner-bound draft carries both configs and Compare labels them as the current config and pinned baseline. The baseline is page-local and is cleared when the result changes or the page reloads. Missing referenced override files block the handoff instead of being ignored.

For PB8, the authenticated handoff stores the validated result location server-side and opens Strategy Explorer with an owner-bound opaque draft ID. The result path is not placed in the browser page or URL.

---

## 2) Start Compare
1. Open **Compare**.
2. PB7 users choose one of the existing PB7 compare modes.
3. PB8 result users select **Stored PB8 Result vs Fresh PB8 Replay**. A two-candidate Pareto handoff uses the same panel for replay A vs replay B.
4. For a stored PB8 result, PBGui automatically starts replay one configured candle before the first stored fill for the selected coin. Set **Compare max candles** high enough to reach the final fill you want to compare.
5. Click **Start Compare**.

PB8 Compare reads bounded fills from the handed-off stored result and executes a fresh bounded native replay from its handed-off config. Stored fills are first filtered to the selected coin and exact fresh-replay window; only then is the fill/order limit applied, so older or out-of-window fills do not consume it. Compare uses fill records and does not reconstruct exact historical resting orders.
It refuses source-less or runtime-identical comparisons. Fills match when timestamps are within the configured tolerance, 1,000 ms by default, order types match, and price and quantity resolve to the same native exchange steps. File order is not used as fill identity. A visible partial-coverage warning means the configured candle limit did not reach the final stored fill.

Interpretation tips:
- Stored-only or fresh-only fills can indicate config, market, start-time, data, or engine-version differences.
- Stored-only or fresh-only rows mean no event within the timestamp tolerance with the same order type, exchange-quantized price, and exchange-quantized quantity exists on the other side.
- Matching fills validate this bounded replay comparison, not future live-account behavior.

---

## 3) Typical debug workflow
1. Verify the same **Exchange** and **Coin**.
2. Verify overlap between stored fill timestamps and the selected replay window.
3. Verify that the handed-off config and coin overrides are the intended versions.
4. Run Compare with **Mismatches only** enabled, then disable it if you need the matching context.
5. Use Movie Builder to inspect candles and fills; do not expect PB8 historical per-frame ideal-order ladders.
