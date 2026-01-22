# Tutorial: Understanding Long vs Short grids

This tutorial explains how Strategy Explorer displays Long and Short grids, and how to avoid common misinterpretations.

---

## 1) Long grids
### Long entry grid
- Represents **buy** orders that open/increase a Long position.
- Usually placed **below** the current price.

### Long close grid
- Represents **sell** orders that reduce/close a Long position.
- Usually placed **above** the entry price (take-profit ladder).

---

## 2) Short grids
### Short entry grid
- Represents **sell** orders that open/increase a Short position.
- Usually placed **above** the current price.

### Short close grid
- Represents **buy** orders that reduce/close a Short position.
- Usually placed **below** the entry price.

---

## 3) Both sides active
If both Long and Short are enabled in the config:

- Snapshot view can show both sets of grids.
- Movie Builder lets you select the side you want to animate.

If you only see fills on one side:
- That’s normal if the strategy/backtest only traded one direction.

---

## 4) Trailing lines
Trailing is path-dependent.

- Trailing thresholds and retracement lines give intuition about when trailing becomes eligible and when it triggers.
- Exact fill sequences can still differ depending on candle path and state injection.

---

## 5) Debugging checklist
If grids look “inverted” or “wrong”:

- Confirm you’re looking at the intended side (Long vs Short).
- Confirm exchange/coin selection.
- Confirm Analysis Time is inside the period you care about.
- If comparing to a backtest, launch Strategy Explorer from that backtest result to auto-align time.
