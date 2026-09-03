# Profit Sweep

## Purpose

Profit Sweep moves a configurable share of realized trading profit from a trading account to that exchange's fixed internal destination. It uses cumulative net realized PnL, funding, fees, exchange corrections, and a high-water mark. Withdrawals and internal transfers are not counted as trading losses, and a loss must be recovered before new profit becomes eligible.

Each exchange user has an independent policy with **Disabled**, **Dry**, **Live**, or **Paused Unknown** status. Profit Sweep never accepts an external address and never performs an on-chain withdrawal.

## Setup And Permissions

Configure the exchange user under **System > API Keys** before opening **System > Profit Sweep**. Use the smallest permission set that supports account reads and internal transfers:

- **Hyperliquid Standard/Manual** requires the configured and approved API agent. The account must remain in Standard/Manual mode.
- **Bybit** requires read/write access and the wallet `AccountTransfer` permission. Withdrawal permission is not required.
- **Binance** requires read access and **Permits Universal Transfer**. Withdrawal permission is not required.
- **Bitget** requires read and transfer access plus the API passphrase. UTA transfers are submitted with borrowing disabled.
- **Hyperliquid Vault** operations use the configured leader API agent. The Vault itself has no private key.

The Overview reports read capability immediately. Write capability is checked from a fresh server-side snapshot when Live or a test transfer is requested. A displayed route does not override missing credentials, the wrong account mode, stale history, liabilities, lockups, or exchange limits.

## Basic Fields

- **Reference capital** is the trading capital retained before profit is swept.
- **Baseline mode** starts accounting either when the policy is enabled or from available lifetime history.
- **Trigger percent** sets the profit hurdle relative to reference capital.
- **Sweep percent** selects the share of each new high-water-mark profit increase.
- **Minimum transfer amount** accumulates smaller due amounts and applies to Dry and Live decisions.
- **Transfer rounding step** rounds the final transfer amount down to a settlement-asset increment after reserves and limits. Use `1` for whole USDC/USDT or `0` to disable rounding; every fractional remainder stays due for a later sweep.
- **Safety reserve amount** with the selected reserve mode keeps transferable balance in the source account.

**Keep trading capital** sets Trigger percent to `0` and Sweep percent to `100`. It does not enable or save the policy. The high-water mark and loss recovery still apply.

## Advanced Fields

Policy limits include fixed, percentage, or max-of-both reserves; optional per-transfer and UTC-day caps; and a separate cap for the first Live catch-up transfer. Schedule fields control debounce, quiet and stabilization periods, normal and Vault cooldowns, jitter, maximum history age, and maximum preflight age.

Vault Advanced fields control withdrawal mode, retained leader equity, leader-share safety buffer, Vault reserve, conditional-cost handling, and activity policy for the selected Main destination. Exchange precision, minimums, transferable balance, margin, lockup, liability, borrowing, and exactly-once safety rules remain server-owned.

## Dry And Live

**Enable Dry** runs scheduled read-only decisions. **Evaluate now** is always a non-committing preview: it does not create an intent, change confirmed totals, sign a request, or move funds. Eligible Dry results are labeled `WOULD TRANSFER` in the Dry Decision Journal.

**Evaluate now** also refreshes the Exchange / Vault balance cards for the source, configured internal destination, and currently transferable amount. For Vault accounts, **Your Vault Equity** is the leader-owned current equity, **Vault TVL** is the total equity across all depositors, and **Your Share** is the leader fraction of that TVL. A successful Live activation or test-transfer action refreshes the same cards. Vault destination changes switch the displayed destination between Main Perps and Main Spot. A confirmed empty Binance Funding Wallet is shown as zero; failed or unsupported exchange balance reads are shown as unavailable.

For a Hyperliquid Leader in Unified or Portfolio Margin mode, PBGui shows **Main Unified** from the shared USDC spot-clearing balance. Hyperliquid reports separate perp `marginSummary` values as meaningless in those modes, often zero. Standard/Manual Leaders continue to show separate Main Perps and Main Spot balances.

A successful Hyperliquid spot-clearing response with an empty balance list means the account has zero Spot USDC and is shown as `0 USDC`. Only a missing or malformed balance response is shown as unavailable.

Before **Enable Live**, choose the Live baseline:

- **Fresh** starts entitlement at the activation snapshot and excludes prior Dry entitlement.
- **Include Dry Period** recomputes entitlement from the current Dry-generation baseline.

The active baseline mode is stored separately from the selected setting. Before any Live transfer has been confirmed, select **Include Dry Period** on an active **Fresh** policy and use **Apply baseline to active Live** with the explicit real-funds confirmation. PBGui then recalculates the Live baseline retroactively from the Dry period and schedules a fresh Live evaluation; previous Dry profit may become immediately due. Ordinary policy saves never trigger this recalculation. The action is blocked after a confirmed Live transfer or while an intent is unresolved, preventing duplicate entitlement.

The optional first-Live catch-up cap limits only the first catch-up; any remainder stays due. Enabling Live requires a shared confirmation, saves the selected settings, and runs server-owned preflight. Live then evaluates, prepares a durable intent before exchange I/O, submits at most once, and reconciles the result. **Disable** prevents future scheduled submissions without deleting transfer history.

## Scheduling

**Hybrid** combines income hints from PBData or newly imported local DB Sync history with a periodic fallback. A hint starts the settlement debounce, while quiet and stabilization periods allow fills, fees, rebates, and funding to settle. **Interval** uses only periodic evaluation. Jitter spreads accounts across time, cooldowns limit successful transfers, and freshness limits reject old or incomplete data.

Hints only wake the scheduler. Every committed decision obtains fresh exchange data and fails closed when history or the final snapshot is incomplete.

## Exchange Routes

- **Hyperliquid Standard/Manual:** USDC, Perps to the user's own Spot balance.
- **Bybit:** USDT or USDC, Unified Trading Account to Funding.
- **Binance:** USDT or USDC, USD-M Futures to Funding.
- **Bitget Classic:** USDT, USDT Futures to Spot.
- **Bitget UTA:** USDT, UTA to Spot/Funding with borrowing disabled.
- **Hyperliquid legacy Vault:** USDC, leader-owned Vault equity to Leader Main Perps, optionally followed by Main Perps to Main Spot.

The server resolves the current Bitget mode and validates every fixed route against the selected exchange user. Routes never cross to another UID or an external destination.

## Vaults And Depositors

Vault accounting uses the leader's own current Vault equity, share, and cashflows instead of assigning total Vault PnL to the leader. Deposits, withdrawals, and profit belonging to other depositors do not become leader sweep entitlement. Attributable leader commission is already held in Main Perps and is therefore diagnostic only in this release; it never causes another withdrawal from the Vault.

Eligibility also considers `maxWithdrawable`, shared margin, retained leader equity, the mandatory leader share plus safety buffer, lockup, positions, and configured reserve. **Flat Only** requires no non-zero position; resting orders do not block because Hyperliquid's withdrawable values already account for the available margin. **Margin Buffered** permits active positions only within the conservative withdrawable cap. `alwaysCloseOnWithdraw` is a provider-owned risk-control flag, not a PBGui transfer prohibition. Ambiguous ownership, a closed or locked Vault, inconsistent shares, or forbidden activity fails closed.

**Main Perps** ends after the Vault withdrawal. **Main Spot** creates a second durable intent and forwards only the amount reconciled as received. Closing cost, forced reduction, cancellation, missing received amount, or unexpected destination activity can pause future sweeps.

## Fees And Conditional Costs

Bybit documents its internal route as fee-free. Binance and Bitget expose no transfer-fee field for these internal routes, so PBGui records no direct fee without treating that as an exchange guarantee. Hyperliquid Perps-to-Spot normally has no gas, trading, or slippage cost for the user's own active address.

A Vault withdrawal can incur `closingCost`, trading fees, or slippage if margin-using positions must be reduced. PBGui records reconciled fee and cost fields and applies the configured conditional-cost policy. Choosing Main Perps avoids the optional second forwarding request.

## Test Transfer And Transfer Back

Supported standard and Hyperliquid Vault accounts show **Test transfer** in **Exchange / Vault**. It is independent of the policy, Dry journal, sweep entitlement, and confirmed Live totals.

1. Click **Test transfer**, enter a positive decimal amount (default `1` for standard accounts, `5` for Vaults), and continue.
2. Review the source, destination, asset, and explicit warning that real funds will move.
3. Confirm to submit one persisted forward operation through the fixed route.
4. Review its status in the Test Transfers table.
5. When the latest forward operation is **Confirmed** and eligible, click **Transfer back** and confirm the fixed reverse route.

For a Hyperliquid Vault, the forward route withdraws from the Vault to Leader Main Perps. This Vault-to-Main route also works when the Leader uses Unified account mode; only optional Main Perps-to-Spot forwarding requires Standard/Manual mode. The explicitly confirmed manual test does not inherit the automatic **Flat Only** policy and permits any positive test withdrawal within the fresh conservative leader-owned Vault cap. Hyperliquid's `alwaysCloseOnWithdraw` remains active as an exchange-owned risk control but does not zero PBGui's cap. The default remains 5 USDC. **Transfer back** is offered only when the reconciled received amount is at least 5 USDC because Hyperliquid rejects smaller Vault deposits.

For standard accounts, the return uses the reconciled received amount when available, otherwise the requested amount. A return never resubmits the forward operation. **Unknown** has no retry or transfer-back action; inspect the exchange and logs instead of creating a blind duplicate.

After a forward or return operation, PBGui performs a separate fresh read-only balance refresh. If that refresh fails, the durable operation status remains authoritative and the page asks you to retry the balance read with **Evaluate now**.

After Hyperliquid accepts a manual test submission, PBGui polls the fixed read-only ledger query for up to ten seconds before classifying the result as Unknown. Ledger-indexing delay never triggers another submission; only reconciliation reads are repeated.

Each forward test action carries one browser-generated idempotency UUID. PBGui atomically claims that persisted operation before exchange I/O, so concurrent requests or an exact repeated forward request after a lost HTTP response return the same operation without submitting again. Transfer back is bound to the confirmed forward operation, permits only one persisted reverse operation, and rejects a repeated request instead of submitting again. A test transfer in the Submitting state blocks an API restart. Startup reconciles interrupted submitted tests through exchange history and never repeats their write request.

Hyperliquid currently records successful `agentSendAsset` movements as non-funding ledger events with `delta.type = "send"`. The signed action contains the canonical token ID (`USDC:0x…`), while the Ledger event reports the symbol (`USDC`). PBGui compares that symbol plus the exact destination, DEX pair, amount, nonce, and time window before confirming the operation.

For Spot-to-Perps returns, the descriptor's logical destination is `default_perps`, while the signed action and Ledger event use the account's own wallet address as `destination`. Reconciliation compares the signed action destination so forward and reverse routes use the same provider identity.

PBGui posts Hyperliquid signed actions through a fixed sealed endpoint and stores only a bounded, address-redacted provider rejection reason. Signatures and request bodies are never persisted or rendered. Older failed Vault test operations created before this diagnostic support can show only that Hyperliquid rejected the action; a new explicitly confirmed test is required to obtain the exact redacted provider guidance.

Hyperliquid L1 submissions use the current canonical envelope containing only `action`, `signature`, and `nonce` when no optional signing context or expiry exists. Null `vaultAddress` and `expiresAfter` fields are omitted exactly like the official SDK. The target Vault remains inside the signed `vaultTransfer` action. PBGui subtracts one micro-USDC when calculating Leader retention so the post-withdrawal balance remains strictly greater than 100 USDC and the configured share floor rather than exactly equal to it. Hyperliquid can likewise report a successful Vault withdrawal's `requestedUsd` one micro-USDC below the signed integer-micro amount; reconciliation accepts only that exact downward quantum while still rejecting broader amount differences.

Persisted descriptors use sorted JSON for stable integrity checks, but Hyperliquid MessagePack hashes depend on object-key order. Before every signature and submission, PBGui reconstructs `agentSendAsset` and `vaultTransfer` actions in the current official schema order. This remains deterministic across API restarts and prepared operations. Both standard-account and Vault Live transfers use their validated API-agent paths.

## Intents And Reconciliation

The **Live Transfer Intents** table shows durable **Prepared**, **Submitting**, **Confirmed**, **Failed**, and **Unknown** states. Prepared is persisted before exchange I/O. Confirmed updates accounting only after reconciliation. Failed is a definite non-transfer result.

Unknown means PBGui cannot prove whether the exchange executed the request. The policy changes to **Paused Unknown** and blocks new Live submissions. **Reconcile** queries the exchange again with the same durable operation identity; it never blindly submits a second transfer. Test-transfer operations remain separate and deliberately provide no retry action for Unknown.

Changes to an active Live policy require an explicit financial confirmation and the exact current policy fingerprint, preventing a stale browser tab from overwriting newer settings or activating settings different from those reviewed. Settlement-asset or baseline-accounting changes, baseline reset, and policy deletion require disabling Live first. If a confirmed Vault withdrawal cannot immediately create its Main-Spot forwarding leg, PBGui pauses the policy and exposes reconciliation of that same first leg; it never performs another Vault withdrawal.

## Troubleshooting

- **Unsupported or unavailable:** verify the exchange type, credentials, permissions, Hyperliquid agent approval, and account mode under API Keys.
- **Live activation rejected:** review the Overview reason, then check complete history, snapshot freshness, asset, liabilities, margin, lockup, and transfer permissions.
- **No sweep occurs:** check mode, trigger, high-water mark recovery, minimum amount, reserve, limits, cooldown, due amount, and next evaluation time.
- **Test transfer rejected:** use a positive amount no larger than the fresh transferable balance. Vault withdrawals below 5 USDC are allowed but cannot offer Transfer back. A returnable Vault test requires a positive conservative leader-owned withdrawal cap, strictly more than 100 USDC and 5% Leader retention after withdrawal, at least 5 USDC received, and enough fresh Leader Main balance for the return. For Binance, enable **Internal/Universal Transfer** for the API key; withdrawals are not required.
- **Bybit Evaluate works but transfers are unavailable:** enable **Account Transfer** permission for the API key. Wallet and transaction-history reads remain sufficient for Dry evaluation; PBGui does not infer a transferable USDT amount from multi-asset collateral totals.
- **Bitget Spot shows unavailable:** Wallet Transfer permission is sufficient to move funds. Enable Bitget Spot read permission only if PBGui should display the Spot balance and query transfer history. When Bitget returns a successful synchronous transfer ID but history reads are forbidden, PBGui confirms from that exchange acknowledgement without resubmitting.

Bitget Classic transfer-history reconciliation uses the required `coin`, `fromType`, and persisted `clientOid` filters. Bitget names the transferred quantity `size`; PBGui matches it exactly against the requested amount for both Futures-to-Spot and Spot-to-Futures.
- **Unknown operation:** do not retry or transfer back. Compare the operation time and amount with exchange history, open Logs, and use Reconcile only for a Live intent.
- **Vault paused:** inspect lockup, positions/orders, leader share, retained equity, destination activity, received amount, and any closing cost or forced reduction.

Browser requests use the PBGui HttpOnly session cookie. API keys, private keys, passphrases, descriptors, fixed-route payloads, and raw exchange responses are not rendered in this page.
