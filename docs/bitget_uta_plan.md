# Bitget UTA Integration Plan

Status: PBGui read-side implementation and offline verification completed. Funded-wallet/capital-movement checks, production source/environment comparison and the approved DOGE leverage-5 setup passed. The subsequently authorized passive order test also passed: one DOGE Buy/Long Post-only order, priced 5% below the fresh bid and tick-floored, was accepted and canceled without any fill. Full PB7 live operation, actual fill/PnL/funding-fee accounting and production migration remain pending. No bots or services were started/changed and no transfers were submitted by the tests.

## Implementation Status

- `bitget_uta.py` provides strict settings detection, native USDT wallet/position/order reads, financial-record income and account-wide executions. Classic routing requires an explicit provider error code; unknown/transitional modes fail closed.
- REST reads use a five-minute per-client mode cache and a per-instance reentrant lock so concurrent chart requests cannot overwrite routing or close a client in use. Public market reads do not probe private account settings.
- Bitget private WebSockets are deliberately not used in this initial rollout, for both Classic and UTA. Dashboard chart position/order subscribers receive full REST snapshots approximately every five seconds; API live and PBData retain their existing polling paths. Public candle streams are unchanged. This avoids retiring clients while consumers still hold references to them.
- UTA history uses cursor pagination, a 90-day retention limit and at most 30 days per request. Stable account-scoped ledger IDs preserve Classic rows and make repeat imports idempotent. Unknown/ambiguous isolated-margin ledger types, non-USDT fees and malformed responses fail explicitly rather than producing partial income. Older or pre-migration Classic history is not reconstructed by v3.
- Income and the scan-start checkpoint are committed in one database transaction; failures roll back both. Execution-side repair preserves native UTA order sides.
- Dashboard Market Close is disabled for UTA and unresolved Bitget modes (HTTP 409 with an explanation); verified Classic parameters remain unchanged. Existing transfer routes and submission safeguards remain unchanged, with shared fail-closed account-mode parsing.
- EN/DE guides and logging registration are updated. API serial is incremented for the runtime change set. The implementation baseline passed the complete offline suite: **8014 passed, 41 skipped**, with 21 dependency/deprecation warnings. The subsequent empty-response fix passed **227 focused offline tests**; the funded-wallet follow-up fixes passed **313 focused offline tests**. The separately authorized private-read attempts are recorded below; no external-bot or live trading tests were run.

The review findings below describe the original PR/baseline, not unresolved claims that the implementation still uses those paths. Remaining live evidence and migration prerequisites are listed at the end.

## Read-Only Acceptance Attempt

Date: 2026-09-08, final checks at 06:12-06:16 UTC. Account: `bitget_UNI_MEME`; PBGui CCXT 4.5.38. No other account was queried. The isolated diagnostic extracted only allowlisted credential fields for that account using a streaming selector, suppressed credential-bearing diagnostic output and file logging, rejected local runtime writes, and enforced a fixed GET endpoint/HTTPS-host allowlist. No API service startup, database import, account setter, order action or transfer was invoked.

| Read | Result |
| --- | --- |
| `/api/v3/account/settings` | PermissionDenied, provider code `40014`; actual account mode/level/holding mode could not be verified. |
| `/api/v3/account/assets` | PermissionDenied, provider code `40014`; wallet/equity/collateral checks remain blocked. |
| `/api/v3/account/financial-records` | PermissionDenied, provider code `40014`; income/funding accounting remains unverified. |
| `/api/v3/position/current-position` | Success `00000`, native `data.list = null`; empty-position normalization verified. |
| `/api/v3/trade/unfilled-orders` | Success `00000`, empty list, no continuation; no open orders reported. |
| `/api/v3/trade/fills` | Success `00000`, native `data.list = null`, `data.cursor = null`, for the preceding seven days; empty-fill normalization verified, no nonempty execution evidence. |

The successful native trade reads were diagnostic probes, not a bypass of PBGui's account-mode gate. End-to-end PBGui reads still correctly stop at settings detection until permission is resolved. The full intended 89/90-day history acceptance did not run.

This attempt found an actual API-shape gap: successful empty positions/fills can use explicit null lists. The parser now accepts those confirmed empty forms; missing fields, failed envelopes and null pages with a continuation cursor still fail closed. New tests replay the sanitized shapes, and the successful native empty payloads were normalized again in memory after the fix.

The initial blocker was missing management-read access: settings, assets and financial records require it. Positions, orders and fills use the separate trade-read scope, whose tested endpoints succeeded. The user subsequently reported that this subaccount UI has one global read/write choice, not independent read/write levels for Management and Trade, and enabled Management on the existing write-capable key. This grants more than the diagnostic needs; the diagnostic remains GET-only. No need to recreate the key was established.

Sources: https://www.bitget.com/docs/uta/quick-start#access-setup and https://www.bitget.com/legacy-docs/uta/account/Get-Account-Setting . The authorized follow-up below supersedes the permission blocker and confirms Hedge for the test account only; Cross/leverage still need operational validation.

## Read-Only Follow-Up And Key Rights

Date: 2026-09-08, 06:36-06:38 UTC. Only `bitget_UNI_MEME` was queried, through the same isolated GET-only harness. A fixed signed GET `/api/v3/account/info` was added for current-key metadata; no API-key listing, write-capability trial, invalid withdrawal request or other mutation was attempted. Only allowlisted permission labels and derived identity/IP-binding facts were reported; credential values, account UIDs and individual IP addresses were not recorded.

| Check | Observed result |
| --- | --- |
| Key scopes | Exactly `uta_mgt` and `uta_trade`; no `withdraw`, no copy-trading scopes, no unrecognized scope labels. |
| Permission type | Native `permType = read_and_write`; one global type, not a per-scope read/write map. |
| Subaccount identity | A positive numeric `parentId` distinct from the calling `userId` was returned. |
| API IP binding | 18 distinct concrete IP addresses reported; no wildcard entries. Enforcement from an unlisted IP was not tested. |
| Account settings | `accountMode = unified`, `accountLevel = basic`, `assetMode = multi_assets`, `holdMode = hedge_mode`. |
| Assets / PBGui wallet | Successful empty assets list and zero account equity; PBGui USDT wallet balance is 0. |
| Positions / regular open orders | PBGui wrappers succeeded, both lists empty. |
| Financial-record income / executions | PBGui wrappers completed the requested 89-day range in bounded windows; both returned zero records with no request failures. |

Permission assessment: no withdrawal scope is reported by Bitget for this key. That is metadata evidence, not an active proof that every possible withdrawal operation is impossible. Management read/write is broader than monitoring: documented operations include leverage/holding-mode changes, internal transfers, repayment, and subaccount-to-parent transfers. Some transfer requests can opt into borrowing. None was called. Absence of a separate transfer scope does not disable transfers authorized by `uta_mgt`. Keep only the IPs genuinely needed; the check does not establish whether all 18 are necessary.

Public references: https://www.bitget.com/legacy-docs/uta/account/Get-Account-Info and https://www.bitget.com/docs/catalog/account/deposit-withdrawal . The detailed endpoint is explicitly UTA; no Classic account-info fallback or unrelated key/account inspection was used.

Result: management-read access is now working, and the empty-account PBGui read paths pass. Hedge is already active on this account, so no manual Hedge change is indicated now. This does not validate nonzero wallet interpretation, real order-side handling, commissions/funding amounts, per-symbol Cross/leverage, or continued PB7 operation. The two production accounts remain unchanged; their modes/permissions were not inspected.

## Funded Wallet And Capital Movement Check

Date: 2026-09-08, final verification at 06:59 UTC. The user transferred the funds; the diagnostic made only allowlisted GET requests on `bitget_UNI_MEME`. No production database import or browser/API-service startup was used.

| Check | Observed result |
| --- | --- |
| UTA USDT wallet | Balance 3, equity 3, available 3, locked 0, debt 0. PBGui `fetch_balance` returned 3. |
| Funding USDT wallet | Balance 2, available 2, frozen 0. Confirmed through `/api/v3/account/funding-assets`. |
| UTA capital ledger | One `TRANSFER_IN`, category `OTHER`, amount 3 USDT, fee 0; cursor traversal completed. |
| Funding capital ledger | One `transfer_in`, amount 2 USDT, through `/api/v3/account/funding-financial-records`; cursor traversal completed. |
| Futures income / executions | Requested 89-day reads remained empty and net income was 0. The capital transfers did not enter trading income. |
| Positions / regular open orders | Empty. Hedge mode remains active. |
| PBGui account snapshot | UTA source 3, Funding destination 2, UTA max-transferable 3, realized net PnL 0; no snapshot errors. Reading max-transferable did not execute a transfer. |

UTA `accountEquity`/`effEquity` were approximately 2.999 in USD valuation while `usdtEquity` and the USDT wallet were exactly 3. These denominations must not be confused with fees or lost USDT. The Funding wallet is separate and is not added to PBGui's UTA trading wallet.

Two integration gaps were corrected during this check: pinned CCXT lacks the generated v3 Funding-assets method, so PBGui now uses a fixed signed GET fallback; the shared account-snapshot history path now accepts the same explicit null terminal ledger response already handled by the income adapter. Missing fields, provider failures and null pages with continuation cursors remain errors. Ten additional regression cases cover fallback routing, permission failure and the funded snapshot; 313 focused tests passed.

An exploratory account-financial-records GET without `category` returned `400172`; this is a query-parameter error, not a permission or deposit failure. The final check used documented `category=OTHER` for UTA capital movements and the separate Funding ledger, and completed without request errors. References: https://www.bitget.com/docs/catalog/account/assets-balance#get-financial-records and https://www.bitget.com/docs/catalog/account/assets-balance#get-funding-financial-records .

This verifies a simple funded USDT account and these two transfers only, not locked margin, borrowing, multi-asset collateral, actual trading PnL/fees/funding payments, or PB7 operation. No further money movement or trading is authorized by this result.

## Review Baseline

- PR: https://github.com/msei99/pbgui/pull/129
- Reviewed head: `9e9a7bb14aedd543fffaaf1dc2a171575db23053` (both commits).
- PR changes only `Exchange.py`, with no automated tests or reported CI checks.
- Test account designated by the user: `bitget_UNI_MEME`, migrated without a bot.
- Production Bitget trading accounts reported by the user: `bitget_DOGEUSDT` and `bitget_Gucky_2coin_1dot25x`. Both must remain on their current Classic setup until PBGui support and continued PB7 operation pass the production-migration gates below. Account state and deployed bot versions have not been independently inspected.
- Existing Profit Sweep and Transfers support already distinguishes Classic and UTA.
- UTA account mode must not be equated with Elite/copy-trading status or hedge/one-way position mode.

## PR Findings

Line references in this section refer to the PR head, not current main.

1. High: `Exchange.py:1479-1488` requests up to 365 days in one fills query. Current Bitget documentation specifies 90-day retention and at most 30 days per request.
2. High: `Exchange.py:1511-1514` advances to `oldest - 1` instead of using the returned cursor. Fills sharing the boundary millisecond can be skipped across full pages.
3. High: `Exchange.py:1474-1515` replaces income with fills only. Trading PnL minus fees does not cover standalone funding payments; complete net income needs financial records or a separately reconciled funding source.
4. Medium: `Exchange.py:367-374` classifies every probe exception as Classic, including authentication, timeout, rate-limit and missing-method failures. Unknown mode must remain an error/unknown state, not a successful Classic detection.
5. Medium: `Exchange.py:1045-1052` swallows balance parsing failures and can report zero or substitute available balance for wallet balance. Malformed responses must be distinguishable from a genuine zero balance.

Additional integration risks identified on the pre-implementation baseline:

- Private WS clients are constructed independently at `Exchange.py:665-696`; sync UTA options do not propagate to them.
- Bitget executions retain 365-day lookback/90-day windows at `Exchange.py:2486-2493`, classic side inversion at `Exchange.py:2505-2526`, and no raw `execPnl` mapping at `Exchange.py:2612-2618`.
- Database side repair also applies classic inversion to all Bitget rows at `Database.py:441-457`; fixing only the fetch mapper is insufficient.
- Dashboard market-close actions use the same sync client. Read-side changes must not implicitly enable unverified UTA order behavior (`api/dashboard.py:914-922`).
- Migration coverage/checkpoints and historical ID overlap need explicit validation (`Database.py:1194-1248`). Do not erase existing history or blindly reset production checkpoints.

## Implementation Sequence

1. Establish contracts and fixtures. Verify PBGui's own pinned CCXT 4.5.38 REST and Pro behavior, endpoint capabilities, balance fields, account settings, financial-record types, fill IDs and close-side semantics. PBGui supports PB7 and PB8 only. The bots' separate CCXT environments are not PBGui dependencies. Port the PR ideas onto current main rather than merging unchanged.
2. Implement account-mode resolution with explicit Classic/UTA/unknown outcomes. Reuse the existing settings-based knowledge in `profit_sweep_exchanges.py:1689`; preserve raw unified/hybrid metadata. Only recognized provider responses establish Classic mode. Bound caching by client/account credentials and handle client recreation after migration. Public market-only clients must not require private probes.
3. Add read-side REST support: wallet balance, equity, positions and open orders. Keep wallet balance, available margin and max-transferable distinct. Apply mode configuration to private WS clients or provide an explicit supported REST fallback. Retain deterministic client cleanup and Classic regression coverage.
4. Implement complete income and execution import. Prefer financial records for accounting and fills for executions after verifying schemas. Use endpoint-specific retention, time windows and cursors; reject partial success on failures. Cover funding, fees/rebates, execution PnL and actual order side without double counting. Make DB side repair mode-aware. Verify stable IDs and repeated-import idempotency across migration before choosing any schema change.
5. Integrate UI and existing transfer paths. Show unsupported/unknown states clearly. Audit market-close parameters for UTA and position mode; gate unsupported actions. Preserve shipped UTA transfer routes, no-borrow behavior and non-replay-safe submission handling. Do not execute transfers during read-only acceptance.
6. Run isolated acceptance, update relevant EN/DE guides and changelog, and increment `api/serial.txt` for the eventual runtime change set. Run the complete offline test suite because shared exchange/runtime behavior is affected. No merge, commit, push, deployment or bot start without approval.

## Test Plan

| Layer | Cases | Acceptance |
| --- | --- | --- |
| Offline mode resolution | Classic, unified, hybrid, invalid credentials, permission denied, timeout, rate limit, missing CCXT method | No silent Classic fallback; no private probe for public-only clients |
| Offline balances | Zero, missing USDT, malformed payload, locked funds, unrealized PnL, multi-asset account | Defined wallet semantics; parse failures are not zero; no equity/available substitution |
| Offline history | More than 100 fills, same-millisecond boundaries, cursor repetition, empty windows, funding both signs, rebates, partial failure | Complete bounded pagination, no duplicates, correct net income, no false checkpoint advancement |
| Offline executions/migration | Open/close long and short, one-way/hedge, classic and UTA rows, repeated import, pre/post-migration overlap | Correct sides/PnL; old rows preserved; DB repair does not corrupt UTA rows |
| Offline integrations | REST/private WS configuration, reconnect/cleanup, dashboard action gates, transfer regression | Consistent mode and unchanged Classic behavior; no unintended write endpoint |
| Private read-only acceptance | Explicitly selected `bitget_UNI_MEME`: settings, assets, positions, orders, fills and financial records | Compare normalized values with Bitget UI/export; distinguish empty data from unsupported/failed reads |

Private acceptance must be separately opt-in, never part of default offline tests or the public-market live marker alone. Load only the selected credential through the approved store; never print secrets, authenticated headers or entire credential files. Allowlist read methods. Store sanitized fixtures and test databases only in isolated temporary paths; never run production import/update jobs as a test.

An account without a bot is not necessarily empty. Inspect rather than assume its positions and orders. If no post-migration fills/funding exist, mark those live cases unverified and use synthetic fixtures. Real orders, cancellations, transfers, leverage/mode changes and bot starts require separate explicit approval with exact scope and limits.

Before historical acceptance, obtain the migration timestamp (preferably UTC) and determine whether old Classic records remain accessible. Preserve existing local history and report exchange retention gaps honestly. A Classic comparison account must be explicitly selected; do not automatically probe all stored accounts.

## Scope And Risk

The refreshed GitNexus CLI impact analysis marks `Exchange.connect` CRITICAL: 16 direct callers and 49 affected symbols within three levels, including dashboard reads/actions, history/executions, market data and transfers. The indexer reported flow-enumeration limits, so graph results are not a proof of complete coverage. Source inspection supplements the graph.

First delivery is PBGui read-side UTA support with safe action gating, not a claim of Passivbot UTA trading compatibility. PBGui does not implement or change the bots' internal trading logic. PB7 is no longer actively developed, as confirmed by the user. Bot code changes remain outside this implementation plan. PBGui implementation acceptance and production-account migration are separate decisions: production migration additionally requires demonstrated continued PB7 operation. A PB8-only test cannot satisfy that requirement. Unresolved relevant PB7 findings block production migration, not development of PBGui support.

PBGui-owned exchange operations remain in scope: data collection, normalization, persistence, dashboard/private streams, and the correctness of manual market-close or transfer actions initiated by PBGui. These direct actions are distinct from the bots' own trading logic. No new bot-start restrictions are planned solely because of the source-review findings.

Reference: https://www.bitget.com/docs/catalog/trading/order-management#get-fill-history (90-day retention, 30-day query windows, cursor pagination; reviewed during planning).

## PB7 And PB8 Trading Source Review

Read-only local source review, not a live trading certification. These checkouts and virtual environments were found locally; their use by any particular running instance was not established.

| Checkout | Version | Local commit | Required / installed CCXT |
| --- | --- | --- | --- |
| `/home/mani/software/pb7` | 7.12.0 | `befaa9b7aa89e00ee55704221b39621ad700ac36` | 4.5.48 / 4.5.48 in `venv_pb7` |
| `/home/mani/software/pb8` | 8.1.0 | `3597dfdcfde099b0960e9941aea6ba3dca9bbaf7` | 4.5.66 / 4.5.66 in `venv_pb8` |

Both implement native UTA detection, propagate mode to REST and WS clients including session recreation, and use v3 create/cancel/position endpoints through CCXT. Both have native UTA balances and fills/PnL handling. Their intended order path is USDT perpetuals in hedge mode with cross margin, not general One-Way or Isolated support. This exists independently of PBGui PR #129.

Findings (paths below are relative to the respective bot checkout):

1. High, PB7: `src/exchanges/bitget.py:1095-1103` logs a failed `set_position_mode(True)` but continues; `:997-1011` still builds hedge-mode orders with `posSide`. A remaining One-Way account is not safely supported. PB8 propagates the setup error at `src/exchanges/bitget.py:863-877`.
2. Medium, PB7: `src/exchanges/bitget.py:354-378` uses `info.posSide` and defaults to long, while the UTA order stream uses `holdSide`. Short updates can be mislabeled. In this version updates primarily trigger refreshes (`src/passivbot.py:7782-7815`), so this alone does not prove incorrect submitted orders. PB8 handles `holdSide` and inconsistent side metadata at `src/exchanges/bitget.py:103-178`.
3. Medium, both: `src/exchanges/ccxt_bot.py:405-416` (PB7) and `:616-627` (PB8) fetch open orders without enabling cursor pagination. The bots limit their own ideal order count to 100, but manual orders, another bot or old orders can exceed that total. Without a separately configured pagination override, reconciliation may see only one page.
4. Scope limitation, both: UTA setup skips setting margin mode, and the order path omits `marginMode`; Bitget documents cross as the default. Both expect hedge mode and lack an alternate One-Way create path with close `reduceOnly`. PB8's One-Way data normalization is not evidence of One-Way trading support.

Assessment: PB8 has stronger safeguards for UTA hedge/cross trading in the reviewed sources, but the user's production requirement is continued PB7 operation. PB7 limitations remain documented without planning bot fixes. Neither source review establishes that `bitget_UNI_MEME` is correctly configured for trading. Passing PBGui tests alone is not permission to migrate either production account.

Before production migration, validate the exact intended PB7 build and CCXT environment using isolated contracts for all four hedge open/close combinations, post-only and cancel routing, short/partial-fill WS events, reconnects, failed hedge/leverage setup and 101 open orders. Existing PB8 tests include `tests/test_order_reconciliation_contract.py:88-165` and `tests/ccxt_upgrade/test_order_contracts.py:500-564`; they can inform cases but do not prove PB7 behavior. These tests were inspected, not executed. No bot source, account setting, order or runtime state was changed during this review.

## Deployed PB7 Verification

Date: 2026-09-08, approximately 17:06-17:20 UTC. Existing monitor state identified the running hosts; strict known-host SSH then inspected only the two selected processes and allowlisted source/configuration metadata. No remote files were written, services changed, exchange requests authenticated, or bot modules started/imported.

| Item | DOGE instance | Gucky instance |
| --- | --- | --- |
| User/instance | `bitget_DOGEUSDT` | `bitget_Gucky_2coin_1dot25x` |
| Observed running host | `manibot62` | `manibot40` |
| PID / start ticks | 1209 / 1371 | 2047806 / 27526222 |
| On-disk PB7 version | 7.12.0 | 7.12.0 |
| Current interpreter Python / CCXT | 3.12.3 / 4.5.48 | 3.12.3 / 4.5.48 |
| Config revision / running revision reported by monitor | 24 / 24 | 56 / 56 |
| Raw configured leverage | 10 | 5 |
| Raw long positions / total wallet exposure limit | 1 / 3 | 1 / 1.25 |
| Raw short positions / total wallet exposure limit | 0 / 0 | 0 / 0 |
| Raw margin preference | Absent; inspected PB7 helper defaults to cross | cross |
| Raw time-in-force | good_till_cancelled | good_till_cancelled |

Both process invocations use `/home/mani/software/venv_pb7/bin/python -u /home/mani/software/pb7/src/main.py` with the exact selected `data/run_v7/<instance>/config_run.json`. Both process working directories are `/home/mani/software/pb7`; the executable target is `/usr/bin/python3.12`. PID start identity and invocation were rechecked after inspection. Current checkout HEAD on both is `befaa9b7aa89e00ee55704221b39621ad700ac36`.

The six selected files are unchanged against HEAD on both hosts and byte-identical to the local test basis. SHA-256:

| Relative PB7 file | Hash |
| --- | --- |
| `src/exchanges/bitget.py` | `9f1249b927d3aed05a7555e8a3b94057d153be53367c0858f0432d8884458c17` |
| `src/exchanges/ccxt_bot.py` | `7af1162dfb6ab5dfd15693b52d6b7613f15aa74da9375f2f94412e03c113ef57` |
| `src/main.py` | `74ee4635f7f59709cc4a3f33930bde0d874ee1f1b69a1ee91eda88534af95d05` |
| `src/passivbot.py` | `7b5f3d35b7d67b4f3abd4b1f3881abd9486ccb254f0b0827769e6ad9584d2758` |
| `src/config/schema.py` | `cbf7f57c5b342c5e2b40096971562451a208da1c4f4169c02618caaf6476497b` |
| `requirements-live.txt` | `a1354f4117f4502682e2e22b44a1d12b5c7bd2d2a39b15d485a174b5c36ab67d` |

Both entire checkouts report dirty state outside this inspected set; no unrelated diffs, filenames or credentials were collected. The six files' mtimes precede their process starts. This is stronger than monitor inventory alone, but does not prove every dependency or already-loaded object in either long-running process. Current environment distribution versions were queried without importing CCXT. The DOGE config file was modified after process start; the Gucky config mtime/start differ nominally by only 8 ms. Raw disk configs are therefore not asserted to be an exact in-memory snapshot. Both configured strategies currently specify long-only with at most one long position; the Gucky name is not evidence of a two-position limit.

## PB7 Offline Order Contracts

Using the matching local PB7 sources and `/home/mani/software/venv_pb7/bin/python` with CCXT 4.5.48, **13 of 13 offline cases passed**. Exact AST-extracted `_build_order_params` and `execute_order` methods were exercised through the installed async CCXT implementation with synthetic market data and intercepted requests. Network/DNS access was blocked; no full bot/Rust imports or runtime/account files were used.

- All four hedge open/close side combinations passed for both GTC and Post-only (eight placement cases), selecting v3 `trade/place-order`, with correct `side`/`posSide`, positive quantity and no Classic `holdSide`/`oneWayMode` or One-way `reduceOnly` fields.
- Cancel selected v3 `trade/cancel-order`; simulated Hedge and leverage setup selected v3 `account/set-hold-mode` and `account/set-leverage`.
- An absent margin preference resolved to cross; safety/count assertions confirmed eleven intercepted requests and zero network attempts.

Limits: UTA mode was explicitly supplied, the config accessor was a strict synthetic lookup, and exchange responses were fabricated. These tests do not validate authentication, real order acceptance, live precision, fills, reconnect behavior, or PB7's non-fatal setup-error handling. Source extraction tests the relevant methods, not a complete running bot.

## Setup And Passive Order Approval

Public Bitget instrument metadata on 2026-09-08 reports DOGEUSDT as an online USDT perpetual with minimum quantity 1 DOGE, integer quantity steps, price step 0.00001, minimum order amount 5 USDT and leverage range 1-75. These values and wallet/mode state must be re-read immediately before any approved order. Sources: `GET /api/v3/market/instruments?category=USDT-FUTURES&symbol=DOGEUSDT` and public tickers. Public requests did not use credentials.

The user explicitly approved **setup only** on `bitget_UNI_MEME`: verify the existing Hedge state and request/read back **DOGEUSDT leverage 5**, leaving Funding and all other symbols/accounts untouched. That scoped operation is completed as recorded below. No ordinary bot start, order, transfer or separate margin-mode setter was included.

The user subsequently approved the passive order test and clarified its scope: calculate the buy price **5% below the current bid**, use Post-only, then remove the order after checking it. There was no authorization to seek a fill deliberately. The bounded controller enforced:

- Only `bitget_UNI_MEME`, DOGEUSDT, a single long entry; no production configuration copied into an unrestricted bot run.
- Opening notional between the fresh exchange minimum and **6 USDT maximum**; proposed leverage 5. The last observed 3 USDT UTA balance is test capital, not a guaranteed loss limit. The 2 USDT Funding balance stays untouched; no borrowing or additional transfers.
- At most one opening Post-only limit order, priced at 95% of the fresh bid and rounded down to the tick. No DCA, short entry, repricing/replacement or intentional fill-wait interval. After native status verification, cancel only that test order promptly.
- Cleanup approval must explicitly include cancellation of the test's remaining opening quantity and closing only the resulting verified long position. A timeout or ambiguous acknowledgement requires status reconciliation, not blind resubmission.
- Compare actual order/fill/position/fee/PnL records with PBGui, then verify no residual test position/order. Do not treat one successful round trip as a PB7 continuous-operation or production-migration certificate.

The scoped leverage setup and this single passive placement/cancellation are completed. No new bot configuration was deployed; the approvals do not authorize another entry, a fill-seeking test, a bot start or production migration.

## Authorized Leverage Setup Result

On 2026-09-08 at 17:35 UTC, using CCXT 4.5.48 and only the selected test credential, the pre-read confirmed Unified/Hedge and no positions or open orders. `symbolConfigList` was valid but contained no DOGEUSDT entry, so the previous DOGE leverage/margin values were not claimed to be known.

Exactly one request was sent to `POST /api/v3/account/set-leverage` with the same payload as the inspected PB7/CCXT UTA leverage path:

```json
{"symbol":"DOGEUSDT","leverage":"5","coin":"USDT","category":"USDT-FUTURES"}
```

No explicit `marginMode` was supplied; Bitget documents Cross as this endpoint's default. No holding-mode or margin-mode setter was called. The request was acknowledged successfully, and a fresh `GET /api/v3/account/settings` then returned exactly one matching DOGEUSDT configuration with `leverage = 5` and `marginMode = crossed`; account `holdMode` remained `hedge_mode`.

The diagnostic enforced one exact POST at both request and HTTP-transport layers, disabled retries/redirects, and allowed only fixed private GETs otherwise. One POST request and one POST transport attempt occurred, with no errors. No raw credentials, account identifiers or authenticated headers were logged. No orders, transfers, borrowing, repayment, bot starts or production-account changes were performed. This verifies the native leverage setup/readback, not full PB7 startup or trading.

## Passive Placement And Cancellation Result

Date: 2026-09-08, approximately 18:20-18:22 UTC. Only `bitget_UNI_MEME` was used. The hash-pinned PB7 `_build_order_params` method and installed CCXT 4.5.48 built the real opening request; this was not a complete PB7 bot launch. A temporary controller was checked with 98 offline cases and the PBGui readback helper with two synthetic cases before execution.

| Item | Observed result |
| --- | --- |
| Fresh best bid / ask | 0.09067 / 0.09068 USDT |
| Opening order | Buy, Long, limit, Post-only; 59 DOGE at 0.08613 USDT |
| Price calculation | `floor(0.09067 * 0.95 / 0.00001) * 0.00001 = 0.08613` |
| Opening notional | 5.08167 USDT, within the 5-USDT minimum and 6-USDT cap |
| Requests sent | Exactly one place-order POST and one cancel-order POST; no replacement or close order |
| Native order verification | Accepted/open state observed, then terminal `cancelled`; cumulative executed quantity 0 |
| Final native account state | No positions and no regular open orders |
| PBGui post-cancel readback | Actual `Exchange.fetch_all_open_orders(None)` with CCXT 4.5.38 found no matching test order |
| Final wallet readback | UTA balance/available 3 USDT, locked 0; Funding balance/available 2 USDT |

There were no request errors, persisted-report failures, fills, transfers or conditional market-close calls. To minimize live-order exposure, the separate PBGui process was invoked only after native cancellation/zero-exposure verification. A live nonempty PBGui order view was therefore **not** tested; the open-order proof in this run came from the native order-info endpoint. Strategy/trigger-order inventory was not part of this check.

The controller persisted a credential-free owner-only intent/report before submission, limited entry to one exact payload, disabled automatic POST retries and redirects, and retained a separately bounded cleanup budget. Reporting failures were tested not to block already-authorized cancellation. Conditional cleanup of unexpected owned fills was never needed.

Result: real passive UTA order placement, owned-order cancellation and post-cancel readback are verified with no execution or balance change. Actual entry/exit fills, fees/PnL/funding accounting, private-stream behavior and sustained PB7 bot operation remain unverified. The two production accounts and bots were not modified.

## PB7 Account Preconditions

The pinned PB7 source and installed CCXT 4.5.48 were rechecked. This is an account-preparation checklist, not authorization to change settings. A normal bot start is not a read-only diagnostic: it sets account parameters and can place orders.

| Item | Requirement and manual action |
| --- | --- |
| Migration state | Confirm completed UTA conversion, not an upgrading/switching state. PB7's assets probe establishes routing, not full account eligibility. |
| Exchange position mode | Require actual `hedge_mode`. If necessary, manually select Hedge on the test account before the first approved start, then read it back. PB7 still calls its setter and can swallow an error; a manual setting reduces reliance on the switch but does not fix that behavior. |
| Strategy hedge setting | PB7 `live.hedge_mode` controls simultaneous strategy long/short exposure, not the exchange's holding-mode switch. Do not toggle it merely to configure the account. |
| Margin mode | Require verified Cross-capable USDT futures and actual Cross positions. PB7 skips the UTA margin-mode setter and omits `marginMode` in orders; Bitget documents Cross as the default. Manual Cross selection is conditional, not universally necessary, and a UI selection alone is not proof of API behavior. |
| Leverage | PB7 requests leverage per symbol and logs failures without aborting. Confirm requested versus actual values. Manual leverage is not normally needed and may be overwritten by PB7. Do not infer leverage from the account name `bitget_Gucky_2coin_1dot25x`; exchange leverage and wallet exposure are different. |
| API permissions | Verify UTA management read/write for bot setup and UTA trading read/write for orders/cancels. Read-only test credentials cannot validate normal startup. Do not enable withdrawals or copy trading merely to satisfy the bot's UTA/Elite terminology. |
| Account subtype | Inspect account mode, level and asset mode separately. Do not require Advanced, multi-asset collateral or Elite speculatively; the reviewed detector does not validate these. Confirm the actual account supports the intended instruments and Cross/Hedge combination. |
| Balance and collateral | PB7 uses account equity denominated in USDT minus unrealized PnL, falling back to USD-denominated fields. This is not necessarily the USDT coin wallet or usable Cross collateral. Verify liabilities, locked funds, collateral eligibility and available margin. Prefer a simple USDT-funded test setup without unrelated activity. |

PB7 source references: detection `src/exchanges/bitget.py:319-346`; equity `:447-471`; order params `:997-1011`; margin/leverage `:1021-1052`; exchange Hedge setter `:1095-1103`; strategy hedge flag `src/passivbot.py:647-654`. The test account's reported modes/scopes are recorded above; actual bot setup, per-symbol settings and production-account migration restrictions remain unverified.

Public references reviewed on 2026-09-08:

- https://www.bitget.com/docs/catalog/account/account-settings#get-account-setting
- https://www.bitget.com/docs/catalog/account/account-settings#change-leverage
- https://www.bitget.com/legacy-docs/uta/account/Change-Position-Mode
- https://www.bitget.com/legacy-docs/uta/account/Get-Account
- https://www.bitget.com/legacy-docs/uta/guide#access-setup

## Production Migration Gates

PBGui implementation and offline tests in gate 2 are completed; the production freeze remains in force. Gate 3 covers funded USDT/Funding balances and capital-transfer exclusion. Production process paths, installed Python/CCXT and six relevant files are checked, with provenance limits noted above. Gate 4's scoped leverage setup is completed. Gate 5 now has passive placement/cancellation and zero-fill cleanup evidence only; actual fills/PnL and full PB7 startup/operation, plus gates 6-7, remain pending. No additional live writes, deployments or bot starts are authorized by the completed tests.

1. Freeze production changes. Keep `bitget_DOGEUSDT` and `bitget_Gucky_2coin_1dot25x` unchanged. Record the exact deployed PB7 revision, CCXT environment and allowlisted non-secret strategy/settings relevant to reproducible testing; local checkout inspection alone is insufficient. Re-check Classic operation as a PBGui regression requirement.
2. Finish PBGui implementation and offline acceptance. Require correct balances, positions, orders, executions, fees/funding, history continuity, idempotent imports and explicit error reporting. Retain the existing transfer safety contracts and block any unsupported PBGui manual actions.
3. Perform separately approved read-only acceptance on `bitget_UNI_MEME`. Verify migration state, account modes, eligible instruments, positions/orders and balance/collateral interpretation. Compare PBGui outputs to provider records. Missing live fills/funding remain unverified, not passed.
4. Approve test-account preparation and controlled PB7 setup separately. Set Hedge only if needed, resolve account capabilities and permissions, and verify leverage read-back. Do not use an unrestricted bot launch as a setup probe. Unexpected setup errors block the next gate, even if the process continues running.
5. Approve bounded live tests only on `bitget_UNI_MEME`. Agree instruments, capital/notional, exposure/loss limits, allowed actions and cleanup in advance. Validate actual PB7 long/short open/close behavior, cancellation, partial fills, fees/PnL and PBGui reconciliation. Even Post-only orders can fill. Use relevant non-secret configuration characteristics from both production bots without copying them into an unrestricted live run.
6. Demonstrate continued PB7 operation and recovery. Under separate approval, test a limited run, order refresh, reconnect and restart reconciliation, and a funding event where applicable. Compare exchange, PB7 and PBGui records; explain all discrepancies. Choose observation duration and event coverage before the run. Unresolved stream-side/order-pagination risks are not waived by a successful first trade.
7. Decide production migration explicitly, one account at a time. Both PBGui and PB7 evidence must pass review first. Define how existing positions/orders and history are handled before conversion; no automatic closing, cancellation or conversion is authorized. Verify account-specific eligibility and retained settings after conversion. Do not assume conversion is reversible or that a software rollback restores Classic mode. Release the second account only after the first is accepted.
