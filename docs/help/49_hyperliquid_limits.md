# Hyperliquid Limits

Open **Information → Hyperliquid Limits** for a wallet-level view of saved Hyperliquid accounts across PB7 and PB8 bots and VPS hosts. Multiple bots using the same wallet share one row and one address limit. The table shows Used, Cap, remaining requests, usage, source status, and the time of the latest sample. Filtering by account, bot, or VPS is preserved when the browser reloads.

Only a VPS running a bot for the wallet polls Hyperliquid `userRateLimit` automatically, once per wallet every five minutes on that VPS. Masters read the VPS agent's cached measurement and do not make periodic Hyperliquid requests. If several bots on the same VPS use a wallet, the VPS reads it once. If the same wallet is used by bots on different VPS hosts, each host currently samples independently. The displayed counter is the newest valid VPS sample. Stale or missing samples are marked explicitly.

Open an active account row to see its 24-hour Used and Cap history. History is collected on the Master only while a bot is running for the account. An account without a running bot is listed as **Not sampled**. To check such an account once, open its saved entry under **System → API-Keys**; PBGui reads the limit once from Hyperliquid and displays Used, Cap, and remaining requests in a card beside Futures Balance. Request counters here and in VPS Manager use apostrophes for thousands (for example, `2'209'548`). That one-time read does not create ongoing history. Opening the API key again performs another single read.

These counters describe Hyperliquid's address-based action quota, not the separate IP REST weight limit. The page updates automatically and has no manual refresh control.
