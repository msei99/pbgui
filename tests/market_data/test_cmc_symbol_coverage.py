"""
Test CoinMarketCap symbol coverage for exchange symbols.

This test validates that all exchange symbols can be matched to CoinMarketCap
API coin data using normalize_symbol() with dynamic exchange symbol mappings.

Purpose:
- Identify symbols that cannot be normalized/matched
- Find coins that can't be matched to CMC
- Suggest similar CMC symbol names for unmatched coins
- Validate that dynamic ignore will work correctly

The test loads real CMC data and checks each exchange symbol. For any
mismatches, it uses fuzzy matching to suggest potential CMC symbol names
that may indicate missing/incorrect exchange mapping data.
"""

import importlib.util
import os
import sys
from pathlib import Path
from difflib import SequenceMatcher
from collections import defaultdict
from datetime import datetime

import pytest
from cmc_pool import CmcPoolClient
from credential_store import CredentialStore

# Import real PBCoinData module directly (bypassing mock in tests/)
# This is needed because the test validates actual normalization and CMC matching
pbcoindata_path = Path(__file__).parent.parent.parent / "PBCoinData.py"
spec = importlib.util.spec_from_file_location("PBCoinData_real", pbcoindata_path)
PBCoinData = importlib.util.module_from_spec(spec)
spec.loader.exec_module(PBCoinData)

# Import Exchange for market data
exchange_path = Path(__file__).parent.parent.parent / "Exchange.py"
spec_exchange = importlib.util.spec_from_file_location("Exchange_real", exchange_path)
Exchange_module = importlib.util.module_from_spec(spec_exchange)
spec_exchange.loader.exec_module(Exchange_module)

# Extract what we need from the real modules
normalize_symbol = PBCoinData.normalize_symbol
CoinData = PBCoinData.CoinData
Exchange = Exchange_module.Exchange


def similarity(a, b):
    """Calculate similarity ratio between two strings (0-1)."""
    return SequenceMatcher(None, a.upper(), b.upper()).ratio()


def price_similarity(price1, price2):
    """
    Calculate price similarity ratio (0-1).

    Uses relative difference: 1 - (|p1-p2| / max(p1,p2))
    Returns 1.0 for identical prices, 0.0 for very different prices.

    Args:
        price1: First price (float)
        price2: Second price (float)

    Returns:
        Similarity score 0-1, or 0 if either price is invalid
    """
    if not price1 or not price2 or price1 <= 0 or price2 <= 0:
        return 0.0

    max_price = max(price1, price2)
    min_price = min(price1, price2)

    # Relative difference
    rel_diff = abs(price1 - price2) / max_price

    # Similarity: 1.0 for identical, approaching 0 for very different
    similarity = 1.0 - min(rel_diff, 1.0)

    return similarity


def find_similar_cmc_symbols(
    normalized_symbol,
    cmc_data,
    exchange_price=None,
    top_n=5,
    min_similarity=0.3,
    price_weight=0.6
):
    """
    Find similar CMC symbols using fuzzy matching with optional price comparison.

    Scoring combines symbol name similarity with price similarity (if available).

    Args:
        normalized_symbol: The normalized exchange symbol
        cmc_data: Dict mapping CMC symbol -> coin data with 'price' key
        exchange_price: Optional exchange price for this symbol (float)
        top_n: Number of suggestions to return
        min_similarity: Minimum combined score threshold (0-1)
        price_weight: Weight for price similarity (0-1), string gets (1-price_weight)

    Returns:
        List of tuples (cmc_symbol, string_similarity, price_similarity, combined_score)
    """
    matches = []

    for cmc_symbol, coin_info in cmc_data.items():
        # Calculate string similarity
        str_score = similarity(normalized_symbol, cmc_symbol)

        # Calculate price similarity if both prices available
        price_score = 0.0
        cmc_price = coin_info.get('price', 0)

        if exchange_price and cmc_price and exchange_price > 0 and cmc_price > 0:
            price_score = price_similarity(exchange_price, cmc_price)
            # Combined score: weighted average
            combined_score = (1 - price_weight) * str_score + price_weight * price_score
        else:
            # No price comparison possible, use only string similarity
            combined_score = str_score
            price_score = None

        if combined_score >= min_similarity:
            matches.append((cmc_symbol, str_score, price_score, combined_score))

    # Sort by combined score (highest first)
    matches.sort(key=lambda x: x[3], reverse=True)
    return matches[:top_n]


@pytest.mark.live_exchange
class TestCMCSymbolCoverage:
    """
    Validate all exchange symbols can be matched to CoinMarketCap.

    This test helps maintain dynamic symbol normalization/mapping by identifying:
    - Exchange symbols that can't be matched to CMC
    - Potential CMC symbol names for unmatched coins
    - Validation that dynamic ignore will work correctly
    """

    @pytest.fixture(scope="class")
    def coin_data(self, tmp_path_factory):
        """Load live CMC data while keeping all writes in a temporary root."""
        repo_root = Path(__file__).resolve().parents[2]
        source_store = CredentialStore(repo_root / "data" / "credentials")
        source_credentials = source_store.active_cmc_credentials()
        if not source_credentials:
            pytest.skip("CoinMarketCap API key is not configured")
        api_key = source_credentials[0]["api_key"]

        sandbox = tmp_path_factory.mktemp("cmc_symbol_coverage")
        original_cwd = Path.cwd()
        os.chdir(sandbox)
        try:
            store = CredentialStore(sandbox / "data" / "credentials")
            store.create_cmc(api_key)
            pool = CmcPoolClient(
                credential_store=store,
                state_root=sandbox / "data" / "credentials" / "cmc_pool",
            )
            cd = CoinData(cmc_pool=pool)
            cd.load_data()
            yield cd
        finally:
            os.chdir(original_cwd)

    @pytest.fixture(scope="class")
    def cmc_symbols(self, coin_data):
        """Extract all CMC symbol names from loaded data."""
        if not coin_data.data or "data" not in coin_data.data:
            pytest.skip("CMC data not available")

        symbols = set()
        for coin in coin_data.data["data"]:
            symbols.add(coin["symbol"].upper())
        return symbols

    @pytest.fixture(scope="class")
    def cmc_data_with_prices(self, coin_data):
        """Build dict of CMC symbols with price data."""
        if not coin_data.data or "data" not in coin_data.data:
            pytest.skip("CMC data not available")

        cmc_dict = {}
        for coin in coin_data.data["data"]:
            cmc_dict[coin["symbol"].upper()] = {
                "id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"],
                "price": coin["quote"]["USD"]["price"] if coin.get("quote", {}).get("USD", {}).get("price") else 0
            }
        return cmc_dict

    @pytest.mark.parametrize("exchange_name", [
        "binance",
        "bybit",
        "bitget",
        "okx",
        "hyperliquid",
        "kucoin",
    ])
    def test_exchange_symbol_cmc_coverage(self, coin_data, cmc_symbols, cmc_data_with_prices, exchange_name):
        """
        Test that all exchange symbols can be matched to CMC.

        This test:
        1. Loads all symbols from each configured exchange
        2. Loads exchange market data (prices)
        3. Normalizes each symbol
        4. Uses normalized symbol directly (dynamic mapping path)
        5. Checks if result exists in CMC data
        6. For mismatches, suggests similar CMC symbols using fuzzy matching + price comparison
        """
        if not coin_data.data or "data" not in coin_data.data:
            pytest.skip("CMC data not available")

        # Switch to target exchange
        if exchange_name not in coin_data.exchanges:
            pytest.skip(f"Exchange {exchange_name} not configured")

        coin_data.exchange = exchange_name

        # Build/load exchange mapping (production source of truth)
        exchange = coin_data.exchange
        mapping = coin_data.load_exchange_mapping(exchange)
        if not mapping:
            built = coin_data.build_mapping(exchange)
            if built:
                mapping = coin_data.load_exchange_mapping(exchange)

        if not mapping:
            pytest.skip(f"No mapping available for {exchange}")

        # Exclude stock-perps (HIP-3) from CMC coverage; they are intentionally not CMC-mapped.
        records = [r for r in mapping if not bool(r.get("is_hip3", False))]
        if not records:
            pytest.skip(f"No non-HIP3 mapping records available for {exchange}")

        symbols = [str(r.get("symbol") or "") for r in records if str(r.get("symbol") or "").strip()]

        # Load exchange market data (prices) - SWAP ONLY
        print(f"\nLoading market data for {exchange}...")
        exchange_prices = {}

        def to_ccxt_swap_symbol(pbgui_symbol):
            """Convert PBCoinData format (BTCUSDT) to CCXT swap format (BTC/USDT:USDT)."""
            # Extract base and quote (assuming USDT or USDC quote)
            if pbgui_symbol.endswith('USDT'):
                base = pbgui_symbol[:-4]
                quote = 'USDT'
            elif pbgui_symbol.endswith('USDC'):
                base = pbgui_symbol[:-4]
                quote = 'USDC'
            else:
                return None  # Skip non-USDT/USDC pairs

            # CCXT swap format: BASE/QUOTE:SETTLE
            return f"{base}/{quote}:{quote}"

        def normalize_ccxt_symbol(ccxt_symbol):
            """Convert CCXT format (BTC/USDT:USDT) back to PBCoinData format (BTCUSDT)."""
            if ':' in ccxt_symbol:
                ccxt_symbol = ccxt_symbol.split(':')[0]
            return ccxt_symbol.replace('/', '')

        try:
            exch_obj = Exchange(exchange)

            # Convert symbols to CCXT swap format and build mapping
            symbol_map = {}  # CCXT format -> PBCoinData format
            ccxt_symbols = []

            for pbgui_symbol in symbols:
                ccxt_symbol = to_ccxt_swap_symbol(pbgui_symbol)
                if ccxt_symbol:
                    ccxt_symbols.append(ccxt_symbol)
                    symbol_map[ccxt_symbol] = pbgui_symbol

            print(f"Converted {len(ccxt_symbols)}/{len(symbols)} symbols to CCXT swap format")

            if not ccxt_symbols:
                print(f"Warning: No USDT/USDC symbols found for {exchange}")
            else:
                # Fetch prices in batches to avoid timeout/rate limits
                batch_size = 50
                total_fetched = 0

                for i in range(0, len(ccxt_symbols), batch_size):
                    batch = ccxt_symbols[i:i+batch_size]
                    try:
                        market_data = exch_obj.fetch_prices(batch, "swap")

                        # Map CCXT format back to PBCoinData format
                        for ccxt_symbol, ticker in market_data.items():
                            if ticker and 'last' in ticker and ticker['last']:
                                pbgui_symbol = symbol_map.get(ccxt_symbol)
                                if pbgui_symbol:
                                    # Ensure price is float, not string
                                    price = ticker['last']
                                    if isinstance(price, str):
                                        price = float(price)
                                    exchange_prices[pbgui_symbol] = price
                                    total_fetched += 1

                        print(f"  Batch {i//batch_size + 1}/{(len(ccxt_symbols)-1)//batch_size + 1}: "
                              f"fetched {len(market_data)} prices (total: {total_fetched})")

                    except Exception as e:
                        print(f"  Batch {i//batch_size + 1} error: {e}")
                        continue

                print(f"Loaded {len(exchange_prices)} prices from {exchange}")
        except Exception as e:
            print(f"Warning: Could not load market data from {exchange}: {e}")
            print("Continuing without price comparison...")

        # Track results
        matched = []
        unmatched = []
        normalized_changed = []

        # Build CMC lookup (by symbol and by id)
        cmc_lookup = {}
        cmc_lookup_by_id = {}
        for coin in coin_data.data["data"]:
            cmc_lookup[coin["symbol"].upper()] = {
                "id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"]
            }
            cmc_lookup_by_id[int(coin["id"])] = {
                "id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"]
            }

        # Test each mapped market record
        for record in records:
            symbol = str(record.get("symbol") or "")
            base_symbol = str(record.get("base") or "")
            normalized = str(record.get("coin") or normalize_symbol(symbol, coin_data._symbol_mappings)).upper()
            normalized_base = str(normalize_symbol(base_symbol, coin_data._symbol_mappings)).upper() if base_symbol else ""
            cmc_id = int(record.get("cmc_id") or 0)

            # Mapping considers this a match when cmc_id resolves to known CMC row
            if cmc_id > 0 and cmc_id in cmc_lookup_by_id:
                cmc_meta = cmc_lookup_by_id[cmc_id]
                matched.append({
                    "exchange_symbol": symbol,
                    "normalized": normalized,
                    "cmc_symbol": cmc_meta["symbol"],
                    "cmc_name": cmc_meta["name"],
                    "normalized_changed": str(symbol).upper() != str(normalized).upper()
                })
                if str(symbol).upper() != str(normalized).upper():
                    normalized_changed.append(symbol)
            elif normalized and normalized in cmc_lookup:
                cmc_meta = cmc_lookup[normalized]
                matched.append({
                    "exchange_symbol": symbol,
                    "normalized": normalized,
                    "cmc_symbol": cmc_meta["symbol"],
                    "cmc_name": cmc_meta["name"],
                    "normalized_changed": str(symbol).upper() != str(normalized).upper()
                })
                if str(symbol).upper() != str(normalized).upper():
                    normalized_changed.append(symbol)
            elif normalized_base and normalized_base in cmc_lookup:
                cmc_meta = cmc_lookup[normalized_base]
                matched.append({
                    "exchange_symbol": symbol,
                    "normalized": normalized_base,
                    "cmc_symbol": cmc_meta["symbol"],
                    "cmc_name": cmc_meta["name"],
                    "normalized_changed": True
                })
                normalized_changed.append(symbol)
            else:
                # Get exchange price for this symbol
                exchange_price = exchange_prices.get(symbol, None)

                # Find similar CMC symbols (with price comparison if available)
                suggestions = find_similar_cmc_symbols(
                    normalized,
                    cmc_data_with_prices,
                    exchange_price=exchange_price,
                    top_n=5,
                    min_similarity=0.3,
                    price_weight=0.6
                )

                unmatched.append({
                    "exchange_symbol": symbol,
                    "normalized": normalized,
                    "attempted_cmc": normalized,
                    "exchange_price": exchange_price,
                    "suggestions": suggestions
                })

        # Generate report
        total = len(symbols)
        matched_count = len(matched)
        unmatched_count = len(unmatched)
        normalized_changed_count = len(normalized_changed)

        print(f"\n{'='*80}")
        print(f"CMC Symbol Coverage Report for {exchange}")
        print(f"{'='*80}")
        print(f"Total symbols tested: {total}")
        print(f"Successfully matched: {matched_count} ({matched_count/total*100:.1f}%)")
        print(f"Normalized changed symbol: {normalized_changed_count} ({normalized_changed_count/total*100:.1f}%)")
        print(f"Unmatched: {unmatched_count} ({unmatched_count/total*100:.1f}%)")

        if unmatched:
            print(f"\n{'='*80}")
            print("UNMATCHED SYMBOLS - Suggested mapping candidates:")
            print(f"{'='*80}")

            # Group by normalized symbol to avoid duplicates
            grouped = defaultdict(list)
            for item in unmatched:
                grouped[item["normalized"]].append(item)

            for normalized, items in sorted(grouped.items()):
                exchange_symbols = [item["exchange_symbol"] for item in items]
                suggestions = items[0]["suggestions"]
                exchange_price = items[0].get("exchange_price")

                print(f"\nNormalized: {normalized}")
                print(f"  Exchange symbols: {', '.join(exchange_symbols)}")
                if exchange_price:
                    print(f"  Exchange price: ${exchange_price:.8f}")

                if suggestions:
                    print(f"  Suggested CMC matches:")
                    for cmc_sym, str_score, price_score, combined_score in suggestions:
                        cmc_price = cmc_data_with_prices[cmc_sym]['price']
                        if price_score is not None:
                            print(f"    - \"{normalized}\": \"{cmc_sym}\",  # "
                                  f"str_sim: {str_score:.2f}, price_sim: {price_score:.2f}, "
                                  f"combined: {combined_score:.2f}, CMC price: ${cmc_price:.8f}")
                        else:
                            print(f"    - \"{normalized}\": \"{cmc_sym}\",  # "
                                  f"similarity: {str_score:.2f} (no price data)")
                else:
                    print(f"  No similar CMC symbols found (check if coin is listed on CMC)")

        # Show examples where normalization changed input symbol but still matched CMC
        if normalized_changed and matched_count > 0:
            print(f"\n{'='*80}")
            print("Examples of working normalization mappings:")
            print(f"{'='*80}")

            symbolmap_examples = [m for m in matched if m["normalized_changed"]][:10]
            for item in symbolmap_examples:
                print(f"  \"{item['normalized']}\": \"{item['cmc_symbol']}\",  # {item['exchange_symbol']} -> {item['cmc_name']}")

        print(f"\n{'='*80}\n")

        # Write report to file
        report_dir = Path.cwd() / "test_results"
        report_dir.mkdir(exist_ok=True)
        report_file = report_dir / f"cmc_coverage_{exchange}.txt"

        with open(report_file, "w") as f:
            f.write(f"{'='*80}\n")
            f.write(f"CMC Symbol Coverage Report for {exchange}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if exchange_prices:
                f.write(f"Price matching: ENABLED ({len(exchange_prices)} prices loaded)\n")
            else:
                f.write(f"Price matching: DISABLED (no exchange prices available)\n")
            f.write(f"{'='*80}\n")
            f.write(f"Total symbols tested: {total}\n")
            f.write(f"Successfully matched: {matched_count} ({matched_count/total*100:.1f}%)\n")
            f.write(f"Normalized changed symbol: {normalized_changed_count} ({normalized_changed_count/total*100:.1f}%)\n")
            f.write(f"Unmatched: {unmatched_count} ({unmatched_count/total*100:.1f}%)\n")

            if unmatched:
                f.write(f"\n{'='*80}\n")
                f.write("UNMATCHED SYMBOLS - Suggested mapping candidates:\n")
                f.write(f"{'='*80}\n")

                grouped = defaultdict(list)
                for item in unmatched:
                    grouped[item["normalized"]].append(item)

                for normalized, items in sorted(grouped.items()):
                    exchange_symbols = [item["exchange_symbol"] for item in items]
                    suggestions = items[0]["suggestions"]
                    exchange_price = items[0].get("exchange_price")

                    f.write(f"\nNormalized: {normalized}\n")
                    f.write(f"  Exchange symbols: {', '.join(exchange_symbols)}\n")
                    if exchange_price:
                        f.write(f"  Exchange price: ${exchange_price:.8f}\n")

                    if suggestions:
                        f.write(f"  Suggested CMC matches:\n")
                        for cmc_sym, str_score, price_score, combined_score in suggestions:
                            cmc_price = cmc_data_with_prices[cmc_sym]['price']
                            if price_score is not None:
                                f.write(f"    - \"{normalized}\": \"{cmc_sym}\",  # "
                                       f"str_sim: {str_score:.2f}, price_sim: {price_score:.2f}, "
                                       f"combined: {combined_score:.2f}, CMC price: ${cmc_price:.8f}\n")
                            else:
                                f.write(f"    - \"{normalized}\": \"{cmc_sym}\",  # "
                                       f"similarity: {str_score:.2f} (no price data)\n")
                    else:
                        f.write(f"  No similar CMC symbols found (check if coin is listed on CMC)\n")

            if normalized_changed and matched_count > 0:
                f.write(f"\n{'='*80}\n")
                f.write("Examples of working normalization mappings:\n")
                f.write(f"{'='*80}\n")

                symbolmap_examples = [m for m in matched if m["normalized_changed"]][:10]
                for item in symbolmap_examples:
                    f.write(f"  \"{item['normalized']}\": \"{item['cmc_symbol']}\",  # {item['exchange_symbol']} -> {item['cmc_name']}\n")

        print(f"Report saved to: {report_file}")

        # Test passes if coverage is reasonable (allow some coins not on CMC)
        coverage_threshold = 0.80  # Dynamic mapping can vary per exchange / listing universe
        coverage = matched_count / total if total > 0 else 0

        assert coverage >= coverage_threshold, (
            f"CMC coverage too low: {coverage:.1%} < {coverage_threshold:.1%}\n"
            f"Check output above or report file: {report_file}"
        )

    def test_symbolmap_consistency(self, coin_data):
        """
        Backward-compatible test name: validate dynamic symbol mappings consistency.

        Checks:
        - Dynamic mapping dict exists and is populated for active exchange symbols
        - Mapping values are normalized uppercase strings
        - normalize_symbol() is stable (idempotent for mapped results)
        """
        symbols = list(coin_data.symbols or [])
        if not symbols:
            pytest.skip("No symbols available for consistency validation")

        mappings = coin_data._symbol_mappings if isinstance(coin_data._symbol_mappings, dict) else {}

        print(f"\n{'='*80}")
        print("Dynamic symbol-mapping consistency check")
        print(f"{'='*80}")
        print(f"Exchange: {coin_data.exchange}")
        print(f"Total exchange symbols: {len(symbols)}")
        print(f"Total dynamic mappings: {len(mappings)}")

        assert len(mappings) > 0, "Dynamic symbol mappings are empty"

        invalid_values = []
        unstable = []
        for key, value in mappings.items():
            if not isinstance(value, str) or not value.strip() or value.upper() != value:
                invalid_values.append((key, value))
                continue

            n1 = normalize_symbol(str(key), mappings)
            n2 = normalize_symbol(str(n1), mappings)
            if str(n1).upper() != str(n2).upper():
                unstable.append((key, n1, n2))

        if invalid_values:
            print(f"Invalid mapping values: {len(invalid_values)}")
            for k, v in invalid_values[:10]:
                print(f"  {k} -> {v}")

        if unstable:
            print(f"Unstable normalize_symbol results: {len(unstable)}")
            for k, n1, n2 in unstable[:10]:
                print(f"  {k}: {n1} -> {n2}")

        print(f"\n{'='*80}\n")

        assert not invalid_values, "Found invalid dynamic mapping values"
        assert not unstable, "normalize_symbol() is not stable for some mapping keys"


# Run test with detailed output
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
