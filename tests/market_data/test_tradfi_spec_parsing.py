"""Tests for TradFi XYZ spec parsing and provider classification."""

from pathlib import Path
import importlib.util

from bs4 import BeautifulSoup


tradfi_sync_path = Path(__file__).parent.parent.parent / "tradfi_sync.py"
tradfi_sync_spec = importlib.util.spec_from_file_location("tradfi_sync_real", tradfi_sync_path)
tradfi_sync = importlib.util.module_from_spec(tradfi_sync_spec)
tradfi_sync_spec.loader.exec_module(tradfi_sync)

market_data_tradfi_path = Path(__file__).parent.parent.parent / "market_data_tradfi.py"
market_data_tradfi_spec = importlib.util.spec_from_file_location("market_data_tradfi_real", market_data_tradfi_path)
market_data_tradfi = importlib.util.module_from_spec(market_data_tradfi_spec)
market_data_tradfi_spec.loader.exec_module(market_data_tradfi)


class TestTradfiSpecParsing:
    """Validate XYZ spec parsing and dynamic TradFi type handling."""

    def test_parse_xyz_spec_row_reads_description_underlying_and_type(self):
        html = """
        <div role="row">
          <div role="cell">BRENTOIL</div>
          <div role="cell">BRENTOIL tracks the value of 1 barrel of Brent Crude Oil.</div>
          <div role="cell"><a href="https://pythdata.app/explore/Commodities.BRENTN6%2FUSD">N6 / USD<span>arrow-up-right</span></a></div>
          <div role="cell">20x</div>
          <div role="cell">±5%</div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find(attrs={"role": "row"})

        parsed = tradfi_sync._parse_xyz_spec_row(row, "2026-04-29T00:00:00+00:00")

        assert parsed is not None
        assert parsed["xyz_coin"] == "BRENTOIL"
        assert parsed["description"] == "BRENTOIL tracks the value of 1 barrel of Brent Crude Oil."
        assert parsed["underlying"] == "N6 / USD"
        assert parsed["underlying_href"] == "https://pythdata.app/explore/Commodities.BRENTN6%2FUSD"
        assert parsed["max_leverage"] == "20x"
        assert parsed["canonical_type"] == "commodity"
        assert parsed["pyth_symbol"] == "Commodities.BRENTN6/USD"

    def test_auto_map_strategy_uses_dynamic_canonical_type(self):
        assert tradfi_sync._auto_map_strategy_for_entry("BRENTOIL", "commodity") == "no_provider"
        assert tradfi_sync._auto_map_strategy_for_entry("GOLD", "commodity") == "mapped_fx"
        assert tradfi_sync._auto_map_strategy_for_entry("URNM", "commodity_etf") == "alias"
        assert tradfi_sync._auto_map_strategy_for_entry("TSLA", "equity_us") == "equity_lookup"

    def test_index_description_without_link_still_maps_to_index(self):
        html = """
        <div role="row">
          <div role="cell">JP225</div>
          <div role="cell">Japan 225 tracks a JPY-denominated, price-weighted index of 225 leading Japanese companies and serves as a widely followed benchmark for the Japanese equity market.</div>
          <div role="cell">JP225 / USD</div>
          <div role="cell">20x</div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find(attrs={"role": "row"})

        parsed = tradfi_sync._parse_xyz_spec_row(row, "2026-04-29T00:00:00+00:00")

        assert parsed is not None
        assert parsed["canonical_type"] == "index_etf"

    def test_normalize_external_href_repairs_pyth_symbol_slash_encoding(self):
        broken = "https://pythdata.app/explore/Equity.US.AMZN/USD"
        encoded = "https://pythdata.app/explore/Equity.US.AMZN%2FUSD"

        assert market_data_tradfi._normalize_external_href(broken) == encoded
        assert market_data_tradfi._normalize_external_href(encoded) == encoded

    def test_mapping_json_rows_use_spec_canonical_type(self):
        entry = {
            "xyz_coin": "BRENTOIL",
            "canonical_type": "equity_us",
            "description": "",
            "spec_source": "mapping.json",
        }
        spec_row = {
            "xyz_coin": "BRENTOIL",
            "canonical_type": "commodity",
            "description": "Brent crude oil contract.",
        }

        merged = market_data_tradfi.apply_tradfi_spec_defaults(entry, spec_row)

        assert merged["canonical_type"] == "commodity"
        assert merged["description"] == "Brent crude oil contract."
        assert market_data_tradfi.resolve_tradfi_canonical_type(merged, spec_row) == "commodity"
