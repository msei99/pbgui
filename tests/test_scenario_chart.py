"""Local scenario charts aggregate isolated fixtures without network access."""
import numpy as np
import pytest

import scenario_chart


def test_daily_chart_ohlcv_and_gaps(tmp_path, monkeypatch):
    """Daily OHLCV preserves extrema and reports missing and partial days."""
    monkeypatch.setattr(scenario_chart, '_root', lambda _: tmp_path)
    folder = tmp_path / '1m' / 'ETH_USDT:USDT'
    folder.mkdir(parents=True)
    data = np.array([(1704067200000, 10, 15, 8, 12, 2), (1704067260000, 12, 16, 9, 14, 3)],
                    dtype=[('ts', 'i8'), ('o', 'f8'), ('h', 'f8'), ('l', 'f8'), ('c', 'f8'), ('bv', 'f8')])
    np.savez_compressed(folder / '20240101.npz', candles=data)
    result = scenario_chart.daily_chart('bybit', '1m', folder.name, '2024-01-01', '2024-01-02')
    assert result['candles'] == [dict(date='2024-01-01', open=10, high=16, low=8, close=14, volume=5, minutes=2)]
    assert result['missing_days'] == ['2024-01-02']
    assert result['incomplete_days'] == ['2024-01-01']
    assert scenario_chart.sources('bybit') == [{'coin': folder.name, 'dataset': '1m'}]


@pytest.mark.parametrize('coin', ['../secret', 'x/y', '..', 'a\x00b'])
def test_chart_rejects_path_identifiers(tmp_path, monkeypatch, coin):
    """User-controlled chart identifiers cannot escape the local data root."""
    monkeypatch.setattr(scenario_chart, '_root', lambda _: tmp_path)
    with pytest.raises(ValueError):
        scenario_chart.daily_chart('bybit', '1m', coin, '2024-01-01', '2024-01-02')


def test_hourly_shards_are_combined_without_duplicate_volume(tmp_path, monkeypatch):
    """Two hourly files of one day must contribute to one candle, deduplicating minutes."""
    monkeypatch.setattr(scenario_chart, '_root', lambda _: tmp_path)
    folder = tmp_path / '1m' / 'ETH'
    folder.mkdir(parents=True)
    first = [1704067200000, 10, 15, 8, 12, 2]
    second = [1704070800000, 12, 16, 9, 14, 3]
    np.savez_compressed(folder / '20240101_00.npz', candles=np.array([first]))
    np.savez_compressed(folder / '20240101_01.npz', candles=np.array([first, second]))
    candle = scenario_chart.daily_chart('bybit', '1m', 'ETH', '2024-01-01', '2024-01-01')['candles'][0]
    assert (candle['open'], candle['close'], candle['volume'], candle['minutes']) == (10, 14, 5, 2)


def test_source_coin_uses_persisted_mapping(tmp_path, monkeypatch):
    """Reference selection uses market mappings instead of guessing symbol prefixes."""
    import json
    root=tmp_path/'data/ohlcv/binanceusdm'
    (root/'1m/ETH_USDT:USDT').mkdir(parents=True)
    mapping=tmp_path/'data/coindata/binance/mapping.json'
    mapping.parent.mkdir(parents=True)
    mapping.write_text(json.dumps([{'coin':'ETH','ccxt_symbol':'ETH/USDT:USDT','quote':'USDT','swap':True}]))
    monkeypatch.setattr(scenario_chart,'_root',lambda _:root)
    assert scenario_chart.sources('binance') == [{'coin':'ETH_USDT:USDT','dataset':'1m','base_coin':'ETH'}]


def test_daily_cache_reuses_summaries_and_invalidates_files(tmp_path, monkeypatch):
    """Repeated and overlapping ranges reuse summaries but file changes stay visible."""
    monkeypatch.setattr(scenario_chart, '_root', lambda _: tmp_path)
    folder = tmp_path / '1m' / 'ETH'
    folder.mkdir(parents=True)
    path = folder / '20240101.npz'
    np.savez_compressed(path, candles=np.array([[1704067200000, 10, 15, 8, 12, 2]]))
    original_load = np.load
    reads = []

    def counted_load(*args, **kwargs):
        """Count decompressions without changing the archive reader."""
        reads.append(args[0])
        return original_load(*args, **kwargs)

    monkeypatch.setattr(np, 'load', counted_load)
    first = scenario_chart.daily_chart('bybit', '1m', 'ETH', '2024-01-01', '2024-01-01')
    first['candles'][0]['close'] = -1
    second = scenario_chart.daily_chart('bybit', '1m', 'ETH', '2024-01-01', '2024-01-02')
    assert len(reads) == 1
    assert second['candles'][0]['close'] == 12
    np.savez_compressed(path, candles=np.array([[1704067200000, 10, 20, 8, 19, 3]]))
    updated = scenario_chart.daily_chart('bybit', '1m', 'ETH', '2024-01-01', '2024-01-01')
    assert len(reads) == 2
    assert updated['candles'][0]['close'] == 19
    path.unlink()
    assert not scenario_chart.daily_chart('bybit', '1m', 'ETH', '2024-01-01', '2024-01-01')['candles']
