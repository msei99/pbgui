"""Secondary asset placeholders must not invalidate a Bitget UTA USDT balance."""

from unittest.mock import Mock

import pytest

import bitget_uta as uta


@pytest.mark.parametrize("secondary", [{"coin": "ETH"}, {"coin": "ETH", "balance": None},
                                       {"coin": "ETH", "balance": ""}])
@pytest.mark.parametrize("usdt", [None, "0", "42.5"])
@pytest.mark.parametrize("secondary_first", [True, False])
def test_secondary_placeholder_preserves_usdt(secondary, usdt, secondary_first):
    """The public fetcher accepts placeholders before or after a valid USDT row."""
    assets = [secondary]
    if usdt is not None:
        assets.append({"coin": "USDT", "balance": usdt})
    if not secondary_first:
        assets.reverse()
    client = Mock()
    client.privateUtaGetV3AccountAssets.return_value = {"code": "00000", "data": {"assets": assets}}
    assert uta.fetch_balance(client) == (float(usdt) if usdt is not None else 0.0)
    client.privateUtaGetV3AccountAssets.assert_called_once()


@pytest.mark.parametrize("value", [None, "", "invalid", "NaN", "Infinity", True])
def test_invalid_usdt_balance_still_raises(value):
    """USDT must retain strict numeric validation even alongside placeholders."""
    assets = [{"coin": "ETH", "balance": None}, {"coin": "USDT", "balance": value}]
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_wallet_balance({"code": "00000", "data": {"assets": assets}})


@pytest.mark.parametrize("secondary", [[None], [{}], [{"coin": ""}],
    [{"coin": "ETH", "balance": None}, {"coin": "ETH", "balance": None}]])
def test_secondary_asset_structure_still_validated(secondary):
    """Ignoring unused balances must not bypass identifiers or duplicate checks."""
    assets = [{"coin": "USDT", "balance": "42.5"}, *secondary]
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_wallet_balance({"code": "00000", "data": {"assets": assets}})
