"""Shared, secret-free presentation of Hyperliquid account abstraction modes."""

SERVICE = "HyperliquidAccountMode"


def mode_presentation(mode):
    """Describe a verified mode without treating missing detection as Standard."""
    label = {"standard_manual": "Standard / Manual", "unified": "Unified Account",
             "portfolio_margin": "Portfolio Margin"}.get(mode, "Account mode not verified")
    unsupported = mode in {"unified", "portfolio_margin"}
    guidance = (
        f"{label} uses shared collateral. PBGui cannot use Profit Sweep or separate "
        "Spot/Perps transfers in this mode. To use Profit Sweep, switch Account Type "
        "to Manual (Standard) in Hyperliquid, then refresh PBGui."
    ) if unsupported else ""
    return {"mode": mode, "label": label, "unsupported": unsupported, "guidance": guidance}


def snapshot_mode(snapshot):
    """Exclude Vaults: their leader's Unified mode does not prohibit Vault transfers."""
    if snapshot.get("exchange") != "hyperliquid" or snapshot.get("account_kind") != "normal":
        return mode_presentation("")
    return mode_presentation((snapshot.get("account") or {}).get("mode", "unknown"))


class UnsupportedAccountMode(RuntimeError):
    """Retain a read-only snapshot while preventing all write-capable operations."""

    def __init__(self, snapshot):
        """Expose only the already sanitized snapshot to preview handlers."""
        self.snapshot = snapshot
        super().__init__(snapshot_mode(snapshot)["guidance"])
