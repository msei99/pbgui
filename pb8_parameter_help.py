"""Read original parameter explanations from the configured PB8 documentation."""

from __future__ import annotations

import copy
import re
import threading
from pathlib import Path

from pbgui_purefunc import pb8dir

SERVICE = "PB8ParameterHelp"
_DOCUMENTS = ("configuration.md", "optimizing.md", "metrics.md", "config.bot.md")
_cache_lock = threading.RLock()
_cache: tuple[tuple, dict] | None = None
_ITEM = re.compile(r"^(\s*)[-*]\s+(?P<label>(?:\*\*[^*]+\*\*|`[^`]+`)(?:(?:,\s*(?:and\s+)?|\s*/\s*)(?:\*\*[^*]+\*\*|`[^`]+`))*)(?P<rest>.*)$")
_KEY = re.compile(r"\*\*([^*]+)\*\*|`([^`]+)`")


def _canonical_key(key: str, section: str, first: str = "") -> str:
    """Resolve documentation shorthand without merging unrelated parameter names."""
    key = key.strip().strip('"').split(":", 1)[0]
    if not re.fullmatch(r"[A-Za-z_][\w.]*", key):
        return ""
    if key.startswith(("bot.", "backtest.", "live.", "logging.", "monitor.", "optimize.")):
        return key.replace("bot.long.", "bot.*.").replace("bot.short.", "bot.*.")
    if section == "bot":
        if key.startswith("entry.") and first.startswith("strategy.trailing_martingale."):
            key = "strategy.trailing_martingale." + key
        elif "." not in key and first.startswith("strategy."):
            key = first.rsplit(".", 1)[0] + "." + key
        for old, new in (("hsl_", "hsl."), ("unstuck_", "unstuck."), ("forager_", "forager.")):
            if key.startswith(old):
                key = new + key[len(old):]
                break
        if key in {"n_positions", "total_wallet_exposure_limit", "we_excess_allowance_pct", "we_excess_allowance_mode", "wallet_exposure_limit"}:
            key = "risk." + key
        if key in {"forager.volume_ema_span", "forager.volatility_ema_span"}:
            key += "_1m"
        return "bot.*." + key
    return section + "." + key if section else key


def _parse_document(text: str, filename: str) -> dict[str, dict]:
    """Extract original Markdown list descriptions, excluding examples in fences."""
    entries = {}
    lines = text.splitlines()
    section = "metrics" if filename == "metrics.md" else ""
    heading = ""
    fenced = False
    for index, line in enumerate(lines):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        if line.startswith("#"):
            heading = line.lstrip("# ")
            if filename == "configuration.md" and line.startswith("## "):
                section = {
                    "Backtest Settings": "backtest", "Logging": "logging", "Monitor": "monitor",
                    "Bot Settings": "bot", "Live Trading Settings": "live",
                    "Optimization Settings": "optimize",
                }.get(heading, "")
            if filename == "optimizing.md":
                if "GPU Backend" in heading:
                    section = "optimize.gpu"
                elif line.startswith("### "):
                    section = "optimize"
            continue
        match = _ITEM.match(line)
        if not match or not section:
            continue
        keys = [key.strip() for a, b in _KEY.findall(match["label"]) for key in (a or b).split(" / ")]
        indent = len(match[1])
        body = [match["rest"].lstrip(": ")]
        for continuation in lines[index + 1:]:
            if continuation.strip() and len(continuation) - len(continuation.lstrip()) <= indent:
                break
            body.append(continuation[indent + 2:] if continuation.startswith(" " * (indent + 2)) else continuation)
        description = "\n".join(body).strip()
        if not description:
            continue
        for key in keys:
            target_section = section
            if section == "optimize" and key.strip('"') in {
                "mirror_short_from_long", "lossless_close_trailing", "forward_tp_grid", "backward_tp_grid"
            }:
                target_section = "optimize.enable_overrides"
            canonical = _canonical_key(key, target_section, keys[0])
            if canonical:
                entries.setdefault(canonical, {"text": description, "source": filename, "heading": heading})
    return entries


def _original_paragraph(text: str, start: str) -> str:
    """Return one original prose paragraph located by its documented opening."""
    position = text.find(start)
    if position < 0:
        return ""
    return text[position:].split("\n\n", 1)[0].strip()


def _build_catalog(documents: dict[str, str]) -> dict:
    """Combine canonical reference entries with prose-only native option help."""
    entries = {}
    for filename in ("configuration.md", "optimizing.md", "metrics.md"):
        entries.update(_parse_document(documents.get(filename, ""), filename))
    prose = {
        "optimize.enable_overrides.couple_unstuck_ema_spans": ("optimizing.md", "This opt-in setting belongs under `optimize`."),
        "runtime.fine_tune_params": ("optimizing.md", "When you only want to adjust a handful of parameters"),
        "runtime.polish_percentage": ("optimizing.md", "Polish still uses relative bounds:"),
        "runtime.polish_bounds_mode": ("optimizing.md", "By default this keeps the polished bounds inside"),
        "bot.*.unstuck.enabled": ("config.bot.md", "Auto unstuck is controlled by"),
        "bot.*.unstuck.ema_span_0": ("config.bot.md", "`bot.<side>.unstuck.ema_span_0` and `ema_span_1` are"),
        "bot.*.risk.position_exposure_enforcer_enabled": ("config.bot.md", "Setting `position_exposure_enforcer_enabled = false` disables"),
        "optimize.limits": ("configuration.md", "The optimizer penalizes backtests whose metric values"),
        "optimize.max_pending_starting_evals_per_cpu": ("optimizing.md", "When you provide many starting configs to a CPU optimizer"),
    }
    for key, (filename, start) in prose.items():
        paragraph = _original_paragraph(documents.get(filename, ""), start)
        if paragraph:
            entries[key] = {"text": paragraph, "source": filename, "heading": ""}
    if "bot.*.unstuck.ema_span_0" in entries:
        entries["bot.*.unstuck.ema_span_1"] = entries["bot.*.unstuck.ema_span_0"]
    # The upstream reference still groups these canonical bot-side fields under Live.
    for key in (
        "entry_cooldown_minutes", "position_exposure_enforcer_enabled", "position_exposure_enforcer_threshold",
        "total_exposure_enforcer_enabled", "total_exposure_enforcer_threshold", "total_exposure_enforcer_policy",
        "total_exposure_entry_gate_enabled", "we_excess_allowance_mode", "risk_we_excess_allowance_pct",
    ):
        if "live." + key in entries:
            leaf = key.removeprefix("risk_")
            entries["bot.*.risk." + leaf] = entries["live." + key]
    for group, leaves in {
        "bot.*.forager.score_weights": ("volume", "ema_readiness", "volatility"),
        "live.approved_coins": ("long", "short"),
        "live.ignored_coins": ("long", "short"),
        "backtest.reducer": ("default",),
    }.items():
        if group in entries:
            for leaf in leaves:
                entries[group + "." + leaf] = entries[group]
    return {"contract_version": 1, "entries": entries}


def get_pb8_parameter_help() -> dict:
    """Return a bounded, signature-invalidated cache of allowlisted local docs."""
    global _cache
    configured = pb8dir()
    if not configured:
        raise FileNotFoundError("PB8 documentation is unavailable: PB8 path is not configured")
    root = Path(configured).resolve()
    paths = []
    for name in _DOCUMENTS:
        path = (root / "docs" / name).resolve()
        if not path.is_relative_to(root):
            raise ValueError("PB8 documentation path leaves the configured installation")
        if path.exists():
            stat = path.stat()
            if stat.st_size > 1024 * 1024:
                raise ValueError("PB8 documentation file exceeds the size limit")
            paths.append((name, path, stat.st_mtime_ns, stat.st_size))
    if not any(name == "configuration.md" for name, *_ in paths):
        raise FileNotFoundError("PB8 docs/configuration.md is unavailable")
    signature = (str(root), tuple((name, mtime, size) for name, _, mtime, size in paths))
    with _cache_lock:
        if _cache is None or _cache[0] != signature:
            documents = {name: path.read_text(encoding="utf-8") for name, path, *_ in paths}
            _cache = (signature, _build_catalog(documents))
        return copy.deepcopy(_cache[1])
