"""Native PB9 field help using PB8's bounded original-document reader."""

from pb8_parameter_help import _build_catalog, _original_paragraph, get_local_parameter_help

SERVICE = "PB9ParameterHelp"


def _catalog(documents):
    """Map native policy paths to their own upstream definitions, without leaf guesses."""
    catalog = _build_catalog(documents)
    entries = catalog["entries"]
    aliases = {
        "bot.*.risk.position_exposure_enforcer.enabled": "live.position_exposure_enforcer.threshold_ratio",
        "bot.*.risk.total_exposure_enforcer.enabled": "live.total_exposure_enforcer.enabled",
        "bot.*.risk.total_exposure_enforcer.policy": "live.total_exposure_enforcer.policy",
    }
    for target, source in aliases.items():
        if source in entries:
            entries[target] = entries[source]
    for target, filename, start in (
        ("bot.*.enabled", "config.bot.md", "Side activation is explicit and never inherits:"),
        ("bot.*.risk.hsl", "configuration.md", "Local HSL settings inherit from"),
        ("bot.portfolio.risk.hsl", "configuration.md", "Local HSL settings inherit from"),
        ("bot.portfolio.risk.loss_realization.memory_ema_span_minutes", "v9_risk_model.md", "Auto-unstuck and the realized-loss gate use the same account-wide drawdown state"),
    ):
        text = _original_paragraph(documents.get(filename, ""), start)
        if text:
            entries[target] = {"text": text, "source": filename, "heading": ""}
    return catalog


def populate_pb9_field_help(repo_dir, fields, *, catalog=None):
    """Attach original native documentation and explicit inheritance context to fields."""
    if catalog is None:
        catalog = get_local_parameter_help(repo_dir, generation="PB9", builder=_catalog,
            document_names=("configuration.md", "optimizing.md", "metrics.md", "config.bot.md", "equity_hard_stop_loss.md", "v9_risk_model.md"))
    entries = catalog["entries"]
    for field in fields:
        path = ".".join(field["path"])
        parts = field["path"]
        shared = ".".join(["bot", "*", *parts[2:]]) if len(parts) > 2 and parts[0] == "bot" and parts[1] in {"global", "long", "short"} else path
        reference = entries.get(path) or entries.get(shared)
        if reference is None and len(parts) > 4 and parts[:1] == ["bot"] and parts[2:4] == ["risk", "hsl"]:
            reference = entries.get("bot.portfolio.risk.hsl" if parts[1] == "portfolio" else "bot.*.risk.hsl")
        text = field.get("description") or (reference or {}).get("text") or "No parameter description is supplied by the installed PB9 documentation."
        if reference:
            text += "\n\nPassivbot · docs/" + reference["source"]
        text += "\n\nConfig: " + path
        if len(parts) > 2 and parts[0] == "bot":
            if parts[1] in {"long", "short"} and parts[2] != "enabled":
                text += "\nOmission inherits bot.global." + ".".join(parts[2:]) + "; editing creates a side override."
            elif parts[1] == "global":
                text += "\nShared default for Long and Short unless a side overrides it."
            elif parts[1] == "portfolio":
                text += "\nPortfolio policy; this is separate from directional defaults."
        field["description"] = text
