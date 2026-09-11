"""Offline contracts for original PB8 parameter documentation and its endpoint."""

from pathlib import Path

import pytest
from fastapi import HTTPException

import pb8_parameter_help as help_docs
from api import v8_instances


@pytest.fixture
def documentation(tmp_path, monkeypatch):
    """Provide isolated upstream-style documents without reading a real installation."""
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "configuration.md").write_text(
        "## Live Trading Settings\n\n"
        "- **risk_input_max_attempts**: Maximum failed attempts per recovery episode.\n"
        "  Successful recovery resets it.\n"
        "- **total_exposure_enforcer_policy**: Original portfolio policy.\n"
        "## Bot Settings\n\n"
        "- **strategy.trailing_martingale.entry.threshold_we_weight**, **threshold_volatility_1m_weight**:\n"
        "  - Original threshold formula.\n"
        "- **forager_volatility_ema_span_1m / forager_volume_ema_span_1m**: Original span explanation.\n"
        "- **hsl_tier_ratios.yellow / hsl_tier_ratios.orange**: Original tier explanation.\n"
        "## Monitor\n\n- **enabled**: Publish monitoring data.\n"
        "## Optimization Settings\n\n"
        "- **enable_overrides**: Candidate transformations.\n"
        "  - **\"mirror_short_from_long\"**: Mirrors bot.short from bot.long.\n"
        "  - **\"forward_tp_grid\"**, **\"backward_tp_grid\"**: Original compatibility restriction.\n"
        "```json\n- **not_a_parameter**: Do not index examples.\n```\n",
        encoding="utf-8",
    )
    (docs / "optimizing.md").write_text(
        "### Shared Pymoo Hyperparameters\n\n"
        "- `optimize.pymoo.shared.mutation_prob`\n"
        "  - Per-individual polynomial-mutation probability.\n"
        "- `optimize.pymoo.shared.mutation_prob_per_variable`\n"
        "  - Per-variable polynomial-mutation probability.\n\n"
        "This opt-in setting belongs under `optimize`. It restores the former coupled EMA search on CPU\n"
        "and Apple MPS: each candidate's effective strategy spans determine its unstuck spans.\n\n"
        "Unrelated later paragraph.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(help_docs, "pb8dir", lambda: str(tmp_path))
    monkeypatch.setattr(help_docs, "_cache", None)
    monkeypatch.setattr(v8_instances, "_log", lambda *_args, **_kwargs: None)
    return docs


def test_original_text_and_grouped_canonical_paths(documentation) -> None:
    """Descriptions retain upstream wording and distinguish similarly named parameters."""
    entries = help_docs.get_pb8_parameter_help()["entries"]
    assert entries["live.risk_input_max_attempts"]["text"] == (
        "Maximum failed attempts per recovery episode.\nSuccessful recovery resets it."
    )
    assert entries["optimize.enable_overrides.couple_unstuck_ema_spans"]["text"].endswith(
        "effective strategy spans determine its unstuck spans."
    )
    assert entries["optimize.enable_overrides.couple_unstuck_ema_spans"]["source"] == "optimizing.md"
    assert entries["optimize.enable_overrides.mirror_short_from_long"]["text"] == "Mirrors bot.short from bot.long."
    assert entries["bot.*.strategy.trailing_martingale.entry.threshold_volatility_1m_weight"]["text"] == "- Original threshold formula."
    assert entries["bot.*.forager.volume_ema_span_1m"]["text"] == "Original span explanation."
    assert entries["bot.*.hsl.tier_ratios.orange"]["text"] == "Original tier explanation."
    assert entries["bot.*.risk.total_exposure_enforcer_policy"]["text"] == "Original portfolio policy."
    assert entries["optimize.pymoo.shared.mutation_prob"]["text"].startswith("- Per-individual")
    assert entries["optimize.pymoo.shared.mutation_prob_per_variable"]["text"].startswith("- Per-variable")
    assert not any("not_a_parameter" in key for key in entries)


def test_document_changes_refresh_cache_and_callers_cannot_mutate_it(documentation) -> None:
    """Documentation updates become visible without a Rust rebuild or stale shared dictionaries."""
    first = help_docs.get_pb8_parameter_help()
    first["entries"].clear()
    assert help_docs.get_pb8_parameter_help()["entries"]
    path = documentation / "configuration.md"
    path.write_text("## Live Trading Settings\n- **risk_input_max_attempts**: Updated original description.\n")
    assert help_docs.get_pb8_parameter_help()["entries"]["live.risk_input_max_attempts"]["text"] == "Updated original description."


def test_doc_symlinks_cannot_escape_the_installation(documentation, tmp_path) -> None:
    """An allowlisted filename must not become a read primitive outside the PB8 root."""
    (documentation / "metrics.md").symlink_to(tmp_path.parent / "outside.md")
    with pytest.raises(ValueError, match="leaves the configured installation"):
        help_docs.get_pb8_parameter_help()


def test_parameter_help_requires_auth_and_does_not_need_rust(documentation) -> None:
    """The same-origin endpoint enforces auth before reading docs and returns safe failures."""
    route = next(route for route in v8_instances.router.routes if route.path == "/parameter-help")
    assert any(dependency.call is v8_instances.require_auth for dependency in route.dependant.dependencies)
    response = v8_instances.get_v8_parameter_help(session=object())
    assert "live.risk_input_max_attempts" in response["entries"]
    (documentation / "configuration.md").unlink()
    with pytest.raises(HTTPException) as error:
        v8_instances.get_v8_parameter_help(session=object())
    assert error.value.status_code == 503
    assert str(documentation) not in error.value.detail
