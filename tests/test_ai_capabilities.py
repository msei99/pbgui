"""Offline contract tests for PBGui AI capabilities and proposals."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
import time

import pytest

import ai_capabilities
from ai_capabilities import AICapabilityError, AICapabilityService, restart_block_reason


def test_tool_catalog_separates_reads_from_proposals(tmp_path: Path) -> None:
    """The initial catalog should expose no direct mutation function."""
    service = AICapabilityService(tmp_path / "capabilities")
    service._runtime_fingerprints = lambda: {
        "v7": {"installed": True},
        "v8": {"installed": True},
    }
    names = {item["function"]["name"] for item in service.chat_completion_tools()}
    response_names = {item["name"] for item in service.responses_tools()}
    message_names = {item["name"] for item in service.messages_tools()}

    assert {
        "get_capability_registry",
        "list_optimizer_configs",
        "get_optimizer_config",
        "get_optimizer_metadata",
        "list_optimizer_runs",
        "list_pb8_optimizer_queue",
        "list_backtests",
        "get_optimizer_run_analysis",
        "rank_optimizer_run_candidates",
        "get_pareto_candidate",
        "select_pareto_candidates",
        "select_backtest_results",
        "perform_page_action",
        "present_user_choices",
        "get_backtest_projection",
        "list_dashboard_templates",
        "get_dashboard_layout",
        "list_config_drafts",
        "get_config_draft",
        "create_config_draft",
        "update_config_draft",
        "propose_pb8_optimizer_config",
        "propose_pb8_config_patch",
        "propose_queue_pb8_config",
        "propose_start_pb8_optimizer_queue",
        "propose_pareto_backtests",
        "propose_dashboard_from_template",
        "propose_dashboard_layout",
        "propose_python_analysis",
        "propose_optimizer_run_python_analysis",
        "propose_workspace_python_analysis",
        "get_python_analysis_result",
        "get_passivbot_installations",
        "read_pbgui_help_topic",
        "search_pbgui_help",
        "search_passivbot_docs",
        "search_passivbot_source",
        "read_passivbot_source",
    } == names
    assert response_names == names
    assert message_names == names
    assert all("parameters" in item for item in service.responses_tools())
    assert all("input_schema" in item for item in service.messages_tools())
    python_tool = next(
        item for item in service.responses_tools() if item["name"] == "propose_python_analysis"
    )
    assert python_tool["parameters"]["properties"]["input_data"]["type"] == "object"
    assert python_tool["parameters"]["properties"]["input_data"] != {}
    assert python_tool["strict"] is False
    assert not any(name.startswith(("save_", "queue_", "delete_", "start_")) for name in names)


def test_passivbot_installations_return_exact_commits_without_local_paths(
    tmp_path: Path, monkeypatch
) -> None:
    """Models should identify installed checkouts without receiving filesystem locations."""
    service = AICapabilityService(tmp_path / "capabilities")

    monkeypatch.setattr(service, "_passivbot_root", lambda version: tmp_path / version)
    monkeypatch.setattr(
        service,
        "_passivbot_git_info",
        lambda root: (
            ("7" if root.name == "v7" else "8") * 40,
            "https://github.com/enarjord/passivbot",
        ),
    )
    monkeypatch.setattr(
        service,
        "_runtime_fingerprint",
        lambda version: {
            "installed": True,
            "commit": ("7" if version == "v7" else "8") * 40,
            "dirty": False,
            "state_digest": "",
        },
    )

    result = service._get_passivbot_installations({})

    assert result["installations"][0] == {
        "version": "v7",
        "installed": True,
        "commit": "7" * 40,
        "dirty": False,
        "runtime_fingerprint": "7" * 40,
        "official_source_url": "https://github.com/enarjord/passivbot/tree/" + "7" * 40,
    }
    assert result["installations"][1]["commit"] == "8" * 40
    assert str(tmp_path) not in str(result)


def test_model_visible_config_removes_secrets_and_host_paths() -> None:
    """Config context must retain strategy data while removing secrets and paths."""
    config = {
        "backtest": {"base_dir": "/private", "start_date": "2024-01-01"},
        "api_key": "secret",
        "bot": {"long": {"risk": {"n_positions": 3}}},
        "pbgui": {"version": 1},
    }

    sanitized = AICapabilityService._sanitize_config(config)

    assert sanitized == {
        "backtest": {"start_date": "2024-01-01"},
        "bot": {"long": {"risk": {"n_positions": 3}}},
    }


@pytest.mark.parametrize("key", ["api_key", "password", "private_key", "token", "base_dir"])
def test_proposal_diff_redacts_sensitive_leaf_values(key: str) -> None:
    """Old and new secret/path values must never enter model or browser proposal context."""
    changes = AICapabilityService._changed_entries({key: "before-secret"}, {key: "after-secret"})

    assert changes == [
        {"path": key, "kind": "changed", "before": "[redacted]", "after": "[redacted]"}
    ]
    assert "secret" not in str(changes)


def test_proposal_diff_omits_unchanged_sensitive_values() -> None:
    """Equal protected values must not appear as meaningless redacted changes."""
    assert AICapabilityService._changed_entries(
        {"base_dir": "/same/path"}, {"base_dir": "/same/path"}
    ) == []


def test_quick_reply_capability_returns_typed_clickable_choices() -> None:
    """Clarifications should use bounded browser actions instead of long free-text questionnaires."""
    result = AICapabilityService._present_user_choices(
        {
            "question": "Which risk profile should I use?",
            "choices": [
                {"label": "Balanced", "value": "Use the balanced preset."},
                {"label": "Stability first", "value": "Use the stability-first preset."},
            ],
        }
    )

    assert result["status"] == "waiting_for_user_choice"
    assert result["ui_action"]["type"] == "chat.quick_replies"
    assert result["ui_action"]["payload"]["choices"][0]["label"] == "Balanced"


def test_backtest_compare_capability_resolves_exact_resources_without_paths(
    tmp_path: Path, monkeypatch
) -> None:
    """Backtest Compare should emit exact safe selectors and never browser-visible host paths."""
    service = AICapabilityService(tmp_path / "capabilities")
    resources = [f"pbgui://backtest/v8/{value * 32}" for value in ("a", "b")]
    items = {
        resources[0]: {
            "path": "/private/result-a",
            "config_name": "grid",
            "result_name": "candidate-a",
            "exchange_dir": "binance",
            "modified": "2026-08-28T10:00:00",
        },
        resources[1]: {
            "path": "/private/result-b",
            "config_name": "martingale",
            "result_name": "candidate-b",
            "exchange_dir": "bybit",
            "modified": "2026-08-28T10:01:00",
        },
    }
    monkeypatch.setattr(service, "_resolve_listed_resource", lambda kind, version, resource: items[resource])

    result = service._select_backtest_results({"version": "v8", "resources": resources})

    assert result["status"] == "queued_for_browser"
    assert result["selected"] == 2
    assert result["ui_action"]["type"] == "backtest.compare_results"
    assert result["ui_action"]["target"] == {"page_key": "v8_backtest", "version": "v8"}
    assert "private" not in json.dumps(result)
    assert result["ui_action"]["payload"]["selectors"][0]["config_name"] == "grid"
    with pytest.raises(AICapabilityError, match="duplicates"):
        service._select_backtest_results({"version": "v8", "resources": [resources[0], resources[0]]})


def test_generic_page_capability_returns_exact_browser_action() -> None:
    """Page actions should transport only an advertised action and exact visible entity."""
    result = AICapabilityService._perform_page_action(
        {
            "page_key": "v8_optimize",
            "action": "show_log",
            "entity_kind": "optimizer_queue_item",
            "entity_name": "optimize_123",
        }
    )

    assert result == {
        "status": "queued_for_browser",
        "action": "show_log",
        "ui_action": {
            "type": "page.perform_action",
            "target": {"page_key": "v8_optimize"},
            "payload": {
                "action": "show_log",
                "entity": {"kind": "optimizer_queue_item", "name": "optimize_123"},
            },
        },
    }
    with pytest.raises(AICapabilityError, match="Invalid page action"):
        AICapabilityService._perform_page_action(
            {
                "page_key": "v8_optimize",
                "action": "show log",
                "entity_kind": "optimizer_queue_item",
                "entity_name": "optimize_123",
            }
        )
    value_result = AICapabilityService._perform_page_action(
        {
            "page_key": "v8_optimize",
            "action": "set_value",
            "entity_kind": "ui_control",
            "entity_name": "control_12",
            "value": "running",
        }
    )
    assert value_result["ui_action"]["payload"]["value"] == "running"
    cross_page = AICapabilityService._perform_page_action(
        {
            "page_key": "v8_backtest",
            "action": "activate_by_label",
            "entity_kind": "ui_control_label",
            "entity_name": "Git Push",
        }
    )
    assert cross_page["ui_action"] == {
        "type": "page.perform_action",
        "target": {"page_key": "v8_backtest"},
        "payload": {
            "action": "activate_by_label",
            "entity": {"kind": "ui_control_label", "name": "Git Push"},
        },
    }


def test_proposal_diff_only_includes_changed_array_entries() -> None:
    """Proposal review should not repeat unchanged list entries."""
    before = {"metrics": ["adg", "sharpe", "drawdown"]}
    after = {"metrics": ["adg", "sharpe", "drawdown", "gain"]}

    assert AICapabilityService._changed_entries(before, after) == [
        {"path": "metrics", "kind": "added", "after": "gain"}
    ]
    assert AICapabilityService._changed_paths(before, after) == ["metrics"]


def test_proposal_diff_recurses_into_changed_array_objects() -> None:
    """Nested object changes in arrays should be shown at the changed leaf."""
    before = {"limits": [{"metric": "drawdown", "value": 0.25}, {"metric": "adg", "value": 0.1}]}
    after = {"limits": [{"metric": "drawdown", "value": 0.2}, {"metric": "adg", "value": 0.1}]}

    assert AICapabilityService._changed_entries(before, after) == [
        {"path": "limits[drawdown].value", "kind": "changed", "before": 0.25, "after": 0.2}
    ]


def test_dashboard_layout_contract_creates_and_edits_semantic_cells(tmp_path: Path, monkeypatch) -> None:
    """AI dashboard operations should map stable cell fields onto validated legacy config keys."""
    from api import dashboards

    service = AICapabilityService(tmp_path / "capabilities")
    existing = {
        "name": "portfolio",
        "rows": 2,
        "cols": 2,
        "dashboard_type_1_1": "ADG",
        "dashboard_adg_users_1_1": ["alice"],
        "dashboard_adg_period_1_1": "ALL_TIME",
        "dashboard_adg_mode_1_1": "line",
    }
    monkeypatch.setattr(dashboards, "list_dashboards", lambda session=None: {"dashboards": ["portfolio"]})
    monkeypatch.setattr(dashboards, "get_dashboard", lambda name, session=None: {"config": copy.deepcopy(existing)})
    monkeypatch.setattr(dashboards, "list_users", lambda session=None: {"users": ["alice", "bob"]})

    name, current, prepared, expected_digest = service._prepare_dashboard_layout(
        {
            "name": "portfolio",
            "create": False,
            "cells": [
                {"row": 1, "column": 1, "users": ["bob"], "period": "LAST_30_DAYS", "mode": "bar"},
                {"row": 1, "column": 2, "type": "BALANCE", "users": ["ALL"]},
            ],
        }
    )

    assert name == "portfolio"
    assert current == existing
    assert expected_digest == service._digest(existing)
    assert prepared["dashboard_adg_users_1_1"] == ["bob"]
    assert prepared["dashboard_adg_period_1_1"] == "LAST_30_DAYS"
    assert prepared["dashboard_adg_mode_1_1"] == "bar"
    assert prepared["dashboard_type_1_2"] == "BALANCE"
    assert prepared["dashboard_balance_users_1_2"] == ["ALL"]


def test_optimizer_run_ranking_scans_every_candidate(tmp_path: Path, monkeypatch) -> None:
    """Complete-run ranking must evaluate candidates beyond the old 200-row preview limit."""
    from api import optimize_v8

    service = AICapabilityService(tmp_path / "capabilities")
    run_resource = service._virtual_uri("optimizer-run", "v8", "managed/run")
    monkeypatch.setattr(
        service,
        "_resolve_listed_resource",
        lambda kind, version, resource: {"path": "managed/run", "name": "full-run"},
    )
    monkeypatch.setattr(
        optimize_v8,
        "list_paretos",
        lambda *args, **kwargs: {
            "paretos": [
                {"path": f"managed/run/pareto/{index}.json", "name": f"candidate-{index}", "summary": {"gain": index, "drawdown": index / 1000}}
                for index in range(759)
            ]
        },
    )

    ranked = service._rank_optimizer_run_candidates(
        {
            "version": "v8",
            "resource": run_resource,
            "criteria": [
                {"metric": "gain", "direction": "max", "weight": 2},
                {"metric": "drawdown", "direction": "min", "weight": 1},
            ],
            "limit": 3,
        }
    )

    assert ranked["scanned"] == 759
    assert ranked["eligible"] == 759
    assert ranked["complete_scan"] is True
    assert len(ranked["ranked"]) == 3

    relaxed = service._rank_optimizer_run_candidates(
        {
            "version": "v8",
            "resource": run_resource,
            "criteria": [{"metric": "gain", "direction": "max", "minimum": 10_000}],
            "limit": 3,
        }
    )
    missing = service._rank_optimizer_run_candidates(
        {
            "version": "v8",
            "resource": run_resource,
            "criteria": [{"metric": "not_a_metric", "direction": "max"}],
            "limit": 3,
        }
    )

    assert relaxed["eligible"] == 0
    assert relaxed["thresholds_relaxed"] is True
    assert relaxed["required_user_clarification"] is True
    assert relaxed["ranked"] == []
    assert len(relaxed["relaxed_suggestions"]) == 3
    assert missing["ranked"] == []
    assert missing["diagnostics"]["required_next_tool"] == "propose_optimizer_run_python_analysis"


def test_proposal_diff_matches_reordered_array_objects_by_identity() -> None:
    """Scoring changes should describe metrics instead of unrelated array indexes."""
    before = {"scoring": [{"metric": "adg", "goal": "max"}, {"metric": "drawdown", "goal": "max"}]}
    after = {"scoring": [{"metric": "drawdown", "goal": "min"}, {"metric": "sortino", "goal": "max"}]}

    assert AICapabilityService._changed_entries(before, after) == [
        {
            "path": "scoring",
            "kind": "removed",
            "item": "adg",
            "before": {"metric": "adg", "goal": "max"},
        },
        {
            "path": "scoring[drawdown].goal",
            "kind": "changed",
            "before": "max",
            "after": "min",
        },
        {
            "path": "scoring",
            "kind": "added",
            "item": "sortino",
            "after": {"metric": "sortino", "goal": "max"},
        },
    ]


def test_queue_preview_binds_runtime_and_override_digests() -> None:
    """Queue approval should expose launch semantics and every sparse override digest."""
    preview = AICapabilityService._queue_preview(
        {
            "backtest": {"exchanges": ["bybit"]},
            "live": {"strategy_kind": "trailing_grid_v7"},
            "optimize": {"backend": "pymoo", "n_cpus": 4, "scoring": ["adg"]},
        },
        {"BTC.json": {"bot": {"long": {"n_positions": 2}}}},
    )

    assert preview["runtime"] == {
        "mode": "fresh",
        "source_name": "",
        "fine_tune": None,
        "polish": None,
        "backend": "pymoo",
        "n_cpus": 4,
        "exchanges": ["bybit"],
        "strategy": "trailing_grid_v7",
        "scoring": ["adg"],
    }
    assert preview["override_files"][0]["name"] == "BTC.json"
    assert preview["override_files"][0]["digest"].startswith("sha256:")


def test_proposal_requires_owner_and_explicit_approval(tmp_path: Path, monkeypatch) -> None:
    """Creating a proposal must not execute it and only its owner may approve it."""
    async def scenario() -> None:
        service = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(service, "_current_pb8_bundle", lambda name: (None, {}, None))
        executed = []
        monkeypatch.setattr(
            service,
            "_execute_proposal",
            lambda proposal: executed.append(proposal.id)
            or {"proposal_id": proposal.id, "status": "executed", "action": proposal.action},
        )

        created = await service._create_proposal(
            "a" * 32,
            "c" * 32,
            "save",
            "ai_test",
            {"config_version": "v8.0.0"},
        )

        assert executed == []
        with pytest.raises(AICapabilityError, match="not found"):
            await service.approve(
                "b" * 32,
                created["proposal_id"],
                created["payload_digest"] if "payload_digest" in created else "",
                "c" * 32,
            )
        proposal = service.proposals[created["proposal_id"]]
        result = await service.approve(
            "a" * 32,
            created["proposal_id"],
            proposal.payload_digest,
            "c" * 32,
        )
        assert result["status"] == "executed"
        assert executed == [created["proposal_id"]]
        assert (
            await service.approve(
                "a" * 32,
                created["proposal_id"],
                proposal.payload_digest,
                "c" * 32,
            )
            == result
        )

    asyncio.run(scenario())


def test_recent_auto_expired_proposal_revives_after_restart(tmp_path: Path, monkeypatch) -> None:
    """Restart should restore recent auto-expired reviews but never explicit terminal decisions."""
    async def scenario() -> None:
        owner = "a" * 32
        first = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(first, "_current_pb8_bundle", lambda name: (None, {}, None))
        proposal = await first._create_proposal(owner, "b" * 32, "save", "demo", {"optimize": {}})
        selected = first.proposals[proposal["proposal_id"]]
        selected.status = "expired"
        selected.created_at = time.time() - 3600
        first._persist_proposal(selected)

        second = AICapabilityService(tmp_path / "capabilities")
        pending = await second.list_proposals(owner, "b" * 32)

        assert len(pending) == 1
        assert pending[0]["status"] == "awaiting_approval"
        await first.shutdown()
        await second.shutdown()

    asyncio.run(scenario())


def test_pending_proposal_migrates_old_redacted_path_side_effects(tmp_path: Path, monkeypatch) -> None:
    """Restart loading should repair protected path normalization in still-current proposals."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "b" * 32
        current = {
            "backtest": {"base_dir": "original", "ohlcv_source_dir": "ohlcv"},
            "optimize": {"x": 0},
        }
        first = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(
            first,
            "_current_pb8_bundle",
            lambda name: (copy.deepcopy(current), {}, "sha256:current"),
        )
        await first._create_proposal(
            owner,
            conversation,
            "save",
            "demo",
            {
                "backtest": {"base_dir": "/normalized", "ohlcv_source_dir": "/normalized-ohlcv"},
                "optimize": {"x": 1},
            },
        )

        second = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(
            second,
            "_current_pb8_bundle",
            lambda name: (copy.deepcopy(current), {}, "sha256:current"),
        )
        pending = await second.list_proposals(owner, conversation)

        assert [item["path"] for item in pending[0]["preview"]["changes"]] == ["optimize.x"]
        loaded = second.proposals[pending[0]["proposal_id"]]
        assert loaded.config["backtest"] == current["backtest"]
        await first.shutdown()
        await second.shutdown()

    asyncio.run(scenario())


def test_dispatch_rejects_unknown_and_oversized_calls(tmp_path: Path) -> None:
    """Model-supplied capability names and payload sizes must fail closed."""
    async def scenario() -> None:
        service = AICapabilityService(tmp_path / "capabilities")
        with pytest.raises(AICapabilityError, match="Unknown"):
            await service.dispatch("a" * 32, "c" * 32, "run_shell", {})
        with pytest.raises(AICapabilityError, match="too large"):
            await service.dispatch(
                "a" * 32,
                "c" * 32,
                "list_optimizer_configs",
                {"padding": "x" * (2 * 1024 * 1024 + 1)},
            )

    asyncio.run(scenario())


def test_save_and_queue_proposal_is_new_config_only(tmp_path: Path, monkeypatch) -> None:
    """The compensating MVP save-and-queue path must not overwrite existing configs."""
    async def scenario() -> None:
        service = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(
            service,
            "_current_pb8_bundle",
            lambda name: ({"config_version": "v8.0.0"}, {}, "sha256:current"),
        )

        with pytest.raises(AICapabilityError, match="limited to a new"):
            await service._create_proposal(
                "a" * 32,
                "c" * 32,
                "save_and_queue",
                "existing",
                {"config_version": "v8.0.0"},
            )

    asyncio.run(scenario())


def test_pb8_queue_start_is_exact_approval_gated_and_idempotent(
    tmp_path: Path, monkeypatch
) -> None:
    """Exact queued IDs should start only after approval and not restart on replay."""
    from api import optimize_v8

    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        now = time.time()
        items = [
            {
                "filename": "1" * 36,
                "name": "martingale_compare",
                "status": "queued",
                "exchange": ["binance", "bybit"],
                "created": "2026-08-27T19:00:00",
                "started_at": None,
            },
            {
                "filename": "2" * 36,
                "name": "grid_compare",
                "status": "queued",
                "exchange": ["binance", "bybit"],
                "created": "2026-08-27T19:01:00",
                "started_at": None,
            },
        ]
        starts = []

        monkeypatch.setattr(optimize_v8, "get_queue", lambda session: {"items": copy.deepcopy(items)})
        monkeypatch.setattr(ai_capabilities, "load_ini_section", lambda section: {"autostart": "False"})

        def start_queue_item(filename, body, session):
            starts.append(filename)
            item = next(item for item in items if item["filename"] == filename)
            item["status"] = "running"
            item["started_at"] = now + 1
            return {"ok": True, "pid": 1000 + len(starts)}

        monkeypatch.setattr(optimize_v8, "start_queue_item", start_queue_item)
        service = AICapabilityService(tmp_path / "capabilities")

        listed = service._list_pb8_optimizer_queue({"limit": 10})
        created = await service._propose_start_pb8_optimizer_queue(
            owner,
            conversation,
            {"queue_ids": [items[0]["filename"], items[1]["filename"]]},
        )

        assert listed["autostart"] is False
        assert [item["queue_id"] for item in listed["items"]] == [items[0]["filename"], items[1]["filename"]]
        assert created["preview"]["job_count"] == 2
        assert starts == []
        proposal = service.proposals[created["proposal_id"]]
        result = await service.approve(owner, proposal.id, proposal.payload_digest, conversation)
        replay = await service.approve(owner, proposal.id, proposal.payload_digest, conversation)

        assert starts == [items[0]["filename"], items[1]["filename"]]
        assert result["action"] == "start_optimize_queue"
        assert result["started_count"] == 2
        assert replay == result
        await service.shutdown()

    asyncio.run(scenario())


def test_pb8_queue_start_revalidates_status_before_any_launch(
    tmp_path: Path, monkeypatch
) -> None:
    """A reviewed queue batch must fail before launching if any exact item changed."""
    from api import optimize_v8

    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        items = [
            {"filename": "1" * 36, "name": "first", "status": "queued", "started_at": None},
            {"filename": "2" * 36, "name": "second", "status": "queued", "started_at": None},
        ]
        starts = []
        monkeypatch.setattr(optimize_v8, "get_queue", lambda session: {"items": copy.deepcopy(items)})
        monkeypatch.setattr(optimize_v8, "start_queue_item", lambda filename, body, session: starts.append(filename))
        service = AICapabilityService(tmp_path / "capabilities")
        created = await service._propose_start_pb8_optimizer_queue(
            owner,
            conversation,
            {"queue_ids": [items[0]["filename"], items[1]["filename"]]},
        )
        items[1]["status"] = "error"
        items[1]["started_at"] = time.time() + 1
        proposal = service.proposals[created["proposal_id"]]

        with pytest.raises(AICapabilityError, match="queue changed"):
            await service.approve(owner, proposal.id, proposal.payload_digest, conversation)

        assert starts == []
        await service.shutdown()

    asyncio.run(scenario())


def test_passivbot_source_search_and_read_are_root_confined(tmp_path: Path, monkeypatch) -> None:
    """Source tools should return bounded exact-version excerpts and skip runtime data."""
    root = tmp_path / "passivbot"
    (root / "src").mkdir(parents=True)
    (root / "docs").mkdir()
    (root / "data").mkdir()
    (root / "passivbot-rust" / "target" / "generated").mkdir(parents=True)
    (root / "src" / "strategy.py").write_text(
        "def calculate_entry():\n    return 'needle'\n", encoding="utf-8"
    )
    (root / "docs" / "optimizer.md").write_text(
        "# Optimizer\nThe needle is documented here.\n", encoding="utf-8"
    )
    (root / "data" / "secret.py").write_text("needle = 'secret'\n", encoding="utf-8")
    (root / "passivbot-rust" / "target" / "generated" / "ignored.py").write_text(
        "needle = 'build artifact'\n", encoding="utf-8"
    )
    service = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(service, "_passivbot_root", lambda version: root)
    monkeypatch.setattr(
        service,
        "_passivbot_git_info",
        lambda selected: ("a" * 40, "https://github.com/enarjord/passivbot"),
    )
    monkeypatch.setattr(service, "_checkout_is_clean", lambda selected: True)
    monkeypatch.setattr(service, "_source_is_clean", lambda selected, relative: True)
    walked = []
    real_walk = ai_capabilities.os.walk

    def tracked_walk(*args, **kwargs):
        for current, directories, filenames in real_walk(*args, **kwargs):
            walked.append(Path(current).relative_to(root).as_posix())
            yield current, directories, filenames

    monkeypatch.setattr(ai_capabilities.os, "walk", tracked_walk)

    source = service._search_passivbot_source({"version": "v8", "query": "needle"})
    docs = service._search_passivbot_docs({"version": "v8", "query": "needle"})
    content = service._read_passivbot_source(
        {"version": "v8", "path": "src/strategy.py", "start_line": 1, "end_line": 2}
    )

    assert [match["path"] for match in source["matches"]] == [
        "docs/optimizer.md",
        "src/strategy.py",
    ]
    assert [match["path"] for match in docs["matches"]] == ["docs/optimizer.md"]
    assert "data/secret.py" not in str(source)
    assert "passivbot-rust/target" not in walked
    assert content["content"] == "1: def calculate_entry():\n2:     return 'needle'"
    assert content["source_url"].endswith("src/strategy.py#L1-L2")


@pytest.mark.parametrize(
    "path",
    ["../secret.py", "/tmp/secret.py", ".git/config", "data/secret.py", "src/link.py"],
)
def test_passivbot_source_read_rejects_unsafe_paths(
    tmp_path: Path,
    monkeypatch,
    path: str,
) -> None:
    """Model-supplied source paths must not traverse, enter data, or follow symlinks."""
    root = tmp_path / "passivbot"
    (root / "src").mkdir(parents=True)
    target = root / "outside.py"
    target.write_text("secret = True\n", encoding="utf-8")
    if hasattr(Path, "symlink_to"):
        try:
            (root / "src" / "link.py").symlink_to(target)
        except OSError:
            pass
    service = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(service, "_passivbot_root", lambda version: root)

    with pytest.raises(AICapabilityError):
        service._read_passivbot_source({"version": "v8", "path": path})


def test_registry_exposes_effects_resources_fingerprints_and_global_limits(
    tmp_path: Path, monkeypatch
) -> None:
    """Discovery must describe enforced effects and path-free runtime identities."""
    service = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(
        service,
        "_runtime_fingerprints",
        lambda: {
            "v7": {"installed": True, "commit": "7" * 40, "dirty": False},
            "v8": {"installed": True, "commit": "8" * 40, "dirty": True},
        },
    )

    registry = service.capability_registry()
    effects = {item["name"]: item["effect"] for item in registry["capabilities"]}

    assert effects["get_backtest_projection"] == "analyze"
    assert effects["create_config_draft"] == "draft"
    assert effects["propose_pb8_optimizer_config"] == "write"
    assert effects["propose_pb8_config_patch"] == "write"
    assert effects["propose_queue_pb8_config"] == "execute"
    assert effects["propose_start_pb8_optimizer_queue"] == "execute"
    assert effects["select_pareto_candidates"] == "ui"
    assert effects["select_backtest_results"] == "ui"
    assert effects["present_user_choices"] == "ui"
    assert effects["propose_pareto_backtests"] == "execute"
    assert effects["propose_dashboard_from_template"] == "write"
    assert effects["propose_dashboard_layout"] == "write"
    assert effects["propose_python_analysis"] == "execute"
    assert effects["rank_optimizer_run_candidates"] == "analyze"
    assert effects["propose_optimizer_run_python_analysis"] == "execute"
    assert effects["propose_workspace_python_analysis"] == "execute"
    assert effects["get_python_analysis_result"] == "analyze"
    assert registry["runtime_fingerprints"]["v8"]["dirty"] is True
    assert registry["limits"]["proposals_global"] == 200
    assert all(str(resource).startswith("pbgui://") for resource in registry["virtual_resources"])


def test_pb8_json_patch_supports_open_bounded_config_edits() -> None:
    """Existing configs should be patchable without regenerating the complete document."""
    config = {
        "optimize": {
            "scoring": [{"metric": "volume_pct_per_day_avg", "goal": "max"}],
            "limits": [{"metric": "drawdown_worst_strategy_eq", "value": 0.25}],
        }
    }

    AICapabilityService._apply_json_patch_operation(
        config,
        {"op": "replace", "path": "/optimize/scoring", "value": [{"metric": "adg_strategy_eq_w", "goal": "max"}]},
    )
    AICapabilityService._apply_json_patch_operation(
        config,
        {"op": "replace", "path": "/optimize/limits/0/value", "value": 0.2},
    )

    assert config["optimize"]["scoring"] == [{"metric": "adg_strategy_eq_w", "goal": "max"}]
    assert config["optimize"]["limits"][0]["value"] == 0.2
    with pytest.raises(AICapabilityError, match="Unsafe JSON Patch path"):
        AICapabilityService._apply_json_patch_operation(
            config, {"op": "replace", "path": "/api_key", "value": "secret"}
        )


def test_patch_validation_preserves_existing_protected_paths() -> None:
    """Runtime normalization must not introduce unrelated redacted path changes."""
    original = {
        "backtest": {"base_dir": "original-base", "ohlcv_source_dir": "original-ohlcv"},
        "optimize": {"limits": [{"value": 0.25}]},
    }
    prepared = {
        "backtest": {
            "base_dir": "/normalized/base",
            "ohlcv_source_dir": "/normalized/ohlcv",
            "checkpoint_path": "/new/path",
        },
        "optimize": {"limits": [{"value": 0.2}]},
    }

    AICapabilityService._preserve_protected_config_fields(original, prepared)

    assert prepared["backtest"] == {
        "base_dir": "original-base",
        "ohlcv_source_dir": "original-ohlcv",
    }
    assert prepared["optimize"]["limits"][0]["value"] == 0.2


def test_full_pb8_proposal_preserves_existing_protected_paths(tmp_path: Path, monkeypatch) -> None:
    """Full-config proposal tools must preserve protected fields just like patch proposals."""
    async def scenario() -> None:
        service = AICapabilityService(tmp_path / "capabilities")
        current = {"backtest": {"base_dir": "original", "ohlcv_source_dir": "ohlcv"}, "optimize": {}}
        prepared = {"backtest": {"base_dir": "/normalized", "ohlcv_source_dir": "/new"}, "optimize": {"x": 1}}
        monkeypatch.setattr(service, "_validate_pb8_config", lambda name, config: copy.deepcopy(prepared))
        monkeypatch.setattr(service, "_current_pb8_bundle", lambda name: (copy.deepcopy(current), {}, "sha256:current"))
        captured = {}

        async def capture(owner, conversation_id, action, name, config, **kwargs):
            captured["config"] = config
            return {"proposal_id": "a" * 32}

        monkeypatch.setattr(service, "_create_proposal", capture)
        await service._propose_pb8_optimizer_config(
            "b" * 32,
            "c" * 32,
            {"name": "demo", "config": {"optimize": {"x": 1}}, "action": "save"},
        )

        assert captured["config"]["backtest"] == current["backtest"]
        await service.shutdown()

    asyncio.run(scenario())


def test_owner_bound_drafts_persist_revise_validate_and_hide_paths(
    tmp_path: Path, monkeypatch
) -> None:
    """Drafts should survive service recreation and reject stale iterative updates."""
    owner = "a" * 32
    first = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(
        first,
        "_validate_pb8_config",
        lambda name, config: {**config, "backtest": {"base_dir": "/private/runtime"}},
    )
    monkeypatch.setattr(first, "_runtime_fingerprint", lambda version: {"commit": "8" * 40})

    created = first._create_config_draft(owner, {"version": "v8", "config": {"bot": {}}})
    draft_id = created["draft_id"]

    assert created["validation"]["valid"] is True
    assert "private" not in str(created)
    second = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(second, "_validate_pb8_config", lambda name, config: config)
    monkeypatch.setattr(second, "_runtime_fingerprint", lambda version: {"commit": "8" * 40})
    loaded = second._get_config_draft(owner, {"draft_id": draft_id})
    revised = second._update_config_draft(
        owner,
        {"draft_id": draft_id, "expected_revision": 1, "config": {"bot": {"long": {}}}},
    )

    assert loaded["revision"] == 1
    assert revised["revision"] == 2
    with pytest.raises(AICapabilityError, match="revision changed"):
        second._update_config_draft(
            owner,
            {"draft_id": draft_id, "expected_revision": 1, "config": {"bot": {}}},
        )
    with pytest.raises(AICapabilityError, match="secrets"):
        second._create_config_draft(owner, {"version": "v8", "config": {"api_key": "secret"}})


def test_proposals_and_terminal_history_survive_service_recreation(
    tmp_path: Path, monkeypatch
) -> None:
    """Pending approvals and their final audit records must be durable and owner-bound."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        first = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(first, "_current_pb8_bundle", lambda name: (None, {}, None))
        created = await first._create_proposal(
            owner, conversation, "save", "durable", {"config_version": "v8.0.0"}
        )

        second = AICapabilityService(tmp_path / "capabilities")
        pending = await second.list_proposals(owner)
        assert pending[0]["proposal_id"] == created["proposal_id"]
        result = await second.reject(
            owner,
            created["proposal_id"],
            pending[0]["payload_digest"],
            conversation,
        )
        history = await AICapabilityService(
            tmp_path / "capabilities"
        ).list_action_history(owner)

        assert result["status"] == "rejected"
        assert history[0]["proposal_id"] == created["proposal_id"]
        assert history[0]["status"] == "rejected"

    asyncio.run(scenario())


def test_restart_blocker_tracks_only_incomplete_durable_action_journals(tmp_path: Path) -> None:
    """An approved staged PB8 action must block restart until its journal completes."""
    root = tmp_path / "capabilities"
    journal = root / "journal"
    journal.mkdir(parents=True)
    path = journal / f"{'a' * 32}.json"
    path.write_text(json.dumps({"phase": "config_saved"}), encoding="utf-8")

    assert "require durable recovery" in restart_block_reason(root)
    path.write_text(json.dumps({"phase": "completed"}), encoding="utf-8")
    assert restart_block_reason(root) == ""


def test_dirty_source_file_never_receives_a_commit_citation(tmp_path: Path, monkeypatch) -> None:
    """A modified checkout file cannot be cited as if its contents came from HEAD."""
    root = tmp_path / "passivbot"
    (root / "src").mkdir(parents=True)
    (root / "src" / "strategy.py").write_text("needle = True\n", encoding="utf-8")
    service = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(service, "_passivbot_root", lambda version: root)
    monkeypatch.setattr(
        service,
        "_passivbot_git_info",
        lambda selected: ("a" * 40, "https://github.com/enarjord/passivbot"),
    )
    monkeypatch.setattr(service, "_checkout_is_clean", lambda selected: False)
    source_checks = []
    monkeypatch.setattr(
        service,
        "_source_is_clean",
        lambda selected, relative: source_checks.append(relative) or False,
    )

    searched = service._search_passivbot_source({"version": "v8", "query": "needle"})
    assert source_checks == []
    read = service._read_passivbot_source({"version": "v8", "path": "src/strategy.py"})

    assert searched["source_state"] == "dirty"
    assert searched["matches"][0]["source_url"] == ""
    assert read["matches_runtime"] is False
    assert read["source_url"] == ""
    assert source_checks == [Path("src/strategy.py")]


def test_passivbot_source_search_has_cooperative_total_deadline(tmp_path: Path, monkeypatch) -> None:
    """A large source tree must return a truncated result once its total budget expires."""
    root = tmp_path / "passivbot"
    (root / "src").mkdir(parents=True)
    (root / "src" / "strategy.py").write_text("needle = True\n", encoding="utf-8")
    service = AICapabilityService(tmp_path / "capabilities")
    monkeypatch.setattr(service, "_passivbot_root", lambda version: root)
    monkeypatch.setattr(service, "_passivbot_git_info", lambda selected: ("a" * 40, ""))
    monkeypatch.setattr(service, "_checkout_is_clean", lambda selected: True)
    moments = iter((0.0, 16.0))
    monkeypatch.setattr(ai_capabilities.time, "monotonic", lambda: next(moments, 16.0))

    result = service._search_passivbot_source({"version": "v8", "query": "needle"})

    assert result["matches"] == []
    assert result["truncated"] is True


def test_backtest_csv_projection_is_bounded_and_path_free(tmp_path: Path) -> None:
    """Equity and fill projections should retain useful fields without artifact locations."""
    result = tmp_path / "result"
    result.mkdir()
    (result / "balance_and_equity.csv").write_text(
        "minute,usd_total_balance,usd_total_equity,private_path\n"
        + "\n".join(f"{index},{100 + index},{99 + index},/secret/{index}" for index in range(20))
        + "\n",
        encoding="utf-8",
    )

    projected = AICapabilityService._csv_projection(
        result,
        "balance_and_equity.csv",
        max_rows=5,
        preferred=("minute", "usd_total_balance", "usd_total_equity"),
    )

    assert len(projected["rows"]) == 5
    assert projected["scanned"] == 20
    assert projected["truncated"] is True
    assert "/secret" not in str(projected)


@pytest.mark.skipif(
    not Path("/usr/bin/bwrap").is_file() or not Path("/usr/bin/prlimit").is_file(),
    reason="Bubblewrap analysis runtime is unavailable",
)
def test_python_analysis_requires_exact_approval_and_runs_isolated(
    tmp_path: Path,
) -> None:
    """Approved code should receive only sanitized JSON in the bounded sandbox."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        service = AICapabilityService(tmp_path / "capabilities")
        code = """import json
import os
import socket
import sys
import numpy as np
import pandas as pd

data = json.load(sys.stdin)
try:
    socket.create_connection((\"1.1.1.1\", 53), timeout=0.1)
    network = True
except OSError:
    network = False
print(json.dumps({
    \"sum\": float(np.asarray(data[\"values\"]).sum()),
    \"mean\": float(pd.Series(data[\"values\"]).mean()),
    \"cwd\": os.getcwd(),
    \"host_home_visible\": os.path.exists(\"/home/mani\"),
    \"network\": network,
    \"keys\": sorted(data),
}))
"""
        created = await service._propose_python_analysis(
            owner,
            conversation,
            {
                "code": code,
                "input_data": {
                    "values": [1, 2, 3],
                    "api_key": "must-not-enter-proposal",
                    "base_dir": "/host/data",
                },
            },
        )
        pending = (await service.list_proposals(owner, conversation))[0]

        assert pending["preview"]["code"] == code
        assert pending["preview"]["input_data"] == {"values": [1, 2, 3]}
        assert pending["preview"]["input_summary"] == {
            "type": "object",
            "bytes": len(b'{"values":[1,2,3]}'),
            "keys": ["values"],
        }
        with pytest.raises(AICapabilityError, match="payload changed"):
            await service.approve(owner, created["proposal_id"], "sha256:" + "0" * 64, conversation)

        result = await service.approve(
            owner,
            created["proposal_id"],
            pending["payload_digest"],
            conversation,
        )

        assert result["analysis_status"] == "completed"
        assert result["output"]["format"] == "json"
        assert result["output"]["value"] == {
            "sum": 6.0,
            "mean": 2.0,
            "cwd": "/work",
            "host_home_visible": False,
            "network": False,
            "keys": ["values"],
        }
        assert not list((tmp_path / "capabilities" / "journal").glob("*.json"))
        assert restart_block_reason(tmp_path / "capabilities") == ""
        assert (await service._get_python_analysis_result(
            owner, conversation, {"proposal_id": created["proposal_id"]}
        )) == result
        history = await service.list_action_history(owner)
        assert history[0]["result"] == result

    asyncio.run(scenario())


def test_optimizer_run_python_analysis_binds_complete_resource_without_previewing_all_rows(
    tmp_path: Path, monkeypatch
) -> None:
    """Complete Pareto data should reach sandbox stdin without bloating model/browser previews."""
    async def scenario() -> None:
        from api import optimize_v8

        service = AICapabilityService(tmp_path / "capabilities")
        run_path = "managed/run"
        run_resource = service._virtual_uri("optimizer-run", "v8", run_path)
        monkeypatch.setattr(
            service,
            "_resolve_listed_resource",
            lambda kind, version, resource: {"path": run_path, "name": "full-run"},
        )
        monkeypatch.setattr(
            optimize_v8,
            "list_paretos",
            lambda *args, **kwargs: {
                "paretos": [
                    {"path": f"managed/run/pareto/{index}.json", "name": f"candidate-{index}", "summary": {"gain": index / 10, "drawdown": index / 100}}
                    for index in range(759)
                ]
            },
        )

        created = await service._propose_optimizer_run_python_analysis(
            "a" * 32,
            "c" * 32,
            {
                "version": "v8",
                "run_resource": run_resource,
                "code": "import json,sys\ndata=json.load(sys.stdin)\nprint(len(data['candidates']))",
            },
        )
        proposal = service.proposals[created["proposal_id"]]

        assert created["preview"]["input_resource"]["candidate_count"] == 759
        assert "input_data" not in created["preview"]
        assert len(proposal.config["input_data"]["candidates"]) == 759
        assert proposal.preview["input_resource"]["digest"].startswith("sha256:")

    asyncio.run(scenario())


def test_workspace_python_mounts_logs_and_masks_sensitive_paths(tmp_path: Path, monkeypatch) -> None:
    """Approved data mounts keep normal logs readable while masking credentials and symlinks."""
    data_root = tmp_path / "data"
    logs = data_root / "logs"
    credentials = data_root / "credentials"
    logs.mkdir(parents=True)
    credentials.mkdir()
    (logs / "PBGui.log").write_text("safe diagnostic", encoding="utf-8")
    (logs / "session_token.log").write_text("must be masked", encoding="utf-8")
    (credentials / "store.json").write_text("{}", encoding="utf-8")
    (data_root / "linked").symlink_to(logs, target_is_directory=True)
    monkeypatch.setattr(ai_capabilities, "PBGDIR", tmp_path)

    mounts = AICapabilityService._python_workspace_mounts(["pbgui_data"])
    rendered = "\n".join(mounts)

    assert str(data_root) in mounts
    assert "/workspace/pbgui_data" in mounts
    assert "/workspace/pbgui_data/logs/PBGui.log" not in rendered
    assert "/workspace/pbgui_data/logs/session_token.log" in rendered
    assert "/workspace/pbgui_data/credentials" in rendered
    assert "/workspace/pbgui_data/linked" in rendered


def test_python_analysis_fails_closed_without_bubblewrap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Approval must never fall back to direct Python when sandboxing is unavailable."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        service = AICapabilityService(tmp_path / "capabilities")
        monkeypatch.setattr(ai_capabilities, "_BWRAP_PATH", tmp_path / "missing-bwrap")
        created = await service._propose_python_analysis(
            owner,
            conversation,
            {"code": "print('unsafe fallback')", "input_data": {}},
        )
        pending = (await service.list_proposals(owner, conversation))[0]

        with pytest.raises(AICapabilityError, match="sandbox is unavailable"):
            await service.approve(
                owner,
                created["proposal_id"],
                pending["payload_digest"],
                conversation,
            )

        assert service.proposals[created["proposal_id"]].status == "failed"
        assert restart_block_reason(tmp_path / "capabilities") == ""

    asyncio.run(scenario())


def test_python_analysis_diagnostics_redact_host_paths(tmp_path: Path) -> None:
    """Sandbox diagnostics returned to a model or browser must not expose host paths."""
    service = AICapabilityService(tmp_path / "capabilities")
    script = tmp_path / "capabilities" / "analysis" / "run-private" / "analysis.py"
    diagnostic = f"{script}: failed in {Path.home()} and {Path(ai_capabilities.PBGDIR)}"

    redacted = service._redact_analysis_stderr(
        diagnostic,
        script,
        Path(ai_capabilities.sys.prefix).resolve(),
    )

    assert str(tmp_path) not in redacted
    assert str(Path.home()) not in redacted
    assert str(Path(ai_capabilities.PBGDIR)) not in redacted
    assert "/analysis.py" in redacted


def test_startup_records_interrupted_python_analysis_without_restart_blocking(
    tmp_path: Path,
) -> None:
    """An abruptly stopped analysis should become terminal and must never replay."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        root = tmp_path / "capabilities"
        first = AICapabilityService(root)
        created = await first._propose_python_analysis(
            owner,
            conversation,
            {"code": "print(1)", "input_data": {}},
        )
        path = first._owner_path(first.proposal_root, owner, created["proposal_id"])
        payload = first._read_private_json(path, first.proposal_root)
        payload["status"] = "executing"
        first._write_private_json(path, payload)

        second = AICapabilityService(root)
        await second.startup()
        loaded = await second._get_python_analysis_result(
            owner,
            conversation,
            {"proposal_id": created["proposal_id"]},
        )
        history = await second.list_action_history(owner)

        assert loaded["status"] == "interrupted"
        assert history[0]["status"] == "interrupted"
        assert restart_block_reason(root) == ""
        assert not list((root / "journal").glob("*.json"))

    asyncio.run(scenario())


@pytest.mark.skipif(
    not Path("/usr/bin/bwrap").is_file() or not Path("/usr/bin/prlimit").is_file(),
    reason="Bubblewrap analysis runtime is unavailable",
)
def test_python_analysis_is_cancelled_and_awaited_on_shutdown(tmp_path: Path) -> None:
    """Capability shutdown should kill, reap, and durably cancel sandbox analysis."""
    async def scenario() -> None:
        owner = "a" * 32
        conversation = "c" * 32
        service = AICapabilityService(tmp_path / "capabilities")
        created = await service._propose_python_analysis(
            owner,
            conversation,
            {"code": "import time\ntime.sleep(30)", "input_data": {}},
        )
        pending = (await service.list_proposals(owner, conversation))[0]
        approval = asyncio.create_task(
            service.approve(owner, created["proposal_id"], pending["payload_digest"], conversation)
        )
        for _ in range(100):
            if service.analysis_tasks:
                break
            await asyncio.sleep(0.01)
        assert service.analysis_tasks

        await service.shutdown()
        with pytest.raises(asyncio.CancelledError):
            await approval
        assert service.proposals[created["proposal_id"]].status == "cancelled"
        assert not service.execution_tasks
        assert not service.analysis_tasks
        assert restart_block_reason(tmp_path / "capabilities") == ""

    asyncio.run(scenario())
