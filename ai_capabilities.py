"""PBGui AI capabilities and approval-gated PB8 optimizer actions."""

from __future__ import annotations

import asyncio
import copy
import csv
from dataclasses import dataclass, field
import gzip
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import psutil
from fastapi import HTTPException

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, load_ini_section
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow


SERVICE = "AIChat"

_MAX_TOOL_RESULT_BYTES = 512 * 1024
_MAX_CONFIG_BYTES = 2 * 1024 * 1024
_PROPOSAL_TTL_SECONDS = 7 * 24 * 60 * 60
_MAX_PROPOSALS_PER_OWNER = 20
_MAX_PROPOSALS_GLOBAL = 200
_MAX_DRAFTS_PER_OWNER = 20
_MAX_DRAFTS_GLOBAL = 200
_MAX_ACTION_HISTORY_PER_OWNER = 200
_MAX_ACTIVE_APPROVALS = 2
_MAX_PROJECTION_ROWS = 200_000
_ACTION_RETENTION_SECONDS = 30 * 24 * 60 * 60
_MAX_SOURCE_FILE_BYTES = 1024 * 1024
_MAX_SOURCE_SCAN_BYTES = 32 * 1024 * 1024
_MAX_SOURCE_FILES = 5000
_SOURCE_SEARCH_TIMEOUT_SECONDS = 5
_MAX_ANALYSIS_CODE_BYTES = 32 * 1024
_MAX_ANALYSIS_INPUT_BYTES = 1024 * 1024
_MAX_ANALYSIS_STDOUT_BYTES = 128 * 1024
_MAX_ANALYSIS_STDERR_BYTES = 32 * 1024
_ANALYSIS_TIMEOUT_SECONDS = 15
_BWRAP_PATH = Path("/usr/bin/bwrap")
_PRLIMIT_PATH = Path("/usr/bin/prlimit")
_SOURCE_EXTENSIONS = {".py", ".rs", ".toml", ".md"}
_SOURCE_EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "backtests",
    "caches",
    "data",
    "logs",
    "node_modules",
    "target",
    "venv",
}
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "password",
    "private_key",
    "secret",
    "token",
)
_PATH_KEYS = {
    "base_config_path",
    "base_dir",
    "checkpoint_path",
    "ohlcv_source_dir",
    "path",
    "seed_path",
    "source",
}


class AICapabilityError(RuntimeError):
    """Safe capability or proposal error."""


@dataclass
class ActionProposal:
    """One immutable durable proposal awaiting explicit owner approval."""

    id: str
    owner: str
    conversation_id: str
    action: str
    name: str
    config: dict[str, Any] | None
    expected_digest: str | None
    create_only: bool
    preview: dict[str, Any]
    payload_digest: str
    draft_id: str = ""
    draft_digest: str = ""
    created_at: float = field(default_factory=time.time)
    status: str = "awaiting_approval"
    result: dict[str, Any] | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    execution_task: asyncio.Task | None = None


@dataclass(frozen=True)
class CapabilityDescriptor:
    """One dynamically projected capability and its enforced effect class."""

    name: str
    description: str
    schema: dict[str, Any]
    effect: str = "read"
    resources: tuple[str, ...] = ()


class AICapabilityService:
    """Dispatch bounded read tools and approval-gated PB8 mutations."""

    def __init__(self, root: Path | None = None) -> None:
        """Initialize private durable workspaces, journals, and bounded registries."""
        self.root = ensure_private_directory(Path(root or Path(PBGDIR) / "data" / "ai" / "capabilities"))
        self.draft_root = ensure_private_directory(self.root / "drafts")
        self.proposal_root = ensure_private_directory(self.root / "proposals")
        self.history_root = ensure_private_directory(self.root / "history")
        self.journal_root = ensure_private_directory(self.root / "journal")
        self.lock_target = self.root / ".write"
        self.proposals: dict[str, ActionProposal] = {}
        self.loaded_proposal_owners: set[str] = set()
        self.execution_tasks: set[asyncio.Task] = set()
        self.analysis_tasks: set[asyncio.Task] = set()
        self.state_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(4)
        self.approval_semaphore = asyncio.Semaphore(_MAX_ACTIVE_APPROVALS)

    async def startup(self) -> None:
        """Recover approved durable actions before accepting new approval work."""
        await self._to_thread_uncancellable(self._interrupt_python_analyses)
        await self._to_thread_uncancellable(self._recover_journals)
        await self._to_thread_uncancellable(self._prune_durable_state)

    async def shutdown(self) -> None:
        """Cancel disposable analyses and await every API-owned approval task."""
        analyses = list(self.analysis_tasks)
        for task in analyses:
            task.cancel()
        if analyses:
            await asyncio.gather(*analyses, return_exceptions=True)
        mutable = list(self.execution_tasks)
        if mutable:
            await asyncio.gather(*(asyncio.shield(task) for task in mutable), return_exceptions=True)
        self.analysis_tasks.clear()
        self.execution_tasks.clear()

    def capability_registry(self) -> dict[str, Any]:
        """Return effect-aware descriptors, virtual resources, limits, and runtime fingerprints."""
        descriptors = self._capability_descriptors()
        return {
            "schema_version": 1,
            "capabilities": [
                {
                    "name": item.name,
                    "description": item.description,
                    "effect": item.effect,
                    "resources": list(item.resources),
                    "input_schema": copy.deepcopy(item.schema),
                }
                for item in descriptors
            ],
            "effect_classes": ["read", "analyze", "ui", "draft", "write", "execute", "delete", "remote"],
            "virtual_resources": [
                "pbgui://optimizer-config/{version}/{name}",
                "pbgui://optimizer-run/{version}/{opaque-id}",
                "pbgui://optimizer-queue/v8/{opaque-id}",
                "pbgui://pareto/{version}/{opaque-id}",
                "pbgui://backtest/{version}/{opaque-id}",
                "pbgui://draft/{version}/{opaque-id}",
            ],
            "runtime_fingerprints": self._runtime_fingerprints(),
            "limits": {
                "tool_result_bytes": _MAX_TOOL_RESULT_BYTES,
                "config_bytes": _MAX_CONFIG_BYTES,
                "proposals_per_owner": _MAX_PROPOSALS_PER_OWNER,
                "proposals_global": _MAX_PROPOSALS_GLOBAL,
                "drafts_per_owner": _MAX_DRAFTS_PER_OWNER,
                "drafts_global": _MAX_DRAFTS_GLOBAL,
                "concurrent_capabilities": 4,
                "concurrent_approved_actions": _MAX_ACTIVE_APPROVALS,
                "python_analysis_code_bytes": _MAX_ANALYSIS_CODE_BYTES,
                "python_analysis_input_bytes": _MAX_ANALYSIS_INPUT_BYTES,
                "python_analysis_timeout_seconds": _ANALYSIS_TIMEOUT_SECONDS,
            },
        }

    def codex_dynamic_tools(self) -> list[dict[str, Any]]:
        """Return canonical Codex dynamic-tool definitions."""
        return [
            {
                "type": "namespace",
                "name": "pbgui",
                "description": "Authenticated PBGui resources and approval-gated optimizer actions.",
                "tools": [self._codex_tool(item) for item in self._tool_specs()],
            }
        ]

    def chat_completion_tools(self) -> list[dict[str, Any]]:
        """Return OpenAI-compatible function definitions for Zen/Go chat models."""
        return [
            {
                "type": "function",
                "function": {
                    "name": item["name"],
                    "description": item["description"],
                    "parameters": item["schema"],
                },
            }
            for item in self._tool_specs()
        ]

    def responses_tools(self) -> list[dict[str, Any]]:
        """Return native OpenAI Responses function definitions."""
        return [
            {
                "type": "function",
                "name": item["name"],
                "description": item["description"],
                "parameters": item["schema"],
                "strict": False,
            }
            for item in self._tool_specs()
        ]

    def messages_tools(self) -> list[dict[str, Any]]:
        """Return native Anthropic Messages tool definitions."""
        return [
            {
                "name": item["name"],
                "description": item["description"],
                "input_schema": item["schema"],
            }
            for item in self._tool_specs()
        ]

    async def dispatch(
        self,
        owner: str,
        conversation_id: str,
        tool: str,
        arguments: object,
    ) -> dict[str, Any]:
        """Validate and execute one model-requested capability."""
        args = arguments if isinstance(arguments, dict) else {}
        encoded = json.dumps(args, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_CONFIG_BYTES:
            raise AICapabilityError("Tool arguments are too large")
        handlers = {
            "get_capability_registry": lambda unused: self.capability_registry(),
            "list_optimizer_configs": self._list_optimizer_configs,
            "get_optimizer_config": self._get_optimizer_config,
            "get_optimizer_metadata": self._get_optimizer_metadata,
            "preview_pb8_scenario_template": self._preview_pb8_scenario_template,
            "list_optimizer_runs": self._list_optimizer_runs,
            "list_pb8_optimizer_queue": self._list_pb8_optimizer_queue,
            "list_backtests": self._list_backtests,
            "get_optimizer_run_analysis": self._get_optimizer_run_analysis,
            "rank_optimizer_run_candidates": self._rank_optimizer_run_candidates,
            "get_pareto_candidate": self._get_pareto_candidate,
            "select_pareto_candidates": self._select_pareto_candidates,
            "select_backtest_results": self._select_backtest_results,
            "perform_page_action": self._perform_page_action,
            "present_user_choices": self._present_user_choices,
            "list_dashboard_templates": self._list_dashboard_templates,
            "get_dashboard_layout": self._get_dashboard_layout,
            "get_backtest_projection": self._get_backtest_projection,
            "list_config_drafts": self._list_config_drafts,
            "get_config_draft": self._get_config_draft,
            "create_config_draft": self._create_config_draft,
            "update_config_draft": self._update_config_draft,
            "propose_pb8_optimizer_config": self._propose_pb8_optimizer_config,
            "propose_pb8_config_patch": self._propose_pb8_config_patch,
            "propose_queue_pb8_config": self._propose_queue_pb8_config,
            "propose_start_pb8_optimizer_queue": self._propose_start_pb8_optimizer_queue,
            "propose_pareto_backtests": self._propose_pareto_backtests,
            "propose_dashboard_from_template": self._propose_dashboard_from_template,
            "propose_dashboard_layout": self._propose_dashboard_layout,
            "propose_python_analysis": self._propose_python_analysis,
            "propose_optimizer_run_python_analysis": self._propose_optimizer_run_python_analysis,
            "propose_workspace_python_analysis": self._propose_workspace_python_analysis,
            "get_python_analysis_result": self._get_python_analysis_result,
            "get_passivbot_installations": self._get_passivbot_installations,
            "read_pbgui_help_topic": self._read_pbgui_help_topic,
            "search_pbgui_help": self._search_pbgui_help,
            "search_passivbot_docs": self._search_passivbot_docs,
            "search_passivbot_source": self._search_passivbot_source,
            "read_passivbot_source": self._read_passivbot_source,
        }
        handler = handlers.get(tool)
        if handler is None:
            raise AICapabilityError("Unknown PBGui capability")
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=5)
        except asyncio.TimeoutError as exc:
            raise AICapabilityError("PBGui capability capacity is busy") from exc
        try:
            try:
                if tool.startswith("propose_") or tool == "get_python_analysis_result":
                    result = await handler(owner, conversation_id, args)
                elif tool in {"list_config_drafts", "get_config_draft", "create_config_draft", "update_config_draft"}:
                    result = await self._to_thread_uncancellable(handler, owner, args)
                else:
                    result = await self._to_thread_uncancellable(handler, args)
            except AICapabilityError:
                raise
            except HTTPException as exc:
                raise AICapabilityError(self._safe_detail(exc.detail)) from exc
            except Exception as exc:
                _log(SERVICE, f"AI capability {tool} failed: {type(exc).__name__}", level="ERROR")
                raise AICapabilityError("PBGui capability failed") from exc
        finally:
            self.semaphore.release()
        self._require_bounded_result(result)
        return result

    async def list_proposals(self, owner: str, conversation_id: str = "") -> list[dict[str, Any]]:
        """Return non-secret pending proposal projections for one owner."""
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            self._cleanup_proposals_unlocked()
            selected = [
                proposal
                for proposal in self.proposals.values()
                if proposal.owner == owner
                and proposal.status == "awaiting_approval"
                and (not conversation_id or proposal.conversation_id == conversation_id)
            ]
            return [self._proposal_projection(item) for item in selected]

    async def list_action_history(self, owner: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return bounded durable proposal decisions and execution outcomes for one owner."""
        selected_limit = max(1, min(_MAX_ACTION_HISTORY_PER_OWNER, int(limit or 50)))
        records = []
        for path in self._owner_files(self.history_root, owner):
            payload = self._read_private_json(path, self.history_root)
            if payload.get("owner") == owner:
                payload.pop("owner", None)
                records.append(self._sanitize_config(payload))
        records.sort(key=lambda item: float(item.get("updated_at") or 0), reverse=True)
        return records[:selected_limit]

    async def approve(
        self,
        owner: str,
        proposal_id: str,
        payload_digest: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        """Execute exactly one approved proposal and return an idempotent result."""
        proposal = await self._owned_proposal(owner, proposal_id)
        async with proposal.lock:
            with advisory_file_lock(self.lock_target):
                durable = self._read_private_json(
                    self._owner_path(self.proposal_root, owner, proposal.id), self.proposal_root
                )
                durable_status = str(durable.get("status") or "")
                if durable_status == "executed" and isinstance(durable.get("result"), dict):
                    proposal.status = "executed"
                    proposal.result = durable["result"]
                elif durable_status in {"executing", "approved_recovery"} and proposal.execution_task is None:
                    raise AICapabilityError("Proposal execution is already owned by the API")
            if not hmac.compare_digest(proposal.payload_digest, str(payload_digest or "")):
                raise AICapabilityError("Proposal payload changed")
            if proposal.conversation_id != conversation_id:
                raise AICapabilityError("Proposal belongs to another conversation")
            if proposal.status == "executed" and proposal.result is not None:
                return copy.deepcopy(proposal.result)
            if proposal.status == "executing" and proposal.execution_task is not None:
                task = proposal.execution_task
            elif proposal.status != "awaiting_approval":
                raise AICapabilityError("Proposal is no longer pending")
            else:
                if time.time() - proposal.created_at > _PROPOSAL_TTL_SECONDS:
                    proposal.status = "expired"
                    self._persist_proposal(proposal)
                    self._persist_history(proposal)
                    raise AICapabilityError("Proposal expired")
                proposal.status = "executing"
                with advisory_file_lock(self.lock_target):
                    durable = self._read_private_json(
                        self._owner_path(self.proposal_root, owner, proposal.id), self.proposal_root
                    )
                    if durable.get("status") != "awaiting_approval":
                        raise AICapabilityError("Proposal is no longer pending")
                    if proposal.action != "python_analysis":
                        self._write_private_json(
                            self.journal_root / f"{proposal.id}.json",
                            self._journal_payload(proposal, phase="prepared"),
                        )
                    self._persist_proposal(proposal, acquire_lock=False)
                task = asyncio.create_task(
                    self._run_proposal_execution(proposal),
                    name=f"ai-approved-action-{proposal.id}",
                )
                proposal.execution_task = task
                self.execution_tasks.add(task)
                task.add_done_callback(self.execution_tasks.discard)
                if proposal.action == "python_analysis":
                    self.analysis_tasks.add(task)
                    task.add_done_callback(self.analysis_tasks.discard)
        return copy.deepcopy(await asyncio.shield(task))

    async def reject(
        self,
        owner: str,
        proposal_id: str,
        payload_digest: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        """Reject one pending proposal without changing PBGui state."""
        proposal = await self._owned_proposal(owner, proposal_id)
        async with proposal.lock:
            if not hmac.compare_digest(proposal.payload_digest, str(payload_digest or "")):
                raise AICapabilityError("Proposal payload changed")
            if proposal.conversation_id != conversation_id:
                raise AICapabilityError("Proposal belongs to another conversation")
            if proposal.status == "awaiting_approval":
                with advisory_file_lock(self.lock_target):
                    durable = self._read_private_json(
                        self._owner_path(self.proposal_root, owner, proposal.id), self.proposal_root
                    )
                    if durable.get("status") != "awaiting_approval":
                        raise AICapabilityError("Proposal is no longer pending")
                    proposal.status = "rejected"
                    self._persist_proposal(proposal, acquire_lock=False)
                self._persist_history(proposal)
            return {"proposal_id": proposal.id, "status": proposal.status}

    async def reject_conversation(self, owner: str, conversation_id: str) -> None:
        """Reject all durable pending proposals when their conversation branch advances."""
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            self._cleanup_proposals_unlocked()
            for proposal in self.proposals.values():
                if (
                    proposal.owner == owner
                    and proposal.conversation_id == conversation_id
                    and proposal.status == "awaiting_approval"
                ):
                    proposal.status = "rejected"
                    self._persist_proposal(proposal)
                    self._persist_history(proposal)

    async def _run_proposal_execution(self, proposal: ActionProposal) -> dict[str, Any]:
        """Run an approved action independently from request cancellation."""
        try:
            async with self.approval_semaphore:
                if proposal.action == "python_analysis":
                    result = await self._execute_python_analysis(proposal)
                else:
                    result = await asyncio.to_thread(self._execute_proposal, proposal)
        except asyncio.CancelledError:
            async with proposal.lock:
                proposal.status = "cancelled"
                proposal.execution_task = None
                self._persist_proposal(proposal)
                self._persist_history(proposal, error="Python analysis was cancelled")
            raise
        except Exception as exc:
            async with proposal.lock:
                proposal.status = "failed"
                proposal.execution_task = None
                self._persist_proposal(proposal)
                self._persist_history(proposal, error=self._safe_detail(str(exc)))
            if isinstance(exc, AICapabilityError):
                raise
            if isinstance(exc, HTTPException):
                raise AICapabilityError(self._safe_detail(exc.detail)) from exc
            _log(SERVICE, f"AI proposal execution failed: {type(exc).__name__}", level="ERROR")
            raise AICapabilityError("Approved PBGui action failed") from exc
        async with proposal.lock:
            proposal.status = "executed"
            proposal.result = result
            proposal.execution_task = None
            self._persist_proposal(proposal)
            self._persist_history(proposal)
        return result

    def _list_optimizer_configs(self, args: dict[str, Any]) -> dict[str, Any]:
        """List bounded PB7/PB8 optimizer config summaries without paths."""
        version = self._version(args)
        limit = self._limit(args, maximum=100)
        if version == "v8":
            from api import optimize_v8

            payload = optimize_v8.list_configs(session=object(), include_result_summary=False)
        else:
            from api import optimize_v7

            payload = optimize_v7.list_configs(session=object())
        configs = []
        for item in payload.get("configs", [])[:limit]:
            projected = self._strip_paths(item)
            if isinstance(projected, dict):
                name = str(projected.get("name") or projected.get("config_name") or "")
                if name:
                    projected["resource"] = f"pbgui://optimizer-config/{version}/{quote(name, safe='')}"
            configs.append(projected)
        return {"version": version, "configs": configs, "returned": len(configs)}

    def _get_optimizer_config(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return one managed optimizer config with secrets and host paths removed."""
        version = self._version(args)
        name = self._name(args.get("name"))
        if version == "v8":
            from api import optimize_v8

            payload = optimize_v8.get_config(name, session=object())
        else:
            from api import optimize_v7

            payload = optimize_v7.get_config(name, session=object())
        config = payload.get("config") if isinstance(payload, dict) else None
        if not isinstance(config, dict):
            raise AICapabilityError("Optimizer config is unavailable")
        return {
            "version": version,
            "name": name,
            "resource": f"pbgui://optimizer-config/{version}/{quote(name, safe='')}",
            "config": self._sanitize_config(config),
        }

    def _get_optimizer_metadata(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return dynamic runtime template and optimizer metadata."""
        version = self._version(args)
        if version == "v8":
            from pb8_config import get_pb8_optimize_metadata

            metadata = get_pb8_optimize_metadata()
            selected = {
                key: metadata.get(key)
                for key in (
                    "template",
                    "strategies",
                    "strategy_specs",
                    "active_bounds",
                    "optimize_parameters",
                    "backends",
                    "scoring",
                    "limits",
                )
                if key in metadata
            }
        else:
            from api.pb7_bridge import (
                get_optimize_backend_options,
                get_optimize_limits_meta_payload,
                get_optimize_metric_sets,
                get_template_config,
            )

            selected = {
                "template": get_template_config(),
                "backends": get_optimize_backend_options(),
                "limits": get_optimize_limits_meta_payload(),
                "metric_sets": get_optimize_metric_sets(),
            }
        return {"version": version, "metadata": self._sanitize_config(selected)}

    def _preview_pb8_scenario_template(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return the same deterministic PB8 scenario preview used by the editor."""
        from scenario_templates import ScenarioTemplateError, generate_scenario_template

        try:
            return generate_scenario_template(args)
        except ScenarioTemplateError as exc:
            raise AICapabilityError(str(exc)) from exc

    def _list_optimizer_runs(self, args: dict[str, Any]) -> dict[str, Any]:
        """List compact optimizer result summaries without filesystem paths."""
        version = self._version(args)
        limit = self._limit(args, maximum=50)
        if version == "v8":
            from api import optimize_v8

            payload = optimize_v8.list_results(session=object())
        else:
            from api import optimize_v7

            payload = optimize_v7.list_results(session=object())
        results = [
            self._resource_projection("optimizer-run", version, item)
            for item in payload.get("results", [])[:limit]
        ]
        return {"version": version, "runs": results, "returned": len(results)}

    def _list_backtests(self, args: dict[str, Any]) -> dict[str, Any]:
        """List compact completed backtest summaries without filesystem paths."""
        version = self._version(args)
        limit = self._limit(args, maximum=50)
        if version == "v8":
            from api import backtest_v8

            payload = backtest_v8.get_results(limit=limit, session=object())
        else:
            from api import backtest_v7

            payload = backtest_v7.list_results(limit=limit, session=object())
        results = [
            self._resource_projection("backtest", version, item)
            for item in payload.get("results", [])[:limit]
        ]
        return {"version": version, "backtests": results, "returned": len(results)}

    def _get_optimizer_run_analysis(self, args: dict[str, Any]) -> dict[str, Any]:
        """Project one optimizer Pareto set through an opaque virtual resource."""
        version = self._version(args)
        resource = self._resource_uri(args.get("resource"), "optimizer-run", version)
        raw = self._resolve_listed_resource("optimizer-run", version, resource)
        result_path = str(raw.get("path") or "")
        if not result_path:
            raise AICapabilityError("Optimizer run is unavailable")
        statistic = str(args.get("statistic") or "mean").strip().lower()
        scenario = str(args.get("scenario") or "Aggregated").strip() or "Aggregated"
        limit = self._limit(args, maximum=200)
        if version == "v8":
            from api import optimize_v8

            payload = optimize_v8.list_paretos(
                result_path,
                scenario=scenario,
                statistic=statistic,
                session=object(),
                metrics="",
            )
        else:
            from api import optimize_v7

            payload = optimize_v7.list_paretos(
                result_path,
                scenario=scenario,
                statistic=statistic,
                session=object(),
            )
        candidates = []
        for item in payload.get("paretos", [])[:limit]:
            if not isinstance(item, dict):
                continue
            candidate_uri = self._virtual_uri("pareto", version, str(item.get("path") or ""))
            candidates.append(
                {
                    "resource": candidate_uri,
                    "name": str(item.get("name") or "")[:128],
                    "modified": item.get("modified"),
                    "metrics": self._sanitize_config(item.get("summary") or {}),
                }
            )
        return {
            "version": version,
            "resource": resource,
            "run": self._compact_result(raw),
            "pareto": candidates,
            "meta": self._strip_paths(self._sanitize_config(payload.get("meta") or {})),
            "returned": len(candidates),
            "truncated": len(payload.get("paretos", [])) > limit,
        }

    def _rank_optimizer_run_candidates(self, args: dict[str, Any]) -> dict[str, Any]:
        """Rank every Pareto candidate with validated weighted normalized criteria."""
        version = self._version(args)
        resource = self._resource_uri(args.get("resource"), "optimizer-run", version)
        raw = self._resolve_listed_resource("optimizer-run", version, resource)
        result_path = str(raw.get("path") or "")
        statistic = str(args.get("statistic") or "mean").strip().lower()
        scenario = str(args.get("scenario") or "Aggregated").strip() or "Aggregated"
        requested_criteria = args.get("criteria")
        if not isinstance(requested_criteria, list) or not 1 <= len(requested_criteria) <= 12:
            raise AICapabilityError("Candidate ranking requires 1 to 12 criteria")
        criteria = []
        for item in requested_criteria:
            if not isinstance(item, dict):
                raise AICapabilityError("Candidate ranking criterion is invalid")
            metric = str(item.get("metric") or "").strip()
            direction = str(item.get("direction") or "").strip().lower()
            if not metric or len(metric) > 128 or direction not in {"min", "max"}:
                raise AICapabilityError("Candidate ranking criterion is invalid")
            try:
                weight = float(item.get("weight", 1.0))
                minimum = float(item["minimum"]) if item.get("minimum") is not None else None
                maximum = float(item["maximum"]) if item.get("maximum") is not None else None
            except (TypeError, ValueError) as exc:
                raise AICapabilityError("Candidate ranking criterion values are invalid") from exc
            if not math.isfinite(weight) or not 0 < weight <= 100:
                raise AICapabilityError("Candidate ranking weight is invalid")
            if minimum is not None and not math.isfinite(minimum):
                raise AICapabilityError("Candidate ranking minimum is invalid")
            if maximum is not None and not math.isfinite(maximum):
                raise AICapabilityError("Candidate ranking maximum is invalid")
            if minimum is not None and maximum is not None and minimum > maximum:
                raise AICapabilityError("Candidate ranking thresholds are invalid")
            criteria.append({"metric": metric, "direction": direction, "weight": weight, "minimum": minimum, "maximum": maximum})
        if len({item["metric"].casefold() for item in criteria}) != len(criteria):
            raise AICapabilityError("Candidate ranking criteria contain duplicate metrics")
        if version == "v8":
            from api import optimize_v8 as module

            payload = module.list_paretos(
                result_path, scenario=scenario, statistic=statistic, session=object(), metrics=""
            )
        else:
            from api import optimize_v7 as module

            payload = module.list_paretos(
                result_path, scenario=scenario, statistic=statistic, session=object()
            )
        rows = []
        comparable_rows = []
        metric_counts = {criterion["metric"]: 0 for criterion in criteria}
        threshold_rejections = {criterion["metric"]: 0 for criterion in criteria}
        paretos = payload.get("paretos", [])
        for item in paretos:
            if not isinstance(item, dict):
                continue
            summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
            keys = {str(key).casefold(): key for key in summary}
            values = {}
            valid = True
            for criterion in criteria:
                actual_key = keys.get(criterion["metric"].casefold())
                value = summary.get(actual_key) if actual_key is not None else None
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                    valid = False
                    break
                number = float(value)
                metric_counts[criterion["metric"]] += 1
                values[criterion["metric"]] = number
            if not valid:
                continue
            comparable_rows.append({"item": item, "values": values})
            thresholds_ok = True
            for criterion in criteria:
                number = values[criterion["metric"]]
                if criterion["minimum"] is not None and number < criterion["minimum"]:
                    threshold_rejections[criterion["metric"]] += 1
                    thresholds_ok = False
                if criterion["maximum"] is not None and number > criterion["maximum"]:
                    threshold_rejections[criterion["metric"]] += 1
                    thresholds_ok = False
            if thresholds_ok:
                rows.append({"item": item, "values": values})
        strict_eligible = len(rows)
        thresholds_relaxed = False
        if not rows and comparable_rows:
            rows = comparable_rows
            thresholds_relaxed = True
        if not rows:
            available_metrics = sorted({str(key) for item in paretos if isinstance(item, dict) and isinstance(item.get("summary"), dict) for key in item["summary"]})
            return {
                "version": version,
                "resource": resource,
                "run": self._compact_result(raw),
                "criteria": criteria,
                "scanned": len(paretos),
                "eligible": 0,
                "complete_scan": True,
                "ranked": [],
                "returned": 0,
                "diagnostics": {
                    "reason": "No candidate contains every requested metric as a finite number",
                    "requested_metric_counts": metric_counts,
                    "available_metrics": available_metrics[:200],
                    "required_next_tool": "propose_optimizer_run_python_analysis",
                },
            }
        ranges = {
            criterion["metric"]: (
                min(row["values"][criterion["metric"]] for row in rows),
                max(row["values"][criterion["metric"]] for row in rows),
            )
            for criterion in criteria
        }
        total_weight = sum(item["weight"] for item in criteria)
        for row in rows:
            score = 0.0
            components = {}
            for criterion in criteria:
                value = row["values"][criterion["metric"]]
                lower, upper = ranges[criterion["metric"]]
                normalized = 1.0 if upper == lower else (value - lower) / (upper - lower)
                if criterion["direction"] == "min":
                    normalized = 1.0 - normalized
                components[criterion["metric"]] = normalized
                score += normalized * criterion["weight"]
            row["score"] = score / total_weight
            row["components"] = components
        rows.sort(key=lambda row: (-row["score"], str(row["item"].get("name") or "")))
        limit = self._limit(args, maximum=50)
        ranked = []
        for row in rows[:limit]:
            item = row["item"]
            path = str(item.get("path") or "")
            ranked.append({
                "resource": self._virtual_uri("pareto", version, path),
                "name": str(item.get("name") or "")[:128],
                "score": round(row["score"], 8),
                "metrics": row["values"],
                "components": {key: round(value, 8) for key, value in row["components"].items()},
            })
        ranked_result = [] if thresholds_relaxed else ranked
        return {
            "version": version,
            "resource": resource,
            "run": self._compact_result(raw),
            "criteria": criteria,
            "scanned": len(paretos),
            "eligible": strict_eligible,
            "ranked_pool": len(rows),
            "complete_scan": True,
            "thresholds_relaxed": thresholds_relaxed,
            "required_user_clarification": thresholds_relaxed,
            "diagnostics": {
                "comparable": len(comparable_rows),
                "threshold_rejections": threshold_rejections,
            },
            "ranked": ranked_result,
            "relaxed_suggestions": ranked if thresholds_relaxed else [],
            "returned": len(ranked_result),
        }

    def _get_pareto_candidate(self, args: dict[str, Any]) -> dict[str, Any]:
        """Read one exact Pareto config through run-bound opaque resources."""
        version = self._version(args)
        run_resource = self._resource_uri(args.get("run_resource"), "optimizer-run", version)
        candidate_resource = self._resource_uri(args.get("candidate_resource"), "pareto", version)
        raw = self._resolve_listed_resource("optimizer-run", version, run_resource)
        result_path = str(raw.get("path") or "")
        if version == "v8":
            from api import optimize_v8 as module

            listed = module.list_paretos(
                result_path,
                scenario="Aggregated",
                statistic="mean",
                session=object(),
                metrics="",
            )
        else:
            from api import optimize_v7 as module

            listed = module.list_paretos(
                result_path,
                scenario="Aggregated",
                statistic="mean",
                session=object(),
            )
        for item in listed.get("paretos", [])[:5000]:
            path = str(item.get("path") or "") if isinstance(item, dict) else ""
            if self._virtual_uri("pareto", version, path) != candidate_resource:
                continue
            payload = module.get_pareto_file(path, session=object())
            return {
                "version": version,
                "resource": candidate_resource,
                "run_resource": run_resource,
                "metrics": self._sanitize_config(item.get("summary") or {}),
                "config": self._sanitize_config(payload),
            }
        raise AICapabilityError("Pareto candidate is no longer available")

    def _select_pareto_candidates(self, args: dict[str, Any]) -> dict[str, Any]:
        """Create one reversible browser action for exact run-bound Pareto candidates."""
        version = self._version(args)
        run_resource = self._resource_uri(args.get("run_resource"), "optimizer-run", version)
        raw = self._resolve_listed_resource("optimizer-run", version, run_resource)
        requested = args.get("candidate_resources")
        if not isinstance(requested, list) or not 1 <= len(requested) <= 100:
            raise AICapabilityError("Select between 1 and 100 Pareto candidates")
        resources = [self._resource_uri(item, "pareto", version) for item in requested]
        if len(set(resources)) != len(resources):
            raise AICapabilityError("Pareto candidate selection contains duplicates")
        result_path = str(raw.get("path") or "")
        if version == "v8":
            from api import optimize_v8 as module

            listed = module.list_paretos(
                result_path, scenario="Aggregated", statistic="mean", session=object(), metrics=""
            )
        else:
            from api import optimize_v7 as module

            listed = module.list_paretos(
                result_path, scenario="Aggregated", statistic="mean", session=object()
            )
        names_by_resource = {
            self._virtual_uri("pareto", version, str(item.get("path") or "")): str(item.get("name") or "")[:128]
            for item in listed.get("paretos", [])[:5000]
            if isinstance(item, dict)
        }
        if any(resource not in names_by_resource for resource in resources):
            raise AICapabilityError("One or more Pareto candidates are unavailable for this optimizer run")
        names = [names_by_resource[resource] for resource in resources]
        mode = str(args.get("mode") or "replace")
        if mode not in {"replace", "add"}:
            raise AICapabilityError("Invalid Pareto selection mode")
        run_name = str(raw.get("name") or raw.get("result") or raw.get("config_name") or "")[:128]
        return {
            "status": "queued_for_browser",
            "selected": len(names),
            "candidate_names": names,
            "ui_action": {
                "type": "optimize.select_paretos",
                "target": {"page_key": f"{version}_optimize", "version": version, "run_name": run_name},
                "payload": {"candidate_names": names, "mode": mode},
            },
        }

    def _select_backtest_results(self, args: dict[str, Any]) -> dict[str, Any]:
        """Select exact managed backtests and open the existing browser compare view."""
        version = self._version(args)
        requested = args.get("resources")
        if not isinstance(requested, list) or not 2 <= len(requested) <= 20:
            raise AICapabilityError("Select between 2 and 20 backtest resources")
        resources = [self._resource_uri(item, "backtest", version) for item in requested]
        if len(set(resources)) != len(resources):
            raise AICapabilityError("Backtest result selection contains duplicates")
        selectors = []
        for resource in resources:
            raw = self._resolve_listed_resource("backtest", version, resource)
            selector = {
                key: raw.get(key)
                for key in ("config_name", "result_name", "exchange_dir", "modified")
                if raw.get(key) is not None
            }
            if not selector.get("config_name") or not selector.get("result_name"):
                raise AICapabilityError("Backtest result cannot be selected safely in the browser")
            selectors.append(selector)
        return {
            "status": "queued_for_browser",
            "selected": len(selectors),
            "ui_action": {
                "type": "backtest.compare_results",
                "target": {"page_key": f"{version}_backtest", "version": version},
                "payload": {"selectors": selectors},
            },
        }

    @staticmethod
    def _perform_page_action(args: dict[str, Any]) -> dict[str, Any]:
        """Create one reversible action advertised by the current PBGui page."""
        page_key = str(args.get("page_key") or "").strip()
        action = str(args.get("action") or "").strip()
        entity_kind = str(args.get("entity_kind") or "").strip()
        entity_name = str(args.get("entity_name") or "").strip()
        value = args.get("value")
        if (
            not page_key
            or len(page_key) > 128
            or any(ord(char) < 32 or ord(char) == 127 for char in page_key)
            or not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", action)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", entity_kind)
            or not entity_name
            or len(entity_name) > 128
            or any(ord(char) < 32 or ord(char) == 127 for char in entity_name)
            or (
                value is not None
                and (
                    not isinstance(value, str)
                    or len(value) > 1000
                    or any(ord(char) < 32 and char not in "\t\n" for char in value)
                )
            )
        ):
            raise AICapabilityError("Invalid page action")
        payload = {
            "action": action,
            "entity": {"kind": entity_kind, "name": entity_name},
        }
        if value is not None:
            payload["value"] = value
        return {
            "status": "queued_for_browser",
            "action": action,
            "ui_action": {
                "type": "page.perform_action",
                "target": {"page_key": page_key},
                "payload": payload,
            },
        }

    @staticmethod
    def _present_user_choices(args: dict[str, Any]) -> dict[str, Any]:
        """Create one persistent clickable clarification action for the active chat."""
        question = str(args.get("question") or "").strip()
        choices = args.get("choices")
        if not question or len(question) > 500 or not isinstance(choices, list) or not 2 <= len(choices) <= 5:
            raise AICapabilityError("Quick reply choices are invalid")
        normalized = []
        for item in choices:
            if not isinstance(item, dict):
                raise AICapabilityError("Quick reply choice is invalid")
            label = str(item.get("label") or "").strip()
            value = str(item.get("value") or "").strip()
            if not label or not value or len(label) > 80 or len(value) > 1000:
                raise AICapabilityError("Quick reply choice is invalid")
            normalized.append({"label": label, "value": value})
        if len({item["value"] for item in normalized}) != len(normalized):
            raise AICapabilityError("Quick reply choices contain duplicates")
        return {
            "status": "waiting_for_user_choice",
            "ui_action": {
                "type": "chat.quick_replies",
                "target": {"page_key": "ai_chat"},
                "payload": {"question": question, "choices": normalized},
            },
        }

    def _get_backtest_projection(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return bounded metrics, downsampled equity, and redacted fill rows."""
        version = self._version(args)
        resource = self._resource_uri(args.get("resource"), "backtest", version)
        raw = self._resolve_listed_resource("backtest", version, resource)
        result_path = str(raw.get("path") or "")
        if not result_path:
            raise AICapabilityError("Backtest result is unavailable")
        max_points = self._bounded_int(args.get("max_points"), default=200, maximum=1000)
        max_fills = self._bounded_int(args.get("max_fills"), default=50, maximum=200)
        if version == "v8":
            from api import backtest_v8 as module
        else:
            from api import backtest_v7 as module
        result_dir = module._resolve_result_dir(result_path)
        analysis = module.get_result_analysis(result_path, session=object())
        return {
            "version": version,
            "resource": resource,
            "summary": self._compact_result(raw),
            "metrics": self._metric_projection(analysis),
            "equity": self._csv_projection(
                result_dir,
                "balance_and_equity.csv",
                max_rows=max_points,
                preferred=("minute", "timestamp", "time", "usd_total_balance", "usd_total_equity", "balance", "equity"),
            ),
            "fills": self._csv_projection(
                result_dir,
                "fills.csv",
                max_rows=max_fills,
                preferred=("minute", "timestamp", "time", "symbol", "coin", "side", "type", "price", "qty", "pnl", "fee_paid"),
            ),
        }

    def _list_config_drafts(self, owner: str, args: dict[str, Any]) -> dict[str, Any]:
        """List owner-bound persistent draft summaries without production paths."""
        version = str(args.get("version") or "").lower()
        if version and version not in {"v7", "v8"}:
            raise AICapabilityError("version must be v7 or v8")
        drafts = []
        for path in self._owner_files(self.draft_root, owner):
            payload = self._read_private_json(path, self.draft_root)
            if version and payload.get("version") != version:
                continue
            drafts.append(self._draft_projection(payload, include_config=False))
        drafts.sort(key=lambda item: float(item.get("updated_at") or 0), reverse=True)
        return {"drafts": drafts[:_MAX_DRAFTS_PER_OWNER], "returned": min(len(drafts), _MAX_DRAFTS_PER_OWNER)}

    def _get_config_draft(self, owner: str, args: dict[str, Any]) -> dict[str, Any]:
        """Read one exact owner-bound draft and its latest validation result."""
        payload = self._load_draft(owner, str(args.get("draft_id") or ""))
        return self._draft_projection(payload, include_config=True)

    def _create_config_draft(self, owner: str, args: dict[str, Any]) -> dict[str, Any]:
        """Persist a private complete config candidate and validate it with its runtime adapter."""
        version = self._version(args)
        config = args.get("config")
        if not isinstance(config, dict):
            raise AICapabilityError("config must be an object")
        self._require_safe_draft(config)
        with advisory_file_lock(self.lock_target):
            owner_files = self._owner_files(self.draft_root, owner)
            if len(owner_files) >= _MAX_DRAFTS_PER_OWNER:
                raise AICapabilityError("Draft limit reached for this owner")
            if self._count_private_files(self.draft_root) >= _MAX_DRAFTS_GLOBAL:
                raise AICapabilityError("Global draft capacity reached")
            draft_id = uuid4().hex
            payload = self._validated_draft_payload(owner, draft_id, version, config, revision=1)
            self._write_private_json(self._owner_path(self.draft_root, owner, draft_id), payload)
        return self._draft_projection(payload, include_config=True)

    def _update_config_draft(self, owner: str, args: dict[str, Any]) -> dict[str, Any]:
        """Replace one draft at an expected revision and rerun immutable validation."""
        draft_id = self._opaque_id(args.get("draft_id"), "draft")
        config = args.get("config")
        if not isinstance(config, dict):
            raise AICapabilityError("config must be an object")
        self._require_safe_draft(config)
        expected = self._bounded_int(args.get("expected_revision"), default=0, maximum=1_000_000)
        with advisory_file_lock(self.lock_target):
            current = self._load_draft(owner, draft_id)
            if expected != int(current.get("revision") or 0):
                raise AICapabilityError("Draft revision changed")
            payload = self._validated_draft_payload(
                owner,
                draft_id,
                str(current["version"]),
                config,
                revision=expected + 1,
                created_at=float(current.get("created_at") or time.time()),
            )
            self._write_private_json(self._owner_path(self.draft_root, owner, draft_id), payload)
        return self._draft_projection(payload, include_config=True)

    def _search_passivbot_docs(self, args: dict[str, Any]) -> dict[str, Any]:
        """Search documentation from the exact installed Passivbot checkout."""
        return self._search_passivbot_tree(args, docs_only=True)

    def _get_passivbot_installations(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return path-free commit identities for configured PB7/PB8 checkouts."""
        del args
        installations = []
        for version in ("v7", "v8"):
            try:
                root = self._passivbot_root(version)
                commit, repository = self._passivbot_git_info(root)
                fingerprint = self._runtime_fingerprint(version)
                installations.append(
                    {
                        "version": version,
                        "installed": True,
                        "commit": commit,
                        "dirty": fingerprint.get("dirty"),
                        "runtime_fingerprint": fingerprint.get("state_digest") or commit,
                        "official_source_url": (
                            f"{repository}/tree/{commit}"
                            if repository and commit and not fingerprint.get("dirty")
                            else ""
                        ),
                    }
                )
            except AICapabilityError as exc:
                installations.append(
                    {
                        "version": version,
                        "installed": False,
                        "commit": "",
                        "official_source_url": "",
                        "status": str(exc),
                    }
                )
        return {"installations": installations}

    def _read_pbgui_help_topic(self, args: dict[str, Any]) -> dict[str, Any]:
        """Read one bounded PBGui help topic in the requested language."""
        topic = str(args.get("topic") or "").strip()
        language = "de" if str(args.get("language") or "en").lower() == "de" else "en"
        if not re.fullmatch(r"[0-9]{2}_[a-z0-9_]+", topic):
            raise AICapabilityError("Invalid PBGui help topic")
        root = Path(PBGDIR) / ("docs/help_de" if language == "de" else "docs/help")
        path = root / f"{topic}.md"
        if path.is_symlink() or not path.is_file():
            raise AICapabilityError("PBGui help topic is unavailable")
        raw = read_regular_file_nofollow(path, root)
        if len(raw) > 256 * 1024:
            raise AICapabilityError("PBGui help topic is too large")
        try:
            content = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AICapabilityError("PBGui help topic is invalid") from exc
        return {"topic": topic, "language": language, "content": content}

    def _search_pbgui_help(self, args: dict[str, Any]) -> dict[str, Any]:
        """Search bounded PBGui help topics without exposing filesystem paths."""
        query = str(args.get("query") or "").strip()
        language = "de" if str(args.get("language") or "en").lower() == "de" else "en"
        if not 2 <= len(query) <= 128 or "\x00" in query:
            raise AICapabilityError("Help query must contain 2 to 128 characters")
        root = Path(PBGDIR) / ("docs/help_de" if language == "de" else "docs/help")
        needle = query.casefold()
        matches = []
        for path in sorted(root.glob("*.md"))[:100]:
            if path.is_symlink():
                continue
            try:
                raw = read_regular_file_nofollow(path, root)
                if len(raw) > 256 * 1024:
                    continue
                lines = raw.decode("utf-8").splitlines()
            except (OSError, RuntimeError, UnicodeDecodeError):
                continue
            for line_number, line in enumerate(lines, start=1):
                if needle in line.casefold():
                    matches.append(
                        {
                            "topic": path.stem,
                            "line": line_number,
                            "excerpt": line.strip()[:500],
                        }
                    )
                    if len(matches) >= 20:
                        return {"language": language, "matches": matches, "truncated": True}
        return {"language": language, "matches": matches, "truncated": False}

    def _search_passivbot_source(self, args: dict[str, Any]) -> dict[str, Any]:
        """Search bounded source text from the exact installed Passivbot checkout."""
        return self._search_passivbot_tree(args, docs_only=False)

    def _search_passivbot_tree(
        self,
        args: dict[str, Any],
        *,
        docs_only: bool,
    ) -> dict[str, Any]:
        """Return bounded literal search matches with commit-pinned source links."""
        version = self._version(args)
        query = str(args.get("query") or "").strip()
        if not 2 <= len(query) <= 128 or "\x00" in query:
            raise AICapabilityError("Search query must contain 2 to 128 characters")
        limit = self._limit(args, maximum=50)
        root = self._passivbot_root(version)
        deadline = time.monotonic() + _SOURCE_SEARCH_TIMEOUT_SECONDS
        commit, repository = self._passivbot_git_info(root)
        checkout_clean = self._checkout_is_clean(root)
        needle = query.casefold()
        matches: list[dict[str, Any]] = []
        scanned_bytes = 0
        scanned_files = 0
        stop_scan = False
        timed_out = False
        for current, directories, filenames in os.walk(root, topdown=True, followlinks=False):
            if time.monotonic() >= deadline:
                timed_out = True
                break
            current_path = Path(current)
            directories[:] = sorted(
                name
                for name in directories
                if name not in _SOURCE_EXCLUDED_PARTS
                and not name.startswith(".")
                and not (current_path / name).is_symlink()
            )
            for filename in sorted(filenames):
                if time.monotonic() >= deadline:
                    timed_out = True
                    stop_scan = True
                    break
                if len(matches) >= limit or scanned_files >= _MAX_SOURCE_FILES:
                    stop_scan = True
                    break
                path = current_path / filename
                try:
                    relative = path.relative_to(root)
                except ValueError:
                    continue
                if path.is_symlink() or not path.is_file() or path.suffix.lower() not in _SOURCE_EXTENSIONS:
                    continue
                if docs_only and not (
                    path.suffix.lower() == ".md"
                    or "docs" in {part.lower() for part in relative.parts}
                    or path.name.lower().startswith("readme")
                ):
                    continue
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                if size > _MAX_SOURCE_FILE_BYTES or scanned_bytes + size > _MAX_SOURCE_SCAN_BYTES:
                    continue
                scanned_files += 1
                scanned_bytes += size
                try:
                    raw = read_regular_file_nofollow(path, root)
                    if len(raw) > _MAX_SOURCE_FILE_BYTES:
                        continue
                    text = raw.decode("utf-8")
                except (OSError, RuntimeError, UnicodeDecodeError):
                    continue
                for line_number, line in enumerate(text.splitlines(), start=1):
                    if needle not in line.casefold():
                        continue
                    matches.append(
                        {
                            "path": relative.as_posix(),
                            "line": line_number,
                            "excerpt": line.strip()[:1000],
                            "source_url": self._source_url(repository, commit, relative)
                            if checkout_clean
                            else "",
                        }
                    )
                    if len(matches) >= limit:
                        break
            if stop_scan:
                break
        return {
            "version": version,
            "commit": commit,
            "matches_runtime": bool(commit) and checkout_clean,
            "source_state": "clean" if checkout_clean else "dirty",
            "matches": matches,
            "returned": len(matches),
            "truncated": timed_out or len(matches) >= limit or scanned_files >= _MAX_SOURCE_FILES,
        }

    def _read_passivbot_source(self, args: dict[str, Any]) -> dict[str, Any]:
        """Read one validated relative source range from an installed checkout."""
        version = self._version(args)
        relative = self._relative_source_path(args.get("path"))
        try:
            start_line = max(1, int(args.get("start_line") or 1))
            end_line = int(args.get("end_line") or start_line + 199)
        except (TypeError, ValueError) as exc:
            raise AICapabilityError("Invalid source line range") from exc
        end_line = min(start_line + 399, max(start_line, end_line))
        root = self._passivbot_root(version)
        target = root.joinpath(*relative.parts)
        self._require_safe_source_path(root, target)
        try:
            if target.stat().st_size > _MAX_SOURCE_FILE_BYTES:
                raise AICapabilityError("Source file is too large")
            raw = read_regular_file_nofollow(target, root)
            if len(raw) > _MAX_SOURCE_FILE_BYTES:
                raise AICapabilityError("Source file is too large")
            lines = raw.decode("utf-8").splitlines()
        except (OSError, RuntimeError, UnicodeDecodeError) as exc:
            raise AICapabilityError("Source file is unavailable") from exc
        selected = lines[start_line - 1 : end_line]
        commit, repository = self._passivbot_git_info(root)
        source_clean = self._source_is_clean(root, relative)
        return {
            "version": version,
            "commit": commit,
            "path": relative.as_posix(),
            "start_line": start_line,
            "end_line": start_line + max(0, len(selected) - 1),
            "content": "\n".join(
                f"{line_number}: {line}"
                for line_number, line in enumerate(selected, start=start_line)
            ),
            "matches_runtime": source_clean and bool(commit),
            "source_state": "clean" if source_clean else "dirty",
            "source_url": self._source_url(repository, commit, relative, start_line, end_line)
            if source_clean
            else "",
        }

    def _validated_draft_payload(
        self,
        owner: str,
        draft_id: str,
        version: str,
        config: dict[str, Any],
        *,
        revision: int,
        created_at: float | None = None,
    ) -> dict[str, Any]:
        """Build a persisted draft even when validation reports actionable errors."""
        candidate = copy.deepcopy(config)
        try:
            if version == "v8":
                prepared = self._validate_pb8_config(f"ai_draft_{draft_id[:8]}", candidate)
                adapter = "pb8_config"
            else:
                from api.pb7_bridge import prepare_override_config

                prepared = prepare_override_config(candidate, verbose=False)
                if not isinstance(prepared, dict):
                    raise AICapabilityError("Installed PB7 validator returned invalid data")
                adapter = "api.pb7_bridge"
            validation = {
                "valid": True,
                "adapter": adapter,
                "runtime_fingerprint": self._runtime_fingerprint(version),
                "errors": [],
            }
            stored_config = prepared
        except Exception as exc:
            if isinstance(exc, HTTPException):
                detail = self._safe_detail(exc.detail)
            elif isinstance(exc, AICapabilityError):
                detail = str(exc)
            else:
                detail = "Installed runtime validation is unavailable"
                _log(SERVICE, f"PB{version[-1]} draft validation failed: {type(exc).__name__}", level="WARNING")
            detail = self._path_free_error(detail)
            validation = {
                "valid": False,
                "adapter": "api.pb7_bridge" if version == "v7" else "pb8_config",
                "runtime_fingerprint": self._runtime_fingerprint(version),
                "errors": [detail[:1000]],
            }
            stored_config = candidate
        now = time.time()
        return {
            "schema_version": 1,
            "id": draft_id,
            "owner": owner,
            "version": version,
            "revision": revision,
            "config": stored_config,
            "digest": self._digest(stored_config),
            "validation": validation,
            "created_at": created_at or now,
            "updated_at": now,
        }

    def _draft_projection(self, payload: dict[str, Any], *, include_config: bool) -> dict[str, Any]:
        """Return a path-free draft projection suitable for a model or browser."""
        version = str(payload.get("version") or "")
        draft_id = str(payload.get("id") or "")
        result = {
            "draft_id": draft_id,
            "resource": f"pbgui://draft/{version}/{draft_id}",
            "version": version,
            "revision": int(payload.get("revision") or 0),
            "digest": str(payload.get("digest") or ""),
            "validation": copy.deepcopy(payload.get("validation") or {}),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
        }
        if include_config:
            result["config"] = self._sanitize_config(payload.get("config") or {})
        return result

    def _load_draft(self, owner: str, draft_id: str) -> dict[str, Any]:
        """Load one regular owner-bound draft file and verify its persisted binding."""
        selected = self._opaque_id(draft_id, "draft")
        path = self._owner_path(self.draft_root, owner, selected)
        if not path.is_file() or path.is_symlink():
            raise AICapabilityError("Draft not found")
        payload = self._read_private_json(path, self.draft_root)
        if payload.get("owner") != owner or payload.get("id") != selected:
            raise AICapabilityError("Draft binding is invalid")
        return payload

    @classmethod
    def _require_safe_draft(cls, config: dict[str, Any]) -> None:
        """Reject secrets and host paths before private model-authored draft persistence."""
        encoded = json.dumps(config, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_CONFIG_BYTES:
            raise AICapabilityError("Draft config is too large")
        for path in cls._sensitive_paths(config):
            if any(part in path.lower() for part in _SENSITIVE_KEY_PARTS) or path == "pbgui":
                raise AICapabilityError("Draft configs cannot contain secrets or PBGui runtime metadata")

    @classmethod
    def _sensitive_paths(cls, value: Any, prefix: str = "") -> list[str]:
        """Return sensitive dotted fields without ever returning their values."""
        if not isinstance(value, dict):
            return []
        paths = []
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if cls._is_sensitive_path(path):
                paths.append(path)
            elif isinstance(item, dict):
                paths.extend(cls._sensitive_paths(item, path))
        return paths

    def _resource_projection(self, kind: str, version: str, item: object) -> dict[str, Any]:
        """Attach an opaque virtual identifier before removing the internal path."""
        if not isinstance(item, dict):
            return {}
        result = self._compact_result(item)
        result["resource"] = self._virtual_uri(kind, version, str(item.get("path") or self._digest(item)))
        return result

    def _resolve_listed_resource(self, kind: str, version: str, resource: str) -> dict[str, Any]:
        """Resolve a virtual resource only by re-enumerating currently managed results."""
        if kind == "optimizer-run":
            if version == "v8":
                from api import optimize_v8 as module
            else:
                from api import optimize_v7 as module
            items = module.list_results(session=object()).get("results", [])
        elif kind == "backtest":
            if version == "v8":
                from api import backtest_v8 as module

                items = module.get_results(limit=0, session=object()).get("results", [])
            else:
                from api import backtest_v7 as module

                items = module.list_results(limit=0, session=object()).get("results", [])
        else:
            raise AICapabilityError("Unsupported virtual resource")
        for item in items[:5000]:
            if isinstance(item, dict) and self._virtual_uri(kind, version, str(item.get("path") or self._digest(item))) == resource:
                return item
        raise AICapabilityError("Virtual resource is no longer available")

    @classmethod
    def _virtual_uri(cls, kind: str, version: str, identity: str) -> str:
        """Build a non-reversible URI that cannot disclose a managed host path."""
        opaque = hashlib.sha256(f"{kind}\0{version}\0{identity}".encode("utf-8")).hexdigest()[:32]
        return f"pbgui://{kind}/{version}/{opaque}"

    @staticmethod
    def _resource_uri(value: object, kind: str, version: str) -> str:
        """Validate one exact virtual resource selector."""
        resource = str(value or "")
        if not re.fullmatch(rf"pbgui://{re.escape(kind)}/{version}/[0-9a-f]{{32}}", resource):
            raise AICapabilityError("Invalid virtual resource")
        return resource

    @staticmethod
    def _bounded_int(value: object, *, default: int, maximum: int) -> int:
        """Parse a positive integer without silently accepting an excessive request."""
        try:
            parsed = int(default if value is None else value)
        except (TypeError, ValueError) as exc:
            raise AICapabilityError("Invalid projection limit") from exc
        if parsed < 0 or parsed > maximum:
            raise AICapabilityError(f"Projection limit must be between 0 and {maximum}")
        return parsed

    @classmethod
    def _metric_projection(cls, value: object) -> dict[str, Any]:
        """Keep bounded finite scalar metrics while dropping paths and nested payloads."""
        if not isinstance(value, dict):
            return {}
        result = {}
        for key, item in value.items():
            if len(result) >= 200 or cls._is_sensitive_path(str(key)):
                continue
            if isinstance(item, bool) or isinstance(item, str):
                result[str(key)] = item if not isinstance(item, str) else item[:500]
            elif isinstance(item, (int, float)) and math.isfinite(float(item)):
                result[str(key)] = item
        return result

    @classmethod
    def _csv_projection(
        cls,
        result_dir: Path,
        filename: str,
        *,
        max_rows: int,
        preferred: tuple[str, ...],
    ) -> dict[str, Any]:
        """Stream and evenly downsample one managed CSV without exposing its path."""
        selected = result_dir / filename
        compressed = result_dir / f"{filename}.gz"
        if not selected.is_file() or selected.is_symlink():
            selected = compressed
        if not selected.is_file() or selected.is_symlink() or max_rows == 0:
            return {"rows": [], "scanned": 0, "truncated": False}
        opener = gzip.open if selected.suffix == ".gz" else open
        reservoir: list[dict[str, Any]] = []
        scanned = 0
        stride = 1
        last_row: dict[str, Any] | None = None
        try:
            with opener(selected, "rt", encoding="utf-8", newline="") as handle:
                for raw in csv.DictReader(handle):
                    scanned += 1
                    if scanned > _MAX_PROJECTION_ROWS:
                        break
                    row = {
                        key: cls._safe_csv_scalar(raw.get(key))
                        for key in preferred
                        if key in raw and not cls._is_sensitive_path(key)
                    }
                    last_row = row
                    if (scanned - 1) % stride == 0:
                        reservoir.append(row)
                    if len(reservoir) > max_rows:
                        reservoir = reservoir[::2]
                        stride *= 2
        except (OSError, EOFError, UnicodeError, csv.Error):
            return {"rows": [], "scanned": 0, "truncated": False, "status": "unavailable"}
        if last_row is not None and reservoir and reservoir[-1] is not last_row:
            if len(reservoir) >= max_rows:
                reservoir[-1] = last_row
            else:
                reservoir.append(last_row)
        return {
            "rows": reservoir,
            "scanned": min(scanned, _MAX_PROJECTION_ROWS),
            "truncated": scanned > len(reservoir),
            "scan_limit_reached": scanned > _MAX_PROJECTION_ROWS,
        }

    @staticmethod
    def _safe_csv_scalar(value: object) -> Any:
        """Return one bounded finite CSV scalar."""
        text = str(value or "")[:500]
        try:
            number = float(text)
            return number if math.isfinite(number) else None
        except ValueError:
            return text

    @staticmethod
    def _path_free_error(value: object) -> str:
        """Return actionable validation text unless it appears to contain a host path."""
        text = str(value or "")[:1000]
        if re.search(r"(?:^|\s)(?:/[^\s]+|[A-Za-z]:\\[^\s]+)", text):
            return "Installed runtime rejected the config; host path details were withheld"
        return text or "Installed runtime rejected the config"

    async def _propose_pb8_optimizer_config(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate a complete PB8 config and create a save or save-and-queue proposal."""
        name = self._name(args.get("name"))
        draft_id = str(args.get("draft_id") or "")
        if draft_id:
            draft = await self._to_thread_uncancellable(self._load_draft, owner, draft_id)
            if draft.get("version") != "v8":
                raise AICapabilityError("Only PB8 drafts can be published")
            if not (draft.get("validation") or {}).get("valid"):
                raise AICapabilityError("PB8 draft is not valid")
            config = copy.deepcopy(draft.get("config"))
            draft_digest = str(draft.get("digest") or "")
        else:
            config = args.get("config")
            draft_digest = ""
        action = str(args.get("action") or "save")
        if action not in {"save", "save_and_queue"}:
            raise AICapabilityError("Unsupported proposal action")
        if not isinstance(config, dict):
            raise AICapabilityError("config must be an object")
        self._require_safe_draft(config)
        prepared = await self._to_thread_uncancellable(self._validate_pb8_config, name, config)
        current, _overrides, _digest = await self._to_thread_uncancellable(
            self._current_pb8_bundle, name
        )
        if isinstance(current, dict):
            self._preserve_protected_config_fields(current, prepared)
        return await self._create_proposal(
            owner,
            conversation_id,
            action,
            name,
            prepared,
            draft_id=draft_id,
            draft_digest=draft_digest,
        )

    async def _propose_pb8_config_patch(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Apply a model-provided JSON Patch to a private snapshot and propose the validated result."""
        name = self._name(args.get("name"))
        operations = args.get("operations")
        action = str(args.get("action") or "save")
        if action not in {"save", "save_and_queue"}:
            raise AICapabilityError("Unsupported proposal action")
        if not isinstance(operations, list) or not 1 <= len(operations) <= 64:
            raise AICapabilityError("operations must contain 1 to 64 JSON Patch operations")
        current, _overrides, _digest = await self._to_thread_uncancellable(
            self._current_pb8_bundle, name
        )
        if not isinstance(current, dict):
            raise AICapabilityError("PB8 config does not exist")
        patched = copy.deepcopy(current)
        for operation in operations:
            self._apply_json_patch_operation(patched, operation)
        self._require_safe_draft(patched)
        prepared = await self._to_thread_uncancellable(self._validate_pb8_config, name, patched)
        self._preserve_protected_config_fields(current, prepared)
        return await self._create_proposal(
            owner, conversation_id, action, name, prepared
        )

    @classmethod
    def _preserve_protected_config_fields(
        cls, original: dict[str, Any], prepared: dict[str, Any]
    ) -> None:
        """Prevent runtime normalization from changing path or sensitive fields not patchable by AI."""
        for key in list(prepared):
            lowered = str(key).lower()
            protected = (
                lowered in _PATH_KEYS
                or lowered.endswith("_path")
                or any(part in lowered for part in _SENSITIVE_KEY_PARTS)
            )
            if protected and key not in original:
                prepared.pop(key, None)
        for key, value in original.items():
            lowered = str(key).lower()
            protected = (
                lowered in _PATH_KEYS
                or lowered.endswith("_path")
                or any(part in lowered for part in _SENSITIVE_KEY_PARTS)
            )
            if protected:
                prepared[key] = copy.deepcopy(value)
            elif isinstance(value, dict) and isinstance(prepared.get(key), dict):
                cls._preserve_protected_config_fields(value, prepared[key])

    @classmethod
    def _apply_json_patch_operation(cls, target: dict[str, Any], operation: object) -> None:
        """Apply one bounded add/replace/remove JSON Patch operation without filesystem effects."""
        if not isinstance(operation, dict) or set(operation) - {"op", "path", "value"}:
            raise AICapabilityError("Invalid JSON Patch operation")
        op = str(operation.get("op") or "")
        path = str(operation.get("path") or "")
        if op not in {"add", "replace", "remove"} or not path.startswith("/") or len(path) > 512:
            raise AICapabilityError("Invalid JSON Patch operation")
        parts = [item.replace("~1", "/").replace("~0", "~") for item in path[1:].split("/")]
        if not parts or len(parts) > 32 or any(not item or cls._is_sensitive_path(item) for item in parts):
            raise AICapabilityError("Unsafe JSON Patch path")
        parent: Any = target
        for part in parts[:-1]:
            if isinstance(parent, dict) and part in parent:
                parent = parent[part]
            elif isinstance(parent, list) and part.isdigit() and int(part) < len(parent):
                parent = parent[int(part)]
            else:
                raise AICapabilityError("JSON Patch path does not exist")
        leaf = parts[-1]
        if isinstance(parent, dict):
            if op in {"replace", "remove"} and leaf not in parent:
                raise AICapabilityError("JSON Patch path does not exist")
            if op == "remove":
                del parent[leaf]
            else:
                parent[leaf] = copy.deepcopy(operation.get("value"))
            return
        if isinstance(parent, list):
            if leaf == "-" and op == "add":
                parent.append(copy.deepcopy(operation.get("value")))
                return
            if not leaf.isdigit():
                raise AICapabilityError("Invalid JSON Patch list index")
            index = int(leaf)
            if op == "add" and 0 <= index <= len(parent):
                parent.insert(index, copy.deepcopy(operation.get("value")))
                return
            if not 0 <= index < len(parent):
                raise AICapabilityError("JSON Patch list index does not exist")
            if op == "remove":
                parent.pop(index)
            else:
                parent[index] = copy.deepcopy(operation.get("value"))
            return
        raise AICapabilityError("JSON Patch parent is not a container")

    async def _propose_queue_pb8_config(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Create an approval proposal to queue one existing PB8 config."""
        name = self._name(args.get("name"))
        return await self._create_proposal(owner, conversation_id, "queue", name, None)

    def _list_pb8_optimizer_queue(self, args: dict[str, Any]) -> dict[str, Any]:
        """List bounded path-free PB8 optimizer queue records for exact follow-up actions."""
        from api import optimize_v8

        limit = self._limit(args, maximum=100)
        items = optimize_v8.get_queue(session=object()).get("items", [])[:limit]
        settings = load_ini_section("optimize_v7")
        return {
            "items": [
                {
                    "queue_id": str(item.get("filename") or ""),
                    "name": str(item.get("name") or ""),
                    "status": str(item.get("status") or "unknown"),
                    "exchanges": list(item.get("exchange") or [])[:10],
                    "created": item.get("created"),
                    "started_at": item.get("started_at"),
                    "error_code": str(item.get("error_code") or ""),
                    "error_reason": self._path_free_error(item.get("error_reason"))
                    if item.get("error_reason")
                    else "",
                }
                for item in items
            ],
            "autostart": str(settings.get("autostart", "False")).lower() == "true",
        }

    async def _propose_start_pb8_optimizer_queue(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Create an exact approval proposal to start existing queued PB8 optimizers."""
        from api import optimize_v8

        raw_ids = args.get("queue_ids")
        if not isinstance(raw_ids, list) or not 1 <= len(raw_ids) <= 4:
            raise AICapabilityError("queue_ids must contain 1 to 4 PB8 queue IDs")
        queue_ids = [optimize_v8._validate_name(str(item or "")) for item in raw_ids]
        if len(set(queue_ids)) != len(queue_ids):
            raise AICapabilityError("PB8 queue IDs must not contain duplicates")
        current = {
            str(item.get("filename") or ""): item
            for item in optimize_v8.get_queue(session=object()).get("items", [])
        }
        jobs = []
        for queue_id in queue_ids:
            item = current.get(queue_id)
            if item is None:
                raise AICapabilityError("PB8 optimizer queue item does not exist")
            if item.get("status") != "queued":
                raise AICapabilityError(
                    f"PB8 optimizer queue item {self._name(item.get('name'))} is already {item.get('status')}"
                )
            jobs.append({"queue_id": queue_id, "name": self._name(item.get("name"))})
        preview = {
            "action": "start_optimize_queue",
            "version": "v8",
            "name": jobs[0]["name"] if len(jobs) == 1 else "PB8 optimizer queue batch",
            "job_count": len(jobs),
            "jobs": copy.deepcopy(jobs),
            "changed_count": len(jobs),
            "changes": [
                {
                    "path": f"queue[{index}].status",
                    "kind": "changed",
                    "item": job["name"],
                    "before": "queued",
                    "after": "running",
                }
                for index, job in enumerate(jobs)
            ],
            "may_start_immediately": True,
        }
        return await self._create_custom_proposal(
            owner,
            conversation_id,
            "start_optimize_queue",
            preview["name"],
            {"jobs": jobs},
            preview,
            expected_digest=self._digest(jobs),
            create_only=False,
        )

    def _list_dashboard_templates(self, unused: dict[str, Any]) -> dict[str, Any]:
        """List dashboard templates and existing names without exposing files."""
        from api import dashboards

        return {
            "templates": dashboards.list_templates(session=object()).get("templates", [])[:100],
            "dashboards": dashboards.list_dashboards(session=object()).get("dashboards", [])[:100],
        }

    @classmethod
    def _dashboard_layout_projection(cls, name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Project raw dashboard keys onto a stable row/column widget contract."""
        rows = max(1, min(10, int(config.get("rows") or 1)))
        cols = max(1, min(2, int(config.get("cols") or 1)))
        prefixes = {
            "BALANCE": "balance", "POSITIONS": "positions", "PNL": "pnl", "ADG": "adg",
            "P+L": "ppl", "INCOME": "income", "TOP": "top_symbols",
        }
        cells = []
        for row in range(1, rows + 1):
            for column in range(1, cols + 1):
                suffix = f"{row}_{column}"
                widget = str(config.get(f"dashboard_type_{suffix}") or "NONE").upper()
                cell: dict[str, Any] = {"row": row, "column": column, "type": widget}
                prefix = prefixes.get(widget)
                if prefix:
                    cell["users"] = list(config.get(f"dashboard_{prefix}_users_{suffix}") or ["ALL"])
                if widget in {"PNL", "ADG", "P+L", "INCOME", "TOP"}:
                    cell["period"] = str(config.get(f"dashboard_{prefix}_period_{suffix}") or "ALL_TIME")
                if widget in {"PNL", "ADG"}:
                    cell["mode"] = str(config.get(f"dashboard_{prefix}_mode_{suffix}") or "bar")
                if widget == "P+L":
                    cell["sum_period"] = str(config.get(f"dashboard_ppl_sum_period_{suffix}") or "DAY")
                if widget == "TOP":
                    cell["top_n"] = int(config.get(f"dashboard_top_symbols_top_{suffix}") or 10)
                if widget == "INCOME":
                    cell["last_n"] = int(config.get(f"dashboard_income_last_{suffix}") or 10)
                    cell["minimum_income"] = float(config.get(f"dashboard_income_filter_{suffix}") or 0)
                if widget == "ORDERS":
                    link = str(config.get(f"dashboard_orders_{suffix}") or "")
                    match = re.fullmatch(r"view_orders_(\d+)_(\d+)", link)
                    if match:
                        cell["positions_row"] = int(match.group(1))
                        cell["positions_column"] = int(match.group(2))
                if f"dashboard_height_{suffix}" in config:
                    cell["height"] = int(config[f"dashboard_height_{suffix}"])
                cells.append(cell)
        return {"name": name, "rows": rows, "columns": cols, "cells": cells}

    def _get_dashboard_layout(self, args: dict[str, Any]) -> dict[str, Any]:
        """Read one existing dashboard through the semantic editor contract."""
        from api import dashboards

        name = self._name(args.get("name"))
        config = dashboards.get_dashboard(name, session=object()).get("config") or {}
        result = self._dashboard_layout_projection(name, config)
        result["widget_types"] = ["NONE", "BALANCE", "PNL", "ADG", "P+L", "INCOME", "TOP", "POSITIONS", "ORDERS"]
        result["periods"] = ["TODAY", "YESTERDAY", "THIS_WEEK", "LAST_WEEK", "THIS_MONTH", "LAST_MONTH", "LAST_7_DAYS", "LAST_30_DAYS", "LAST_90_DAYS", "LAST_180_DAYS", "LAST_365_DAYS", "THIS_YEAR", "LAST_YEAR", "ALL_TIME"]
        result["available_users"] = dashboards.list_users(session=object()).get("users", [])[:500]
        return result

    def _prepare_dashboard_layout(self, args: dict[str, Any]) -> tuple[str, dict[str, Any] | None, dict[str, Any], str | None]:
        """Apply validated semantic cell edits while preserving unrelated dashboard keys."""
        from api import dashboards

        name = self._name(args.get("name"))
        create = bool(args.get("create"))
        exists = name in dashboards.list_dashboards(session=object()).get("dashboards", [])
        if create and exists:
            raise AICapabilityError("Dashboard already exists")
        if not create and not exists:
            raise AICapabilityError("Dashboard does not exist")
        current = dashboards.get_dashboard(name, session=object()).get("config") if exists else None
        prepared = copy.deepcopy(current or {"name": name, "rows": 1, "cols": 1})
        try:
            rows = int(args.get("rows") if args.get("rows") is not None else prepared.get("rows") or 1)
            cols = int(args.get("columns") if args.get("columns") is not None else prepared.get("cols") or 1)
        except (TypeError, ValueError) as exc:
            raise AICapabilityError("Invalid dashboard dimensions") from exc
        if not 1 <= rows <= 10 or not 1 <= cols <= 2:
            raise AICapabilityError("Dashboard layout supports 1-10 rows and 1-2 columns")
        prepared.update({"name": name, "rows": rows, "cols": cols})
        cells = args.get("cells") or []
        if not isinstance(cells, list) or len(cells) > 20:
            raise AICapabilityError("Invalid dashboard cell updates")
        allowed_types = {"NONE", "BALANCE", "PNL", "ADG", "P+L", "INCOME", "TOP", "POSITIONS", "ORDERS"}
        allowed_periods = {"TODAY", "YESTERDAY", "THIS_WEEK", "LAST_WEEK", "LAST_WEEK_NOW", "THIS_MONTH", "LAST_MONTH", "LAST_MONTH_NOW", "LAST_7_DAYS", "LAST_30_DAYS", "LAST_90_DAYS", "LAST_180_DAYS", "LAST_365_DAYS", "THIS_QUARTER", "LAST_QUARTER", "LAST_QUARTER_NOW", "THIS_YEAR", "LAST_YEAR", "LAST_YEAR_NOW", "ALL_TIME"}
        available_users = set(dashboards.list_users(session=object()).get("users", []))
        prefixes = {"BALANCE": "balance", "POSITIONS": "positions", "PNL": "pnl", "ADG": "adg", "P+L": "ppl", "INCOME": "income", "TOP": "top_symbols"}
        requested_types = {}
        for item in cells:
            if isinstance(item, dict) and item.get("type") is not None:
                try:
                    requested_types[(int(item.get("row")), int(item.get("column")))] = str(item["type"]).upper()
                except (TypeError, ValueError):
                    pass
        seen_positions = set()
        for cell in cells:
            if not isinstance(cell, dict):
                raise AICapabilityError("Invalid dashboard cell update")
            try:
                row, column = int(cell.get("row")), int(cell.get("column"))
            except (TypeError, ValueError) as exc:
                raise AICapabilityError("Invalid dashboard cell position") from exc
            if not 1 <= row <= rows or not 1 <= column <= cols:
                raise AICapabilityError("Dashboard cell is outside the selected layout")
            if (row, column) in seen_positions:
                raise AICapabilityError("Dashboard cell update is duplicated")
            seen_positions.add((row, column))
            suffix = f"{row}_{column}"
            type_key = f"dashboard_type_{suffix}"
            old_type = str(prepared.get(type_key) or "NONE").upper()
            widget = str(cell.get("type") or old_type).upper()
            if widget not in allowed_types:
                raise AICapabilityError("Unsupported dashboard widget type")
            if "type" in cell and widget != old_type:
                for key in list(prepared):
                    if key.startswith("dashboard_") and key.endswith(f"_{suffix}") and key != f"dashboard_height_{suffix}":
                        prepared.pop(key, None)
            prepared[type_key] = widget
            prefix = prefixes.get(widget)
            if prefix:
                prepared.setdefault(f"dashboard_{prefix}_users_{suffix}", ["ALL"])
            if widget in {"PNL", "ADG", "P+L", "INCOME", "TOP"}:
                prepared.setdefault(f"dashboard_{prefix}_period_{suffix}", "ALL_TIME")
            if widget in {"PNL", "ADG"}:
                prepared.setdefault(f"dashboard_{prefix}_mode_{suffix}", "bar")
            if widget == "P+L":
                prepared.setdefault(f"dashboard_ppl_sum_period_{suffix}", "DAY")
            if widget == "TOP":
                prepared.setdefault(f"dashboard_top_symbols_top_{suffix}", 10)
            if widget == "INCOME":
                prepared.setdefault(f"dashboard_income_last_{suffix}", 10)
                prepared.setdefault(f"dashboard_income_filter_{suffix}", 0)
            if "users" in cell:
                users = cell.get("users")
                if not isinstance(users, list) or not users or len(users) > 100:
                    raise AICapabilityError("Dashboard widget users are invalid")
                selected_users = [str(user or "").strip() for user in users]
                if any(user != "ALL" and user not in available_users for user in selected_users):
                    raise AICapabilityError("Dashboard widget contains an unknown user")
                if not prefix:
                    raise AICapabilityError("This dashboard widget does not support users")
                prepared[f"dashboard_{prefix}_users_{suffix}"] = selected_users
            if "period" in cell:
                period = str(cell.get("period") or "").upper()
                if widget not in {"PNL", "ADG", "P+L", "INCOME", "TOP"} or period not in allowed_periods:
                    raise AICapabilityError("Dashboard widget period is invalid")
                prepared[f"dashboard_{prefix}_period_{suffix}"] = period
            if "mode" in cell:
                mode = str(cell.get("mode") or "").lower()
                if widget not in {"PNL", "ADG"} or mode not in {"bar", "line"}:
                    raise AICapabilityError("Dashboard widget mode is invalid")
                prepared[f"dashboard_{prefix}_mode_{suffix}"] = mode
            if "sum_period" in cell:
                value = str(cell.get("sum_period") or "").upper()
                if widget != "P+L" or value not in {"DAY", "WEEK", "MONTH"}:
                    raise AICapabilityError("Dashboard P+L sum period is invalid")
                prepared[f"dashboard_ppl_sum_period_{suffix}"] = value
            for field, key_prefix, minimum, maximum in (
                ("top_n", "dashboard_top_symbols_top", 1, 100),
                ("last_n", "dashboard_income_last", 1, 100),
                ("height", "dashboard_height", 120, 2000),
            ):
                if field in cell:
                    try:
                        value = int(cell[field])
                    except (TypeError, ValueError) as exc:
                        raise AICapabilityError(f"Dashboard {field} is invalid") from exc
                    if not minimum <= value <= maximum:
                        raise AICapabilityError(f"Dashboard {field} is outside the supported range")
                    prepared[f"{key_prefix}_{suffix}"] = value
            if "minimum_income" in cell:
                if widget != "INCOME":
                    raise AICapabilityError("Minimum income is only valid for INCOME widgets")
                try:
                    prepared[f"dashboard_income_filter_{suffix}"] = float(cell["minimum_income"])
                except (TypeError, ValueError) as exc:
                    raise AICapabilityError("Minimum income is invalid") from exc
            if "positions_row" in cell or "positions_column" in cell:
                if widget != "ORDERS":
                    raise AICapabilityError("Positions links are only valid for ORDERS widgets")
                try:
                    link_row = int(cell.get("positions_row"))
                    link_column = int(cell.get("positions_column"))
                except (TypeError, ValueError) as exc:
                    raise AICapabilityError("Orders widget link is invalid") from exc
                if not 1 <= link_row <= rows or not 1 <= link_column <= cols:
                    raise AICapabilityError("Orders widget link is outside the selected layout")
                linked_type = requested_types.get(
                    (link_row, link_column), prepared.get(f"dashboard_type_{link_row}_{link_column}")
                )
                if linked_type != "POSITIONS":
                    raise AICapabilityError("Orders widget must link to a POSITIONS cell")
                prepared[f"dashboard_orders_{suffix}"] = f"view_orders_{link_row}_{link_column}"
        for key in list(prepared):
            match = re.fullmatch(r"dashboard_type_(\d+)_(\d+)", key)
            if match and (int(match.group(1)) > rows or int(match.group(2)) > cols):
                suffix = f"{match.group(1)}_{match.group(2)}"
                for candidate in list(prepared):
                    if candidate.endswith(f"_{suffix}"):
                        prepared.pop(candidate, None)
        return name, current, prepared, self._digest(current) if current is not None else None

    async def _propose_dashboard_layout(
        self, owner: str, conversation_id: str, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Propose creating or editing a dashboard through semantic cell operations."""
        name, current, prepared, expected_digest = self._prepare_dashboard_layout(args)
        before_layout = self._dashboard_layout_projection(name, current or {})
        after_layout = self._dashboard_layout_projection(name, prepared)
        changes = []
        if (before_layout["rows"], before_layout["columns"]) != (after_layout["rows"], after_layout["columns"]):
            changes.append({
                "path": "layout.dimensions",
                "kind": "changed" if current is not None else "added",
                "before": {"rows": before_layout["rows"], "columns": before_layout["columns"]},
                "after": {"rows": after_layout["rows"], "columns": after_layout["columns"]},
            })
        before_cells = {(item["row"], item["column"]): item for item in before_layout["cells"]}
        after_cells = {(item["row"], item["column"]): item for item in after_layout["cells"]}
        for cell in args.get("cells") or []:
            position = (int(cell["row"]), int(cell["column"]))
            before_cell = before_cells.get(position, {"row": position[0], "column": position[1], "type": "NONE"})
            after_cell = after_cells[position]
            if before_cell != after_cell:
                changes.append({
                    "path": f"layout.row_{position[0]}.column_{position[1]}",
                    "kind": "changed" if current is not None else "added",
                    "before": before_cell,
                    "after": after_cell,
                })
        if current is None and not changes:
            changes.append({"path": "layout", "kind": "added", "after": after_layout})
        if not changes:
            raise AICapabilityError("Dashboard proposal does not change anything")
        if len(changes) > 200:
            raise AICapabilityError("Dashboard proposal is too broad for safe review")
        preview = {
            "action": "save_dashboard_layout",
            "name": name,
            "create_only": current is None,
            "layout": self._dashboard_layout_projection(name, prepared),
            "changed_count": len(changes),
            "changes": changes,
            "may_start_immediately": False,
        }
        return await self._create_custom_proposal(
            owner,
            conversation_id,
            "save_dashboard_layout",
            name,
            {"dashboard": prepared},
            preview,
            expected_digest=expected_digest,
            create_only=current is None,
        )

    async def _propose_dashboard_from_template(
        self, owner: str, conversation_id: str, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Propose creating one dashboard from an existing validated template."""
        from api import dashboards

        name = self._name(args.get("name"))
        template = self._name(args.get("template"))
        if name in dashboards.list_dashboards(session=object()).get("dashboards", []):
            raise AICapabilityError("Dashboard already exists")
        if template not in dashboards.list_templates(session=object()).get("templates", []):
            raise AICapabilityError("Dashboard template does not exist")
        try:
            template_config = json.loads(dashboards._template_file(template).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AICapabilityError("Dashboard template is unavailable") from exc
        if not isinstance(template_config, dict):
            raise AICapabilityError("Dashboard template is invalid")
        payload = {"template": template, "name": name, "config": template_config}
        preview = {
            "action": "create_dashboard",
            "name": name,
            "template": template,
            "template_digest": self._digest(template_config),
            "changed_count": 1,
            "changes": [{"path": "dashboard", "kind": "added", "after": {"name": name, "template": template}}],
            "may_start_immediately": False,
        }
        return await self._create_custom_proposal(
            owner, conversation_id, "create_dashboard", name, payload, preview
        )

    async def _propose_pareto_backtests(
        self, owner: str, conversation_id: str, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Propose an exact run-bound matrix of PB8 Pareto backtests."""
        version = self._version(args)
        if version != "v8":
            raise AICapabilityError("AI backtest queue proposals currently require PB8")
        run_resource = self._resource_uri(args.get("run_resource"), "optimizer-run", version)
        raw = self._resolve_listed_resource("optimizer-run", version, run_resource)
        resources = args.get("candidate_resources")
        exchanges = args.get("exchanges")
        if not isinstance(resources, list) or not 1 <= len(resources) <= 10:
            raise AICapabilityError("Select between 1 and 10 Pareto candidates")
        if not isinstance(exchanges, list) or not 1 <= len(exchanges) <= 5:
            raise AICapabilityError("Select between 1 and 5 backtest exchanges")
        candidate_resources = [self._resource_uri(item, "pareto", version) for item in resources]
        selected_exchanges = [str(item or "").strip().lower() for item in exchanges]
        if len(set(candidate_resources)) != len(candidate_resources) or len(set(selected_exchanges)) != len(selected_exchanges):
            raise AICapabilityError("Backtest proposal contains duplicates")
        from api import backtest_v8, optimize_v8

        allowed = set(backtest_v8.get_settings(session=object()).get("exchange_options", []))
        if any(exchange not in allowed for exchange in selected_exchanges):
            raise AICapabilityError("Backtest proposal contains an unsupported exchange")
        result_path = str(raw.get("path") or "")
        listed = optimize_v8.list_paretos(
            result_path, scenario="Aggregated", statistic="mean", session=object(), metrics=""
        )
        items_by_resource = {
            self._virtual_uri("pareto", version, str(item.get("path") or "")): item
            for item in listed.get("paretos", [])[:5000]
            if isinstance(item, dict)
        }
        candidates = []
        for resource in candidate_resources:
            item = items_by_resource.get(resource)
            if item is None:
                raise AICapabilityError("One or more Pareto candidates are unavailable for this optimizer run")
            bundle = optimize_v8.get_pareto_file(str(item.get("path") or ""), session=object())
            candidates.append({
                "resource": resource,
                "name": str(item.get("name") or "pareto_backtest")[:128],
                "config": bundle.get("config") or {},
                "override_configs": bundle.get("override_configs") or {},
            })
        payload = {"run_resource": run_resource, "candidates": candidates, "exchanges": selected_exchanges}
        preview = {
            "action": "queue_backtests",
            "name": str(raw.get("name") or raw.get("result") or "Pareto candidates")[:128],
            "candidates": [{"resource": item["resource"], "name": item["name"]} for item in candidates],
            "exchanges": selected_exchanges,
            "job_count": len(candidates) * len(selected_exchanges),
            "changed_count": len(candidates) * len(selected_exchanges),
            "changes": [
                {"path": "backtests", "kind": "added", "item": item["name"], "after": {"exchanges": selected_exchanges}}
                for item in candidates
            ],
            "may_start_immediately": bool(backtest_v8.get_settings(session=object()).get("autostart")),
        }
        return await self._create_custom_proposal(
            owner, conversation_id, "queue_backtests", preview["name"], payload, preview
        )

    async def _propose_python_analysis(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Create an approval-bound proposal for open Python over sanitized JSON."""
        code = args.get("code")
        if not isinstance(code, str) or not code.strip() or "\x00" in code:
            raise AICapabilityError("Python analysis code is required")
        code_bytes = code.encode("utf-8")
        if len(code_bytes) > _MAX_ANALYSIS_CODE_BYTES:
            raise AICapabilityError("Python analysis code is too large")
        analysis_input = self._sanitize_config(args.get("input_data"))
        input_bytes = json.dumps(
            analysis_input, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        if len(input_bytes) > _MAX_ANALYSIS_INPUT_BYTES:
            raise AICapabilityError("Python analysis input is too large after sanitization")
        payload = {"code": code, "input_data": analysis_input}
        preview = {
            "action": "python_analysis",
            "name": "",
            "code": code,
            "code_bytes": len(code_bytes),
            "input_data": analysis_input,
            "input_summary": self._analysis_input_summary(analysis_input, len(input_bytes)),
            "may_start_immediately": False,
        }
        proposal = ActionProposal(
            id=uuid4().hex,
            owner=owner,
            conversation_id=conversation_id,
            action="python_analysis",
            name="Python analysis",
            config=payload,
            expected_digest=None,
            create_only=True,
            preview=preview,
            payload_digest=self._digest({"action": "python_analysis", **payload}),
        )
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            self._cleanup_proposals_unlocked()
            with advisory_file_lock(self.lock_target):
                if self._has_pending_conversation(owner, conversation_id):
                    raise AICapabilityError("This conversation already has a pending action proposal")
                pending_states = {"awaiting_approval", "executing", "approved_recovery"}
                if self._count_owner_status_records(owner, pending_states) >= _MAX_PROPOSALS_PER_OWNER:
                    raise AICapabilityError("Pending proposal limit reached for this owner")
                if self._count_status_records(self.proposal_root, pending_states) >= _MAX_PROPOSALS_GLOBAL:
                    raise AICapabilityError("Global proposal capacity reached")
                self.proposals[proposal.id] = proposal
                self._persist_proposal(proposal, acquire_lock=False)
        return {
            "proposal_id": proposal.id,
            "status": proposal.status,
            "preview": copy.deepcopy(preview),
            "message": "PBGui requires explicit user approval before this analysis is executed.",
        }

    async def _propose_optimizer_run_python_analysis(
        self, owner: str, conversation_id: str, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Bind open Python to the complete sanitized Pareto dataset of one optimizer run."""
        version = self._version(args)
        run_resource = self._resource_uri(args.get("run_resource"), "optimizer-run", version)
        raw = self._resolve_listed_resource("optimizer-run", version, run_resource)
        result_path = str(raw.get("path") or "")
        statistic = str(args.get("statistic") or "mean").strip().lower()
        scenario = str(args.get("scenario") or "Aggregated").strip() or "Aggregated"
        if version == "v8":
            from api import optimize_v8 as module

            listed = module.list_paretos(
                result_path, scenario=scenario, statistic=statistic, session=object(), metrics=""
            )
        else:
            from api import optimize_v7 as module

            listed = module.list_paretos(
                result_path, scenario=scenario, statistic=statistic, session=object()
            )
        candidates = []
        for item in listed.get("paretos", []):
            if not isinstance(item, dict):
                continue
            path = str(item.get("path") or "")
            candidates.append({
                "resource": self._virtual_uri("pareto", version, path),
                "name": str(item.get("name") or "")[:128],
                "metrics": self._sanitize_config(item.get("summary") or {}),
            })
        dataset = {
            "schema_version": 1,
            "kind": "optimizer_run_paretos",
            "version": version,
            "run_resource": run_resource,
            "run": self._compact_result(raw),
            "scenario": scenario,
            "statistic": statistic,
            "candidates": candidates,
        }
        encoded = json.dumps(dataset, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_ANALYSIS_INPUT_BYTES:
            raise AICapabilityError("Complete optimizer dataset is too large for one Python analysis")
        result = await self._propose_python_analysis(
            owner,
            conversation_id,
            {"code": args.get("code"), "input_data": dataset},
        )
        proposal = self.proposals.get(str(result.get("proposal_id") or ""))
        if proposal is None:
            raise AICapabilityError("Python analysis proposal could not be prepared")
        proposal.preview.pop("input_data", None)
        proposal.preview["input_resource"] = {
            "kind": "optimizer_run_paretos",
            "resource": run_resource,
            "version": version,
            "scenario": scenario,
            "statistic": statistic,
            "candidate_count": len(candidates),
            "bytes": len(encoded),
            "digest": self._digest(dataset),
        }
        proposal.preview["input_summary"] = copy.deepcopy(proposal.preview["input_resource"])
        self._persist_proposal(proposal)
        result["preview"] = copy.deepcopy(proposal.preview)
        return result

    async def _propose_workspace_python_analysis(
        self, owner: str, conversation_id: str, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Bind open Python to approved read-only PBGui data and source roots."""
        requested = args.get("roots")
        if not isinstance(requested, list) or not 1 <= len(requested) <= 3:
            raise AICapabilityError("Select at least one Python workspace root")
        roots = [str(item or "").strip() for item in requested]
        allowed = {"pbgui_data", "pb7", "pb8"}
        if any(root not in allowed for root in roots) or len(set(roots)) != len(roots):
            raise AICapabilityError("Python workspace roots are invalid")
        self._python_workspace_mounts(roots)
        input_data = {
            "schema_version": 1,
            "kind": "pbgui_readonly_workspace",
            "mounts": {root: f"/workspace/{root}" for root in roots},
            "access": "read_only",
            "excluded": [
                "credentials, API keys, tokens, passwords, sessions, cookies and SSH material",
                ".env files, private-key/certificate files, Git metadata and virtual environments",
                "all symbolic links",
            ],
        }
        result = await self._propose_python_analysis(
            owner,
            conversation_id,
            {"code": args.get("code"), "input_data": input_data},
        )
        proposal = self.proposals.get(str(result.get("proposal_id") or ""))
        if proposal is None or not isinstance(proposal.config, dict):
            raise AICapabilityError("Python workspace proposal could not be prepared")
        proposal.config["workspace_roots"] = roots
        proposal.preview["workspace"] = copy.deepcopy(input_data)
        proposal.payload_digest = self._digest({"action": "python_analysis", **proposal.config})
        self._persist_proposal(proposal)
        result["preview"] = copy.deepcopy(proposal.preview)
        return result

    async def _get_python_analysis_result(
        self,
        owner: str,
        conversation_id: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Return one durable analysis result only to its owner and conversation."""
        proposal = await self._owned_proposal(owner, str(args.get("proposal_id") or ""))
        if proposal.conversation_id != conversation_id or proposal.action != "python_analysis":
            raise AICapabilityError("Python analysis proposal not found")
        if proposal.result is not None:
            return copy.deepcopy(proposal.result)
        return {"proposal_id": proposal.id, "status": proposal.status}

    @staticmethod
    def _analysis_input_summary(value: Any, encoded_bytes: int) -> dict[str, Any]:
        """Describe the exact sanitized JSON shown with an analysis proposal."""
        if value is None:
            json_type = "null"
        elif isinstance(value, bool):
            json_type = "boolean"
        elif isinstance(value, dict):
            json_type = "object"
        elif isinstance(value, list):
            json_type = "array"
        elif isinstance(value, str):
            json_type = "string"
        else:
            json_type = "number"
        summary = {"type": json_type, "bytes": encoded_bytes}
        if isinstance(value, dict):
            summary["keys"] = list(value.keys())
        elif isinstance(value, list):
            summary["items"] = len(value)
        return summary

    async def _execute_python_analysis(self, proposal: ActionProposal) -> dict[str, Any]:
        """Run approved Python in a fail-closed, bounded bubblewrap sandbox."""
        if not _BWRAP_PATH.is_file() or not os.access(_BWRAP_PATH, os.X_OK):
            raise AICapabilityError("Python analysis sandbox is unavailable")
        if not _PRLIMIT_PATH.is_file() or not os.access(_PRLIMIT_PATH, os.X_OK):
            raise AICapabilityError("Python analysis resource limiter is unavailable")
        payload = proposal.config if isinstance(proposal.config, dict) else {}
        code = payload.get("code")
        if not isinstance(code, str):
            raise AICapabilityError("Python analysis proposal is invalid")
        analysis_input = payload.get("input_data")
        stdin = json.dumps(analysis_input, allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"
        workspace_mounts = self._python_workspace_mounts(payload.get("workspace_roots"))
        runtime_root = Path(sys.prefix).resolve()
        executable = Path(sys.executable)
        if not runtime_root.is_dir() or not executable.is_file():
            raise AICapabilityError("Python analysis runtime is unavailable")
        analysis_root = ensure_private_directory(self.root / "analysis")
        with tempfile.TemporaryDirectory(prefix="run-", dir=analysis_root) as directory:
            script = Path(directory) / "analysis.py"
            atomic_write_private_text(script, code)
            command = self._python_analysis_command(
                runtime_root,
                executable,
                script,
                self._python_analysis_nproc_limit(),
                workspace_mounts,
            )
            start_task = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
                    start_new_session=True,
                )
            )
            try:
                process = await asyncio.shield(start_task)
            except asyncio.CancelledError:
                process = await start_task
                await self._kill_process_group(process)
                raise
            stdout_task = asyncio.create_task(
                self._read_bounded_stream(process.stdout, _MAX_ANALYSIS_STDOUT_BYTES)
            )
            stderr_task = asyncio.create_task(
                self._read_bounded_stream(process.stderr, _MAX_ANALYSIS_STDERR_BYTES)
            )
            timed_out = False
            try:
                await self._write_python_analysis_stdin(process, stdin)
                await asyncio.wait_for(process.wait(), timeout=_ANALYSIS_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                timed_out = True
                await self._kill_process_group(process)
            except asyncio.CancelledError:
                await self._kill_process_group(process)
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                raise
            try:
                stdout, stdout_truncated = await stdout_task
                stderr, stderr_truncated = await stderr_task
            except asyncio.CancelledError:
                await self._kill_process_group(process)
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                raise
            stderr_text = self._redact_analysis_stderr(
                stderr.decode("utf-8", errors="replace"),
                script,
                runtime_root,
            )
        output = self._analysis_output(stdout)
        return {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "python_analysis",
            "analysis_status": "timeout" if timed_out else ("completed" if process.returncode == 0 else "failed"),
            "exit_code": process.returncode,
            "output": output,
            "stderr": stderr_text,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
        }

    @staticmethod
    async def _write_python_analysis_stdin(process: Any, payload: bytes) -> bool:
        """Feed sandbox input while treating an already exited child as a normal failed run."""
        if process.stdin is None:
            return False
        delivered = True
        try:
            process.stdin.write(payload)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            delivered = False
        except RuntimeError as exc:
            if "closed" not in str(exc).lower():
                raise
            delivered = False
        finally:
            try:
                process.stdin.close()
            except (BrokenPipeError, ConnectionResetError, RuntimeError):
                pass
        return delivered

    @staticmethod
    def _resolve_python_workspace_roots(root_ids: object) -> dict[str, Path]:
        """Resolve approved workspace IDs without accepting model-supplied host paths."""
        if not root_ids:
            return {}
        if not isinstance(root_ids, list):
            raise AICapabilityError("Python workspace roots are invalid")
        from pbgui_purefunc import pb7dir, pb8dir

        configured = {
            "pbgui_data": Path(PBGDIR) / "data",
            "pb7": str(pb7dir() or "").strip(),
            "pb8": str(pb8dir() or "").strip(),
        }
        resolved = {}
        for root_id in root_ids:
            if root_id not in configured:
                raise AICapabilityError("Python workspace root is invalid")
            configured_path = configured[root_id]
            if not configured_path:
                raise AICapabilityError(f"Python workspace root {root_id} is unavailable")
            source = Path(configured_path).expanduser()
            if source.is_symlink():
                raise AICapabilityError(f"Python workspace root {root_id} is unavailable")
            try:
                source = source.resolve(strict=True)
            except OSError as exc:
                raise AICapabilityError(f"Python workspace root {root_id} is unavailable") from exc
            if not source.is_dir():
                raise AICapabilityError(f"Python workspace root {root_id} is unavailable")
            resolved[root_id] = source
        return resolved

    @staticmethod
    def _python_workspace_path_denied(path: Path, *, directory: bool) -> bool:
        """Return whether a workspace path may contain authentication or private material."""
        name = path.name.casefold()
        if directory and name in {
            ".git", ".ssh", ".venv", "venv", "node_modules", "credentials", "credential",
            "secrets", "secret", "sessions", "session", "private_keys", "api-keys", "api_keys",
        }:
            return True
        if directory:
            return False
        if name == "pbgui.ini" or name == ".env" or name.startswith(".env."):
            return True
        if path.suffix.casefold() in {".pem", ".key", ".p12", ".pfx", ".crt"}:
            return True
        if name in {"authorized_keys", "known_hosts", "auth.json", "api-keys.json", "api_keys.json"}:
            return True
        return any(
            marker in name
            for marker in (
                "api_key", "apikey", "credential", "password", "private_key", "secret", "token",
                "session", "cookie", "id_rsa", "id_ed25519",
            )
        )

    @classmethod
    def _python_workspace_mounts(cls, root_ids: object) -> list[str]:
        """Build read-only root mounts with sensitive paths and symlinks masked."""
        roots = cls._resolve_python_workspace_roots(root_ids)
        if not roots:
            return []
        mounts = ["--dir", "/workspace"]
        masks: list[tuple[bool, str]] = []
        for root_id, source in roots.items():
            destination_root = f"/workspace/{root_id}"
            mounts.extend(["--ro-bind", str(source), destination_root])
            for current, directories, files in os.walk(source, topdown=True, followlinks=False):
                current_path = Path(current)
                retained_directories = []
                for name in directories:
                    path = current_path / name
                    relative = path.relative_to(source).as_posix()
                    denied = path.is_symlink() or cls._python_workspace_path_denied(path, directory=True)
                    if denied:
                        masks.append((True, f"{destination_root}/{relative}"))
                    else:
                        retained_directories.append(name)
                directories[:] = retained_directories
                for name in files:
                    path = current_path / name
                    if not (path.is_symlink() or cls._python_workspace_path_denied(path, directory=False)):
                        continue
                    relative = path.relative_to(source).as_posix()
                    masks.append((False, f"{destination_root}/{relative}"))
                if len(masks) > 4096:
                    raise AICapabilityError("Python workspace contains too many sensitive paths to mask safely")
        for directory, destination in masks:
            mounts.extend(["--tmpfs", destination] if directory else ["--ro-bind", "/dev/null", destination])
        return mounts

    @staticmethod
    def _python_analysis_command(
        runtime_root: Path,
        executable: Path,
        script: Path,
        nproc_limit: int,
        workspace_mounts: list[str] | None = None,
    ) -> list[str]:
        """Build the fixed argv-only prlimit and bubblewrap sandbox command."""
        mounts = ["--ro-bind", "/usr", "/usr"]
        for system_path in (Path("/lib"), Path("/lib64")):
            if system_path.exists():
                mounts.extend(["--ro-bind", str(system_path), str(system_path)])
        if runtime_root == Path("/usr"):
            sandbox_python = str(executable)
            runtime_mounts: list[str] = []
        else:
            sandbox_python = f"/runtime/bin/{executable.name}"
            runtime_mounts = ["--ro-bind", str(runtime_root), "/runtime"]
        return [
            str(_PRLIMIT_PATH),
            "--as=1610612736:1610612736",
            "--cpu=10:10",
            "--fsize=1048576:1048576",
            "--nofile=64:64",
            f"--nproc={nproc_limit}:{nproc_limit}",
            "--core=0:0",
            str(_BWRAP_PATH),
            "--die-with-parent",
            "--new-session",
            "--unshare-all",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--setenv",
            "HOME",
            "/tmp",
            "--setenv",
            "PATH",
            "/runtime/bin:/usr/bin:/bin",
            "--setenv",
            "PYTHONHASHSEED",
            "0",
            "--setenv",
            "PYTHONDONTWRITEBYTECODE",
            "1",
            "--setenv",
            "OPENBLAS_NUM_THREADS",
            "1",
            "--setenv",
            "OMP_NUM_THREADS",
            "1",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            "/work",
            *mounts,
            *runtime_mounts,
            *(workspace_mounts or []),
            "--ro-bind",
            str(script),
            "/analysis.py",
            "--chdir",
            "/work",
            "--",
            sandbox_python,
            "-I",
            "/analysis.py",
        ]

    @staticmethod
    def _python_analysis_nproc_limit() -> int:
        """Allow at most 64 new tasks above this service user's current baseline."""
        uid = os.getuid()
        current_tasks = 0
        for process in psutil.process_iter(["uids", "num_threads"]):
            try:
                uids = process.info.get("uids")
                if uids is not None and uids.real == uid:
                    current_tasks += max(1, int(process.info.get("num_threads") or 1))
            except (psutil.AccessDenied, psutil.NoSuchProcess, TypeError, ValueError):
                continue
        return max(64, current_tasks + 64)

    @staticmethod
    async def _read_bounded_stream(
        stream: asyncio.StreamReader | None,
        limit: int,
    ) -> tuple[bytes, bool]:
        """Drain a subprocess stream while retaining at most the configured bytes."""
        if stream is None:
            return b"", False
        retained = bytearray()
        truncated = False
        while True:
            chunk = await stream.read(8192)
            if not chunk:
                break
            remaining = limit - len(retained)
            if remaining > 0:
                retained.extend(chunk[:remaining])
            if len(chunk) > remaining:
                truncated = True
        return bytes(retained), truncated

    @staticmethod
    async def _kill_process_group(process: asyncio.subprocess.Process) -> None:
        """Kill and reap one isolated sandbox process group idempotently."""
        if process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        await process.wait()

    @staticmethod
    def _analysis_output(stdout: bytes) -> dict[str, Any]:
        """Return bounded stdout as strict JSON when possible, otherwise text."""
        text = stdout.decode("utf-8", errors="replace")
        stripped = text.strip()
        if stripped:
            try:
                value = json.loads(
                    stripped,
                    parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
                )
                return {"format": "json", "value": value}
            except (json.JSONDecodeError, ValueError):
                pass
        return {"format": "text", "text": text}

    def _redact_analysis_stderr(
        self,
        stderr: str,
        script: Path,
        runtime_root: Path,
    ) -> str:
        """Remove host workspace and runtime paths from returned diagnostics."""
        redacted = stderr
        replacements = {
            str(script): "/analysis.py",
            str(script.parent): "/work",
            str(runtime_root): "/runtime",
            str(self.root): "[private-ai-state]",
            str(Path(PBGDIR)): "[pbgui-root]",
            str(Path.home()): "[host-home]",
        }
        for source in sorted(replacements, key=len, reverse=True):
            if source:
                redacted = redacted.replace(source, replacements[source])
        return redacted

    async def _create_custom_proposal(
        self,
        owner: str,
        conversation_id: str,
        action: str,
        name: str,
        payload: dict[str, Any],
        preview: dict[str, Any],
        *,
        expected_digest: str | None = None,
        create_only: bool = True,
    ) -> dict[str, Any]:
        """Create one immutable approval proposal for a typed non-config action."""
        encoded = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_CONFIG_BYTES:
            raise AICapabilityError("Action proposal is too large")
        proposal = ActionProposal(
            id=uuid4().hex,
            owner=owner,
            conversation_id=conversation_id,
            action=action,
            name=name,
            config=copy.deepcopy(payload),
            expected_digest=expected_digest,
            create_only=create_only,
            preview=copy.deepcopy(preview),
            payload_digest=self._digest({"action": action, "name": name, "payload": payload}),
        )
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            self._cleanup_proposals_unlocked()
            with advisory_file_lock(self.lock_target):
                if self._has_pending_conversation(owner, conversation_id):
                    raise AICapabilityError("This conversation already has a pending action proposal")
                pending_states = {"awaiting_approval", "executing", "approved_recovery"}
                if self._count_owner_status_records(owner, pending_states) >= _MAX_PROPOSALS_PER_OWNER:
                    raise AICapabilityError("Pending proposal limit reached for this owner")
                if self._count_status_records(self.proposal_root, pending_states) >= _MAX_PROPOSALS_GLOBAL:
                    raise AICapabilityError("Global proposal capacity reached")
                self.proposals[proposal.id] = proposal
                self._persist_proposal(proposal, acquire_lock=False)
        return {
            "proposal_id": proposal.id,
            "status": proposal.status,
            "preview": copy.deepcopy(preview),
            "message": "PBGui requires explicit user approval before this action is executed.",
        }

    async def _create_proposal(
        self,
        owner: str,
        conversation_id: str,
        action: str,
        name: str,
        config: dict[str, Any] | None,
        *,
        draft_id: str = "",
        draft_digest: str = "",
    ) -> dict[str, Any]:
        """Bind one validated action to current config state and an immutable digest."""
        current, current_overrides, current_digest = await self._to_thread_uncancellable(
            self._current_pb8_bundle, name
        )
        create_only = current is None
        if action == "save_and_queue" and not create_only:
            raise AICapabilityError("save_and_queue is limited to a new PB8 config")
        if action == "queue" and current is None:
            raise AICapabilityError("PB8 config does not exist")
        if action != "queue" and current_overrides:
            raise AICapabilityError("AI overwrite proposals do not support existing sparse overrides")
        changed = self._changed_paths(current or {}, config or current or {})
        changes = self._changed_entries(current or {}, config or current or {})
        if len(changes) > 200:
            raise AICapabilityError("Proposal changes are too broad for safe review")
        settings = load_ini_section("optimize_v7")
        queue_config = config or current or {}
        queue_summary = self._queue_preview(queue_config, current_overrides)
        preview = {
            "action": action,
            "version": "v8",
            "name": name,
            "create_only": create_only,
            "changed_paths": changed[:100],
            "changed_count": len(changed),
            "changes": changes,
            "changes_truncated": False,
            "queue": queue_summary if action in {"queue", "save_and_queue"} else None,
            "may_start_immediately": action in {"queue", "save_and_queue"}
            and str(settings.get("autostart", "False")).lower() == "true",
        }
        payload_digest = self._digest(
            {
                "action": action,
                "name": name,
                "config": config,
                "expected_digest": current_digest,
                "draft_id": draft_id,
                "draft_digest": draft_digest,
                "queue": queue_summary if action in {"queue", "save_and_queue"} else None,
            }
        )
        proposal = ActionProposal(
            id=uuid4().hex,
            owner=owner,
            conversation_id=conversation_id,
            action=action,
            name=name,
            config=copy.deepcopy(config),
            expected_digest=current_digest,
            create_only=create_only,
            preview=preview,
            payload_digest=payload_digest,
            draft_id=draft_id,
            draft_digest=draft_digest,
        )
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            self._cleanup_proposals_unlocked()
            with advisory_file_lock(self.lock_target):
                if self._has_pending_conversation(owner, conversation_id):
                    raise AICapabilityError("This conversation already has a pending action proposal")
                pending_states = {"awaiting_approval", "executing", "approved_recovery"}
                if self._count_owner_status_records(owner, pending_states) >= _MAX_PROPOSALS_PER_OWNER:
                    raise AICapabilityError("Pending proposal limit reached for this owner")
                if self._count_status_records(self.proposal_root, pending_states) >= _MAX_PROPOSALS_GLOBAL:
                    raise AICapabilityError("Global proposal capacity reached")
                self.proposals[proposal.id] = proposal
                self._persist_proposal(proposal, acquire_lock=False)
        return {
            "proposal_id": proposal.id,
            "status": proposal.status,
            "preview": copy.deepcopy(preview),
            "message": "PBGui requires explicit user approval before this action is executed.",
        }

    def _execute_proposal(self, proposal: ActionProposal) -> dict[str, Any]:
        """Revalidate config state and execute one already approved PB8 action."""
        if proposal.action in {"queue_backtests", "create_dashboard", "save_dashboard_layout", "start_optimize_queue"}:
            journal = self._load_journal(proposal.id) or self._journal_payload(proposal, phase="prepared")
            self._write_private_json(self.journal_root / f"{proposal.id}.json", journal)
            if proposal.action == "queue_backtests":
                result = self._execute_pareto_backtests(proposal)
            elif proposal.action == "create_dashboard":
                result = self._execute_dashboard_create(proposal)
            elif proposal.action == "start_optimize_queue":
                result = self._execute_optimizer_queue_start(proposal, journal)
            else:
                result = self._execute_dashboard_layout_save(proposal)
            self._complete_journal(journal, result)
            return result
        from api import optimize_v8

        if proposal.draft_id:
            draft = self._load_draft(proposal.owner, proposal.draft_id)
            if not hmac.compare_digest(str(draft.get("digest") or ""), proposal.draft_digest):
                raise AICapabilityError("Draft changed after proposal creation")
        journal = self._journal_payload(proposal, phase="prepared")
        existing_journal = self._load_journal(proposal.id)
        if existing_journal:
            journal = existing_journal
        self._write_private_json(self.journal_root / f"{proposal.id}.json", journal)
        with optimize_v8._config_lock():
            current, overrides, digest = self._current_pb8_bundle(proposal.name)
            intended_digest = self._digest({"config": proposal.config, "overrides": {}}) if proposal.config else None
            already_saved = (
                proposal.action in {"save", "save_and_queue"}
                and intended_digest is not None
                and digest == intended_digest
            )
            if digest != proposal.expected_digest and not already_saved:
                raise AICapabilityError("PB8 config changed after proposal creation")
            if proposal.action == "queue":
                assert current is not None
                prepared = optimize_v8.load_pb8_config(optimize_v8._config_file(proposal.name))
            elif already_saved:
                assert current is not None
                prepared = current
                overrides = {}
            else:
                assert proposal.config is not None
                prepared = optimize_v8._save_config_bundle(
                    proposal.name,
                    proposal.config,
                    create_only=proposal.create_only,
                )
                overrides = {}
                journal["phase"] = "config_saved"
                journal["updated_at"] = time.time()
                self._write_private_json(self.journal_root / f"{proposal.id}.json", journal)
        if proposal.action == "queue":
            options = optimize_v8._validate_launch_options(optimize_v8._runtime_options_from_config(prepared))
            queued = optimize_v8._create_queue_record(
                proposal.name,
                prepared,
                options,
                overrides,
                operation_id=proposal.id,
            )
            result = {"proposal_id": proposal.id, "status": "executed", "action": "queue", **queued}
            self._complete_journal(journal, result)
            return result
        if proposal.action == "save":
            result = {
                "proposal_id": proposal.id,
                "status": "executed",
                "action": "save",
                "name": proposal.name,
            }
            self._complete_journal(journal, result)
            return result
        options = optimize_v8._validate_launch_options(optimize_v8._runtime_options_from_config(prepared))
        queued = optimize_v8._create_queue_record(
            proposal.name,
            prepared,
            options,
            {},
            operation_id=proposal.id,
        )
        result = {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "save_and_queue",
            "name": proposal.name,
            **queued,
        }
        self._complete_journal(journal, result)
        return result

    @staticmethod
    def _execute_pareto_backtests(proposal: ActionProposal) -> dict[str, Any]:
        """Queue an idempotent candidate-by-exchange PB8 backtest matrix."""
        from api import backtest_v8

        payload = proposal.config or {}
        candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
        exchanges = payload.get("exchanges") if isinstance(payload.get("exchanges"), list) else []
        queued = []
        for candidate_index, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                raise AICapabilityError("Backtest proposal candidate is invalid")
            for exchange_index, exchange in enumerate(exchanges):
                config = copy.deepcopy(candidate.get("config") or {})
                backtest = config.setdefault("backtest", {})
                if not isinstance(backtest, dict):
                    raise AICapabilityError("Backtest proposal config is invalid")
                backtest["exchanges"] = [exchange]
                operation_id = f"{proposal.id}_{candidate_index}_{exchange_index}"
                response = backtest_v8.add_to_queue(
                    {
                        "name": str(candidate.get("name") or "pareto_backtest"),
                        "config": config,
                        "override_configs": candidate.get("override_configs") or {},
                        "operation_id": operation_id,
                    },
                    session=object(),
                )
                queued.append({"candidate": candidate.get("name"), "exchange": exchange, "filename": response["filename"]})
        return {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "queue_backtests",
            "queued_count": len(queued),
            "queued": queued,
        }

    def _execute_optimizer_queue_start(
        self, proposal: ActionProposal, journal: dict[str, Any]
    ) -> dict[str, Any]:
        """Start exact reviewed PB8 queue records with durable per-item recovery."""
        from api import optimize_v8

        jobs = (proposal.config or {}).get("jobs")
        if not isinstance(jobs, list) or not 1 <= len(jobs) <= 4:
            raise AICapabilityError("PB8 optimizer start proposal is invalid")
        current = {
            str(item.get("filename") or ""): item
            for item in optimize_v8.get_queue(session=object()).get("items", [])
        }
        completed = set(journal.get("started_queue_ids") or [])
        for job in jobs:
            if not isinstance(job, dict):
                raise AICapabilityError("PB8 optimizer start proposal is invalid")
            queue_id = optimize_v8._validate_name(str(job.get("queue_id") or ""))
            expected_name = self._name(job.get("name"))
            item = current.get(queue_id)
            if item is None or self._name(item.get("name")) != expected_name:
                raise AICapabilityError("PB8 optimizer queue changed after proposal creation")
            already_started = (
                queue_id in completed
                or (
                    item.get("status") in {"running", "complete"}
                    and float(item.get("started_at") or 0) >= proposal.created_at
                )
            )
            if item.get("status") != "queued" and not already_started:
                raise AICapabilityError("PB8 optimizer queue changed after proposal creation")
        started = []
        for job in jobs:
            queue_id = str(job["queue_id"])
            if queue_id not in completed:
                item = current[queue_id]
                if item.get("status") == "queued":
                    response = optimize_v8.start_queue_item(queue_id, None, session=object())
                    started.append(
                        {"queue_id": queue_id, "name": str(job["name"]), "pid": response.get("pid")}
                    )
                else:
                    started.append({"queue_id": queue_id, "name": str(job["name"]), "already_started": True})
                completed.add(queue_id)
                journal["started_queue_ids"] = sorted(completed)
                journal["phase"] = "starting_jobs"
                journal["updated_at"] = time.time()
                self._write_private_json(self.journal_root / f"{proposal.id}.json", journal)
            else:
                started.append({"queue_id": queue_id, "name": str(job["name"]), "already_started": True})
        return {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "start_optimize_queue",
            "name": proposal.name,
            "started_count": len(started),
            "started": started,
        }

    @staticmethod
    def _execute_dashboard_create(proposal: ActionProposal) -> dict[str, Any]:
        """Create one dashboard from the exact approved template and name."""
        from api import dashboards

        payload = proposal.config or {}
        if proposal.name in dashboards.list_dashboards(session=object()).get("dashboards", []):
            current = dashboards.get_dashboard(proposal.name, session=object()).get("config")
            if current != payload.get("config"):
                raise AICapabilityError("Dashboard changed after proposal creation")
            return {
                "proposal_id": proposal.id,
                "status": "executed",
                "action": "create_dashboard",
                "name": proposal.name,
                "template": payload.get("template"),
                "idempotent": True,
            }
        response = dashboards.save_dashboard(proposal.name, payload.get("config") or {}, session=object())
        if response.get("status") != "ok":
            raise AICapabilityError("Dashboard could not be created")
        return {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "create_dashboard",
            "name": proposal.name,
            "template": payload.get("template"),
        }

    @classmethod
    def _execute_dashboard_layout_save(cls, proposal: ActionProposal) -> dict[str, Any]:
        """Persist the exact approved dashboard after optimistic concurrency validation."""
        from api import dashboards

        payload = proposal.config or {}
        prepared = payload.get("dashboard") if isinstance(payload.get("dashboard"), dict) else None
        if prepared is None:
            raise AICapabilityError("Dashboard proposal is invalid")
        exists = proposal.name in dashboards.list_dashboards(session=object()).get("dashboards", [])
        current = dashboards.get_dashboard(proposal.name, session=object()).get("config") if exists else None
        intended_digest = cls._digest(prepared)
        if current is not None and hmac.compare_digest(cls._digest(current), intended_digest):
            return {
                "proposal_id": proposal.id,
                "status": "executed",
                "action": "save_dashboard_layout",
                "name": proposal.name,
                "idempotent": True,
            }
        if proposal.create_only and current is not None:
            raise AICapabilityError("Dashboard was created after proposal review")
        current_digest = cls._digest(current) if current is not None else None
        if current_digest != proposal.expected_digest:
            raise AICapabilityError("Dashboard changed after proposal creation")
        response = dashboards.save_dashboard(proposal.name, prepared, session=object())
        if response.get("status") != "ok":
            raise AICapabilityError("Dashboard could not be saved")
        return {
            "proposal_id": proposal.id,
            "status": "executed",
            "action": "save_dashboard_layout",
            "name": proposal.name,
        }

    def _validate_pb8_config(self, name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Run the full no-override PB8 optimize validation pipeline in private staging."""
        from api import optimize_v8

        encoded = json.dumps(config, allow_nan=False).encode("utf-8")
        if len(encoded) > _MAX_CONFIG_BYTES:
            raise AICapabilityError("PB8 config is too large")
        temp_root = ensure_private_directory(self.root / "validation")
        with tempfile.TemporaryDirectory(prefix="proposal-", dir=temp_root) as directory:
            stage = Path(directory)
            normalized = optimize_v8._normalize_config(config, name)
            optimize_v8._write_json(stage / optimize_v8._CONFIG_FILENAME, normalized)
            prepared = optimize_v8.prepare_pb8_config(
                normalized,
                base_config_path=str(stage / optimize_v8._CONFIG_FILENAME),
            )
            if optimize_v8._override_filenames(prepared):
                raise AICapabilityError("AI PB8 proposals do not support sparse override files yet")
            optimize_v8._validate_optimizer_overrides(
                prepared,
                base_config_path=str(stage / optimize_v8._CONFIG_FILENAME),
            )
            optimize_v8._validate_forager_optimize_search_space(prepared)
            optimize_v8._write_json(stage / optimize_v8._CONFIG_FILENAME, prepared)
            optimize_v8.validate_pb8_override_bundle(stage / optimize_v8._CONFIG_FILENAME)
            return prepared

    def _current_pb8_bundle(
        self, name: str
    ) -> tuple[dict[str, Any] | None, dict[str, dict], str | None]:
        """Return current config, sparse overrides, and full bundle digest under lock."""
        from api import optimize_v8

        with optimize_v8._config_lock():
            path = optimize_v8._config_file(name)
            if not path.exists():
                return None, {}, None
            if not path.is_file() or path.is_symlink():
                raise AICapabilityError("PB8 config target is unsafe")
            config = optimize_v8.load_pb8_config(path)
            overrides = optimize_v8._load_override_payloads(config, optimize_v8._config_dir(name))
            return config, overrides, self._digest({"config": config, "overrides": overrides})

    async def _owned_proposal(self, owner: str, proposal_id: str) -> ActionProposal:
        """Resolve one fixed-length owner-bound proposal ID."""
        if len(proposal_id) != 32 or any(char not in "0123456789abcdef" for char in proposal_id):
            raise AICapabilityError("Invalid proposal")
        async with self.state_lock:
            self._load_proposals_for_owner(owner)
            proposal = self.proposals.get(proposal_id)
            if proposal is None or proposal.owner != owner:
                raise AICapabilityError("Proposal not found")
            return proposal

    def _cleanup_proposals_unlocked(self) -> None:
        """Drop expired terminal or pending proposal payloads."""
        expiry = time.time() - _PROPOSAL_TTL_SECONDS
        cutoff = time.time() - _ACTION_RETENTION_SECONDS
        for proposal_id, proposal in list(self.proposals.items()):
            if proposal.status == "awaiting_approval" and proposal.created_at < expiry:
                proposal.status = "expired"
                self._persist_proposal(proposal)
                self._persist_history(proposal)
            if proposal.created_at < cutoff and proposal.status not in {"executing"}:
                self.proposals.pop(proposal_id, None)
                with advisory_file_lock(self.lock_target):
                    self._owner_path(
                        self.proposal_root, proposal.owner, proposal.id
                    ).unlink(missing_ok=True)

    @staticmethod
    def _proposal_projection(proposal: ActionProposal) -> dict[str, Any]:
        """Return a non-secret proposal projection for browser confirmation."""
        return {
            "proposal_id": proposal.id,
            "conversation_id": proposal.conversation_id,
            "status": proposal.status,
            "preview": copy.deepcopy(proposal.preview),
            "payload_digest": proposal.payload_digest,
            "created_at": proposal.created_at,
        }

    def _persist_proposal(self, proposal: ActionProposal, *, acquire_lock: bool = True) -> None:
        """Atomically persist the complete non-secret proposal state for restart recovery."""
        payload = {
            "schema_version": 1,
            "id": proposal.id,
            "owner": proposal.owner,
            "conversation_id": proposal.conversation_id,
            "action": proposal.action,
            "name": proposal.name,
            "config": proposal.config,
            "expected_digest": proposal.expected_digest,
            "create_only": proposal.create_only,
            "preview": proposal.preview,
            "payload_digest": proposal.payload_digest,
            "draft_id": proposal.draft_id,
            "draft_digest": proposal.draft_digest,
            "created_at": proposal.created_at,
            "status": proposal.status,
            "result": proposal.result,
            "updated_at": time.time(),
        }
        path = self._owner_path(self.proposal_root, proposal.owner, proposal.id)
        if acquire_lock:
            with advisory_file_lock(self.lock_target):
                self._write_private_json(path, payload)
        else:
            self._write_private_json(path, payload)

    def _load_proposals_for_owner(self, owner: str) -> None:
        """Load one owner's bounded durable proposals exactly once per service process."""
        if owner in self.loaded_proposal_owners:
            return
        for path in self._owner_files(self.proposal_root, owner)[:_MAX_ACTION_HISTORY_PER_OWNER]:
            try:
                payload = self._read_private_json(path, self.proposal_root)
                proposal_id = self._opaque_id(payload.get("id"), "proposal")
                if payload.get("owner") != owner:
                    continue
                status = str(payload.get("status") or "")
                revived = (
                    status == "expired"
                    and time.time() - float(payload.get("created_at") or 0) <= _PROPOSAL_TTL_SECONDS
                )
                if revived:
                    status = "awaiting_approval"
                if status == "executing":
                    status = (
                        "interrupted"
                        if payload.get("action") == "python_analysis"
                        else "approved_recovery"
                    )
                self.proposals[proposal_id] = ActionProposal(
                    id=proposal_id,
                    owner=owner,
                    conversation_id=self._opaque_id(payload.get("conversation_id"), "conversation"),
                    action=str(payload.get("action") or ""),
                    name=self._name(payload.get("name")),
                    config=payload.get("config") if isinstance(payload.get("config"), dict) else None,
                    expected_digest=payload.get("expected_digest"),
                    create_only=bool(payload.get("create_only")),
                    preview=payload.get("preview") if isinstance(payload.get("preview"), dict) else {},
                    payload_digest=str(payload.get("payload_digest") or ""),
                    draft_id=str(payload.get("draft_id") or ""),
                    draft_digest=str(payload.get("draft_digest") or ""),
                    created_at=float(payload.get("created_at") or 0),
                    status=status,
                    result=payload.get("result") if isinstance(payload.get("result"), dict) else None,
                )
                self._repair_pending_protected_fields(self.proposals[proposal_id])
                if revived:
                    self._persist_proposal(self.proposals[proposal_id])
                    with advisory_file_lock(self.lock_target):
                        self._owner_path(self.history_root, owner, proposal_id).unlink(missing_ok=True)
            except (AICapabilityError, OSError, RuntimeError, TypeError, ValueError):
                continue
        self.loaded_proposal_owners.add(owner)

    def _repair_pending_protected_fields(self, proposal: ActionProposal) -> None:
        """Migrate only protected normalization side effects in a still-current pending proposal."""
        if (
            proposal.status != "awaiting_approval"
            or proposal.action not in {"save", "save_and_queue"}
            or not isinstance(proposal.config, dict)
        ):
            return
        current, current_overrides, current_digest = self._current_pb8_bundle(proposal.name)
        if not isinstance(current, dict) or current_digest != proposal.expected_digest:
            return
        before = copy.deepcopy(proposal.config)
        self._preserve_protected_config_fields(current, proposal.config)
        changes = self._changed_entries(current, proposal.config)
        changed_paths = self._changed_paths(current, proposal.config)
        if (
            before == proposal.config
            and proposal.preview.get("changes") == changes
            and proposal.preview.get("changed_paths") == changed_paths[:100]
        ):
            return
        queue_summary = (
            self._queue_preview(proposal.config, current_overrides)
            if proposal.action == "save_and_queue"
            else None
        )
        proposal.preview.update(
            {
                "changed_paths": changed_paths[:100],
                "changed_count": len(changed_paths),
                "changes": changes,
                "queue": queue_summary,
            }
        )
        proposal.payload_digest = self._digest(
            {
                "action": proposal.action,
                "name": proposal.name,
                "config": proposal.config,
                "expected_digest": proposal.expected_digest,
                "draft_id": proposal.draft_id,
                "draft_digest": proposal.draft_digest,
                "queue": queue_summary,
            }
        )
        self._persist_proposal(proposal)

    def _persist_history(self, proposal: ActionProposal, *, error: str = "") -> None:
        """Write a durable path-free terminal action record and prune oldest owner records."""
        payload = {
            "schema_version": 1,
            "proposal_id": proposal.id,
            "owner": proposal.owner,
            "conversation_id": proposal.conversation_id,
            "action": proposal.action,
            "name": proposal.name,
            "status": proposal.status,
            "preview": proposal.preview,
            "result": proposal.result,
            "error": self._path_free_error(error) if error else "",
            "created_at": proposal.created_at,
            "updated_at": time.time(),
        }
        with advisory_file_lock(self.lock_target):
            self._write_private_json(
                self._owner_path(self.history_root, proposal.owner, proposal.id), payload
            )
            files = self._owner_files(self.history_root, proposal.owner)
            for path in sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)[
                _MAX_ACTION_HISTORY_PER_OWNER:
            ]:
                path.unlink(missing_ok=True)

    def _journal_payload(self, proposal: ActionProposal, *, phase: str) -> dict[str, Any]:
        """Build an approved action journal sufficient for idempotent startup recovery."""
        return {
            "schema_version": 1,
            "proposal_id": proposal.id,
            "owner": proposal.owner,
            "conversation_id": proposal.conversation_id,
            "action": proposal.action,
            "name": proposal.name,
            "config": proposal.config,
            "expected_digest": proposal.expected_digest,
            "create_only": proposal.create_only,
            "preview": proposal.preview,
            "payload_digest": proposal.payload_digest,
            "draft_id": proposal.draft_id,
            "draft_digest": proposal.draft_digest,
            "created_at": proposal.created_at,
            "phase": phase,
            "updated_at": time.time(),
        }

    def _load_journal(self, proposal_id: str) -> dict[str, Any]:
        """Read one regular journal when it exists."""
        path = self.journal_root / f"{self._opaque_id(proposal_id, 'proposal')}.json"
        if not path.is_file() or path.is_symlink():
            return {}
        return self._read_private_json(path, self.journal_root)

    def _complete_journal(self, journal: dict[str, Any], result: dict[str, Any]) -> None:
        """Mark one staged action complete only after all durable effects exist."""
        journal["phase"] = "completed"
        journal["result"] = result
        journal["updated_at"] = time.time()
        self._write_private_json(
            self.journal_root / f"{self._opaque_id(journal.get('proposal_id'), 'proposal')}.json",
            journal,
        )

    def _recover_journals(self) -> None:
        """Replay approved incomplete PB8 actions using queue idempotency keys."""
        for path in sorted(self.journal_root.glob("*.json"))[:_MAX_PROPOSALS_GLOBAL]:
            if path.is_symlink() or not path.is_file():
                continue
            try:
                payload = self._read_private_json(path, self.journal_root)
                if payload.get("phase") == "completed":
                    continue
                proposal = ActionProposal(
                    id=self._opaque_id(payload.get("proposal_id"), "proposal"),
                    owner=self._opaque_id(payload.get("owner"), "owner"),
                    conversation_id=self._opaque_id(payload.get("conversation_id"), "conversation"),
                    action=str(payload.get("action") or ""),
                    name=self._name(payload.get("name")),
                    config=payload.get("config") if isinstance(payload.get("config"), dict) else None,
                    expected_digest=payload.get("expected_digest"),
                    create_only=bool(payload.get("create_only")),
                    preview=payload.get("preview") if isinstance(payload.get("preview"), dict) else {},
                    payload_digest=str(payload.get("payload_digest") or ""),
                    draft_id=str(payload.get("draft_id") or ""),
                    draft_digest=str(payload.get("draft_digest") or ""),
                    created_at=float(payload.get("created_at") or 0),
                    status="executing",
                )
                result = self._execute_proposal(proposal)
                proposal.status = "executed"
                proposal.result = result
                self._persist_proposal(proposal)
                self._persist_history(proposal)
            except Exception as exc:
                _log(SERVICE, f"AI staged action recovery remains blocked: {type(exc).__name__}", level="ERROR")

    def _interrupt_python_analyses(self) -> None:
        """Record sandbox work interrupted by an ungraceful prior API exit."""
        with advisory_file_lock(self.lock_target):
            for directory in self.proposal_root.iterdir():
                if not directory.is_dir() or directory.is_symlink():
                    continue
                for path in directory.glob("*.json"):
                    if path.is_symlink() or not path.is_file():
                        continue
                    try:
                        payload = self._read_private_json(path, self.proposal_root)
                        if payload.get("action") != "python_analysis" or payload.get("status") not in {
                            "executing",
                            "approved_recovery",
                        }:
                            continue
                        payload["status"] = "interrupted"
                        payload["result"] = None
                        payload["updated_at"] = time.time()
                        self._write_private_json(path, payload)
                        history = {
                            "schema_version": 1,
                            "proposal_id": payload.get("id"),
                            "owner": payload.get("owner"),
                            "conversation_id": payload.get("conversation_id"),
                            "action": "python_analysis",
                            "name": "Python analysis",
                            "status": "interrupted",
                            "preview": payload.get("preview") if isinstance(payload.get("preview"), dict) else {},
                            "result": None,
                            "error": "API stopped before Python analysis completed",
                            "created_at": payload.get("created_at"),
                            "updated_at": time.time(),
                        }
                        self._write_private_json(
                            self._owner_path(
                                self.history_root,
                                self._opaque_id(payload.get("owner"), "owner"),
                                self._opaque_id(payload.get("id"), "proposal"),
                            ),
                            history,
                        )
                        history_files = self._owner_files(
                            self.history_root,
                            self._opaque_id(payload.get("owner"), "owner"),
                        )
                        for stale in sorted(
                            history_files,
                            key=lambda item: item.stat().st_mtime,
                            reverse=True,
                        )[_MAX_ACTION_HISTORY_PER_OWNER:]:
                            stale.unlink(missing_ok=True)
                    except (AICapabilityError, OSError, RuntimeError, TypeError, ValueError):
                        continue

    def _prune_durable_state(self) -> None:
        """Remove old terminal proposals and completed journals without touching pending work."""
        cutoff = time.time() - _ACTION_RETENTION_SECONDS
        with advisory_file_lock(self.lock_target):
            for directory in self.proposal_root.iterdir():
                if not directory.is_dir() or directory.is_symlink():
                    continue
                for path in directory.glob("*.json"):
                    if path.is_symlink() or not path.is_file():
                        continue
                    try:
                        payload = self._read_private_json(path, self.proposal_root)
                        terminal = payload.get("status") in {
                            "executed",
                            "failed",
                            "rejected",
                            "expired",
                            "cancelled",
                            "interrupted",
                        }
                        stale = float(payload.get("updated_at") or payload.get("created_at") or 0) < cutoff
                        if terminal and stale:
                            path.unlink(missing_ok=True)
                    except (AICapabilityError, OSError, TypeError, ValueError):
                        continue
            for path in self.journal_root.glob("*.json"):
                if path.is_symlink() or not path.is_file():
                    continue
                try:
                    payload = self._read_private_json(path, self.journal_root)
                    if payload.get("phase") == "completed" and float(payload.get("updated_at") or 0) < cutoff:
                        path.unlink(missing_ok=True)
                except (AICapabilityError, OSError, TypeError, ValueError):
                    continue

    @staticmethod
    def _opaque_id(value: object, label: str) -> str:
        """Validate one fixed-length lowercase opaque identifier."""
        selected = str(value or "")
        if len(selected) != 32 or any(char not in "0123456789abcdef" for char in selected):
            raise AICapabilityError(f"Invalid {label}")
        return selected

    def _owner_path(self, root: Path, owner: str, item_id: str) -> Path:
        """Resolve one owner-bound JSON path below a private state root."""
        selected_owner = self._opaque_id(owner, "owner")
        selected_id = self._opaque_id(item_id, "item")
        directory = ensure_private_directory(root / selected_owner)
        return directory / f"{selected_id}.json"

    def _owner_files(self, root: Path, owner: str) -> list[Path]:
        """List regular JSON state files for one exact owner directory."""
        selected_owner = self._opaque_id(owner, "owner")
        directory = root / selected_owner
        if not directory.is_dir() or directory.is_symlink():
            return []
        return [
            path
            for path in directory.glob("*.json")
            if path.is_file() and not path.is_symlink() and re.fullmatch(r"[0-9a-f]{32}\.json", path.name)
        ]

    @staticmethod
    def _count_private_files(root: Path) -> int:
        """Count bounded regular state records for global capacity enforcement."""
        count = 0
        for directory in root.iterdir():
            if not directory.is_dir() or directory.is_symlink():
                continue
            count += sum(1 for path in directory.glob("*.json") if path.is_file() and not path.is_symlink())
            if count >= max(_MAX_DRAFTS_GLOBAL, _MAX_PROPOSALS_GLOBAL):
                break
        return count

    def _count_status_records(self, root: Path, statuses: set[str]) -> int:
        """Count durable records in selected states for cross-owner global limits."""
        count = 0
        for directory in root.iterdir():
            if not directory.is_dir() or directory.is_symlink():
                continue
            for path in directory.glob("*.json"):
                if path.is_symlink() or not path.is_file():
                    continue
                try:
                    if self._read_private_json(path, root).get("status") in statuses:
                        count += 1
                except AICapabilityError:
                    count += 1
                if count >= _MAX_PROPOSALS_GLOBAL:
                    return count
        return count

    def _count_owner_status_records(self, owner: str, statuses: set[str]) -> int:
        """Count one owner's durable pending records while the shared lock is held."""
        count = 0
        for path in self._owner_files(self.proposal_root, owner):
            try:
                if self._read_private_json(path, self.proposal_root).get("status") in statuses:
                    count += 1
            except AICapabilityError:
                count += 1
        return count

    def _has_pending_conversation(self, owner: str, conversation_id: str) -> bool:
        """Enforce one pending proposal per conversation across API processes."""
        for path in self._owner_files(self.proposal_root, owner):
            try:
                payload = self._read_private_json(path, self.proposal_root)
            except AICapabilityError:
                continue
            if (
                payload.get("conversation_id") == conversation_id
                and payload.get("status") == "awaiting_approval"
            ):
                return True
        return False

    @staticmethod
    def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
        """Atomically write one owner-only finite JSON state file."""
        atomic_write_private_text(path, json.dumps(payload, indent=4, allow_nan=False) + "\n")

    @staticmethod
    def _read_private_json(path: Path, root: Path) -> dict[str, Any]:
        """Read one bounded no-follow private JSON object."""
        raw = read_regular_file_nofollow(path, root)
        if len(raw) > _MAX_CONFIG_BYTES * 2:
            raise AICapabilityError("AI state record is too large")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AICapabilityError("AI state record is invalid") from exc
        if not isinstance(payload, dict):
            raise AICapabilityError("AI state record is invalid")
        return payload

    def _capability_descriptors(self) -> list[CapabilityDescriptor]:
        """Build descriptors dynamically against the currently installed runtimes."""
        fingerprints = self._runtime_fingerprints()
        descriptors = []
        for item in self._tool_specs():
            versions = set(item.get("versions") or ())
            if not versions and "version" in (item.get("schema", {}).get("properties") or {}):
                versions = {"v7", "v8"}
            if versions and not any(fingerprints.get(version, {}).get("installed") for version in versions):
                continue
            descriptors.append(
                CapabilityDescriptor(
                    name=item["name"],
                    description=item["description"],
                    schema=item["schema"],
                    effect=item.get("effect", "read"),
                    resources=tuple(item.get("resources") or ()),
                )
            )
        return descriptors

    def _tool_specs(self) -> list[dict[str, Any]]:
        """Return the dynamic model-visible capability catalog."""
        version_schema = {"type": "string", "enum": ["v7", "v8"]}
        return [
            {
                "name": "get_capability_registry",
                "description": "Discover current PBGui capabilities, effect classes, limits, virtual resources, and runtime fingerprints.",
                "schema": self._object_schema({}, []),
            },
            {
                "name": "list_optimizer_configs",
                "description": "List managed PB7 or PB8 optimizer configuration summaries.",
                "schema": AICapabilityService._object_schema(
                    {"version": version_schema, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
                    ["version"],
                ),
            },
            {
                "name": "get_optimizer_config",
                "description": "Read one managed optimizer config by version and name.",
                "schema": AICapabilityService._object_schema(
                    {"version": version_schema, "name": {"type": "string", "maxLength": 128}},
                    ["version", "name"],
                ),
            },
            {
                "name": "get_optimizer_metadata",
                "description": "Load current runtime optimizer template, bounds, metrics, and strategies.",
                "schema": AICapabilityService._object_schema({"version": version_schema}, ["version"]),
            },
            {
                "name": "preview_pb8_scenario_template",
                "description": "Preview deterministic PB8 rolling-window, walk-forward, or sweep-cycle training and holdout scenarios without saving or queueing anything. Holdout scenarios are never included in the training scenario list.",
                "schema": self._object_schema(
                    {
                        "template": {
                            "type": "string",
                            "enum": ["rolling_windows", "walk_forward", "sweep_cycles"],
                        },
                        "start_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "end_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "window_days": {"type": "integer", "minimum": 1, "maximum": 3650},
                        "stride_days": {"type": "integer", "minimum": 1, "maximum": 3650},
                        "training_windows": {"type": "integer", "minimum": 1, "maximum": 48},
                        "holdout_windows": {"type": "integer", "minimum": 0, "maximum": 16},
                        "exchange_mode": {"type": "string", "enum": ["inherit", "per_exchange"]},
                        "exchanges": {
                            "type": "array",
                            "maxItems": 8,
                            "items": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,40}$"},
                        },
                        "balance_multiplier": {"type": "number", "minimum": 1.01, "maximum": 100},
                        "starting_balance": {"type": "number", "minimum": 1, "maximum": 1000000000},
                        "refill_cost": {"type": "number", "minimum": 0, "maximum": 1000000000},
                        "cooldown_days": {"type": "integer", "minimum": 0, "maximum": 3650},
                    },
                    ["template", "start_date", "end_date", "window_days", "stride_days", "training_windows"],
                ),
                "effect": "read",
                "versions": ["v8"],
            },
            {
                "name": "list_optimizer_runs",
                "description": "List recent optimizer result summaries without filesystem paths.",
                "schema": AICapabilityService._object_schema(
                    {"version": version_schema, "limit": {"type": "integer", "minimum": 1, "maximum": 50}},
                    ["version"],
                ),
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}"],
            },
            {
                "name": "list_pb8_optimizer_queue",
                "description": "List current PB8 optimizer queue IDs, names, statuses, exchanges, errors, and autostart state before queue follow-up actions.",
                "schema": self._object_schema(
                    {"limit": {"type": "integer", "minimum": 1, "maximum": 100}}, []
                ),
                "resources": ["pbgui://optimizer-queue/v8/{opaque-id}"],
                "versions": ["v8"],
            },
            {
                "name": "list_backtests",
                "description": "List recent completed backtest summaries without filesystem paths.",
                "schema": AICapabilityService._object_schema(
                    {"version": version_schema, "limit": {"type": "integer", "minimum": 1, "maximum": 50}},
                    ["version"],
                ),
                "resources": ["pbgui://backtest/{version}/{opaque-id}"],
            },
            {
                "name": "list_dashboard_templates",
                "description": "List existing PBGui dashboard templates and dashboard names before proposing creation.",
                "schema": self._object_schema({}, []),
            },
            {
                "name": "get_dashboard_layout",
                "description": "Read one existing dashboard as a semantic row, column, widget, user, period, mode, and linkage layout suitable for precise edits.",
                "schema": self._object_schema(
                    {"name": {"type": "string", "maxLength": 128}}, ["name"]
                ),
            },
            {
                "name": "get_optimizer_run_analysis",
                "description": "Read bounded Pareto candidates and metrics from an optimizer-run virtual resource.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "resource": {"type": "string", "maxLength": 128},
                        "scenario": {"type": "string", "maxLength": 128},
                        "statistic": {"type": "string", "maxLength": 32},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                    },
                    ["version", "resource"],
                ),
                "effect": "analyze",
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}", "pbgui://pareto/{version}/{opaque-id}"],
            },
            {
                "name": "rank_optimizer_run_candidates",
                "description": "Scan and rank every Pareto candidate using explicit weighted min/max metrics and optional thresholds. Qualitative goals require user clarification first. If strict thresholds yield zero matches, alternatives are returned only as relaxed_suggestions with required_user_clarification and must not be selected automatically.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "resource": {"type": "string", "maxLength": 128},
                        "scenario": {"type": "string", "maxLength": 128},
                        "statistic": {"type": "string", "maxLength": 32},
                        "criteria": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 12,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "metric": {"type": "string", "maxLength": 128},
                                    "direction": {"type": "string", "enum": ["min", "max"]},
                                    "weight": {"type": "number", "exclusiveMinimum": 0, "maximum": 100},
                                    "minimum": {"type": "number"},
                                    "maximum": {"type": "number"},
                                },
                                "required": ["metric", "direction"],
                                "additionalProperties": False,
                            },
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                    },
                    ["version", "resource", "criteria"],
                ),
                "effect": "analyze",
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}", "pbgui://pareto/{version}/{opaque-id}"],
            },
            {
                "name": "get_pareto_candidate",
                "description": "Read one run-bound Pareto candidate config and metrics without filesystem paths.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "run_resource": {"type": "string", "maxLength": 128},
                        "candidate_resource": {"type": "string", "maxLength": 128},
                    },
                    ["version", "run_resource", "candidate_resource"],
                ),
                "effect": "analyze",
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}", "pbgui://pareto/{version}/{opaque-id}"],
            },
            {
                "name": "select_pareto_candidates",
                "description": "Select exact Pareto candidates in the currently open PBGui Optimize table. This is a reversible browser action and does not modify files or start jobs.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "run_resource": {"type": "string", "maxLength": 128},
                        "candidate_resources": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 100,
                            "items": {"type": "string", "maxLength": 128},
                        },
                        "mode": {"type": "string", "enum": ["replace", "add"]},
                    },
                    ["version", "run_resource", "candidate_resources"],
                ),
                "effect": "ui",
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}", "pbgui://pareto/{version}/{opaque-id}"],
            },
            {
                "name": "select_backtest_results",
                "description": "Select 2-20 exact managed backtest result resources in the currently open matching PB7/PB8 Backtest page and open its existing Results Compare chart. Use this instead of generic control clicks.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "resources": {
                            "type": "array",
                            "minItems": 2,
                            "maxItems": 20,
                            "items": {"type": "string", "maxLength": 128},
                        },
                    },
                    ["version", "resources"],
                ),
                "effect": "ui",
                "resources": ["pbgui://backtest/{version}/{opaque-id}"],
            },
            {
                "name": "perform_page_action",
                "description": "Run one reversible action explicitly advertised in the current PBGui page context for an exact visible entity. Use the page_key, action id, entity_kind, and entity_name exactly as advertised. This only controls existing PBGui UI functions; it cannot execute arbitrary JavaScript or read data for the model.",
                "schema": self._object_schema(
                    {
                        "page_key": {"type": "string", "maxLength": 128},
                        "action": {"type": "string", "maxLength": 64},
                        "entity_kind": {"type": "string", "maxLength": 128},
                        "entity_name": {"type": "string", "maxLength": 128},
                        "value": {"type": "string", "maxLength": 1000},
                    },
                    ["page_key", "action", "entity_kind", "entity_name"],
                ),
                "effect": "ui",
            },
            {
                "name": "present_user_choices",
                "description": "Present 2-5 concise clickable quick replies when a clarification is genuinely required. The selected value is submitted as the user's next message.",
                "schema": self._object_schema(
                    {
                        "question": {"type": "string", "maxLength": 500},
                        "choices": {
                            "type": "array",
                            "minItems": 2,
                            "maxItems": 5,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": {"type": "string", "maxLength": 80},
                                    "value": {"type": "string", "maxLength": 1000},
                                },
                                "required": ["label", "value"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    ["question", "choices"],
                ),
                "effect": "ui",
            },
            {
                "name": "get_backtest_projection",
                "description": "Read bounded scalar metrics, downsampled equity, and redacted fills from a backtest virtual resource.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "resource": {"type": "string", "maxLength": 128},
                        "max_points": {"type": "integer", "minimum": 0, "maximum": 1000},
                        "max_fills": {"type": "integer", "minimum": 0, "maximum": 200},
                    },
                    ["version", "resource"],
                ),
                "effect": "analyze",
                "resources": ["pbgui://backtest/{version}/{opaque-id}"],
            },
            {
                "name": "list_config_drafts",
                "description": "List private persistent PB7/PB8 config drafts owned by this user.",
                "schema": self._object_schema({"version": version_schema}, []),
                "effect": "read",
                "resources": ["pbgui://draft/{version}/{opaque-id}"],
            },
            {
                "name": "get_config_draft",
                "description": "Read one private config draft with its revision and latest runtime validation.",
                "schema": self._object_schema({"draft_id": {"type": "string", "minLength": 32, "maxLength": 32}}, ["draft_id"]),
                "effect": "read",
                "resources": ["pbgui://draft/{version}/{opaque-id}"],
            },
            {
                "name": "create_config_draft",
                "description": "Create a private non-production PB7/PB8 config draft and validate it with the installed runtime adapter.",
                "schema": self._object_schema({"version": version_schema, "config": {"type": "object"}}, ["version", "config"]),
                "effect": "draft",
                "resources": ["pbgui://draft/{version}/{opaque-id}"],
            },
            {
                "name": "update_config_draft",
                "description": "Replace one private draft at an exact revision and rerun installed-runtime validation.",
                "schema": self._object_schema(
                    {
                        "draft_id": {"type": "string", "minLength": 32, "maxLength": 32},
                        "expected_revision": {"type": "integer", "minimum": 1},
                        "config": {"type": "object"},
                    },
                    ["draft_id", "expected_revision", "config"],
                ),
                "effect": "draft",
                "resources": ["pbgui://draft/{version}/{opaque-id}"],
            },
            {
                "name": "propose_pb8_optimizer_config",
                "description": "Validate a complete PB8 optimizer config and propose save or save-and-queue. Never executes without user approval.",
                "schema": AICapabilityService._object_schema(
                    {
                        "name": {"type": "string", "maxLength": 128},
                        "config": {"type": "object"},
                        "draft_id": {"type": "string", "minLength": 32, "maxLength": 32},
                        "action": {"type": "string", "enum": ["save", "save_and_queue"]},
                    },
                    ["name", "action"],
                ),
                "effect": "write",
                "versions": ["v8"],
            },
            {
                "name": "propose_pb8_config_patch",
                "description": "Apply arbitrary bounded JSON Patch operations to an existing PB8 config snapshot, validate the result, and create an exact save preview. Never writes without approval.",
                "schema": self._object_schema(
                    {
                        "name": {"type": "string", "maxLength": 128},
                        "operations": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 64,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "op": {"type": "string", "enum": ["add", "replace", "remove"]},
                                    "path": {"type": "string", "maxLength": 512},
                                    "value": {
                                        "anyOf": [
                                            {"type": "object", "additionalProperties": True},
                                            {
                                                "type": "array",
                                                "items": {
                                                    "anyOf": [
                                                        {"type": "object", "additionalProperties": True},
                                                        {"type": "string"},
                                                        {"type": "number"},
                                                        {"type": "boolean"},
                                                        {"type": "null"},
                                                    ]
                                                },
                                            },
                                            {"type": "string"},
                                            {"type": "number"},
                                            {"type": "boolean"},
                                            {"type": "null"},
                                        ]
                                    },
                                },
                                "required": ["op", "path"],
                                "additionalProperties": False,
                            },
                        },
                        "action": {"type": "string", "enum": ["save", "save_and_queue"]},
                    },
                    ["name", "operations"],
                ),
                "effect": "write",
                "versions": ["v8"],
            },
            {
                "name": "propose_queue_pb8_config",
                "description": "Propose queueing an existing PB8 optimizer config. Queueing does not mean the job started. Never executes without user approval.",
                "schema": AICapabilityService._object_schema(
                    {"name": {"type": "string", "maxLength": 128}}, ["name"]
                ),
                "effect": "execute",
                "versions": ["v8"],
            },
            {
                "name": "propose_start_pb8_optimizer_queue",
                "description": "Propose immediately starting 1-4 exact existing queued PB8 optimizer jobs by IDs obtained from list_pb8_optimizer_queue. Never starts without user approval.",
                "schema": self._object_schema(
                    {
                        "queue_ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 4,
                            "items": {"type": "string", "maxLength": 128},
                        }
                    },
                    ["queue_ids"],
                ),
                "effect": "execute",
                "resources": ["pbgui://optimizer-queue/v8/{opaque-id}"],
                "versions": ["v8"],
            },
            {
                "name": "propose_pareto_backtests",
                "description": "Propose queueing the exact Cartesian matrix of PB8 Pareto candidates and exchanges. Never queues or starts jobs without user approval.",
                "schema": self._object_schema(
                    {
                        "version": {"type": "string", "enum": ["v8"]},
                        "run_resource": {"type": "string", "maxLength": 128},
                        "candidate_resources": {
                            "type": "array", "minItems": 1, "maxItems": 10,
                            "items": {"type": "string", "maxLength": 128},
                        },
                        "exchanges": {
                            "type": "array", "minItems": 1, "maxItems": 5,
                            "items": {"type": "string", "maxLength": 32},
                        },
                    },
                    ["version", "run_resource", "candidate_resources", "exchanges"],
                ),
                "effect": "execute",
                "versions": ["v8"],
                "resources": ["pbgui://optimizer-run/v8/{opaque-id}", "pbgui://pareto/v8/{opaque-id}"],
            },
            {
                "name": "propose_dashboard_from_template",
                "description": "Propose creating one named PBGui dashboard from an existing template. Never writes without user approval.",
                "schema": self._object_schema(
                    {
                        "template": {"type": "string", "maxLength": 128},
                        "name": {"type": "string", "maxLength": 128},
                    },
                    ["template", "name"],
                ),
                "effect": "write",
            },
            {
                "name": "propose_dashboard_layout",
                "description": "Propose creating or editing a dashboard with semantic cell operations. Supports layout, widget placement, users, periods, chart modes, widget options, heights, and Orders-to-Positions links. Never writes without approval.",
                "schema": self._object_schema(
                    {
                        "name": {"type": "string", "maxLength": 128},
                        "create": {"type": "boolean"},
                        "rows": {"type": "integer", "minimum": 1, "maximum": 10},
                        "columns": {"type": "integer", "minimum": 1, "maximum": 2},
                        "cells": {
                            "type": "array",
                            "maxItems": 20,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "row": {"type": "integer", "minimum": 1, "maximum": 10},
                                    "column": {"type": "integer", "minimum": 1, "maximum": 2},
                                    "type": {"type": "string", "enum": ["NONE", "BALANCE", "PNL", "ADG", "P+L", "INCOME", "TOP", "POSITIONS", "ORDERS"]},
                                    "users": {"type": "array", "minItems": 1, "maxItems": 100, "items": {"type": "string", "maxLength": 128}},
                                    "period": {"type": "string", "maxLength": 32},
                                    "mode": {"type": "string", "enum": ["bar", "line"]},
                                    "sum_period": {"type": "string", "enum": ["DAY", "WEEK", "MONTH"]},
                                    "top_n": {"type": "integer", "minimum": 1, "maximum": 100},
                                    "last_n": {"type": "integer", "minimum": 1, "maximum": 100},
                                    "minimum_income": {"type": "number"},
                                    "positions_row": {"type": "integer", "minimum": 1, "maximum": 10},
                                    "positions_column": {"type": "integer", "minimum": 1, "maximum": 2},
                                    "height": {"type": "integer", "minimum": 120, "maximum": 2000},
                                },
                                "required": ["row", "column"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    ["name", "create", "cells"],
                ),
                "effect": "write",
            },
            {
                "name": "propose_python_analysis",
                "description": "Propose a bounded Python analysis over sanitized JSON input. Shows exact code and input before approval and never executes without approval.",
                "schema": self._object_schema(
                    {
                        "code": {"type": "string", "minLength": 1, "maxLength": _MAX_ANALYSIS_CODE_BYTES},
                        "input_data": {
                            "type": "object",
                            "description": "Sanitized JSON values to analyze, grouped under descriptive keys.",
                            "additionalProperties": True,
                        },
                    },
                    ["code", "input_data"],
                ),
                "effect": "execute",
            },
            {
                "name": "propose_optimizer_run_python_analysis",
                "description": "Propose sandboxed Python over the complete sanitized Pareto metrics dataset of one optimizer-run resource. PBGui resolves all candidates server-side, binds row count and dataset digest to approval, and never exposes host paths or network access.",
                "schema": self._object_schema(
                    {
                        "version": version_schema,
                        "run_resource": {"type": "string", "maxLength": 128},
                        "scenario": {"type": "string", "maxLength": 128},
                        "statistic": {"type": "string", "maxLength": 32},
                        "code": {"type": "string", "minLength": 1, "maxLength": _MAX_ANALYSIS_CODE_BYTES},
                    },
                    ["version", "run_resource", "code"],
                ),
                "effect": "execute",
                "resources": ["pbgui://optimizer-run/{version}/{opaque-id}", "pbgui://pareto/{version}/{opaque-id}"],
            },
            {
                "name": "propose_workspace_python_analysis",
                "description": "Propose sandboxed Python with approved read-only mounts for PBGui data, PB7, and PB8. Normal logs are readable; credential, API-key, token, password, session, cookie, SSH, private-key, .env, Git, virtual-environment, and symlink paths are always masked. No network or writes.",
                "schema": self._object_schema(
                    {
                        "roots": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 3,
                            "items": {"type": "string", "enum": ["pbgui_data", "pb7", "pb8"]},
                        },
                        "code": {"type": "string", "minLength": 1, "maxLength": _MAX_ANALYSIS_CODE_BYTES},
                    },
                    ["roots", "code"],
                ),
                "effect": "execute",
            },
            {
                "name": "get_python_analysis_result",
                "description": "Read the durable bounded result of a Python analysis from this conversation.",
                "schema": self._object_schema(
                    {"proposal_id": {"type": "string", "minLength": 32, "maxLength": 32}},
                    ["proposal_id"],
                ),
                "effect": "analyze",
            },
            {
                "name": "get_passivbot_installations",
                "description": "Return exact installed PB7/PB8 commit identities and official commit-pinned source roots without local filesystem paths.",
                "schema": AICapabilityService._object_schema({}, []),
            },
            {
                "name": "read_pbgui_help_topic",
                "description": "Read one canonical PBGui help topic for the current page in English or German.",
                "schema": AICapabilityService._object_schema(
                    {
                        "topic": {"type": "string", "maxLength": 128},
                        "language": {"type": "string", "enum": ["en", "de"]},
                    },
                    ["topic"],
                ),
            },
            {
                "name": "search_pbgui_help",
                "description": "Search canonical PBGui help topics for page and field guidance.",
                "schema": AICapabilityService._object_schema(
                    {
                        "query": {"type": "string", "minLength": 2, "maxLength": 128},
                        "language": {"type": "string", "enum": ["en", "de"]},
                    },
                    ["query"],
                ),
            },
            {
                "name": "search_passivbot_docs",
                "description": "Search official documentation in the exact installed PB7 or PB8 checkout and return commit-pinned links.",
                "schema": AICapabilityService._object_schema(
                    {
                        "version": version_schema,
                        "query": {"type": "string", "minLength": 2, "maxLength": 128},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                    },
                    ["version", "query"],
                ),
            },
            {
                "name": "search_passivbot_source",
                "description": "Search the exact installed Passivbot source code without executing or importing it.",
                "schema": AICapabilityService._object_schema(
                    {
                        "version": version_schema,
                        "query": {"type": "string", "minLength": 2, "maxLength": 128},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                    },
                    ["version", "query"],
                ),
            },
            {
                "name": "read_passivbot_source",
                "description": "Read at most 400 lines from a source path returned by Passivbot search.",
                "schema": AICapabilityService._object_schema(
                    {
                        "version": version_schema,
                        "path": {"type": "string", "maxLength": 512},
                        "start_line": {"type": "integer", "minimum": 1},
                        "end_line": {"type": "integer", "minimum": 1},
                    },
                    ["version", "path"],
                ),
            },
        ]

    @staticmethod
    def _codex_tool(item: dict[str, Any]) -> dict[str, Any]:
        """Convert one registry item to canonical Codex dynamic-tool shape."""
        return {
            "type": "function",
            "name": item["name"],
            "description": item["description"],
            "deferLoading": False,
            "inputSchema": item["schema"],
        }

    @staticmethod
    def _object_schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
        """Build one strict object JSON schema."""
        return {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        }

    @staticmethod
    def _version(args: dict[str, Any]) -> str:
        """Validate a PB generation selector."""
        value = str(args.get("version") or "").lower()
        if value not in {"v7", "v8"}:
            raise AICapabilityError("version must be v7 or v8")
        return value

    @staticmethod
    def _name(value: object) -> str:
        """Apply PB8-strength validation to every model-supplied managed name."""
        name = str(value or "").strip()
        if (
            not name
            or name.startswith(".")
            or name in {".", ".."}
            or any(char in name for char in ("/", "\\", "\x00"))
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or len(name.encode("utf-8")) > 128
        ):
            raise AICapabilityError("Invalid managed name")
        return name

    @staticmethod
    def _limit(args: dict[str, Any], *, maximum: int) -> int:
        """Return a bounded positive list limit."""
        try:
            value = int(args.get("limit") or min(20, maximum))
        except (TypeError, ValueError) as exc:
            raise AICapabilityError("Invalid limit") from exc
        return max(1, min(maximum, value))

    @staticmethod
    def _passivbot_root(version: str) -> Path:
        """Resolve only the configured installed PB7/PB8 source root."""
        if version == "v7":
            from pbgui_purefunc import pb7dir

            raw = str(pb7dir() or "")
        else:
            from pbgui_purefunc import pb8_runtime_status

            raw = str(pb8_runtime_status().get("pb8dir") or "")
        if not raw:
            raise AICapabilityError(f"PB{version[-1]} source is not configured")
        unresolved = Path(os.path.abspath(Path(raw).expanduser()))
        if unresolved.is_symlink() or not unresolved.is_dir():
            raise AICapabilityError("Passivbot source root is unavailable or unsafe")
        return unresolved

    @staticmethod
    def _relative_source_path(value: object) -> Path:
        """Validate one relative allowlisted Passivbot source path."""
        text = str(value or "").strip().replace("\\", "/")
        path = Path(text)
        if (
            not text
            or path.is_absolute()
            or any(part in {"", ".", ".."} or part.startswith(".") for part in path.parts)
            or any(part in _SOURCE_EXCLUDED_PARTS for part in path.parts)
            or path.suffix.lower() not in _SOURCE_EXTENSIONS
            or len(text.encode("utf-8")) > 512
            or any(ord(char) < 32 or ord(char) == 127 for char in text)
        ):
            raise AICapabilityError("Invalid Passivbot source path")
        return path

    @staticmethod
    def _require_safe_source_path(root: Path, target: Path) -> None:
        """Reject traversal, symlinks, non-files, and disallowed source extensions."""
        try:
            relative = target.relative_to(root)
        except ValueError as exc:
            raise AICapabilityError("Passivbot source path escaped its root") from exc
        current = root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise AICapabilityError("Passivbot source path contains a symlink")
        if not target.is_file() or target.suffix.lower() not in _SOURCE_EXTENSIONS:
            raise AICapabilityError("Passivbot source file is unavailable")

    @staticmethod
    def _passivbot_git_info(root: Path) -> tuple[str, str]:
        """Return validated commit and official GitHub repository for citations."""
        def command(*args: str) -> str:
            try:
                result = subprocess.run(
                    ["git", "-C", str(root), *args],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=5,
                    env={"PATH": os.environ.get("PATH", "")},
                )
            except (OSError, subprocess.TimeoutExpired):
                return ""
            return result.stdout.strip() if result.returncode == 0 else ""

        commit = command("rev-parse", "HEAD")
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            commit = ""
        remote = command("remote", "get-url", "origin")
        if remote.startswith("git@github.com:"):
            remote = "https://github.com/" + remote.removeprefix("git@github.com:")
        remote = remote.removesuffix(".git").rstrip("/")
        if not re.fullmatch(r"https://github\.com/enarjord/passivbot", remote, re.IGNORECASE):
            remote = ""
        return commit, remote

    @staticmethod
    def _git_command(root: Path, *args: str) -> tuple[int, str]:
        """Run one bounded read-only git query with a minimal environment."""
        try:
            result = subprocess.run(
                ["git", "-C", str(root), *args],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
                env={"PATH": os.environ.get("PATH", "")},
            )
        except (OSError, subprocess.TimeoutExpired):
            return 1, ""
        return result.returncode, result.stdout

    @classmethod
    def _checkout_is_clean(cls, root: Path) -> bool:
        """Return true only when tracked and untracked source state matches HEAD."""
        code, output = cls._git_command(root, "status", "--porcelain", "--untracked-files=all")
        return code == 0 and not output.strip()

    @classmethod
    def _source_is_clean(cls, root: Path, relative: Path) -> bool:
        """Return whether a cited file exactly matches the commit used in its URL."""
        code, output = cls._git_command(
            root,
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            relative.as_posix(),
        )
        return code == 0 and not output.strip()

    def _runtime_fingerprints(self) -> dict[str, dict[str, Any]]:
        """Return current path-free PB7/PB8 source identities for capability consumers."""
        return {version: self._runtime_fingerprint(version) for version in ("v7", "v8")}

    def _runtime_fingerprint(self, version: str) -> dict[str, Any]:
        """Build a dirty-aware runtime fingerprint without revealing checkout paths."""
        try:
            root = self._passivbot_root(version)
            commit, repository = self._passivbot_git_info(root)
            code, status = self._git_command(root, "status", "--porcelain", "--untracked-files=all")
            dirty = code != 0 or bool(status.strip())
            state_digest = hashlib.sha256(status.encode("utf-8")).hexdigest() if dirty else ""
            return {
                "installed": True,
                "commit": commit,
                "dirty": dirty,
                "state_digest": f"sha256:{state_digest}" if state_digest else "",
                "official_source_url": f"{repository}/tree/{commit}" if repository and commit and not dirty else "",
            }
        except AICapabilityError as exc:
            return {"installed": False, "commit": "", "dirty": None, "state_digest": "", "status": str(exc)}

    @staticmethod
    def _source_url(
        repository: str,
        commit: str,
        relative: Path,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        """Build one exact official GitHub blob citation without arbitrary hosts."""
        if not repository or not commit:
            return ""
        url = f"{repository}/blob/{commit}/{quote(relative.as_posix())}"
        if start_line is not None:
            url += f"#L{max(1, start_line)}"
            if end_line is not None and end_line > start_line:
                url += f"-L{end_line}"
        return url

    @classmethod
    def _sanitize_config(cls, value: Any, *, depth: int = 0) -> Any:
        """Remove secret and host-path fields from model-visible config context."""
        if depth > 12:
            return None
        if isinstance(value, dict):
            result = {}
            for key, item in list(value.items())[:1000]:
                key_text = str(key)
                lowered = key_text.lower()
                if key_text == "pbgui" or lowered in _PATH_KEYS or any(part in lowered for part in _SENSITIVE_KEY_PARTS):
                    continue
                result[key_text] = cls._sanitize_config(item, depth=depth + 1)
            return result
        if isinstance(value, list):
            return [cls._sanitize_config(item, depth=depth + 1) for item in value[:1000]]
        if isinstance(value, str):
            return value[:4096]
        if isinstance(value, (bool, int, float)) or value is None:
            return value
        return str(value)[:4096]

    @classmethod
    def _strip_paths(cls, value: Any) -> Any:
        """Recursively remove path-like fields from resource summaries."""
        if isinstance(value, dict):
            return {
                str(key): cls._strip_paths(item)
                for key, item in value.items()
                if str(key).lower() not in _PATH_KEYS and not str(key).lower().endswith("_path")
            }
        if isinstance(value, list):
            return [cls._strip_paths(item) for item in value]
        return value

    @classmethod
    def _compact_result(cls, item: object) -> dict[str, Any]:
        """Project one result onto bounded scalar/list summary fields."""
        if not isinstance(item, dict):
            return {}
        allowed = {
            "name",
            "config_name",
            "result_name",
            "exchange",
            "exchange_dir",
            "exchanges",
            "coins",
            "start_date",
            "end_date",
            "modified",
            "mode",
            "scenario_count",
            "pareto_count",
            "adg",
            "gain",
            "drawdown_worst",
            "sharpe_ratio",
            "starting_balance",
            "final_balance",
            "final_equity",
            "liquidated",
            "status",
        }
        return {key: cls._strip_paths(value) for key, value in item.items() if key in allowed}

    @staticmethod
    def _digest(value: object) -> str:
        """Return a stable SHA-256 digest for finite JSON content."""
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _changed_paths(cls, before: Any, after: Any, prefix: str = "") -> list[str]:
        """Return sorted dotted paths whose JSON values differ."""
        return [item["path"] for item in cls._changed_entries(before, after, prefix)]

    @staticmethod
    def _list_identity_key(before: list[Any], after: list[Any]) -> str | None:
        """Return a stable field that identifies objects in both lists."""
        items = before + after
        if not items or not all(isinstance(item, dict) for item in items):
            return None
        for key in ("metric", "name", "id", "symbol", "coin", "exchange", "key"):
            before_values = [item.get(key) for item in before]
            after_values = [item.get(key) for item in after]
            if (
                all(value is not None and not isinstance(value, (dict, list)) for value in before_values + after_values)
                and len(set(before_values)) == len(before_values)
                and len(set(after_values)) == len(after_values)
            ):
                return key
        return None

    @classmethod
    def _changed_entries(cls, before: Any, after: Any, prefix: str = "") -> list[dict[str, Any]]:
        """Return redacted old/new values for informed proposal review."""
        if before == after:
            return []
        if cls._is_sensitive_path(prefix):
            return [{"path": prefix or "config", "kind": "changed", "before": "[redacted]", "after": "[redacted]"}]
        if isinstance(before, dict) and isinstance(after, dict):
            changes: list[dict[str, Any]] = []
            for key in sorted(set(before) | set(after)):
                path = f"{prefix}.{key}" if prefix else str(key)
                if key in before and key in after and before[key] == after[key]:
                    continue
                if cls._is_sensitive_path(path):
                    changes.append({"path": path, "kind": "changed", "before": "[redacted]", "after": "[redacted]"})
                    continue
                if key not in before:
                    changes.append(
                        {"path": path, "kind": "added", "after": cls._sanitize_config(after[key])}
                    )
                elif key not in after:
                    changes.append(
                        {"path": path, "kind": "removed", "before": cls._sanitize_config(before[key])}
                    )
                else:
                    changes.extend(cls._changed_entries(before[key], after[key], path))
            return changes
        if isinstance(before, list) and isinstance(after, list):
            changes: list[dict[str, Any]] = []
            identity_key = cls._list_identity_key(before, after)
            if identity_key:
                before_items = {item[identity_key]: item for item in before}
                after_items = {item[identity_key]: item for item in after}
                for identity, item in before_items.items():
                    if identity not in after_items:
                        changes.append({
                            "path": prefix or "config",
                            "kind": "removed",
                            "item": str(identity),
                            "before": cls._sanitize_config(item),
                        })
                for identity, item in after_items.items():
                    if identity not in before_items:
                        changes.append({
                            "path": prefix or "config",
                            "kind": "added",
                            "item": str(identity),
                            "after": cls._sanitize_config(item),
                        })
                    else:
                        item_path = f"{prefix}[{identity}]" if prefix else f"config[{identity}]"
                        changes.extend(cls._changed_entries(before_items[identity], item, item_path))
                return changes
            if all(not isinstance(item, (dict, list)) for item in before + after):
                unmatched_after = list(after)
                for item in before:
                    if item in unmatched_after:
                        unmatched_after.remove(item)
                    else:
                        changes.append({
                            "path": prefix or "config",
                            "kind": "removed",
                            "before": cls._sanitize_config(item),
                        })
                for item in unmatched_after:
                    changes.append({
                        "path": prefix or "config",
                        "kind": "added",
                        "after": cls._sanitize_config(item),
                    })
                return changes
            for index in range(max(len(before), len(after))):
                path = f"{prefix}[{index}]"
                if index >= len(before):
                    changes.append({"path": path, "kind": "added", "after": cls._sanitize_config(after[index])})
                elif index >= len(after):
                    changes.append({"path": path, "kind": "removed", "before": cls._sanitize_config(before[index])})
                else:
                    changes.extend(cls._changed_entries(before[index], after[index], path))
            return changes
        return [
            {
                "path": prefix or "config",
                "kind": "changed",
                "before": cls._sanitize_config(before),
                "after": cls._sanitize_config(after),
            }
        ]

    @staticmethod
    def _is_sensitive_path(path: str) -> bool:
        """Return whether any dotted path segment names a secret or host path field."""
        parts = [part.lower() for part in str(path or "").split(".") if part]
        return any(
            part == "pbgui"
            or part in _PATH_KEYS
            or any(sensitive in part for sensitive in _SENSITIVE_KEY_PARTS)
            for part in parts
        )

    @classmethod
    def _queue_preview(
        cls,
        config: dict[str, Any],
        overrides: dict[str, dict],
    ) -> dict[str, Any]:
        """Build a path-free launch and bundle summary for informed queue approval."""
        backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
        optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
        live = config.get("live") if isinstance(config.get("live"), dict) else {}
        from api import optimize_v8

        launch_options = optimize_v8._validate_launch_options(
            optimize_v8._runtime_options_from_config(config)
        )
        runtime_source = str(launch_options.get("source") or "").strip()
        runtime = cls._sanitize_config(
            {
                "mode": launch_options.get("mode", "fresh"),
                "source_name": Path(runtime_source).name if runtime_source else "",
                "fine_tune": launch_options.get("fine_tune"),
                "polish": launch_options.get("polish"),
                "backend": optimize.get("backend"),
                "n_cpus": optimize.get("n_cpus"),
                "exchanges": backtest.get("exchanges"),
                "strategy": live.get("strategy_kind"),
                "scoring": optimize.get("scoring"),
            }
        )
        return {
            "runtime": runtime,
            "override_files": [
                {"name": name, "digest": cls._digest(payload)}
                for name, payload in sorted(overrides.items())
            ],
        }

    @staticmethod
    async def _to_thread_uncancellable(function, *args):
        """Keep threadpool ownership until the real worker finishes after cancellation."""
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return task.result()

    @staticmethod
    def _safe_detail(value: object) -> str:
        """Return a bounded error detail without serializing arbitrary payloads."""
        if isinstance(value, str):
            return value[:1000]
        if isinstance(value, dict) and isinstance(value.get("message"), str):
            return value["message"][:1000]
        return "PBGui capability rejected the request"

    @staticmethod
    def _require_bounded_result(value: object) -> None:
        """Reject oversized or non-finite tool results before model ingestion."""
        try:
            encoded = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise AICapabilityError("PBGui capability returned invalid data") from exc
        if len(encoded) > _MAX_TOOL_RESULT_BYTES:
            raise AICapabilityError("PBGui capability result is too large")


_SERVICE: AICapabilityService | None = None


def get_ai_capability_service() -> AICapabilityService:
    """Return the process-owned AI capability service."""
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = AICapabilityService()
    return _SERVICE


def restart_block_reason(root: Path | None = None) -> str:
    """Block API restart while an approved PB8 action journal is incomplete or unreadable."""
    journal_root = Path(root or Path(PBGDIR) / "data" / "ai" / "capabilities") / "journal"
    if not journal_root.exists():
        return ""
    if not journal_root.is_dir() or journal_root.is_symlink():
        return "AI action recovery state is unsafe; restart is blocked"
    incomplete = 0
    try:
        for path in journal_root.glob("*.json"):
            if path.is_symlink() or not path.is_file():
                return "AI action recovery state is unsafe; restart is blocked"
            raw = read_regular_file_nofollow(path, journal_root)
            if len(raw) > _MAX_CONFIG_BYTES * 2:
                return "AI action recovery state is invalid; restart is blocked"
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict) or payload.get("phase") != "completed":
                incomplete += 1
    except (OSError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError):
        return "AI action recovery state could not be verified; restart is blocked"
    if incomplete:
        label = "action" if incomplete == 1 else "actions"
        return f"{incomplete} approved AI PB8 {label} require durable recovery"
    return ""


async def startup() -> None:
    """Recover approved durable actions under API lifecycle ownership."""
    await get_ai_capability_service().startup()


async def shutdown() -> None:
    """Finish process-owned approval tasks before releasing capability state."""
    global _SERVICE
    service = _SERVICE
    _SERVICE = None
    if service is not None:
        await service.shutdown()
