"""Offline contracts for OpenRouter Jev optimizer decisions."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from ai_chat import AICredentialStore, AIChatService, AIChatError, owner_key
from ai_openrouter import JEV_MODEL, OpenRouterDecisionError, check_jev_budget, _noul_answers, _user_options, decide_backtest_candidates, decide_backtest_results, decide_general_choice, decide_user_jev_request, parse_structured_jev_request


@pytest.fixture(autouse=True)
def _stub_pricing_for_existing_decision_contracts(monkeypatch):
    """Keep existing provider-response tests focused on decision behavior."""
    async def accepted(*args, **kwargs):
        """Accept a request already covered by the dedicated pricing tests."""
        return None
    monkeypatch.setattr("ai_openrouter.check_jev_budget", accepted)


class _PricingResponse:
    """Provide one official model-pricing shape without network access."""

    status = 200

    def __init__(self, prompt: str, completion: str = "0") -> None:
        self.payload = {"data": {"pricing": {"prompt": prompt, "completion": completion}}}
        self.content = self

    async def __aenter__(self):
        """Enter the pricing response."""
        return self

    async def __aexit__(self, *args):
        """Leave the pricing response."""
        return False

    async def read(self, size):
        """Return one bounded provider-reported model price."""
        return json.dumps(self.payload).encode()[:size]


class _PricingSession:
    """Record preflight price fetches and any billable sends."""

    def __init__(self, price: str = "0.000000042") -> None:
        self.price = price
        self.requests = []

    def get(self, url, **kwargs):
        """Return the price for the pinned Jev model."""
        self.requests.append((url, kwargs))
        return _PricingResponse(self.price)


def test_jev_usd_budget_checks_current_model_price_before_send() -> None:
    """A low limit blocks the entire analysis before any billable request."""
    session = _PricingSession()
    request = {"model": JEV_MODEL, "state": {"data": "x" * 10000},
               "questions": {"q": {"type": "noul", "instructions": "Choose"}}}
    with pytest.raises(OpenRouterDecisionError, match="configured USD budget"):
        asyncio.run(check_jev_budget(session, "test-key", [request], 0.0001))
    assert len(session.requests) == 1
    assert session.requests[0][0].endswith("/v1/model/typesafe/jev-1.13")
    asyncio.run(check_jev_budget(session, "test-key", [request], 0.01))


def test_jev_budget_fails_closed_when_price_is_not_verified() -> None:
    """A missing or nonzero output price never authorizes a decision call."""
    session = _PricingSession("invalid")
    with pytest.raises(OpenRouterDecisionError, match="pricing could not be checked"):
        asyncio.run(check_jev_budget(session, "test-key", [{"state": "x"}], 0.01))


class _FakeCapabilities:
    """Provide two same-named runs and complete, paginated Pareto metrics."""

    def __init__(self, count=12) -> None:
        self.count = count
        self.calls = []
        self.candidates = [
            {"resource": f"pbgui://pareto/v8/{index:032x}", "name": f"pareto-{index:03d}",
             "metrics": {"adg": 0.1 + index / 1000, "drawdown_worst": 0.2,
                         "sharpe_ratio": 1.2 + index / 100, "unusual_metric": index * 3}}
            for index in range(count)
        ]

    async def list_proposals(self, owner, conversation_id):
        """Return no pending proposals for the isolated chat."""
        return []

    async def dispatch(self, owner, conversation_id, tool, args):
        """Record managed capability requests and return the exact run."""
        self.calls.append((tool, args))
        if tool == "list_optimizer_runs":
            return {"runs": [
                {"name": "backtests", "result_id": "other", "resource": "pbgui://optimizer-run/v8/other", "pareto_count": 1},
                {"name": "backtests", "result_id": "chosen", "resource": "pbgui://optimizer-run/v8/chosen", "pareto_count": self.count},
            ]}
        if tool == "get_optimizer_run_analysis":
            assert args["resource"] == "pbgui://optimizer-run/v8/chosen"
            assert args["all_metrics"] is True
            offset = args["offset"]
            limit = args["limit"]
            return {"pareto": self.candidates[offset:offset + limit], "total": self.count, "offset": offset}
        if tool == "select_pareto_candidates":
            assert args["run_resource"] == "pbgui://optimizer-run/v8/chosen"
            assert args["mode"] == "replace"
            names = [next(item["name"] for item in self.candidates if item["resource"] == resource) for resource in args["candidate_resources"]]
            return {"ui_action": {"type": "optimize.select_paretos", "target": {"version": "v8", "run_name": "backtests", "result_id": "chosen"}, "payload": {"candidate_names": names, "mode": "replace"}}}
        raise AssertionError(tool)


class _FakeContent:
    """Supply one bounded JSON body."""

    def __init__(self, payload) -> None:
        self.payload = json.dumps(payload).encode()

    async def read(self, size):
        """Return the simulated response body."""
        return self.payload[:size]


class _FakeResponse:
    """Model the aiohttp response context manager."""

    def __init__(self, payload, status=200) -> None:
        self.status = status
        self.content = _FakeContent(payload)

    async def __aenter__(self):
        """Enter the simulated response."""
        return self

    async def __aexit__(self, *args):
        """Leave the simulated response."""
        return False


class _FakeSession:
    """Capture every Jev batch and answer each candidate independently."""

    def __init__(self) -> None:
        self.requests = []

    def post(self, url, **kwargs):
        """Return a deterministic Noul decision for every question in the batch."""
        self.requests.append((url, kwargs))
        questions = kwargs["json"]["questions"]
        answers = {key: {"type": "noul", "noul": int(key[1:]) / 1000} for key in questions}
        return _FakeResponse({"answers": answers})


def _run_context(count=12):
    """Identify the selected run despite duplicate display names."""
    return {"entities": [{"kind": "optimizer_run", "version": "v8", "name": f"backtests | paretos={count}", "result_id": "chosen"}]}


def test_openrouter_key_is_separate_and_owner_only(tmp_path: Path) -> None:
    """Removing OpenRouter does not remove the existing OpenCode key."""
    store = AICredentialStore(tmp_path / "credentials")
    owner = owner_key("alice")
    store.save_go_key(owner, "opencode-key-123456789")
    store.save_openrouter_key(owner, "openrouter-key-123456789")
    path = tmp_path / "credentials" / f"{owner}.openrouter.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert store.load_openrouter_key(owner) == "openrouter-key-123456789"
    store.delete_openrouter_key(owner)
    assert store.configured(owner)
    assert not store.openrouter_configured(owner)
    with pytest.raises(AIChatError, match="not connected"):
        store.load_openrouter_key(owner)


def test_jev_profiles_all_candidates_with_bounded_payloads() -> None:
    """Every Pareto is assessed while the full metric scan stays local and bounded."""
    capabilities = _FakeCapabilities(count=204)
    session = _FakeSession()
    selected = []

    async def capture(result):
        """Record the trusted browser selection action."""
        selected.append(result)

    reply = asyncio.run(decide_backtest_candidates(capabilities, session, "test-key-123456789", "owner", "conversation", JEV_MODEL, "Which ten should I backtest?", _run_context(204), capture, selection_limit=10))
    assert "all 204 Pareto candidates" in reply
    assert "214" not in reply
    assert "1. pareto-203" in reply
    assert "10. pareto-194" in reply
    assert 1 <= len(session.requests) < 7
    assert sum(len(request["json"]["questions"]) for _, request in session.requests) == 204
    assert all(request["json"]["state"]["candidates"] for _, request in session.requests)
    assert all(request["json"]["state"]["metrics_considered"] == 4 for _, request in session.requests)
    assert all("unusual_metric" in request["json"]["state"]["metric_names"] for _, request in session.requests)
    assert all(len(json.dumps(request["json"], ensure_ascii=False, separators=(",", ":")).encode()) <= 30_000
               for _, request in session.requests)
    assert not any(name == "rank_optimizer_run_candidates" for name, _ in capabilities.calls)
    assert [args["offset"] for name, args in capabilities.calls if name == "get_optimizer_run_analysis"] == list(range(0, 204, 24))
    assert selected[0]["ui_action"]["payload"]["candidate_names"] == [f"pareto-{index:03d}" for index in range(203, 193, -1)]
    assert "/home/" not in json.dumps([request["json"] for _, request in session.requests])




def test_jev_respects_model_supplied_maximum_of_five() -> None:
    """The approved model argument, not a fixed top-ten slice, bounds browser selection."""
    capabilities = _FakeCapabilities(count=12)
    captured = []

    async def capture(result):
        """Record the trusted selection action."""
        captured.append(result)

    reply = asyncio.run(decide_backtest_candidates(
        capabilities, _FakeSession(), "test-key-123456789", "owner", "conversation",
        JEV_MODEL, "Which candidates should I backtest?", _run_context(),
        capture_selection=capture, selection_limit=5,
    ))
    assert "following 5 candidates" in reply
    assert len(captured[0]["ui_action"]["payload"]["candidate_names"]) == 5
    assert captured[0]["ui_action"]["payload"]["candidate_names"][0] == "pareto-011"


def test_jev_without_limit_uses_its_yes_no_decisions() -> None:
    """An uncapped Jev decision can mark a model-determined count rather than ten."""
    class DecisiveSession(_FakeSession):
        """Answer yes for six of twelve candidates."""

        def post(self, url, **kwargs):
            """Return model probabilities across the full candidate set."""
            self.requests.append((url, kwargs))
            answers = {
                key: {"type": "noul", "noul": int(key[1:]) / 12}
                for key in kwargs["json"]["questions"]
            }
            return _FakeResponse({"answers": answers})

    capabilities = _FakeCapabilities(count=12)
    captured = []

    async def capture(result):
        """Record the trusted selection action."""
        captured.append(result)

    reply = asyncio.run(decide_backtest_candidates(
        capabilities, DecisiveSession(), "test-key-123456789", "owner", "conversation",
        JEV_MODEL, "Which candidates should I backtest?", _run_context(),
        capture_selection=capture,
    ))
    assert "following 6 candidates" in reply
    assert len(captured[0]["ui_action"]["payload"]["candidate_names"]) == 6




def test_jev_high_dimension_run_has_small_preflighted_requests() -> None:
    """A 204-by-212 run packs complete profiles within Jev's context window."""
    capabilities = _FakeCapabilities(count=204)
    for index, candidate in enumerate(capabilities.candidates):
        candidate["metrics"].update({
            f"additional_metric_{column:03d}": (index * (column + 3) % 997) / 1000
            for column in range(208)
        })
    session = _FakeSession()
    reply = asyncio.run(decide_backtest_candidates(
        capabilities, session, "test-key-123456789", "owner", "conversation",
        JEV_MODEL, "Which ten should I backtest?", _run_context(204),
    ))
    assert "212 metrics" in reply
    assert sum(len(request["json"]["questions"]) for _, request in session.requests) == 204
    assert all(request["json"]["state"]["metrics_considered"] == 212 for _, request in session.requests)
    assert all(len(request["json"]["state"]["metric_names"]) <= 24 for _, request in session.requests)
    assert all(request["json"]["state"]["profiled_distinct_metrics"] > 0 for _, request in session.requests)
    assert all(len(json.dumps(request["json"], ensure_ascii=False, separators=(",", ":")).encode()) <= 30_000
               for _, request in session.requests)


def test_jev_total_transfer_uses_usd_budget_instead_of_old_byte_ceiling() -> None:
    '''A large run previews above 100 KB while live pricing still guards billing.'''
    capabilities = _FakeCapabilities(count=500)
    for index, candidate in enumerate(capabilities.candidates):
        candidate['metrics'].update({
            f'additional_metric_{column:03d}': (index * (column + 3) % 997) / 1000
            for column in range(208)
        })
    preview = asyncio.run(decide_backtest_candidates(
        capabilities, None, '', 'owner', 'conversation', JEV_MODEL,
        'Which candidates should I backtest?', _run_context(500), preview_only=True,
    ))
    requests = preview['requests']
    sizes = [len(json.dumps(request, ensure_ascii=False, separators=(',', ':')).encode())
             for request in requests]

    assert preview['candidate_count'] == 500
    assert sum(len(request['questions']) for request in requests) == 500
    assert sum(sizes) > 100_000
    assert max(sizes) <= 30_000
    pricing = _PricingSession()
    with pytest.raises(OpenRouterDecisionError, match='configured USD budget'):
        asyncio.run(check_jev_budget(pricing, 'test-key', requests, 0.01))
    assert len(pricing.requests) == 1  # Only the model-price lookup occurred.


def test_jev_skips_indistinguishable_candidates_without_provider_cost() -> None:
    """Identical numeric profiles cannot justify billable Jev ranking calls."""
    capabilities = _FakeCapabilities(count=12)
    shared = dict(capabilities.candidates[0]["metrics"])
    for candidate in capabilities.candidates:
        candidate["metrics"] = dict(shared)
    session = _FakeSession()
    reply = asyncio.run(decide_backtest_candidates(
        capabilities, session, "test-key-123456789", "owner", "conversation",
        JEV_MODEL, "Which ten should I backtest?", _run_context(12),
    ))
    assert "same numeric metrics" in reply
    assert not session.requests


def test_jev_rejects_oversized_question_before_provider_call() -> None:
    """A question beyond the byte ceiling cannot incur a partial provider charge."""
    session = _FakeSession()
    with pytest.raises(OpenRouterDecisionError, match="no data was sent"):
        asyncio.run(decide_backtest_candidates(
            _FakeCapabilities(count=12), session, "test-key-123456789",
            "owner", "conversation", JEV_MODEL, "X" * 20_000, _run_context(12),
        ))
    assert not session.requests

@pytest.mark.parametrize("answers", [
    {"candidate_1": {"type": "noul", "noul": 0.8}},
    {"candidate_1": {"type": "noul", "noul": True}, "candidate_2": {"type": "noul", "noul": 0.5}},
    {"candidate_1": {"type": "noul", "noul": 1.1}, "candidate_2": {"type": "noul", "noul": 0.5}},
    {"candidate_1": {"type": "choice", "choice": "yes"}, "candidate_2": {"type": "noul", "noul": 0.5}},
])
def test_jev_rejects_untrusted_or_incomplete_answers(answers) -> None:
    """Incomplete or malformed model probabilities cannot become recommendations."""
    with pytest.raises(OpenRouterDecisionError):
        _noul_answers({"answers": answers}, {"candidate_1", "candidate_2"})


def test_jev_provider_rejection_does_not_multiply_requests() -> None:
    """A rejected compact request stops without billable splitting or selection."""
    class RejectingSession(_FakeSession):
        """Reject every Jev batch."""

        def post(self, url, **kwargs):
            """Record the one attempted batch."""
            self.requests.append((url, kwargs))
            return _FakeResponse({"error": {"message": "Context length exceeded"}}, 400)

    session = RejectingSession()
    selected = []

    async def capture(result):
        """Record a trusted browser action only after full success."""
        selected.append(result)

    with pytest.raises(OpenRouterDecisionError, match="could not complete"):
        asyncio.run(decide_backtest_candidates(
            _FakeCapabilities(count=12), session, "test-key-123456789",
            "owner", "conversation", JEV_MODEL, "Which ten should I backtest?",
            _run_context(12), capture,
        ))
    assert len(session.requests) == 1
    assert not selected


def test_jev_single_candidate_http_400_reports_safe_category() -> None:
    """An irreducible provider rejection never exposes a raw response body."""
    class RejectingSession(_FakeSession):
        """Reject every provider call."""

        def post(self, url, **kwargs):
            """Return an error with sensitive raw text that must stay hidden."""
            return _FakeResponse({"error": {"message": "Context length exceeded: secret-prompt"}}, 400)

    with pytest.raises(OpenRouterDecisionError, match="could not complete") as error:
        asyncio.run(decide_backtest_candidates(_FakeCapabilities(count=1), RejectingSession(), "test-key-123456789", "owner", "conversation", JEV_MODEL, "Which should I backtest?", _run_context(1)))
    assert "secret-prompt" not in str(error.value)


def test_jev_rejects_ambiguous_display_name_without_run_id() -> None:
    """Duplicate names do not silently select the one-candidate run."""
    session = _FakeSession()
    reply = asyncio.run(decide_backtest_candidates(_FakeCapabilities(), session, "test-key-123456789", "owner", "conversation", JEV_MODEL, "Which should I backtest?", {"entities": [{"kind": "optimizer_run", "version": "v8", "name": "backtests"}]}))
    assert "Several Optimize runs match" in reply
    assert not session.requests


def test_openrouter_lists_jev_as_decision_model(tmp_path: Path) -> None:
    """Connected owners see Jev with an explicit decision capability."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")
    models = asyncio.run(service.models(owner, "openrouter"))
    assert models[0]["id"] == JEV_MODEL
    assert models[0]["decision"] is True
    assert models[0]["tools"] is False


def test_jev_explains_a_german_question_in_german() -> None:
    """The local decision explanation follows the user's German question."""
    reply = asyncio.run(decide_backtest_candidates(_FakeCapabilities(), _FakeSession(), "test-key-123456789", "owner", "conversation", JEV_MODEL, "Welche Resultate sollte ich backtesten?", _run_context(), selection_limit=5))
    assert "fuer einen Validierungs-Backtest markiert" in reply
    assert "1. pareto-011" in reply


def test_jev_rejects_malformed_answer_envelope() -> None:
    """A malformed external envelope becomes a classified error."""
    with pytest.raises(OpenRouterDecisionError):
        _noul_answers({"answers": []}, {"candidate_1"})


def test_stale_selected_run_does_not_contact_openrouter() -> None:
    """A deleted selected run gives an explanation without sending stale data."""
    capabilities = _FakeCapabilities()
    session = _FakeSession()
    context = {"entities": [{"kind": "optimizer_run", "version": "v8", "name": "deleted", "result_id": "deleted"}]}
    reply = asyncio.run(decide_backtest_candidates(capabilities, session, "test-key-123456789", "owner", "conversation", JEV_MODEL, "Which should I backtest?", context))
    assert "no longer available" in reply
    assert not session.requests

class _KeyContent:
    """Supply a bounded streamed OpenRouter key response."""

    def __init__(self, payload) -> None:
        self.payload = json.dumps(payload).encode("utf-8")

    async def iter_chunked(self, _size):
        """Yield one JSON response chunk."""
        yield self.payload


class _KeyResponse:
    """Model a successful key usage response."""

    def __init__(self, payload) -> None:
        self.status = 200
        self.content = _KeyContent(payload)

    async def __aenter__(self):
        """Enter the provider response."""
        return self

    async def __aexit__(self, *_args):
        """Leave the provider response."""
        return False


class _KeySession:
    """Capture the server-only key usage request."""

    def __init__(self, payload) -> None:
        self.payload = payload
        self.request = None

    def get(self, url, **kwargs):
        """Return a provider-reported key summary."""
        self.request = (url, kwargs)
        return _KeyResponse(self.payload)


def test_openrouter_usage_uses_only_provider_reported_key_values(tmp_path: Path) -> None:
    """Usage reports the connected key's spend and limit without local counters."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")
    session = _KeySession({"data": {
        "usage_daily": 0.0012, "usage_weekly": 0.0034, "usage_monthly": 0.0123,
        "limit": 10, "limit_remaining": 9.8, "limit_reset": "monthly", "usage": 1.25,
        "api_key": "must-not-leak", "usage_weekly_byok": 90,
    }})

    async def fake_http_session():
        """Return the isolated provider fixture."""
        return session

    service._http_session = fake_http_session
    result = asyncio.run(service.usage(owner, "openrouter"))
    assert result == {
        "provider": "openrouter", "connected": True,
        "spend": {"daily": 0.0012, "weekly": 0.0034, "monthly": 0.0123},
        "key_limit_usd": 10.0, "key_limit_remaining_usd": 9.8, "key_limit_reset": "monthly",
    }
    url, request = session.request
    assert url.endswith("/v1/key")
    assert request["headers"]["Authorization"] == "Bearer openrouter-key-123456789"
    assert request["allow_redirects"] is False


def test_openrouter_new_chat_is_available_after_twenty_conversations(tmp_path: Path) -> None:
    """The earlier 20-chat cap cannot block a new Jev conversation."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")

    async def create_chats():
        """Create more than the previous cap in isolated owner storage."""
        return [await service.create_conversation(owner, "openrouter", JEV_MODEL) for _ in range(21)]

    ids = asyncio.run(create_chats())
    assert len(set(ids)) == 21
    assert len(asyncio.run(service.list_conversations(owner))) == 21


def test_jev_chat_after_new_conversation_returns_a_decision(tmp_path: Path) -> None:
    """A new OpenRouter conversation reaches the Jev adapter with page context."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")
    service.capabilities = _FakeCapabilities()
    class DecisiveSession(_FakeSession):
        """Make Jev affirm the candidates it considers worth backtesting."""

        def post(self, url, **kwargs):
            """Answer the candidate questions without a fixed selection count."""
            self.requests.append((url, kwargs))
            answers = {
                key: {"type": "noul", "noul": int(key[1:]) / 12}
                for key in kwargs["json"]["questions"]
            }
            return _FakeResponse({"answers": answers})

    session = DecisiveSession()

    async def fake_http_session():
        """Return the isolated Jev response fixture."""
        return session

    service._http_session = fake_http_session

    async def run_turn():
        """Create and use the same conversation as the drawer."""
        conversation_id = await service.create_conversation(
            owner, "openrouter", JEV_MODEL, context={"entities": [
                {"kind": "optimizer_run", "version": "v8", "name": "backtests | paretos=12", "result_id": "chosen"},
            ]},
        )
        return await service.chat(owner, "openrouter", JEV_MODEL, "Which should I backtest?", conversation_id)

    result = asyncio.run(run_turn())
    assert "pareto-011" in result["reply"]
    assert session.requests
    conversation = asyncio.run(service.get_conversation(owner, result["conversation_id"]))
    assert conversation["ui_actions"][0]["type"] == "optimize.select_paretos"
    assert len(conversation["ui_actions"][0]["payload"]["candidate_names"]) == 6


class _ChoiceSession:
    """Answer one structured Jev Choice without network access."""

    def __init__(self, answer=None):
        self.requests = []
        self.answer = answer

    def post(self, url, **kwargs):
        """Capture the typed request and return a provider-shaped answer."""
        self.requests.append((url, kwargs))
        criteria = kwargs["json"]["questions"]["best"]["criteria"]
        answer = self.answer or {"type": "choice", "choice": "option_2",
                                 "probabilities": {key: (0.8 if key == "option_2" else 0.2 / (len(criteria) - 1)) for key in criteria},
                                 "confidence": 0.9}
        return _FakeResponse({"answers": {"best": answer}})


def test_jev_chooses_user_supplied_options_without_optimizer_data() -> None:
    """An ordinary typed alternatives question reaches the Choice API."""
    question = "Welche Option ist sicherer?\nOptionen:\n1. Bot A mit 20% Drawdown\n2. Bot B mit 8% Drawdown"
    options = _user_options(question)
    assert options == ["Bot A mit 20% Drawdown", "Bot B mit 8% Drawdown"]
    session = _ChoiceSession()
    reply = asyncio.run(decide_general_choice(session, "test-key", JEV_MODEL, question, options))
    assert "Option 2" in reply
    assert "Konfidenz 90%" in reply
    request = session.requests[0][1]["json"]
    assert request["questions"]["best"]["type"] == "choice"
    assert request["questions"]["best"]["criteria"]["option_1"] == options[0]


def test_jev_rejects_incomplete_choice_probabilities() -> None:
    """Do not present fabricated or incomplete provider probabilities."""
    session = _ChoiceSession({"type": "choice", "choice": "option_2",
                              "probabilities": {"option_2": 1}, "confidence": 0.9})
    with pytest.raises(OpenRouterDecisionError, match="invalid choice"):
        asyncio.run(decide_general_choice(session, "test-key", JEV_MODEL, "Choose", ["A", "B"]))


class _BacktestCapabilities:
    """Supply two path-free managed backtest projections."""

    def __init__(self):
        self.calls = []

    async def dispatch(self, owner, conversation_id, tool, args):
        """Record only read-capability calls."""
        self.calls.append((tool, args))
        if tool == "list_backtests":
            return {"backtests": [{"resource": "pbgui://backtest/v8/one", "name": "one"},
                                  {"resource": "pbgui://backtest/v8/two", "name": "two"}]} if args["version"] == "v8" else {"backtests": []}
        if tool == "get_backtest_projection":
            return {"metrics": {"drawdown_worst": 0.08 if args["resource"].endswith("two") else 0.2}}
        raise AssertionError(tool)


def test_jev_compares_managed_backtests() -> None:
    """Backtest questions fetch server projections and leave PBGui unchanged."""
    capabilities = _BacktestCapabilities()
    session = _ChoiceSession()
    reply = asyncio.run(decide_backtest_results(capabilities, session, "test-key", "owner", "chat", JEV_MODEL,
                                                "Welche Backtests sind besser?", {"entities": [{"kind": "backtest_archive", "version": "v8"}]}))
    assert "2 aktuelle Backtest-Resultate" in reply
    assert "Option 2" in reply
    assert "drawdown_worst" not in reply
    assert "drawdown_worst" in session.requests[0][1]["json"]["questions"]["best"]["criteria"]["option_2"]
    assert [tool for tool, _ in capabilities.calls] == ["list_backtests", "get_backtest_projection", "get_backtest_projection"]


class _StructuredSession:
    """Capture a typed Jev request and provide a deterministic provider answer."""

    def __init__(self, answers):
        self.answers = answers
        self.requests = []

    def post(self, url, **kwargs):
        """Return one provider-shaped decision envelope."""
        self.requests.append((url, kwargs))
        return _FakeResponse({"answers": self.answers})


def test_jev_accepts_all_three_user_supplied_question_types() -> None:
    """Explicit JSON can combine Choice, Score, and Noul in one request."""
    spec = {"state": {"incident": "CSV fails for every workspace; JSON works"}, "questions": {
        "route": {"type": "choice", "instructions": "Which team should handle the incident?",
                  "criteria": {"billing": "Payment issues", "technical": "Product errors"}},
        "severity": {"type": "score", "instructions": "How severe is the incident?",
                     "criteria": ["Cosmetic", "Workaround exists", "No workaround"]},
        "blocked": {"type": "noul", "instructions": "Is the customer completely blocked?"},
    }}
    message = "```jev\n" + json.dumps(spec) + "\n```"
    assert parse_structured_jev_request(message) == spec
    session = _StructuredSession({
        "route": {"type": "choice", "choice": "technical", "probabilities": {"billing": 0.1, "technical": 0.9}, "confidence": 0.8},
        "severity": {"type": "score", "score": 1.2, "legend": {"0": "Cosmetic", "1": "Workaround exists", "2": "No workaround"},
                     "probabilities": {"0": 0, "1": 0.8, "2": 0.2}, "confidence": 0.7},
        "blocked": {"type": "noul", "noul": 0.15},
    })
    reply = asyncio.run(decide_user_jev_request(session, "test-key", JEV_MODEL, spec))
    assert "technical" in reply and "1.20" in reply and "15%" in reply
    request = session.requests[0][1]["json"]
    assert request["state"] == spec["state"]
    assert request["questions"] == spec["questions"]
    assert set(request) == {"model", "state", "questions"}


def test_jev_user_json_cannot_select_pb_gui_tools() -> None:
    """A user-defined decision cannot add PBGui reads or actions to its payload."""
    spec = {"state": "hello", "questions": {"ok": {"type": "noul", "instructions": "Is this okay?"}},
            "sources": [{"tool": "get_optimizer_config", "args": {"version": "v8", "name": "private"}}]}
    session = _StructuredSession({})
    with pytest.raises(OpenRouterDecisionError, match="approved preview"):
        asyncio.run(decide_user_jev_request(session, "test-key", JEV_MODEL, spec))
    assert not session.requests


@pytest.mark.parametrize("answer", [
    {"type": "noul", "noul": float("nan")},
    {"type": "noul", "noul": 1.5},
    {"type": "choice", "choice": "x", "probabilities": {"x": 1}, "confidence": 1},
])
def test_jev_rejects_invalid_structured_answers(answer) -> None:
    """Incomplete and non-finite provider answers are never presented."""
    spec = {"state": "case", "questions": {"ok": {"type": "noul", "instructions": "Is this okay?"}}}
    with pytest.raises(OpenRouterDecisionError):
        asyncio.run(decide_user_jev_request(_StructuredSession({"ok": answer}), "test-key", JEV_MODEL, spec))


def test_jev_chat_structured_turn_does_not_read_pb_gui(tmp_path: Path) -> None:
    """An explicit user JSON decision bypasses every PBGui capability."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")
    session = _StructuredSession({"okay": {"type": "noul", "noul": 0.65}})

    async def fake_http_session():
        """Return the isolated provider fixture."""
        return session

    class NoReads:
        """Fail if the Jev JSON path reads managed PBGui resources."""

        async def dispatch(self, *_args):
            """Reject every attempted PBGui read."""
            raise AssertionError("PBGui read was not authorized")

    service._http_session = fake_http_session
    service.capabilities = NoReads()
    spec = {"state": {"price": 12}, "questions": {"okay": {"type": "noul", "instructions": "Is the price acceptable?"}}}
    reply = asyncio.run(service._openrouter_jev_chat(owner, JEV_MODEL, json.dumps(spec), {}, "conversation"))
    assert "65%" in reply
    assert session.requests[0][1]["json"]["state"] == {"price": 12}


@pytest.mark.parametrize("message,expected_type", [
    ("Daten:\nCPU-Backtests laufen, GPU nicht.\nJa/Nein: Sind alle Backtests blockiert?", "noul"),
    ("Data:\nSome tasks work, others wait.\nScore: How severe is the outage?\nLevels:\n1. No impact\n2. Some delayed\n3. All stopped", "score"),
])
def test_jev_plain_text_templates_cover_noul_and_score(message: str, expected_type: str) -> None:
    """Users can ask typed Jev questions without authoring JSON."""
    spec = parse_structured_jev_request(message)
    assert spec["questions"]["decision"]["type"] == expected_type
    assert spec["state"]


def test_jev_plain_text_requires_explicit_data_and_levels() -> None:
    """Missing evidence or score levels fail before contacting a provider."""
    with pytest.raises(OpenRouterDecisionError):
        parse_structured_jev_request("Daten:\nBewertung: Wie schwer ist es?")
    with pytest.raises(OpenRouterDecisionError):
        parse_structured_jev_request("Daten:\nEtwas ist kaputt.\nBewertung: Wie schwer ist es?")


def test_jev_noul_can_include_true_false_criteria() -> None:
    """User-supplied Noul criteria are passed through without inferred facts."""
    spec = {"state": "CPU jobs still run", "questions": {"blocked": {
        "type": "noul", "instructions": "Are all jobs blocked?",
        "criteria": {"true": "No job can run", "false": "At least one job can run"},
    }}}
    session = _StructuredSession({"blocked": {"type": "noul", "noul": 0.1}})
    asyncio.run(decide_user_jev_request(session, "test-key", JEV_MODEL, spec))
    assert session.requests[0][1]["json"]["questions"]["blocked"]["criteria"] == spec["questions"]["blocked"]["criteria"]


def test_jev_approved_optimizer_payload_cannot_change_before_billing() -> None:
    """A stale transfer review blocks every billable Jev request."""
    session = _FakeSession()
    with pytest.raises(OpenRouterDecisionError, match="changed after Jev approval"):
        asyncio.run(decide_backtest_candidates(
            _FakeCapabilities(count=2), session, "test-key", "owner", "conversation",
            JEV_MODEL, "Which Pareto candidate should I backtest?", _run_context(2),
            expected_payload_digest="0" * 64,
        ))
    assert session.requests == []
