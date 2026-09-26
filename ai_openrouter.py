"""OpenRouter Jev decisions over explicit user data and bounded PBGui results."""

from __future__ import annotations

import asyncio
import hashlib
from bisect import bisect_left
import json
import math
import re
import statistics
from typing import Any

import aiohttp

from logging_helpers import human_log as _log

SERVICE = "AIOpenRouter"
JEV_MODEL = "typesafe/jev-1.13"
OPENROUTER_API = "https://openrouter.ai/api"
DEFAULT_JEV_BUDGET_USD = 0.01
_JEV_MAX_REQUEST_BYTES = 30_000  # Below Jev 1.13's 32K-token context even at one token per byte.


class OpenRouterDecisionError(RuntimeError):
    """A safe, user-facing Jev decision error."""


async def check_jev_budget(
    session: aiohttp.ClientSession, api_key: str, requests: list[dict[str, Any]],
    max_cost_usd: float,
) -> None:
    """Fail closed before billing when current provider pricing exceeds the budget.

    UTF-8 bytes conservatively bound text tokens; a second factor covers
    provider-side framing. This is a preflight estimate, not reported spend.
    """
    try:
        async with session.get(
            f"{OPENROUTER_API}/v1/model/{JEV_MODEL}",
            headers={"Authorization": f"Bearer {api_key}"},
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:
            if response.status != 200:
                raise OpenRouterDecisionError("Jev pricing is unavailable; no data was sent")
            raw = await response.content.read(64 * 1024 + 1)
            if len(raw) > 64 * 1024:
                raise OpenRouterDecisionError("Jev pricing response is too large; no data was sent")
            model = json.loads(raw)
        data = model.get("data") if isinstance(model, dict) else None
        pricing = data.get("pricing") if isinstance(data, dict) else None
        prompt_price = float(pricing.get("prompt")) if isinstance(pricing, dict) else float("nan")
        completion_price = float(pricing.get("completion")) if isinstance(pricing, dict) else float("nan")
        if not (math.isfinite(prompt_price) and prompt_price >= 0
                and math.isfinite(completion_price) and completion_price == 0):
            raise OpenRouterDecisionError("Jev pricing cannot be verified; no data was sent")
        total_bytes = sum(len(json.dumps(request, allow_nan=False, ensure_ascii=False,
                                         separators=(",", ":")).encode("utf-8")) for request in requests)
        if total_bytes * 2 * prompt_price > max_cost_usd:
            raise OpenRouterDecisionError("Jev analysis exceeds the configured USD budget; no data was sent")
    except OpenRouterDecisionError:
        raise
    except (aiohttp.ClientError, TimeoutError, ValueError, TypeError, KeyError) as exc:
        raise OpenRouterDecisionError("Jev pricing could not be checked; no data was sent") from exc


def _run_identity(context: dict[str, Any]) -> tuple[str, str, str, int | None]:
    """Extract a run's exact result ID and optional legacy Pareto count."""
    for entity in context.get("entities", []):
        if entity.get("kind") == "optimizer_run" and entity.get("version") in {"v7", "v8"}:
            label = str(entity.get("name") or "")
            count_match = re.search(r"(?:^| \| )paretos=(\d+)(?: \| |$)", label)
            return entity["version"], label.split(" | ", 1)[0], str(entity.get("result_id") or ""), int(count_match.group(1)) if count_match else None
    return "", "", "", None


def _noul_answers(payload: object, candidate_ids: set[str]) -> dict[str, float]:
    """Accept only complete, finite Jev backtest-priority probabilities."""
    answers = payload.get("answers") if isinstance(payload, dict) else None
    if not isinstance(answers, dict) or set(answers) != candidate_ids:
        raise OpenRouterDecisionError("Jev returned incomplete candidate decisions")
    probabilities = {}
    for identifier in candidate_ids:
        answer = answers.get(identifier)
        value = answer.get("noul") if isinstance(answer, dict) and answer.get("type") == "noul" else None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
            raise OpenRouterDecisionError("Jev returned an invalid candidate probability")
        probabilities[identifier] = float(value)
    return probabilities


def _is_german(question: str) -> bool:
    """Match common German decision wording for the local result explanation."""
    return bool(re.search(r"\b(welche|welcher|welches|sollte|sollen|backtesten|ergebnisse|resultate)\b", question.casefold()))


def _user_options(question: str) -> list[str]:
    """Read explicitly enumerated choices without guessing choices from prose."""
    lines = question.splitlines()
    start = next((index for index, line in enumerate(lines) if re.match(r"^\s*(?:options|optionen|alternativen|varianten)\s*:\s*$", line, re.I)), None)
    if start is None:
        matches = [re.match(r"^\s*(?:option|variante)\s+[A-Za-z0-9]{1,12}\s*[:.)-]\s*(.+)$", line, re.I) for line in lines]
        options = [match.group(1).strip() for match in matches if match]
    else:
        options = []
        for line in lines[start + 1:]:
            match = re.match(r"^\s*(?:[-*]\s+|\d{1,2}[.)]\s+|(?:option|variante)\s+[A-Za-z0-9]{1,12}\s*[:.)-]\s*)(.+)$", line, re.I)
            if match:
                options.append(match.group(1).strip())
            elif options and line.strip():
                break
    if not 2 <= len(options) <= 20 or any(not option or len(option) > 500 for option in options):
        return []
    return options


def parse_structured_jev_request(message: str) -> dict[str, Any] | None:
    """Recognize an explicitly supplied Jev request, never ordinary chat prose."""
    source = message.strip()
    if source.startswith("```jev"):
        if not source.endswith("```"):
            raise OpenRouterDecisionError("The Jev JSON block is incomplete")
        source = source[6:-3].strip()
    elif not source.startswith("{"):
        return _parse_text_jev_request(source)
    try:
        request = json.loads(source)
    except (TypeError, ValueError) as exc:
        raise OpenRouterDecisionError("Jev request must contain valid JSON") from exc
    if not isinstance(request, dict) or "questions" not in request:
        raise OpenRouterDecisionError("Jev request needs a questions object")
    return request


def _parse_text_jev_request(message: str) -> dict[str, Any] | None:
    """Parse explicit Data plus Yes/No or Score templates without guessing intent."""
    lines = message.splitlines()
    if not lines or not re.fullmatch(r"\s*(?:Daten|Data)\s*:\s*", lines[0], re.I):
        return None
    question_index = next((index for index, line in enumerate(lines[1:], 1)
                           if re.match(r"\s*(?:Ja/Nein|Yes/No|Bewertung|Score)\s*:", line, re.I)), None)
    if question_index is None:
        raise OpenRouterDecisionError("Jev data needs a Yes/No or Score question")
    state = "\n".join(lines[1:question_index]).strip()
    match = re.fullmatch(r"\s*(Ja/Nein|Yes/No|Bewertung|Score)\s*:\s*(.+?)\s*", lines[question_index], re.I)
    if not state or not match:
        raise OpenRouterDecisionError("Jev needs data and a question on one line")
    kind = "noul" if match.group(1).casefold() in {"ja/nein", "yes/no"} else "score"
    question = {"type": kind, "instructions": match.group(2)}
    rest = lines[question_index + 1:]
    if kind == "score":
        if not rest or not re.fullmatch(r"\s*(?:Stufen|Levels)\s*:\s*", rest[0], re.I):
            raise OpenRouterDecisionError("Jev Score needs ordered Stufen/Levels")
        levels = []
        for line in rest[1:]:
            if not line.strip():
                continue
            level = re.fullmatch(r"\s*\d{1,2}[.)]\s+(.+?)\s*", line)
            if not level:
                raise OpenRouterDecisionError("Jev Score levels need numbered descriptions")
            levels.append(level.group(1))
        question["criteria"] = levels
    elif any(line.strip() for line in rest):
        raise OpenRouterDecisionError("Jev Yes/No question has unexpected trailing text")
    return {"state": state, "questions": {"decision": question}}


def _valid_probability(value: object) -> bool:
    """Accept finite provider probabilities only."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and 0 <= value <= 1


def _validated_jev_questions(value: object) -> dict[str, dict[str, Any]]:
    """Bound the three documented Jev question shapes."""
    if not isinstance(value, dict) or not 1 <= len(value) <= 8:
        raise OpenRouterDecisionError("Jev needs 1 to 8 structured questions")
    questions = {}
    for name, question in value.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", name) or not isinstance(question, dict):
            raise OpenRouterDecisionError("Jev question IDs are invalid")
        kind = question.get("type")
        instruction = question.get("instructions")
        if kind not in {"choice", "score", "noul"} or not isinstance(instruction, str) or not 5 <= len(instruction.strip()) <= 1000:
            raise OpenRouterDecisionError("Jev questions need a supported type and instructions")
        if set(question) - {"type", "instructions", "criteria"}:
            raise OpenRouterDecisionError("Jev question contains unsupported fields")
        normalized = {"type": kind, "instructions": instruction.strip()}
        criteria = question.get("criteria")
        if kind == "choice":
            if not isinstance(criteria, dict) or not 2 <= len(criteria) <= 20 or any(
                not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", key)
                or not isinstance(description, str) or not 1 <= len(description.strip()) <= 500
                for key, description in criteria.items()
            ):
                raise OpenRouterDecisionError("Jev Choice needs 2 to 20 described options")
            normalized["criteria"] = {key: description.strip() for key, description in criteria.items()}
        elif kind == "score":
            if not isinstance(criteria, list) or not 2 <= len(criteria) <= 10 or any(
                not isinstance(description, str) or not 1 <= len(description.strip()) <= 500 for description in criteria
            ):
                raise OpenRouterDecisionError("Jev Score needs 2 to 10 ordered levels")
            normalized["criteria"] = [description.strip() for description in criteria]
        elif criteria is not None:
            if not isinstance(criteria, dict) or set(criteria) != {"true", "false"} or any(
                not isinstance(description, str) or not 1 <= len(description.strip()) <= 500
                for description in criteria.values()
            ):
                raise OpenRouterDecisionError("Jev Noul criteria must describe true and false")
            normalized["criteria"] = {key: criteria[key].strip() for key in ("true", "false")}
        questions[name] = normalized
    return questions


def _format_structured_jev_answers(payload: object, questions: dict[str, dict[str, Any]]) -> str:
    """Show complete typed provider answers without computing judgments locally."""
    answers = payload.get("answers") if isinstance(payload, dict) else None
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise OpenRouterDecisionError("Jev returned incomplete structured answers")
    lines = []
    for name, question in questions.items():
        answer = answers[name]
        kind = question["type"]
        if not isinstance(answer, dict) or answer.get("type") != kind:
            raise OpenRouterDecisionError("Jev returned an invalid answer type")
        lines.append(f"{name} — {question['instructions']}")
        if kind == "noul":
            if not _valid_probability(answer.get("noul")):
                raise OpenRouterDecisionError("Jev returned an invalid Noul probability")
            lines.append(f"Yes probability / Ja-Wahrscheinlichkeit: {answer['noul']:.0%}")
        else:
            expected = question["criteria"]
            probabilities = answer.get("probabilities")
            confidence = answer.get("confidence")
            keys = set(expected) if kind == "choice" else {str(index) for index in range(len(expected))}
            if not isinstance(probabilities, dict) or set(probabilities) != keys or any(
                not _valid_probability(probability) for probability in probabilities.values()
            ) or not _valid_probability(confidence):
                raise OpenRouterDecisionError("Jev returned invalid answer probabilities")
            if kind == "choice":
                choice = answer.get("choice")
                if choice not in expected:
                    raise OpenRouterDecisionError("Jev returned an unknown option")
                lines.append(f"Choice / Wahl: {choice} — {expected[choice]} (confidence / Konfidenz {confidence:.0%})")
                lines.extend(f"- {key}: {probabilities[key]:.0%}" for key in expected)
            else:
                score = answer.get("score")
                legend = answer.get("legend")
                if (isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score)
                    or not 0 <= score <= len(expected) - 1 or not isinstance(legend, dict)
                    or set(legend) != keys or any(legend[str(index)] != description for index, description in enumerate(expected))):
                    raise OpenRouterDecisionError("Jev returned an invalid Score")
                lines.append(f"Score / Bewertung: {score:.2f} (0–{len(expected) - 1}; confidence / Konfidenz {confidence:.0%})")
                lines.extend(f"- {index}: {probabilities[str(index)]:.0%} — {description}" for index, description in enumerate(expected))
        lines.append("")
    return "\n".join(lines).strip()


def prepare_user_jev_payload(
    model: str, spec: dict[str, Any], source_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the exact bounded Decisions payload shown for transfer approval."""
    if model != JEV_MODEL or set(spec) - {"state", "questions", "sources"}:
        raise OpenRouterDecisionError("Jev request contains unsupported fields")
    state = spec.get("state")
    if not isinstance(state, (str, dict, list)):
        raise OpenRouterDecisionError("Jev state must be text, an object, or an array")
    questions = _validated_jev_questions(spec.get("questions"))
    sources = spec.get("sources", [])
    if not isinstance(sources, list):
        raise OpenRouterDecisionError("Jev sources must be a list")
    if sources:
        names = [item.get("name") for item in sources if isinstance(item, dict)]
        if (len(names) != len(sources) or any(not isinstance(name, str) for name in names)
            or len(set(names)) != len(names) or not isinstance(source_data, dict)
            or set(names) != set(source_data)):
            raise OpenRouterDecisionError("Jev PBGui data needs an approved preview")
        state = {"input": state, "pbgui": source_data}
    elif source_data:
        raise OpenRouterDecisionError("Jev data does not match the request")
    request = {"model": model, "state": state, "questions": questions}
    try:
        encoded = json.dumps(request, allow_nan=False, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise OpenRouterDecisionError("Jev state is not valid JSON data") from exc
    if len(encoded) > 24_000:
        raise OpenRouterDecisionError("Jev data exceeds 24 KB; narrow the PBGui sources")
    return request


async def decide_user_jev_request(
    session: aiohttp.ClientSession, api_key: str, model: str, spec: dict[str, Any],
    approved_payload: dict[str, Any] | None = None,
    max_cost_usd: float = DEFAULT_JEV_BUDGET_USD,
) -> str:
    """Run the user request, using only the exact previously approved PBGui data."""
    if approved_payload is None:
        request = prepare_user_jev_payload(model, spec)
    else:
        state = approved_payload.get("state") if isinstance(approved_payload, dict) else None
        source_data = state.get("pbgui") if isinstance(state, dict) else None
        request = prepare_user_jev_payload(model, spec, source_data)
        if request != approved_payload:
            raise OpenRouterDecisionError("Jev approval no longer matches the request")
    await check_jev_budget(session, api_key, [request], max_cost_usd)
    try:
        async with session.post(f"{OPENROUTER_API}/alpha/decisions", headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=request, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status != 200:
                _log(SERVICE, f"OpenRouter Jev structured request failed with HTTP {response.status}", level="WARNING")
                raise OpenRouterDecisionError("OpenRouter could not complete the Jev decision")
            raw = await response.content.read(2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024:
                raise OpenRouterDecisionError("OpenRouter decision response is too large")
            payload = json.loads(raw)
    except OpenRouterDecisionError:
        raise
    except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
        _log(SERVICE, f"OpenRouter Jev structured request failed: {type(exc).__name__}", level="WARNING")
        raise OpenRouterDecisionError("OpenRouter Jev is temporarily unavailable") from exc
    return _format_structured_jev_answers(payload, request["questions"])


async def decide_general_choice(
    session: aiohttp.ClientSession, api_key: str, model: str, question: str,
    options: list[str], context: dict[str, Any] | None = None, labels: list[str] | None = None,
    max_cost_usd: float = DEFAULT_JEV_BUDGET_USD,
) -> str:
    """Let Jev choose among explicit, bounded options using its Choice primitive."""
    if model != JEV_MODEL or not 2 <= len(options) <= 20:
        raise OpenRouterDecisionError("Jev needs 2 to 20 explicit options")
    if labels is not None and (len(labels) != len(options) or any(not label or len(label) > 160 for label in labels)):
        raise OpenRouterDecisionError("Jev choice labels are invalid")
    display = labels or options
    criteria = {f"option_{index + 1}": option for index, option in enumerate(options)}
    question_text = question[:12000]
    safe_context = {}
    if isinstance(context, dict):
        for key in ("page_key", "section", "title"):
            if isinstance(context.get(key), str):
                safe_context[key] = context[key][:200]
    request = {"model": model, "state": {"user_goal_and_data": question_text, "page": safe_context},
               "questions": {"best": {"type": "choice", "instructions": "Which listed option best satisfies the user's stated goal and evidence? Use only supplied data; do not assume missing facts.", "criteria": criteria}}}
    await check_jev_budget(session, api_key, [request], max_cost_usd)
    try:
        async with session.post(f"{OPENROUTER_API}/alpha/decisions", headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=request, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status != 200:
                _log(SERVICE, f"OpenRouter Jev choice failed with HTTP {response.status}", level="WARNING")
                raise OpenRouterDecisionError("OpenRouter could not complete the Jev decision")
            raw = await response.content.read(2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024:
                raise OpenRouterDecisionError("OpenRouter decision response is too large")
            payload = json.loads(raw)
    except OpenRouterDecisionError:
        raise
    except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
        _log(SERVICE, f"OpenRouter Jev choice failed: {type(exc).__name__}", level="WARNING")
        raise OpenRouterDecisionError("OpenRouter Jev is temporarily unavailable") from exc
    answer = payload.get("answers", {}).get("best") if isinstance(payload, dict) and isinstance(payload.get("answers"), dict) else None
    probabilities = answer.get("probabilities") if isinstance(answer, dict) else None
    choice = answer.get("choice") if isinstance(answer, dict) else None
    confidence = answer.get("confidence") if isinstance(answer, dict) else None
    if (not isinstance(answer, dict) or answer.get("type") != "choice" or choice not in criteria
        or not isinstance(probabilities, dict) or set(probabilities) != set(criteria)
        or any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1 for value in probabilities.values())
        or isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not math.isfinite(confidence) or not 0 <= confidence <= 1):
        raise OpenRouterDecisionError("Jev returned an invalid choice")
    german = _is_german(question) or bool(re.search(r"\b(entscheidung|bevorzuge|bewerte|optionen|varianten)\b", question.casefold()))
    heading = f"Jev waehlt Option {int(choice.split('_')[1])}: {display[int(choice.split('_')[1]) - 1]} (Konfidenz {confidence:.0%})." if german else f"Jev chooses option {int(choice.split('_')[1])}: {display[int(choice.split('_')[1]) - 1]} (confidence {confidence:.0%})."
    lines = [heading, "", "Jev-Wahrscheinlichkeiten:" if german else "Jev probabilities:"]
    for key in sorted(criteria, key=lambda item: -probabilities[item]):
        lines.append(f"- {key.replace('option_', 'Option ')}: {probabilities[key]:.0%} — {display[int(key.split('_')[1]) - 1]}")
    lines.append("\nGrundlage sind nur die uebergebenen Angaben; fehlende Daten wurden nicht ergaenzt." if german else "\nOnly the supplied data was considered; missing facts were not filled in.")
    return "\n".join(lines)


async def decide_backtest_results(capabilities: Any, session: aiohttp.ClientSession, api_key: str,
                                  owner: str, conversation_id: str, model: str, question: str,
                                  context: dict[str, Any], max_cost_usd: float = DEFAULT_JEV_BUDGET_USD) -> str:
    """Compare recent managed backtest results with their projected metrics."""
    versions = {entity.get("version") for entity in context.get("entities", []) if isinstance(entity, dict) and entity.get("kind", "").startswith("backtest_") and entity.get("version") in {"v7", "v8"}}
    if not versions:
        versions = {"v7", "v8"}
    records = []
    labels = []
    for version in sorted(versions):
        listed = await capabilities.dispatch(owner, conversation_id, "list_backtests", {"version": version, "limit": 10})
        for item in listed.get("backtests", []):
            if not isinstance(item, dict) or not isinstance(item.get("resource"), str):
                continue
            projection = await capabilities.dispatch(owner, conversation_id, "get_backtest_projection", {"version": version, "resource": item["resource"], "max_points": 1, "max_fills": 1})
            label = str(item.get("name") or item.get("result_name") or item.get("config_name") or item["resource"][-12:])[:100]
            metrics = projection.get("metrics") if isinstance(projection, dict) else None
            display_label = f"{version} {label} ({item['resource'][-8:]})"
            labels.append(display_label)
            records.append(display_label + ": " + json.dumps({"summary": item, "metrics": metrics}, ensure_ascii=False, allow_nan=False, separators=(",", ":"))[:1800])
    if len(records) < 2:
        return "Jev braucht mindestens zwei verfuegbare Backtest-Resultate fuer einen Vergleich." if _is_german(question) else "Jev needs at least two available backtest results to compare."
    reply = await decide_general_choice(session, api_key, model, question, records, context, labels, max_cost_usd)
    scope = f"Jev verglich {len(records)} aktuelle Backtest-Resultate (maximal zehn pro Version).\n\n" if _is_german(question) else f"Jev compared {len(records)} recent backtest results (up to ten per version).\n\n"
    return scope + reply


async def decide_backtest_candidates(
    capabilities: Any,
    session: aiohttp.ClientSession,
    api_key: str,
    owner: str,
    conversation_id: str,
    model: str,
    question: str,
    context: dict[str, Any],
    capture_selection: Any = None,
    max_cost_usd: float = DEFAULT_JEV_BUDGET_USD,
    preview_only: bool = False,
    expected_payload_digest: str = "",
    selection_limit: int | None = None,
) -> str | dict[str, Any]:
    """Profile every Pareto metric and assess candidates with bounded Jev calls."""
    if model != JEV_MODEL:
        raise OpenRouterDecisionError("Selected Jev model is unavailable")
    if not isinstance(question, str) or not question.strip() or len(question) > 4000:
        raise OpenRouterDecisionError("Jev analysis question is invalid; no data was sent")
    german = _is_german(question)
    if selection_limit is not None and (
        isinstance(selection_limit, bool) or not isinstance(selection_limit, int)
        or not 1 <= selection_limit <= 50
    ):
        raise OpenRouterDecisionError("Jev candidate limit must be between 1 and 50")
    version, run_name, result_id, legacy_count = _run_identity(context)
    versions = [version] if version else ["v7", "v8"]
    runs = []
    for current_version in versions:
        result = await capabilities.dispatch(owner, conversation_id, "list_optimizer_runs", {"version": current_version, "limit": 50})
        runs.extend((current_version, item) for item in result.get("runs", []) if isinstance(item, dict))
    if result_id:
        matches = [(v, item) for v, item in runs if v == version and item.get("result_id") == result_id]
    elif run_name:
        matches = [(v, item) for v, item in runs if v == version and item.get("name") == run_name and (legacy_count is None or item.get("pareto_count") == legacy_count)]
    else:
        matches = [(v, item) for v, item in runs if item.get("name") and str(item["name"]).casefold() in question.casefold()]
        if not matches and len(runs) == 1:
            matches = runs
    if len(matches) != 1:
        if not matches and (result_id or run_name):
            return ("Das ausgewaehlte Optimize-Resultat ist nicht mehr verfuegbar. Oeffne es erneut und frage mit Seitenkontext." if german else "The selected Optimize result is no longer available. Open it again and ask with page context.")
        return ("Mehrere Optimize-Laeufe passen. Oeffne das gewuenschte Resultat auf der Pareto-Seite und frage mit aktiviertem Seitenkontext erneut." if german else "Several Optimize runs match. Open the intended result on its Pareto page and ask again with page context enabled.")
    version, selected_run = matches[0]
    resource = selected_run.get("resource")
    if not isinstance(resource, str):
        raise OpenRouterDecisionError("Optimizer result reference is unavailable")
    candidates = []
    offset = 0
    while True:
        analysis = await capabilities.dispatch(owner, conversation_id, "get_optimizer_run_analysis", {"version": version, "resource": resource, "limit": 24, "offset": offset, "all_metrics": True})
        total = analysis.get("total")
        page = analysis.get("pareto")
        if not isinstance(total, int) or total < 0 or total > 1000 or not isinstance(page, list):
            raise OpenRouterDecisionError("Optimizer result is too large or incomplete for Jev")
        candidates.extend(page)
        offset += len(page)
        if offset >= total:
            break
        if not page or offset > 1000:
            raise OpenRouterDecisionError("Optimizer Pareto listing changed during Jev analysis")
    if not candidates:
        return ("Dieses Optimize-Resultat hat keine Pareto-Kandidaten." if german else "This Optimize result has no Pareto candidates.")
    if len({item.get("resource") for item in candidates if isinstance(item, dict)}) != len(candidates):
        raise OpenRouterDecisionError("Optimizer Pareto listing is inconsistent")
    metric_values: dict[str, list[float]] = {}
    records = []
    for index, item in enumerate(candidates):
        metrics = item.get("metrics") if isinstance(item, dict) else None
        if not isinstance(metrics, dict) or not isinstance(item.get("resource"), str):
            raise OpenRouterDecisionError("Optimizer Pareto metrics are incomplete")
        finite = {key: value for key, value in metrics.items() if isinstance(key, str) and isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)}
        for key, value in finite.items():
            metric_values.setdefault(key, []).append(float(value))
        records.append({"id": f"c{index + 1}", "name": str(item.get("name") or "")[:128], "metrics": finite, "resource": item["resource"]})
    if not metric_values:
        return ("Dieses Resultat enthaelt keine endlichen Kennzahlen fuer eine Jev-Bewertung." if german else "This result has no finite metrics for Jev to assess.")
    # Jev receives exactly one bounded decision request. PBGui profiles every
    # numeric column locally, and names precisely which columns reached Jev raw.
    metric_names = sorted(metric_values)
    values_by_metric = {
        key: [item["metrics"].get(key) for item in records]
        for key in metric_names
    }
    variable = [
        key for key in metric_names
        if len(set(values_by_metric[key])) > 1
    ]
    goal_words = {word for word in re.findall(r"[a-z0-9_]{3,}", question.casefold())}
    risk_words = ("drawdown", "loss", "risk", "volatility", "fee", "cost", "liquidation", "exposure", "margin")
    return_words = ("gain", "profit", "return", "adg", "sortino", "sharpe", "roi", "equity", "balance")
    def direction(key: str) -> str:
        lowered = key.casefold()
        if any(word in lowered for word in risk_words):
            return "risk"
        if any(word in lowered for word in return_words):
            return "return"
        return "other"

    def metric_priority(key: str) -> tuple[int, int, float, str]:
        """Prefer goal-matched and decision-relevant columns, without ranking candidates."""
        values = metric_values[key]
        median = statistics.median(values)
        spread = max(values) - min(values)
        goal_match = any(word in key.casefold() for word in goal_words)
        return (int(goal_match), int(direction(key) != "other"),
                min(spread / (abs(median) + 1e-9), 1_000_000), key)

    # Duplicate and constant columns do not need per-candidate values.
    seen_signatures: set[tuple[Any, ...]] = set()
    informative = []
    for key in sorted(variable, key=metric_priority, reverse=True):
        signature = tuple(values_by_metric[key])
        if signature not in seen_signatures:
            informative.append(key)
            seen_signatures.add(signature)
    if not informative and len(records) > 1:
        return ("Alle Pareto-Kandidaten haben dieselben numerischen Kennzahlen; Jev kann daraus keine sinnvolle Reihenfolge ableiten."
                if german else "All Pareto candidates have the same numeric metrics; Jev cannot derive a meaningful ranking.")
    if not informative:
        informative = sorted(metric_names, key=metric_priority, reverse=True)

    def compact_number(value: int | float | None) -> int | float | None:
        """Bound numeric serialization while retaining eight significant digits."""
        if value is None or isinstance(value, int):
            return value
        return float(format(value, ".8g"))

    def percentiles(values: list[int]) -> list[int] | None:
        """Summarize the full set of omitted columns without a hidden score."""
        if not values:
            return None
        ordered = sorted(values)
        return [ordered[round((len(ordered) - 1) * quantile)] for quantile in (0.1, 0.5, 0.9)]

    selected = informative[:24]
    omitted = [key for key in informative if key not in selected]
    constants = [key for key in metric_names if key not in variable]
    constant_reference = {key: compact_number(metric_values[key][0])
                          for key in sorted(constants, key=metric_priority, reverse=True)[:8]}
    grouped_by_candidate = [
        {"risk": [], "return": [], "other": []}
        for _ in records
    ]
    for key in omitted:
        column = values_by_metric[key]
        ordered = sorted(value for value in column if value is not None)
        if len(ordered) < 2 or ordered[0] == ordered[-1]:
            continue
        group = direction(key)
        for index, value in enumerate(column):
            if value is None:
                continue
            rank = round(100 * bisect_left(ordered, value) / (len(ordered) - 1))
            grouped_by_candidate[index][group].append(100 - rank if group == "risk" else rank)
    candidate_states = {
        item["id"]: {
            "id": item["id"],
            "v": [compact_number(item["metrics"].get(key)) for key in selected],
            "p": [percentiles(grouped_by_candidate[index][group])
                  for group in ("risk", "return", "other")],
        }
        for index, item in enumerate(records)
    }
    reference = [
        [compact_number(min(metric_values[key])),
         compact_number(statistics.median(metric_values[key])),
         compact_number(max(metric_values[key]))]
        for key in selected
    ]

    def make_request(batch: list[dict[str, Any]]) -> dict[str, Any]:
        """Share compact run context across independent candidate questions."""
        return {
            "model": model,
            "state": {
                "goal": question,
                "metric_names": selected,
                "metrics_considered": len(metric_names),
                "profiled_distinct_metrics": len(omitted),
                "constant_metrics": constant_reference,
                "candidate_count": len(records),
                "reference": reference,
                "profile_groups": ["risk favorability", "return favorability", "other relative position"],
                "legend": "v aligns with metric_names; reference aligns with metric_names as [min,median,max]; each p group is [10th,50th,90th percentile] on a 0-100 scale.",
                "decision": "Compare each candidate's values and profile with the run under state.goal when deciding holdout-backtest priority.",
                "candidates": [candidate_states[item["id"]] for item in batch],
            },
            "questions": {
                item["id"]: {
                    "type": "noul",
                    "instructions": f"Prioritize {item['id']} for holdout backtest?",
                }
                for item in batch
            },
        }

    def payload_size(batch: list[dict[str, Any]]) -> int:
        """Measure bytes before any billable request."""
        return len(json.dumps(make_request(batch), allow_nan=False, ensure_ascii=False,
                              separators=(",", ":")).encode("utf-8"))

    # Fill each request up to the model's context-safe size. The total transfer
    # is controlled by the owner's configured USD budget before any billable call.
    planned: list[tuple[list[dict[str, Any]], int]] = []
    batch: list[dict[str, Any]] = []
    batch_size = 0
    for record in records:
        expanded = [*batch, record]
        expanded_size = payload_size(expanded)
        if expanded_size <= _JEV_MAX_REQUEST_BYTES:
            batch, batch_size = expanded, expanded_size
            continue
        if not batch:
            raise OpenRouterDecisionError(
                "One candidate exceeds Jev's context-safe request size; no data was sent"
            )
        planned.append((batch, batch_size))
        batch = [record]
        batch_size = payload_size(batch)
        if batch_size > _JEV_MAX_REQUEST_BYTES:
            raise OpenRouterDecisionError(
                "One candidate exceeds Jev's context-safe request size; no data was sent"
            )
    if batch:
        planned.append((batch, batch_size))
    outbound_requests = [make_request(batch) for batch, _ in planned]
    outbound_json = json.dumps(outbound_requests, allow_nan=False, ensure_ascii=False,
                               separators=(",", ":")).encode("utf-8")
    outbound_digest = hashlib.sha256(outbound_json).hexdigest()
    if preview_only:
        return {"requests": outbound_requests, "sha256": outbound_digest,
                "candidate_count": len(records), "metric_count": len(metric_names)}
    if expected_payload_digest and outbound_digest != expected_payload_digest:
        raise OpenRouterDecisionError("Optimizer results changed after Jev approval; no data was sent")
    await check_jev_budget(session, api_key, outbound_requests, max_cost_usd)
    raw_columns = len(selected)
    semaphore = asyncio.Semaphore(3)

    async def assess_batch(batch: list[dict[str, Any]]) -> dict[str, float]:
        """Return validated probabilities from one planned, bounded Jev request."""
        request = make_request(batch)
        try:
            async with semaphore:
                async with session.post(
                    f"{OPENROUTER_API}/alpha/decisions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=request, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60),
                ) as response:
                    if response.status != 200:
                        _log(SERVICE, f"OpenRouter Jev request failed with HTTP {response.status}", level="WARNING")
                        raise OpenRouterDecisionError("OpenRouter could not complete the Jev decision")
                    raw = await response.content.read(2 * 1024 * 1024 + 1)
                    if len(raw) > 2 * 1024 * 1024:
                        raise OpenRouterDecisionError("OpenRouter decision response is too large")
                    return _noul_answers(json.loads(raw), set(request["questions"]))
        except OpenRouterDecisionError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            _log(SERVICE, f"OpenRouter Jev request failed: {type(exc).__name__}", level="WARNING")
            raise OpenRouterDecisionError("OpenRouter Jev is temporarily unavailable") from exc

    tasks = [asyncio.create_task(assess_batch(batch)) for batch, _ in planned]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    probabilities = {key: probability for batch in results for key, probability in batch.items()}
    ranked = sorted(records, key=lambda item: (-probabilities[item["id"]], item["name"]))
    winners = ranked[:selection_limit] if selection_limit is not None else [
        item for item in ranked if probabilities[item["id"]] > 0.5
    ]
    if capture_selection is not None and winners:
        selected = await capabilities.dispatch(owner, conversation_id, "select_pareto_candidates", {"version": version, "run_resource": resource, "candidate_resources": [item["resource"] for item in winners], "mode": "replace"})
        await capture_selection(selected)
    if german:
        lines = [f"Jev bewertete alle {len(records)} Pareto-Kandidaten des {version}-Laufs {selected_run.get('name')} mit lokal aus allen {len(metric_values)} Kennzahlen abgeleiteten Profilen; {raw_columns} unterschiedliche Kennzahlen wurden mit Einzelwerten uebermittelt. Die folgenden {len(winners)} Kandidaten sind fuer einen Validierungs-Backtest markiert:", ""]
    else:
        lines = [f"Jev assessed all {len(records)} Pareto candidates in {version} run {selected_run.get('name')} using profiles derived locally from all {len(metric_values)} metrics; {raw_columns} distinct metrics were sent as individual values. The following {len(winners)} candidates are marked for validation backtests:", ""]
    for position, item in enumerate(winners, 1):
        label = "Jev-Backtest-Prioritaet" if german else "Jev backtest priority"
        lines.append(f"{position}. {item['name']} — {label} {probabilities[item['id']]:.0%}")
    if winners:
        lines.append("\nDer erste Kandidat ist Jevs risk-adjusted Champion fuer die Backtest-Reihenfolge. Die Bewertung nutzt Optimize-Metriken; der Holdout-Backtest muss sie unabhaengig pruefen." if german else "\nThe first candidate is Jev's risk-adjusted champion for backtest priority. This assessment uses Optimize metrics; validate independently with holdout backtests.")
    else:
        lines.append("\nJev empfiehlt auf Grundlage dieser Optimize-Metriken keinen Backtest-Kandidaten." if german else "\nJev did not recommend a backtest candidate from these Optimize metrics.")
    return "\n".join(lines)
