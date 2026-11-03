"""OpenRouter connection, authentication and HTTP for the shared Jev decisions."""

from __future__ import annotations

import asyncio
import json
import math
from typing import Any

import aiohttp

from ai_jev import OpenRouterDecisionError
from logging_helpers import human_log as _log

SERVICE = "AIOpenRouter"
OPENROUTER_API = "https://openrouter.ai/api"


async def _read_bounded_body(content, maximum):
    """Wait for EOF, including fragmented responses, without unbounded buffering."""
    try:
        raw = await content.readexactly(maximum + 1)
    except asyncio.IncompleteReadError as exc:
        raw = exc.partial
    if len(raw) > maximum:
        raise OpenRouterDecisionError("OpenRouter response is too large")
    return raw


class OpenRouterJevTransport:
    """Bind one session and key to the connection contract Jev decisions require."""

    def __init__(self, session: aiohttp.ClientSession, api_key: str) -> None:
        """Keep the owner's key server-side for this decision's requests only."""
        self.session = session
        self.api_key = api_key

    def _headers(self, *, json_body: bool = False) -> dict[str, str]:
        """Build the authentication headers of one OpenRouter request."""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        return {**headers, "Content-Type": "application/json"} if json_body else headers

    async def check_access(self) -> None:
        """Verify authentication and remaining key quota; never submit a billed decision."""
        try:
            async with self.session.get(f"{OPENROUTER_API}/v1/key", headers=self._headers(), allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    raise OpenRouterDecisionError(f"Jev preflight failed: OpenRouter authentication/access HTTP {response.status}; research not started")
                payload = json.loads(await _read_bounded_body(response.content, 64 * 1024))
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, dict) or "limit_remaining" not in data:
                raise OpenRouterDecisionError("Jev preflight returned invalid key information; research not started")
            remaining = data["limit_remaining"]
            if remaining is not None:
                if isinstance(remaining, bool) or not isinstance(remaining, (int, float)) or not math.isfinite(remaining):
                    raise OpenRouterDecisionError("Jev preflight returned invalid quota information; research not started")
                if remaining <= 0:
                    raise OpenRouterDecisionError("OpenRouter key spending limit exhausted; research not started")
        except OpenRouterDecisionError:
            raise
        except TimeoutError as exc:
            raise OpenRouterDecisionError("Jev preflight timed out; research not started") from exc
        except aiohttp.ClientError as exc:
            raise OpenRouterDecisionError("Jev preflight connection failed; research not started") from exc
        except (ValueError, TypeError) as exc:
            raise OpenRouterDecisionError("Jev preflight returned invalid JSON; research not started") from exc

    async def model_pricing(self, model: str) -> Any:
        """Return the provider's current pricing object for one decision model."""
        try:
            async with self.session.get(
                f"{OPENROUTER_API}/v1/model/{model}",
                headers=self._headers(),
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status != 200:
                    raise OpenRouterDecisionError("Jev pricing is unavailable; no data was sent")
                raw = await _read_bounded_body(response.content, 64 * 1024)
                if len(raw) > 64 * 1024:
                    raise OpenRouterDecisionError("Jev pricing response is too large; no data was sent")
                document = json.loads(raw)
            data = document.get("data") if isinstance(document, dict) else None
            return data.get("pricing") if isinstance(data, dict) else None
        except OpenRouterDecisionError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError, TypeError, KeyError) as exc:
            raise OpenRouterDecisionError("Jev pricing could not be checked; no data was sent") from exc

    async def send_structured(self, request: dict[str, Any]) -> Any:
        """Submit one validated user or research request without an automatic retry."""
        try:
            async with self.session.post(f"{OPENROUTER_API}/alpha/decisions", headers=self._headers(json_body=True), json=request, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=None, connect=30, sock_read=180)) as response:
                if response.status != 200:
                    _log(SERVICE, f"OpenRouter Jev structured request failed with HTTP {response.status}", level="WARNING")
                    raise OpenRouterDecisionError(f"Jev request failed (OpenRouter HTTP {response.status})")
                raw = await _read_bounded_body(response.content, 2 * 1024 * 1024)
                if len(raw) > 2 * 1024 * 1024:
                    raise OpenRouterDecisionError("OpenRouter decision response is too large")
                return json.loads(raw)
        except OpenRouterDecisionError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            _log(SERVICE, f"OpenRouter Jev structured request failed: {type(exc).__name__}", level="WARNING")
            if isinstance(exc, TimeoutError):
                message = "Jev response timed out; no automatic retry was sent"
            elif isinstance(exc, ValueError):
                message = "Jev returned invalid JSON; no automatic retry was sent"
            else:
                message = "Jev connection failed; no automatic retry was sent"
            raise OpenRouterDecisionError(message) from exc

    async def send_decision(self, request: dict[str, Any], action: str) -> Any:
        """Submit one bounded choice or candidate decision under a short deadline."""
        try:
            async with self.session.post(
                f"{OPENROUTER_API}/alpha/decisions",
                headers=self._headers(json_body=True),
                json=request, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60),
            ) as response:
                if response.status != 200:
                    _log(SERVICE, f"OpenRouter Jev {action} failed with HTTP {response.status}", level="WARNING")
                    raise OpenRouterDecisionError("OpenRouter could not complete the Jev decision")
                raw = await response.content.read(2 * 1024 * 1024 + 1)
                if len(raw) > 2 * 1024 * 1024:
                    raise OpenRouterDecisionError("OpenRouter decision response is too large")
                return json.loads(raw)
        except OpenRouterDecisionError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            _log(SERVICE, f"OpenRouter Jev {action} failed: {type(exc).__name__}", level="WARNING")
            raise OpenRouterDecisionError("OpenRouter Jev is temporarily unavailable") from exc
