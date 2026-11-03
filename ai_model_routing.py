"""Server-side mapping of one connection and model to the chat or decision adapter."""
from __future__ import annotations

from ai_jev import JEV_MODEL

SERVICE = "AIModelRouting"
CHAT_ADAPTER = "chat"
DECISION_ADAPTER = "decision"
# Explicit configuration: provider catalog metadata never adds a decision model.
_DECISION_MODELS = {"openrouter": frozenset({JEV_MODEL})}


def model_adapter(provider: str, model: str) -> str:
    """Return the adapter that must serve this exact connection and model."""
    return DECISION_ADAPTER if model in _DECISION_MODELS.get(provider, frozenset()) else CHAT_ADAPTER


def is_decision_model(provider: str, model: str) -> bool:
    """Return whether this connection and model use structured decisions."""
    return model_adapter(provider, model) == DECISION_ADAPTER
