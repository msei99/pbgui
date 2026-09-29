"""Offline input-token estimates; no claim to reproduce Jev's private tokenizer."""
from __future__ import annotations

import base64
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path

JEV_CONTEXT_TOKENS = 32_000
_PROXY_HASH = '223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7'
# cl100k_base pattern from OpenAI tiktoken 0.12.0; MIT license in vendor/ai_tokenizer.
_PATTERN = r"""'(?i:[sdmt]|ll|ve|re)|[^\r\n\p{L}\p{N}]?+\p{L}++|\p{N}{1,3}+| ?[^\s\p{L}\p{N}]++[\r\n]*+|\s++$|\s*[\r\n]|\s+(?!\S)|\s"""


@lru_cache(maxsize=1)
def _proxy_encoding():
    """Load a hash-pinned local vocabulary without tiktoken's download/cache path."""
    import tiktoken
    path = Path(__file__).parent / 'vendor' / 'ai_tokenizer' / 'cl100k_base.tiktoken'
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != _PROXY_HASH:
        raise ValueError('Token vocabulary integrity check failed')
    ranks = {base64.b64decode(token): int(rank) for token, rank in
             (line.split() for line in data.splitlines() if line)}
    return tiktoken.Encoding(name='pbgui_cl100k_proxy', pat_str=_PATTERN,
                             mergeable_ranks=ranks, special_tokens={})


def estimate_jev_input_tokens(request: dict) -> int:
    """Estimate the full input using BPE plus 25% reserve and 512 framing tokens.

    The provider is the authority for actual token counts and context limits.
    This proxy is intentionally not advertised as an exact Jev tokenizer.
    """
    text = json.dumps(request, allow_nan=False, ensure_ascii=False, separators=(',', ':'))
    tokens = len(_proxy_encoding().encode_ordinary(text))
    return math.ceil(tokens * 1.25) + 512
