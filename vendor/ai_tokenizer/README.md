# Local token-counting proxy

OpenAI cl100k_base vocabulary from tiktoken 0.12.0 (MIT; see LICENSE).
Source: https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken
SHA256: 223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7

This is NOT Jev’s tokenizer. Jev’s tokenizer and a preflight token-count API are
not publicly specified. PBGui uses this local BPE proxy with 25% reserve plus
512 framing tokens and labels the result an estimate. No runtime downloads.
