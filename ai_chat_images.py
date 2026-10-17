"""Bounded private chat images and native multimodal provider inputs."""
from __future__ import annotations

import base64
import hashlib
import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_bytes, ensure_private_directory_tree, read_regular_file_nofollow
from support_debug_crypto import decoded_screenshots

SERVICE = 'AIChatImages'
MAX_CONVERSATION_BYTES = 20 * 1024 * 1024
MEDIA_TYPES = {'.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp'}


class ChatImage(BaseModel):
    """Accept raster bytes only, without URLs, paths or original filenames."""
    model_config = ConfigDict(extra='forbid')
    media_type: str = Field(max_length=32)
    data_base64: str = Field(min_length=1, max_length=(4 * 1024 * 1024 + 2) // 3 * 4)


class ChatImageStore:
    """Keep at most 20 MiB per conversation under its validated owner's directory."""

    def __init__(self, root: Path):
        """Use the chat service's private storage root."""
        self.root = root

    def directory(self, owner: str, conversation_id: str) -> Path:
        """Validate both identifiers before resolving a private directory."""
        if not all(re.fullmatch(r'[0-9a-f]{32}', value) for value in (owner, conversation_id)):
            raise ValueError('Invalid image owner or conversation')
        target = self.root / owner / conversation_id
        for path in (self.root, self.root / owner, target):
            if path.is_symlink():
                raise ValueError('Invalid image directory')
        return target

    def save(self, owner: str, conversation_id: str, images: list[dict]) -> list[dict]:
        """Validate an entire batch and its quota before writing generated filenames."""
        decoded = decoded_screenshots({'screenshots': images})
        if not decoded:
            return []
        directory = self.directory(owner, conversation_id)
        ensure_private_directory_tree(self.root, directory)
        records = [(hashlib.sha256(data).hexdigest() + suffix, data, MEDIA_TYPES[suffix]) for suffix, data in decoded]
        with advisory_file_lock(self.root / '.lock'):
            existing = {path.name: path.stat().st_size for path in directory.iterdir() if path.is_file() and not path.is_symlink() and path.suffix in MEDIA_TYPES}
            added = {name: len(data) for name, data, _ in records if name not in existing}
            if sum(existing.values()) + sum(added.values()) > MAX_CONVERSATION_BYTES:
                raise ValueError('This chat has reached its 20 MiB image limit. Start a new chat for more images.')
            for name, data, _ in records:
                atomic_write_private_bytes(directory / name, data)
        return [{'id': name, 'media_type': media, 'size': len(data)} for name, data, media in records]

    def read(self, owner: str, conversation_id: str, image: dict) -> tuple[Path, bytes]:
        """Read only content-addressed local raster files and verify their bytes."""
        name = image.get('id', '')
        if not isinstance(name, str) or not re.fullmatch(r'[0-9a-f]{64}\.(png|jpg|webp)', name):
            raise ValueError('Invalid chat image')
        directory = self.directory(owner, conversation_id)
        path = directory / name
        if not path.exists() or path.stat().st_size > 4 * 1024 * 1024:
            raise ValueError('Chat image is unavailable')
        data = read_regular_file_nofollow(path, directory)
        if hashlib.sha256(data).hexdigest() != name.split('.')[0]:
            raise ValueError('Chat image is invalid')
        return path, data

    def clear(self, owner: str, conversation_id: str) -> None:
        """Delete generated image files when their conversation is removed or pruned."""
        directory = self.directory(owner, conversation_id)
        if not directory.exists():
            return
        with advisory_file_lock(self.root / '.lock'):
            for path in directory.iterdir():
                if re.fullmatch(r'[0-9a-f]{64}\.(png|jpg|webp)', path.name):
                    path.unlink(missing_ok=True)
            if not any(directory.iterdir()):
                directory.rmdir()
            if directory.parent.exists() and not any(directory.parent.iterdir()):
                directory.parent.rmdir()

    def native_history(self, owner: str, conversation_id: str, history: list[dict], protocol: str) -> list[dict]:
        """Convert private image references to the selected provider's image blocks."""
        result = []
        for message in history:
            item = {'role': message['role'], 'content': message['content']}
            if message.get('images'):
                parts = [{'type': 'input_text' if protocol == 'responses' else 'text', 'text': item['content']}]
                for image in message['images']:
                    _, data = self.read(owner, conversation_id, image)
                    media = MEDIA_TYPES[Path(image['id']).suffix]
                    encoded = base64.b64encode(data).decode('ascii')
                    url = 'data:' + media + ';base64,' + encoded
                    if protocol == 'responses':
                        parts.append({'type': 'input_image', 'image_url': url})
                    elif protocol == 'chat':
                        parts.append({'type': 'image_url', 'image_url': {'url': url}})
                    elif protocol == 'messages':
                        parts.append({'type': 'image', 'source': {'type': 'base64', 'media_type': media, 'data': encoded}})
                    else:
                        raise ValueError('This model protocol cannot accept images')
                item['content'] = parts
            result.append(item)
        return result
