"""Immutable generation identity for the shared optimizer workflow engine."""

from dataclasses import dataclass

SERVICE = 'Optimize'


@dataclass(frozen=True)
class OptimizeProfile:
    """Keep filenames, process identities and routes tied to one supported runtime."""

    version: str = 'v8'

    def __post_init__(self):
        """Reject unknown generations instead of selecting a legacy execution path."""
        if self.version not in {'v8', 'v9'}:
            raise ValueError('Unsupported optimizer generation')

    def text(self, value):
        """Expand existing PB8-owned identifiers for the selected engine instance."""
        if self.version == 'v8':
            return value
        return value.replace('pb8', 'pb9').replace('PBv8', 'PBv9').replace('v8', 'v9').replace('V8', 'V9').replace('opt8', 'opt9')
