"""Explicit immutable worker contracts; unpublished native images never rent GPUs."""
from dataclasses import dataclass
import re

from vast_provider import VastError

SERVICE = 'Vast'


@dataclass(frozen=True)
class WorkerProfile:
    """Bind one schema generation to a reviewed source and immutable image."""
    runtime: str
    revision: str
    image: str | None
    schema: str
    repository: str = 'https://github.com/enarjord/passivbot'

    @property
    def published(self):
        """A tag or missing digest is never an executable paid worker contract."""
        return bool(self.image and re.fullmatch(r'ghcr\.io/msei99/pbgui-pb[89]-worker@sha256:[0-9a-f]{64}',self.image))

    def require_published(self):
        """Stop before job publication, credential lookup or any provider request."""
        if not self.published:
            raise VastError('PB9 Vast worker is not published yet. Local optimization is available; cloud rental stays disabled until the native worker image is built, verified and published.',503)
        return self


PB9 = WorkerProfile('pb9','bf7329b039595e65acdf5c14475078912aeede96',
                    'ghcr.io/msei99/pbgui-pb9-worker@sha256:7ee6a7bf0f6bb0c235e87ab2988efd1d2f0529f4348f1d9c20a13d8384395bcb','v9.0.0',
                    'https://github.com/enarjord/passivbot')


def worker_profile(runtime='pb8'):
    """Keep the existing PB8 contract and fail closed for unknown generations."""
    if runtime=='pb8':
        from vast_jobs import IMAGE,REVISION
        return WorkerProfile('pb8',REVISION,IMAGE,'v8')
    if runtime=='pb9':
        return PB9
    raise VastError('Unsupported optimizer runtime',422)


def config_runtime(config):
    """A PB9 schema can never fall through to legacy package-version detection."""
    schema=config.get('config_version') if isinstance(config,dict) else None
    if isinstance(schema,str) and re.fullmatch(r'v9\.\d+\.\d+',schema):return 'pb9'
    if schema is None or isinstance(schema,str) and re.fullmatch(r'v8\.\d+\.\d+',schema):return 'pb8'
    raise VastError('Unsupported cloud config schema',422)
