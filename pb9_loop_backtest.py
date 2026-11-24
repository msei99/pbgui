"""Adapt native PB9 backtest ownership to the shared Loop backend interface."""
from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

SERVICE = 'PB9Loop'


class NativeBacktestLoopAPI:
    """Use the actual PB9 queue, process proofs and canonical config gateway."""

    @staticmethod
    def module():
        """Import lazily so the API owns background startup and shutdown."""
        from api import backtest_v9
        return backtest_v9

    def _queue_lock(self):
        return self.module()._storage_lock()

    def _queue_file(self, identifier):
        return self.module()._job_path(identifier)

    def _snapshot_file(self, identifier):
        return self.module()._job_path(identifier, '.config.json')

    def _read_json(self, path):
        return self.module()._read(path)

    def atomic_write_json(self, path, value):
        return self.module()._write(path, value)

    def _load_queue(self):
        return self.module()._queue()

    def add_to_queue(self, body, session=None):
        from pb9_config import PB9CancelSignal
        module = self.module()
        config = body.get('config')
        if not isinstance(config, dict):
            raise ValueError('PB9 Loop backtest requires a native config')
        return module._enqueue(body, PB9CancelSignal(), loop_identity=body.get('loop_identity'))

    def _queue_status(self, record):
        status, process = self.module()._job_status(record['filename'])
        return status, (process or {}).get('pid')

    def _read_runner_state(self, identifier):
        module = self.module()
        path = module._job_path(identifier, '.state.json')
        return module._read(path) if path.is_file() else {}

    def _terminate_verified(self, identifier):
        return self.module()._stop(identifier)

    def _log_dir(self):
        """Use the native managed backtest log directory."""
        return Path(self.module().PBGDIR) / 'data' / 'logs' / 'backtests_v9'

    def _result_analysis_paths(self, name):
        module = self.module()
        root = module._safe(module._root('results'), module._name(name))
        paths = sorted(root.glob('**/analysis.json'))
        for path in paths:
            path.resolve().relative_to(root.resolve())
            if any(parent.is_symlink() for parent in (path, *path.parents) if parent != root.parent):
                raise ValueError('Native Loop result path must not contain symlinks')
        return [path for path in paths if path.is_file()]

    def _launch(self, identifier, loop_id=None, **_kwargs):
        from pb8_loop_store import authorize_native_job
        module = self.module()
        record = module._read(module._job_path(identifier))
        authorize_native_job(Path(module.PBGDIR), record, loop_id)
        from pb9_config import PB9CancelSignal
        return module._start(identifier, PB9CancelSignal(), loop_id=loop_id)

    @property
    def _worker(self):
        """Keep launch and stop behind exact Loop authorization and native process checks."""
        return SimpleNamespace(launch=self._launch)
