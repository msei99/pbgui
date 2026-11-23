"""First-use dependency imports without production runtime reads or network calls."""

import asyncio
from pathlib import Path
import subprocess
import sys
import threading

import pytest


def test_pareto_and_exchange_import_without_unused_heavy_dependencies(tmp_path):
    """Cold imports skip Pandas/CCXT Pro, but real first-use exports still work."""
    root = Path(__file__).resolve().parents[1]
    script = tmp_path / 'cold_import.py'
    script.write_text('''import sys, os, importlib, importlib.abc
root = sys.argv[1]
sys.path.insert(0, root)
def audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes)):
        path = os.path.abspath(os.fsdecode(args[0]))
        if path == root + '/pbgui.ini' or path.startswith(root + '/data/'):
            raise AssertionError('Production runtime access forbidden')
    if event == 'socket.connect':
        raise AssertionError('Network access forbidden')
sys.addaudithook(audit)
class BlockHeavy(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {'pandas', 'ccxt.pro'}:
            raise AssertionError('Unused heavy dependency imported at startup: ' + fullname)
blocker = BlockHeavy()
sys.meta_path.insert(0, blocker)
import Exchange
from ParetoDataLoader import ParetoDataLoader
assert 'pandas' not in sys.modules
assert 'ccxt.pro' not in sys.modules
sys.meta_path.remove(blocker)
loader = ParetoDataLoader.__new__(ParetoDataLoader)
loader.get_all_configs = lambda: []
assert loader.to_dataframe().empty
assert Exchange.ccxt_pro is importlib.import_module('ccxt.pro')
''')
    result = subprocess.run([sys.executable, str(script), str(root)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_websocket_import_keeps_loop_responsive_and_joins_on_cancellation(monkeypatch):
    """Cancelling first-use initialization cannot leave its import thread behind."""
    import Exchange

    async def scenario():
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def load():
            """Replace package loading with a controllable isolated worker."""
            entered.set()
            assert release.wait(3)
            finished.set()
            return object()

        monkeypatch.setattr(Exchange, '_load_ccxt_pro', load)
        task = asyncio.create_task(Exchange._load_ccxt_pro_owned())
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    asyncio.run(scenario())
