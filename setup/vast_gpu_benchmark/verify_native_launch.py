"""Bounded offline CPU smoke for a PB9 image using disposable synthetic fixtures.

Run inside the candidate container, with a fixture root mounted read-only.
This verifies CLI/data contracts, not physical GPU execution or paid rentals.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

SERVICE = 'VastWorker'


def verify(fixture):
    """Evaluate one scenario, then consume its saved Pareto as a compressed seed."""
    import passivbot_rust  # The native stamp verifier checks the loaded module.
    from rust_utils import verify_loaded_runtime_extension
    from optimize_cli import optimizer_argv
    identity = verify_loaded_runtime_extension()
    assert identity['runtime_compiled_source_stamp'] == identity['expected_source_fingerprint']
    assert Path('/opt/pbgui/worker.py').is_file()
    assert optimizer_argv('cli', 'config', {'scenarios':['train']}, runtime='v9') == ['cli', 'config', '--scenarios', 'train']
    fixture = Path(fixture)
    config = json.loads((fixture/'launch/config.json').read_text())
    with tempfile.TemporaryDirectory(prefix='native-launch-', dir='/tmp') as temporary:
        root = Path(temporary)
        caches = root/'caches'; caches.mkdir()
        shutil.copytree(fixture/'launch/work/caches/binance', caches/'binance')
        for source in (fixture/'data/coindata').glob('first_ohlcv_timestamps_unified*'):
            shutil.copy2(source, caches/source.name)
        config['backtest'].update(offline=True, hlcvs_data_dir=None, ohlcv_source_dir=str(fixture/'ohlcv'),
            base_dir=str(root/'backtests'), suite_enabled=False,
            scenarios=[{'label':'train','start_date':'2025-01-01','end_date':'2025-01-03'},
                       {'label':'unused','start_date':'2025-01-04','end_date':'2025-01-06'}])
        config['optimize'].update(backend='pymoo', iters=4, n_cpus=1, population_size=4, compress_results_file=False)
        path = root/'config.json'; path.write_text(json.dumps(config))
        base = [sys.executable, '/opt/passivbot/src/optimize.py', str(path), '--scenarios', 'train']
        first = subprocess.run(base, cwd=root, capture_output=True, text=True, timeout=180)
        if first.returncode:
            raise RuntimeError('Fresh native smoke failed: ' + (first.stdout + first.stderr)[-2500:])
        directories = list((root/'optimize_results').glob('*'))
        assert len(directories) == 1
        source = directories[0]
        assert list((source/'pareto').glob('*.json')) and (source/'checkpoint.pkl').is_file()
        candidate = json.loads(next((source/'pareto').glob('*.json')).read_text())
        assert candidate['suite_metrics']['scenario_labels'] == ['train']
        session = json.loads((source/'session.json').read_text())
        report = root/'prepared.json'
        report.write_text(json.dumps(session['preparation']['report']))
        second = subprocess.run([*base, '--start', str(source/'pareto'), '--filter-starting-configs',
                                 '--compress-starting-configs', '2', '--preparation-manifest', str(report)],
                                cwd=root, capture_output=True, text=True, timeout=180)
        if second.returncode:
            raise RuntimeError('Seeded native smoke failed: ' + (second.stdout + second.stderr)[-2500:])
        changed = json.loads(report.read_text()); changed['cases'] = [{'changed': True}]
        report.write_text(json.dumps(changed))
        rejected = subprocess.run([*base, '--preparation-manifest', str(report)],
                                  cwd=root, capture_output=True, text=True, timeout=180)
        assert rejected.returncode != 0 and 'Prepared inputs differ' in rejected.stdout + rejected.stderr
        print(json.dumps({'fresh_exit':first.returncode, 'seeded_exit':second.returncode,
            'scenario_filter':True, 'seed_filter':True, 'seed_compression':True,
            'preparation_verified':True, 'changed_report_rejected':True,
            'worker_entry':True, 'rust_verified':True, 'shared_argv_verified':True,
            'source_revision':Path('/opt/pb8-revision').read_text().strip(), 'network':'disabled by container'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', required=True, type=Path)
    verify(parser.parse_args().fixture)
