"""Prepare an allowlisted PB8/PB9 Docker context using the single shared recipe."""
import argparse
import json
from pathlib import Path
import shutil
import sys

ROOT=Path(__file__).resolve().parents[2]
SERVICE='Vast'


def prepare(runtime,output):
    """Copy public build inputs only; never include PBGui config, keys or user data."""
    sys.path.insert(0,str(ROOT))
    from vast_worker_profiles import worker_profile
    profile=worker_profile(runtime)
    output=Path(output)
    if output.exists():raise ValueError('Build context must be a new directory')
    source=Path(__file__).resolve().parent
    recipe=(source/'Dockerfile').read_text()
    from vast_jobs import REVISION
    if recipe.count(REVISION)!=2:raise ValueError('Shared Docker revision contract changed')
    recipe=recipe.replace(REVISION,profile.revision)
    recipe=recipe.replace('https://github.com/enarjord/passivbot.git', profile.repository + '.git')
    if runtime == 'pb9':
        recipe=recipe.replace('git checkout --detach "$PB8_REVISION"',
                              'git fetch origin "$PB8_REVISION" && git checkout --detach "$PB8_REVISION"')
    if runtime=='pb9':
        recipe=recipe.replace('# pip/maturin builds',
            "RUN cd /opt/passivbot && python -m pip install --no-cache-dir -r requirements-live.txt '.[full,gpu-cuda]' && python -m pip check && python -m pip freeze > /opt/pb8-pip-freeze.txt\n# pip/maturin builds")
        recipe += ('\nCOPY cloud_worker.py /opt/pbgui/worker.py\n'
                   'COPY optimize_cli.py pb9_optimize_options.py /opt/pbgui/\n'
                   'RUN python -m py_compile /opt/pbgui/worker.py /opt/pbgui/optimize_cli.py /opt/pbgui/pb9_optimize_options.py\n'
                   'LABEL pbgui.worker.protocol="native-launch-v1"\n')
    recipe=recipe.replace('WORKDIR /work',f'ENV PBGUI_WORKER_RUNTIME={runtime}\nWORKDIR /work')
    output.mkdir(parents=True,mode=0o700)
    (output/'Dockerfile').write_text(recipe)
    for name in ('run.py','cloud_worker.py'):
        shutil.copy2(source/name,output/name)
    if runtime == 'pb9':
        for name in ('optimize_cli.py', 'pb9_optimize_options.py'):
            shutil.copy2(ROOT/name, output/name)
    (output/'worker_candidate.json').write_text(json.dumps({
        'schema_version':1,'state':'unpublished_candidate','runtime':runtime,
        'upstream_repository':profile.repository,
        'base_revision':profile.revision,'config_schema':profile.schema,'overlays':[],
    },indent=4)+'\n')
    return output


def main():
    """Emit a reviewable local context; building/publishing remain explicit commands."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime',choices=['pb8','pb9'],required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    prepare(args.runtime,args.output)


if __name__=='__main__':main()
