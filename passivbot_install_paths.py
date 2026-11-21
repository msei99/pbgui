"""Static slot-separation checks shared by local and remote bot installation."""
import argparse
import configparser
import sys
from pathlib import Path

SERVICE = 'PassivbotInstall'


def validate_install_paths(product, pbgui_dir, repo_dir, venv_dir):
    """Reject overlapping runtime slots before fetching, stopping or installing."""
    if product not in ('pb8', 'pb9'):
        raise ValueError('Unsupported install runtime')
    paths = [Path(value).expanduser() for value in (pbgui_dir, repo_dir, venv_dir)]
    if any(not p.is_absolute() for p in paths):
        raise ValueError('Install paths must be absolute')
    for path in paths:
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('Install paths must not contain symlinks')
    gui, repo, venv = [p.resolve() for p in paths]
    config = configparser.ConfigParser(interpolation=None)
    ini = gui/'pbgui.ini'
    if ini.exists():
        with ini.open(encoding='utf-8') as handle:
            config.read_file(handle)
    protected = [gui, (gui.parent/'venv_pbgui').resolve()]
    if sys.prefix != sys.base_prefix:
        protected.append(Path(sys.prefix).resolve())
    for slot in ('pb7', 'pb8', 'pb9'):
        if slot == product:
            continue
        for suffix in ('dir', 'venv'):
            value = config.get('main', slot+suffix, fallback='').strip()
            if value:
                path = Path(value).expanduser()
                if not path.is_absolute():
                    raise ValueError('Configured runtime paths must be absolute')
                protected.append((path.parent.parent if suffix == 'venv' else path).resolve())
        protected.extend([gui.parent/slot, gui.parent/('venv_'+slot)])
    def overlaps(a, b):
        """Include equal paths and both nesting directions."""
        return a == b or a in b.parents or b in a.parents
    if overlaps(repo, venv) or any(overlaps(path, other) for path in (repo, venv) for other in protected):
        raise ValueError('Passivbot source and environment must be separate from PBGui and other runtimes')


def main():
    """Expose a dependency-free preflight for installer playbooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('product', choices=['pb8', 'pb9'])
    parser.add_argument('pbgui_dir')
    parser.add_argument('repo_dir')
    parser.add_argument('venv_dir')
    args = parser.parse_args()
    validate_install_paths(args.product, args.pbgui_dir, args.repo_dir, args.venv_dir)


if __name__ == '__main__':
    main()
