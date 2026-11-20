"""Native PB9 entry point for the existing detached optimizer runner."""

import os
from pathlib import Path
import sys

SERVICE = 'OptimizeV9'

# Isolated Python excludes the script directory; select only this trusted code.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pb8_optimize_runner import main


if __name__ == '__main__':
    os.umask(0o077)
    raise SystemExit(main(runtime='v9'))
