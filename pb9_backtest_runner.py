"""PB9 entry point for the shared independent backtest runner."""
from pathlib import Path
import sys
import os

SERVICE = "BacktestsV9"

# -I excludes the script directory. Select only this trusted PBGui checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pb8_backtest_runner import main

if __name__ == "__main__":
    os.umask(0o077)
    import logging_helpers
    logging_helpers.LOG_ROOT = Path(sys.argv[2]).parent.parent / "logs"
    raise SystemExit(main(runtime="v9"))
