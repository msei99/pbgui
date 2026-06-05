#!/usr/bin/env python3
"""Entry point for the PBGui master installer."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from setup.installer import cli, web  # type: ignore
else:
    from . import cli, web


def main() -> int:
    """Run the installer in browser or CLI mode."""
    parser = argparse.ArgumentParser(description="PBGui master installer")
    parser.add_argument("--cli", action="store_true", help="run terminal prompts instead of the browser wizard")
    parser.add_argument("--host", default="127.0.0.1", help="wizard bind host, default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8088, help="wizard port, default: 8088")
    args = parser.parse_args()

    if args.cli:
        return cli.run_cli()
    return web.run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    raise SystemExit(main())
