#!/usr/bin/env bash
set -eu

if [ "$#" -ne 1 ]; then
  printf '{"ok":false,"error":"missing source node id"}\n' >&2
  exit 1
fi

base="$(CDPATH= cd "$(dirname "$0")" && pwd)"
parent="${base%/*}"
if [ -x "$parent/venv_pbgui/bin/python" ]; then
  py="$parent/venv_pbgui/bin/python"
elif [ -x "$parent/venv_pbgui312/bin/python" ]; then
  py="$parent/venv_pbgui312/bin/python"
elif [ -x "$base/.venv/bin/python" ]; then
  py="$base/.venv/bin/python"
else
  py="python3"
fi

exec "$py" "$base/cluster_sync_command.py" --cluster-root "$base/data/cluster" --remote-node "$1" --allow-join
