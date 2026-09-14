"""Read exact-validation backlog observations from existing PB8 profile logs."""

from datetime import datetime, timezone
import json
import re


def parse_exact_queue(raw_log: bytes) -> dict | None:
    """Return the newest complete profile; pending includes unconsumed completions."""
    for line in reversed(raw_log[-65536:].decode("utf-8", errors="replace").splitlines()):
        if "[gpu-profile]" not in line:
            continue
        stamp = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z", line)
        if not stamp:
            continue
        try:
            record = json.loads(line.split("[gpu-profile]", 1)[1].strip())
            if not isinstance(record, dict) or record.get("event") != "generation":
                continue
            pending = record.get("exact_inflight")
            completed = record.get("exact_completed")
            if any(type(value) is not int or value < 0 for value in (pending, completed)):
                continue
            sampled_at = datetime.strptime(stamp[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        except (ValueError, TypeError):
            continue
        return {"outstanding": pending, "completed": completed, "sampled_at": sampled_at}
    return None
