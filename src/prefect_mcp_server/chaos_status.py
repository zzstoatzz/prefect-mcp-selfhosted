import json
import math
import os
import re
import stat
import time
from pathlib import Path
from typing import Any


def read_record(path: Path) -> Any:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024**2:
            raise ValueError("status input must be a regular file below 2 MiB")
        data = source.read(2 * 1024**2 + 1)
        if len(data) > 2 * 1024**2:
            raise ValueError("status input grew beyond 2 MiB")
        return json.loads(data)


def summary(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("experiment record must be an object")
    selected = {
        key: record[key]
        for key in (
            "started",
            "scenario",
            "seed",
            "image",
            "revision",
            "passed",
            "seconds",
            "failure",
            "cleanup_failure",
            "reason",
        )
        if key in record
    }
    for key, value in selected.items():
        if not isinstance(value, (str, int, float, bool, type(None))):
            raise ValueError("experiment summary fields must be scalar values")
        if isinstance(value, str):
            selected[key] = value[:4096]
    return selected


def inspect_chaos(state: Path, *, now: float | None = None) -> dict[str, Any]:
    observed = time.time() if now is None else now
    result: dict[str, Any] = {
        "observed_at": observed,
        "scope": "isolated chaos lab; not production health",
        "state_directory": str(state),
        "status": "unknown",
        "runtime_observed": False,
        "note": "State files do not prove a process is running or a timer is enabled.",
    }
    try:
        if not state.is_dir() or state.is_symlink():
            raise ValueError("configured chaos state directory is unavailable")
        history = read_record(state / "history.json")
        if not isinstance(history, list):
            raise ValueError("history must be an array")
        if len(history) > 512:
            raise ValueError("history exceeds the 512-entry inspection limit")
        recent = []
        for item in history:
            if not isinstance(item, dict):
                raise ValueError("invalid history entry")
            started, seconds = item.get("started"), item.get("seconds")
            if not all(
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(value)
                and value >= 0
                for value in (started, seconds)
            ):
                raise ValueError("invalid history timestamp or duration")
            if started > observed - 86400:
                recent.append((started, seconds))
        eligible = observed
        for boundary in sorted(
            {observed, *(math.ceil(start + 86400) for start, _ in recent)}
        ):
            remaining = [
                (start, seconds)
                for start, seconds in recent
                if start > boundary - 86400
            ]
            if len(remaining) < 24 and sum(seconds for _, seconds in remaining) < 3200:
                eligible = boundary
                break
        result["budget"] = {
            "experiments_last_24h": len(recent),
            "runtime_seconds_last_24h": sum(seconds for _, seconds in recent),
            "experiment_limit": 24,
            "start_runtime_limit_seconds": 3200,
            "not_before": eligible,
            "policy": "runner v1: 24 starts, 3200-second start cutoff, 400-second reserve",
        }
        stopped = state / "STOPPED.json"
        active = state / "active.json"
        result["status"] = "idle_recorded"
        if active.exists() or active.is_symlink():
            result["active_record"] = summary(read_record(active))
            result["status"] = "unfinished_record"
        if stopped.exists() or stopped.is_symlink():
            result["stop_record"] = summary(read_record(stopped))
            result["status"] = "latched"
        result["budget_available"] = eligible <= observed
        reports = state / "reports"
        if reports.is_symlink():
            raise ValueError("report directory must not be a symlink")
        entries = []
        if reports.exists():
            with os.scandir(reports) as listing:
                for entry in listing:
                    if len(entries) >= 128:
                        raise ValueError("too many report entries to inspect")
                    entries.append(entry)
        names = sorted(
            entry.name
            for entry in entries
            if re.fullmatch(r"\d+-\d+", entry.name)
            and entry.is_dir(follow_symlinks=False)
        )[-3:]
        result["recent_results"] = []
        for name in reversed(names):
            path = reports / name / "result.json"
            if path.exists() or path.is_symlink():
                result["recent_results"].append(
                    {"report": name, **summary(read_record(path))}
                )
    except (OSError, ValueError, TypeError) as exc:
        result.update(status="unknown", error=f"{type(exc).__name__}: {exc}")
    return result
