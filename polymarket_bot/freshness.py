from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from typing import Any

CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return subprocess.CompletedProcess(command, 127, stdout="", stderr="")


def _parse_chronyc_tracking(text: str) -> float | None:
    match = re.search(
        r"^\s*Last offset\s*:\s*([+-]?\d+(?:\.\d+)?)\s+seconds?\s*$",
        text,
        re.IGNORECASE | re.MULTILINE,
    )
    if match:
        return float(match.group(1))

    match = re.search(
        r"^\s*System time\s*:\s*([+-]?\d+(?:\.\d+)?)\s+seconds?"
        r"(?:\s+(fast|slow)(?:\s+of\s+NTP\s+time)?)?\s*$",
        text,
        re.IGNORECASE | re.MULTILINE,
    )
    if not match:
        return None
    value = float(match.group(1))
    direction = (match.group(2) or "").lower()
    if direction == "fast":
        return abs(value)
    if direction == "slow":
        return -abs(value)
    return value


def _duration_seconds(text: str) -> float | None:
    match = re.fullmatch(
        r"\s*([+-]?\d+(?:\.\d+)?)\s*(ns|nsec|us|usec|µs|ms|msec|s|sec|seconds?)?\s*",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    value = float(match.group(1))
    unit = (match.group(2) or "s").lower()
    factors = {
        "ns": 1e-9,
        "nsec": 1e-9,
        "us": 1e-6,
        "usec": 1e-6,
        "µs": 1e-6,
        "ms": 1e-3,
        "msec": 1e-3,
        "s": 1.0,
        "sec": 1.0,
        "second": 1.0,
        "seconds": 1.0,
    }
    return value * factors[unit]


def _parse_timedatectl_timesync(text: str) -> float | None:
    for line in text.splitlines():
        if "offset" not in line.lower():
            continue
        separator = "=" if "=" in line else ":" if ":" in line else None
        if not separator:
            continue
        key, value = line.split(separator, 1)
        if "offset" not in key.lower():
            continue
        parsed = _duration_seconds(value)
        if parsed is not None:
            return parsed
    return None


def _parse_ntpq_peers(text: str) -> float | None:
    for line in text.splitlines():
        stripped = line.lstrip()
        if not stripped.startswith("*"):
            continue
        columns = stripped[1:].split()
        # remote refid st t when poll reach delay offset jitter
        if len(columns) < 10:
            continue
        try:
            return float(columns[8]) / 1000.0
        except (TypeError, ValueError):
            continue
    return None


def resolve_clock_offset(runner: CommandRunner | None = None) -> dict[str, Any]:
    """Return a numeric host-clock offset, trying installed sync tools in order."""
    run = runner or _run
    probes = (
        (["chronyc", "tracking"], "chronyc", _parse_chronyc_tracking),
        (["timedatectl", "show-timesync", "--all"], "systemd-timesyncd", _parse_timedatectl_timesync),
        (["ntpq", "-pn"], "ntpq", _parse_ntpq_peers),
    )
    for command, source, parser in probes:
        result = run(command)
        if result.returncode != 0:
            continue
        offset = parser(result.stdout or "")
        if offset is not None:
            return {
                "clock_offset_seconds": float(offset),
                "clock_offset_source": source,
            }
    return {
        "clock_offset_seconds": None,
        "clock_offset_source": "none",
    }


def clock_offset_gate(result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fail closed when offset is unavailable or exceeds one second."""
    resolved = dict(result or resolve_clock_offset())
    offset = resolved.get("clock_offset_seconds")
    if not isinstance(offset, (int, float)):
        return {
            **resolved,
            "clock_offset_pass": False,
            "clock_offset_reason": "clock_offset_unavailable",
        }
    if abs(float(offset)) > 1.0:
        return {
            **resolved,
            "clock_offset_pass": False,
            "clock_offset_reason": "clock_offset_exceeds_1s",
        }
    return {
        **resolved,
        "clock_offset_pass": True,
        "clock_offset_reason": "ok",
    }


def main() -> None:
    print(json.dumps(clock_offset_gate(), sort_keys=True))


if __name__ == "__main__":
    main()
