from __future__ import annotations

import subprocess

from fastapi.testclient import TestClient

from polymarket_bot import status_api
from polymarket_bot.freshness import clock_offset_gate, resolve_clock_offset


def _runner(outputs: dict[str, tuple[int, str]]):
    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        code, stdout = outputs.get(command[0], (127, ""))
        return subprocess.CompletedProcess(command, code, stdout=stdout, stderr="")

    return run


def test_healthz_returns_minimal_liveness_body():
    response = TestClient(status_api.app).get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "service": "polymarket-copybot-status-api"}


def test_clock_offset_uses_chronyc_first():
    result = resolve_clock_offset(
        _runner({"chronyc": (0, "Last offset     : -0.000321 seconds\n")})
    )

    assert result == {
        "clock_offset_seconds": -0.000321,
        "clock_offset_source": "chronyc",
    }
    assert clock_offset_gate(result)["clock_offset_pass"] is True


def test_clock_offset_falls_back_to_systemd_timesyncd():
    result = resolve_clock_offset(
        _runner(
            {
                "chronyc": (127, ""),
                "timedatectl": (0, "ServerName=time.example.net\nOffset=750ms\n"),
            }
        )
    )

    assert result == {
        "clock_offset_seconds": 0.75,
        "clock_offset_source": "systemd-timesyncd",
    }
    assert clock_offset_gate(result)["clock_offset_pass"] is True


def test_clock_offset_falls_back_to_ntpq_and_converts_ms_to_seconds():
    result = resolve_clock_offset(
        _runner(
            {
                "chronyc": (127, ""),
                "timedatectl": (1, ""),
                "ntpq": (
                    0,
                    "     remote           refid      st t when poll reach   delay   offset  jitter\n"
                    "*192.0.2.10     198.51.100.1     2 u   15   64  377    1.234   -2.500   0.100\n",
                ),
            }
        )
    )

    assert result == {
        "clock_offset_seconds": -0.0025,
        "clock_offset_source": "ntpq",
    }
    assert clock_offset_gate(result)["clock_offset_pass"] is True


def test_clock_offset_all_sources_fail_closed():
    result = resolve_clock_offset(_runner({}))
    gate = clock_offset_gate(result)

    assert result == {
        "clock_offset_seconds": None,
        "clock_offset_source": "none",
    }
    assert gate == {
        **result,
        "clock_offset_pass": False,
        "clock_offset_reason": "clock_offset_unavailable",
    }


def test_clock_offset_over_one_second_fails_gate():
    result = {
        "clock_offset_seconds": 1.000001,
        "clock_offset_source": "systemd-timesyncd",
    }

    assert clock_offset_gate(result) == {
        **result,
        "clock_offset_pass": False,
        "clock_offset_reason": "clock_offset_exceeds_1s",
    }
