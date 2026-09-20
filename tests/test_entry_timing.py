"""Entry timestamp vs Gamma real-world / market end (fixtures, no network)."""

from __future__ import annotations

import json
from pathlib import Path

from polymarket_bot.entry_timing import (
    VERDICT_PRECEDES,
    VERDICT_SUSPICIOUS,
    VERDICT_UNKNOWN,
    GammaTimingFetcher,
    classify_ledger,
    classify_pair,
    extract_market_timing,
    load_offline_meta,
    parse_ts,
    timing_summary,
)
from polymarket_bot.scorecard_slices import timing_line_from_analysis


def _entry(pid: str, ts: str, token: str = "tok-yes", cid: str = "0xcond") -> dict:
    return {
        "type": "entry",
        "position_id": pid,
        "token": token,
        "condition_id": cid,
        "market": cid,
        "ts": ts,
        "wallet": "0xw",
    }


def _res(pid: str, ts: str, pnl: float, token: str = "tok-yes") -> dict:
    return {
        "type": "resolution",
        "position_id": pid,
        "token": token,
        "ts": ts,
        "pnl": pnl,
    }


def test_parse_ts_iso_and_millis() -> None:
    iso = parse_ts("2026-09-19T12:00:00Z")
    assert iso is not None and iso.hour == 12
    ms = parse_ts(1_000_000_000_000)
    assert ms is not None and ms.year == 2001
    assert parse_ts("not-a-date") is None
    assert parse_ts("") is None


def test_extract_uses_earliest_of_enddate_and_closed_time() -> None:
    meta = extract_market_timing(
        {
            "endDate": "2026-09-19T18:00:00Z",
            "closedTime": "2026-09-19T21:00:00Z",
            "umaResolutionStatus": "resolved",
            "conditionId": "0xabc",
        }
    )
    assert meta["real_world_end_ts"] == "2026-09-19T18:00:00+00:00"
    assert meta["real_world_end_source"].startswith("earliest(")


def test_game_start_alone_is_not_an_outcome_time() -> None:
    meta = extract_market_timing({"gameStartTime": "2026-09-19T17:00:00Z"})
    assert meta["real_world_end_ts"] is None
    assert meta["game_start"] is not None


def test_precedes_when_entry_before_enddate() -> None:
    row = classify_pair(
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 80.0),
        {"endDate": "2026-09-19T18:00:00Z"},
    )
    assert row["verdict"] == VERDICT_PRECEDES
    assert row["lead_s"] == 6 * 3600
    assert row["hold_s"] == 8 * 3600
    assert row["won"] is True


def test_suspicious_when_entry_after_end_before_onchain() -> None:
    row = classify_pair(
        _entry("p1", "2026-09-19T19:00:00+00:00"),
        _res("p1", "2026-09-19T22:00:00+00:00", -100.0),
        {"endDate": "2026-09-19T18:00:00Z", "closedTime": "2026-09-19T21:00:00Z"},
    )
    assert row["verdict"] == VERDICT_SUSPICIOUS
    assert row["won"] is False
    assert row["lead_s"] == -3600
    assert row["onchain_source"] == "resolution"


def test_unknown_without_gamma_timestamps() -> None:
    row = classify_pair(
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 50.0),
        {"umaResolutionStatus": "resolved"},
    )
    assert row["verdict"] == VERDICT_UNKNOWN
    assert row["unknown_reason"] == "status_without_timestamp"


def test_unknown_when_only_game_start() -> None:
    row = classify_pair(
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 50.0),
        extract_market_timing({"gameStartTime": "2026-09-19T17:00:00Z"}),
    )
    assert row["verdict"] == VERDICT_UNKNOWN
    assert row["unknown_reason"] == "gameStartTime_is_not_outcome_time"


def test_sell_exit_is_not_onchain_resolution() -> None:
    closed = {
        "type": "exit",
        "position_id": "p1",
        "ts": "2026-09-19T12:01:00+00:00",
        "pnl": -6.25,
        "token": "tok-yes",
    }
    row = classify_pair(
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        closed,
        {"endDate": "2026-09-19T18:00:00Z"},
    )
    assert row["onchain_source"] == "exit"
    assert row["onchain_resolution_ts"] is None
    assert row["verdict"] == VERDICT_PRECEDES


def test_unparseable_enddate_fails_closed() -> None:
    row = classify_pair(
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 10.0),
        {"endDate": "sometime this week", "closedTime": ""},
    )
    assert row["verdict"] == VERDICT_UNKNOWN
    assert "unparseable" in str(row["unknown_reason"])


def test_fetcher_error_is_unknown_not_crash() -> None:
    def boom(_token: str) -> list:
        raise RuntimeError("gamma down")

    fetcher = GammaTimingFetcher(event_by_token=boom)
    rows = [
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 10.0),
    ]
    classified = classify_ledger(rows, fetcher)
    assert classified[0]["verdict"] == VERDICT_UNKNOWN
    assert fetcher.failures == 1


def test_offline_meta_by_token_skips_network() -> None:
    def no_net(_token: str) -> list:
        raise AssertionError("must not hit Gamma")

    fetcher = GammaTimingFetcher(
        event_by_token=no_net,
        meta_by_token={"tok-yes": {"endDate": "2026-09-19T18:00:00Z"}},
    )
    rows = [
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 80.0),
        _entry("p2", "2026-09-19T19:30:00+00:00", token="tok-yes"),
        _res("p2", "2026-09-19T22:00:00+00:00", -100.0),
    ]
    classified = classify_ledger(rows, fetcher)
    summary = timing_summary(classified)
    assert summary["precedes"] == 1
    assert summary["suspicious"] == 1
    assert summary["wins"]["precedes_pct_of_known"] == 100.0
    assert summary["losses"]["suspicious"] == 1
    assert summary["lead_s"]["p50"] is not None
    assert fetcher.lookups == 0
    assert "endDate is a scheduled market end" in summary["limitations"][0]


def test_condition_id_filter_must_match_exactly() -> None:
    def events(_cid: str) -> list:
        return [
            {
                "markets": [
                    {
                        "conditionId": "0xother",
                        "endDate": "2026-09-19T18:00:00Z",
                        "question": "wrong market",
                    }
                ]
            }
        ]

    fetcher = GammaTimingFetcher(events_by_condition=events)
    meta = fetcher.lookup("tok", "0xcond")
    assert meta == {}


def test_load_offline_meta_accepts_market_list() -> None:
    by_token, by_cid = load_offline_meta(
        {
            "markets": [
                {
                    "conditionId": "0xabc",
                    "clobTokenIds": ["Tok-A"],
                    "endDate": "2026-09-19T18:00:00Z",
                }
            ]
        }
    )
    assert "0xabc" in by_cid
    assert "tok-a" in by_token


def test_script_offline_against_temp_ledger(tmp_path: Path) -> None:
    paper = tmp_path / "paper"
    paper.mkdir()
    ledger = paper / "ledger.jsonl"
    rows = [
        _entry("p1", "2026-09-19T12:00:00+00:00"),
        _res("p1", "2026-09-19T20:00:00+00:00", 80.0),
        _entry("p2", "2026-09-19T19:00:00+00:00", token="tok-no"),
        _res("p2", "2026-09-19T22:00:00+00:00", -100.0, token="tok-no"),
        _entry("p3", "2026-09-19T12:00:00+00:00", token="tok-miss", cid="0xmiss"),
        _res("p3", "2026-09-19T16:00:00+00:00", 10.0, token="tok-miss"),
    ]
    ledger.write_text("".join(json.dumps(r) + "\n" for r in rows))
    meta = tmp_path / "meta.json"
    meta.write_text(
        json.dumps(
            {
                "by_token": {
                    "tok-yes": {"endDate": "2026-09-19T18:00:00Z"},
                    "tok-no": {
                        "endDate": "2026-09-19T18:00:00Z",
                        "closedTime": "2026-09-19T21:00:00Z",
                    },
                }
            }
        )
    )
    out = tmp_path / "analysis.json"
    import subprocess
    import sys

    proc = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "scripts" / "analyze_entry_timing.py"),
            "--ledger",
            str(ledger),
            "--meta",
            str(meta),
            "--offline",
            "--output",
            str(out),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    summary = payload["summary"]
    assert summary["closed_paired"] == 3
    assert summary["precedes"] == 1
    assert summary["suspicious"] == 1
    assert summary["unknown"] == 1
    assert summary["fetcher"]["offline"] is True
    assert summary["fetcher"]["network_lookups"] == 0
    assert out.exists()
    line = timing_line_from_analysis(json.loads(out.read_text()))
    assert line.startswith("TIMING ")
    assert "suspicious=1" in line


def test_script_offline_without_meta_does_not_claim_proven(tmp_path: Path) -> None:
    paper = tmp_path / "paper"
    paper.mkdir()
    ledger = paper / "ledger.jsonl"
    ledger.write_text(
        json.dumps(_entry("p1", "2026-09-19T12:00:00+00:00"))
        + "\n"
        + json.dumps(_res("p1", "2026-09-19T20:00:00+00:00", 80.0))
        + "\n"
    )
    import subprocess
    import sys

    proc = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "scripts" / "analyze_entry_timing.py"),
            "--ledger",
            str(ledger),
            "--offline",
            "--output",
            str(tmp_path / "out.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "INCONCLUSIVE" in proc.stdout
    assert "precedes=0" in proc.stdout
