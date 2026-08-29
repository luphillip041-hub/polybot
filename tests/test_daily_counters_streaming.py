"""Regression: daily counters must stream history, never materialize it.

2026-08-29 restart loop: startup's full-history read built ~800MB of row
dicts, crossing cgroup memory.high; reclaim throttling stretched startup past
the watchdog's 10-min silence window, which restarted the service, which
re-paid the same read — a self-sustaining stall loop.
"""

from __future__ import annotations

import gzip
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import polymarket_bot.ledger_history as lh
from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.paper_follower import PaperConfig, PaperFollowerDaemon


def _daemon(paper: Path, root: Path) -> PaperFollowerDaemon:
    cfg = PaperConfig(
        paper_dir=paper,
        ledger_path=paper / "ledger.jsonl",
        state_path=paper / "state.json",
        allowlist_path=paper / "allowlist.json",
        data_quality_path=paper / "data_quality.json",
        max_ws_age_seconds=999999999,
    )
    cfg.score_ratchet_enabled = False
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
    acfg = ArchiveConfig(
        archive_dir=root / "archive",
        state_path=root / "shadow_state.json",
        followup_queue_path=root / "followups.json",
    )
    (root / "archive").mkdir(exist_ok=True)
    return PaperFollowerDaemon(cfg, acfg)


def test_daily_counters_stream_and_exclude_quarantined(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper = root / "paper"
        paper.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        old = "2026-08-20T00:00:00+00:00"

        # Archive segment (gz): ancient rows — must not count
        archive = paper / "ledger_archive"
        archive.mkdir()
        old_rows = [
            {"type": "entry", "ts": old, "position_id": "p-old", "wallet_fill_price": 0.5},
            {"type": "resolution", "ts": old, "position_id": "p-old", "pnl": 999.0},
        ]
        with gzip.open(archive / "ledger-20260820-000000-000000.jsonl.gz", "wt") as f:
            for r in old_rows:
                f.write(json.dumps(r) + "\n")

        # Live ledger: 2 entries today (one sub-10c quarantined) + resolutions
        live_rows = [
            {"type": "entry", "ts": f"{today}T01:00:00+00:00", "position_id": "p1", "wallet_fill_price": 0.5},
            {"type": "entry", "ts": f"{today}T02:00:00+00:00", "position_id": "p2", "wallet_fill_price": 0.05},
            {"type": "resolution", "ts": f"{today}T03:00:00+00:00", "position_id": "p1", "pnl": 40.0},
            {"type": "resolution", "ts": f"{today}T04:00:00+00:00", "position_id": "p2", "pnl": 500.0},
        ]
        (paper / "ledger.jsonl").write_text("".join(json.dumps(r) + "\n" for r in live_rows))

        # Fail loudly if anything tries to materialize full history
        monkeypatch.setattr(
            lh, "read_ledger_history", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("materialized history"))
        )

        daemon = _daemon(paper, root)
        assert daemon._accepts_today == 2
        # p2 was a quarantined low-price entry -> its +500 is excluded
        assert daemon._pnl_today == 40.0
