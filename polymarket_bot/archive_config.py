from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import CONFIG


@dataclass(frozen=True)
class ArchiveConfig:
    # Public-data-only order book archive. No keys, no order placement.
    top_n_markets: int = 50
    gamma_event_limit: int = 200
    snapshot_interval_seconds: int = 30
    wallet_poll_interval_seconds: int = 60
    retention_days: int = 45
    tracked_wallets: list[str] = field(default_factory=list)
    tracked_wallet_limit_from_scores: int = 25
    trade_poll_limit: int = 100
    followup_offsets_seconds: tuple[int, ...] = (60, 300, 900)
    max_write_interval_per_token_seconds: float = 30.0
    heartbeat_interval_seconds: int = 60
    max_tokens: int = 400
    archive_dir: Path = CONFIG.runs_dir / "book_archive"
    state_path: Path = CONFIG.runs_dir / "shadow_journal_state.json"
    followup_queue_path: Path = CONFIG.runs_dir / "book_archive" / "followup_queue.json"
    user_agent: str = CONFIG.user_agent

    @classmethod
    def load(cls, path: str | Path | None = None) -> "ArchiveConfig":
        cfg_path = Path(path or os.getenv("POLYMARKET_ARCHIVE_CONFIG", CONFIG.root / "archive_config.json"))
        if not cfg_path.exists():
            return cls()
        raw = json.loads(cfg_path.read_text())
        allowed = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        vals: dict[str, Any] = {k: v for k, v in raw.items() if k in allowed}
        for key in ("archive_dir", "state_path", "followup_queue_path"):
            if key in vals:
                vals[key] = Path(vals[key])
        if "followup_offsets_seconds" in vals:
            vals["followup_offsets_seconds"] = tuple(int(x) for x in vals["followup_offsets_seconds"])
        return cls(**vals)

    def to_jsonable(self) -> dict[str, Any]:
        out = asdict(self)
        out["archive_dir"] = str(self.archive_dir)
        out["state_path"] = str(self.state_path)
        out["followup_queue_path"] = str(self.followup_queue_path)
        out["followup_offsets_seconds"] = list(self.followup_offsets_seconds)
        return out
