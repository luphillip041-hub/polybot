from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import CONFIG


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ[key])
    except (KeyError, ValueError, TypeError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ[key])
    except (KeyError, ValueError, TypeError):
        return default


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class ArchiveConfig:
    # Public-data-only order book archive. No keys, no order placement.
    top_n_markets: int = _env_int("TOP_N_MARKETS", 10)
    gamma_event_limit: int = _env_int("GAMMA_EVENT_LIMIT", 200)
    snapshot_interval_seconds: int = _env_int("SNAPSHOT_INTERVAL_SEC", 30)
    wallet_poll_interval_seconds: int = _env_int("WALLET_POLL_INTERVAL_SEC", 15)
    retention_days: int = _env_int("RETENTION_DAYS", 45)
    tracked_wallets: list[str] = field(default_factory=list)
    tracked_wallet_limit_from_scores: int = _env_int("WALLET_LIMIT_FROM_SCORES", 25)
    trade_poll_limit: int = _env_int("TRADE_POLL_LIMIT", 100)
    followup_offsets_seconds: tuple[int, ...] = (60, 300, 900)
    max_write_interval_per_token_seconds: float = _env_float(
        "MAX_WRITE_INTERVAL_SEC", 30.0
    )
    heartbeat_interval_seconds: int = _env_int("HEARTBEAT_INTERVAL_SEC", 60)
    max_tokens: int = _env_int("MAX_TOKENS", 800)
    archive_dir: Path = CONFIG.runs_dir / "book_archive"
    state_path: Path = CONFIG.runs_dir / "shadow_journal_state.json"
    followup_queue_path: Path = CONFIG.runs_dir / "book_archive" / "followup_queue.json"
    user_agent: str = CONFIG.user_agent

    # Resolution
    resolution_poll_seconds: int = _env_int("RESOLUTION_POLL_SEC", 1800)

    # Paper follower
    paper_dir: Path = Path(_env_str("PAPER_DIR", str(CONFIG.runs_dir / "paper")))
    stale_fill_seconds: int = _env_int("STALE_FILL_SECONDS", 480)
    paper_poll_interval_seconds: int = _env_int("PAPER_POLL_INTERVAL_SEC", 3)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "ArchiveConfig":
        cfg_path = Path(
            path or os.getenv("ARCHIVE_CONFIG", CONFIG.root / "archive_config.json")
        )
        if not cfg_path.exists():
            return cls()
        raw = json.loads(cfg_path.read_text())
        allowed = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        vals: dict[str, Any] = {k: v for k, v in raw.items() if k in allowed}
        for key in ("archive_dir", "state_path", "followup_queue_path", "paper_dir"):
            if key in vals:
                vals[key] = Path(vals[key])
        if "followup_offsets_seconds" in vals:
            vals["followup_offsets_seconds"] = tuple(
                int(x) for x in vals["followup_offsets_seconds"]
            )
        return cls(**vals)

    def to_jsonable(self) -> dict[str, Any]:
        out = asdict(self)
        out["archive_dir"] = str(self.archive_dir)
        out["state_path"] = str(self.state_path)
        out["followup_queue_path"] = str(self.followup_queue_path)
        out["paper_dir"] = str(self.paper_dir)
        out["followup_offsets_seconds"] = list(self.followup_offsets_seconds)
        return out
