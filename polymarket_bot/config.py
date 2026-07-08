from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BotConfig:
    root: Path = Path(__file__).resolve().parents[1]
    runs_dir: Path = root / "runs"
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    max_spread: float = 0.12
    min_volume_24h: float = 1000.0
    min_liquidity: float = 1000.0
    min_score: float = 70.0
    paper_order_size_usd: float = 25.0
    user_agent: str = "Hermes-Polymarket-Copybot/0.1"


CONFIG = BotConfig()
