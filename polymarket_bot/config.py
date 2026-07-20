from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Load .env if present — never fails, never warns
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def env_int(key: str, default: int) -> int:
    """Read an integer from env, falling back to default."""
    try:
        return int(os.environ[key])
    except (KeyError, ValueError, TypeError):
        return default


def env_float(key: str, default: float) -> float:
    """Read a float from env, falling back to default."""
    try:
        return float(os.environ[key])
    except (KeyError, ValueError, TypeError):
        return default


def env_str(key: str, default: str) -> str:
    """Read a string from env, falling back to default."""
    return os.environ.get(key, default)


def env_bool(key: str, default: bool) -> bool:
    """Read a bool from env, falling back to default."""
    val = os.environ.get(key)
    if val is None:
        return default
    return val.lower() in ("true", "1", "yes")


@dataclass
class BotConfig:
    """Application configuration, sourced from env vars with sensible defaults.

    Fields are evaluated lazily — env vars are read at instantiation time,
    not at class definition time. This makes testing with patched env vars
    predictable.
    """

    def __init__(self, **kwargs: Any) -> None:
        # Allow explicit overrides via kwargs
        self.root: Path = Path(kwargs.get("root", "")) or Path(
            env_str("PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
        )
        self.runs_dir: Path = Path(
            env_str("RUNS_DIR", str(self.root / "runs"))
        )

        # API endpoints
        self.gamma_base: str = kwargs.get("gamma_base", env_str("GAMMA_API", "https://gamma-api.polymarket.com"))
        self.clob_base: str = kwargs.get("clob_base", env_str("CLOB_HOST", "https://clob.polymarket.com"))
        self.data_api: str = kwargs.get("data_api", env_str("DATA_API", "https://data-api.polymarket.com"))
        self.polygon_rpc_url: str = kwargs.get("polygon_rpc_url", env_str("POLYGON_RPC_URL", "https://polygon-rpc.com"))

        # Screening thresholds
        self.max_spread: float = kwargs.get("max_spread", env_float("MAX_SPREAD", 0.12))
        self.min_volume_24h: float = kwargs.get("min_volume_24h", env_float("MIN_VOLUME_24H", 1000.0))
        self.min_liquidity: float = kwargs.get("min_liquidity", env_float("MIN_LIQUIDITY", 1000.0))
        self.min_score: float = kwargs.get("min_score", env_float("MIN_SCORE", 70.0))

        # Paper trading
        self.paper_order_size_usd: float = kwargs.get("paper_order_size_usd", env_float("PAPER_ORDER_SIZE_USD", 25.0))
        self.stale_fill_seconds: int = kwargs.get("stale_fill_seconds", env_int("STALE_FILL_SECONDS", 480))
        self.max_open_positions: int = kwargs.get("max_open_positions", env_int("MAX_OPEN_POSITIONS", 150))
        self.paper_poll_interval_seconds: int = kwargs.get("paper_poll_interval_seconds", env_int("PAPER_POLL_INTERVAL_SEC", 3))

        # Misc
        self.user_agent: str = kwargs.get("user_agent", env_str("USER_AGENT", "Hermes-Polymarket-Copybot/0.1"))
        self.status_api_port: int = kwargs.get("status_api_port", env_int("STATUS_API_PORT", 8710))

        # Webhook URLs
        self.discord_webhook_url: str = kwargs.get("discord_webhook_url", env_str("DISCORD_WEBHOOK_URL", ""))
        self.telegram_bot_token: str = kwargs.get("telegram_bot_token", env_str("TELEGRAM_BOT_TOKEN", ""))
        self.telegram_chat_id: str = kwargs.get("telegram_chat_id", env_str("TELEGRAM_CHAT_ID", ""))


# Singleton
CONFIG = BotConfig()
