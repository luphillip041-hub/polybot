"""Telegram notifications (optional).

Usage:
    from polymarket_bot.alerts import send_telegram

    send_telegram("Bot started, tracking 5 wallets")

Silently no-ops if TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID aren't set.
Reads creds directly from environment (not cached CONFIG) so tests
can patch env vars reliably.
"""

from __future__ import annotations

import logging
import os

import requests

LOG = logging.getLogger("alerts")


def _token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "")


def _chat_id() -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(message: str) -> bool:
    """Send a Telegram message. Returns True on success or if not configured."""
    token = _token()
    chat_id = _chat_id()
    if not token or not chat_id:
        return True  # silently no-op if not configured

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        LOG.warning("Telegram send failed: %s", e)
        return False
