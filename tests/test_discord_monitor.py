from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from polymarket_bot.discord_monitor import (
    DISK_MB_PER_DAY_LIMIT,
    FOLLOWUP_STUCK_SECONDS,
    ORANGE_DISK,
    ORANGE_FOLLOWUPS_STUCK,
    ORANGE_WS,
    RED_INACTIVE,
    RED_UNREACHABLE,
    UNREACHABLE_GRACE_SECONDS,
    MonitorState,
    evaluate_poll,
    render_daily_digest,
    render_gaps,
    render_status_card,
    render_wallets,
)


def status(**overrides):
    base = {
        "generated_at": "2026-07-08T13:00:00+00:00",
        "archiver": {
            "service_active": True,
            "ws_connected": True,
            "last_ws_message_age_s": 10,
            "markets": 2,
            "tokens": 4,
            "book_rows_this_hour": 10,
            "mb_per_day": 1.2,
            "retention_days": 45,
            "retention_gb": 0.5,
        },
        "gaps_today": [],
        "coverage_pct_today": 99.9,
        "shadow": {
            "fills_today": 1,
            "followups_pending": 0,
            "followups_completed_today": 2,
            "followups_missed_today": 0,
            "last_fill_ts": "2026-07-08T12:00:00+00:00",
        },
        "wallets": [
            {"name": "alpha", "fills_today": 1, "fills_7d": 4, "last_fill_ts": "2026-07-08T12:00:00+00:00", "markets_touched_7d": 2},
            {"name": "zero", "fills_today": 0, "fills_7d": 0, "last_fill_ts": "2026-07-04T12:00:00+00:00", "markets_touched_7d": 0},
        ],
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key].update(value)
        else:
            base[key] = value
    return base


class DiscordMonitorTests(unittest.TestCase):
    def test_unreachable_alert_after_grace_then_recovery(self):
        st = MonitorState()
        now = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
        self.assertEqual(evaluate_poll(None, st, now, "down"), [])
        msgs = evaluate_poll(None, st, now + timedelta(seconds=UNREACHABLE_GRACE_SECONDS + 1), "down")
        self.assertIn(RED_UNREACHABLE, st.active_conditions)
        self.assertIn("🔴", msgs[0])
        rec = evaluate_poll(status(), st, now + timedelta(seconds=UNREACHABLE_GRACE_SECONDS + 61))
        self.assertTrue(any("🟢" in m and "reachable" in m for m in rec))

    def test_orange_conditions_and_cooldown(self):
        st = MonitorState()
        now = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
        bad = status(
            archiver={"last_ws_message_age_s": 120, "mb_per_day": DISK_MB_PER_DAY_LIMIT + 1},
            gaps_today=[{"start_ts": "a", "end_ts": "b", "duration_s": 5, "reason": "ws_stale"}],
            shadow={"followups_missed_today": 1},
        )
        msgs = evaluate_poll(status(), st, now)
        self.assertEqual(msgs, [])
        msgs = evaluate_poll(bad, st, now + timedelta(seconds=1))
        text = "\n".join(msgs)
        self.assertIn("websocket stale", text)
        self.assertIn("disk anomaly", text)
        self.assertIn("ws_stale", text)
        self.assertIn("missed followups", text)
        repeat = evaluate_poll(bad, st, now + timedelta(minutes=1))
        self.assertFalse(any("websocket stale" in m for m in repeat))
        self.assertIn(ORANGE_WS, st.active_conditions)
        self.assertIn(ORANGE_DISK, st.active_conditions)

    def test_service_inactive_and_recovery(self):
        st = MonitorState()
        now = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
        msgs = evaluate_poll(status(archiver={"service_active": False}), st, now)
        self.assertIn(RED_INACTIVE, st.active_conditions)
        self.assertIn("service inactive", msgs[0])
        rec = evaluate_poll(status(), st, now + timedelta(minutes=1))
        self.assertTrue(any("archive service active" in m for m in rec))

    def test_followups_stuck_same_nonzero_for_30m(self):
        st = MonitorState()
        now = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
        self.assertEqual(evaluate_poll(status(shadow={"followups_pending": 3}), st, now), [])
        msgs = evaluate_poll(status(shadow={"followups_pending": 3}), st, now + timedelta(seconds=FOLLOWUP_STUCK_SECONDS + 1))
        self.assertIn(ORANGE_FOLLOWUPS_STUCK, st.active_conditions)
        self.assertIn("queue stuck", msgs[0])
        rec = evaluate_poll(status(shadow={"followups_pending": 0}), st, now + timedelta(seconds=FOLLOWUP_STUCK_SECONDS + 2))
        self.assertTrue(any("unstuck" in m for m in rec))

    def test_render_slash_command_cards(self):
        s = status(gaps_today=[{"start_ts": "a", "end_ts": "b", "duration_s": 5, "reason": "unit"}])
        self.assertIn("Polymarket Copybot Status", render_status_card(s))
        self.assertIn("alpha", render_wallets(s, datetime(2026, 7, 8, 13, 0, tzinfo=UTC)))
        self.assertIn("unit", render_gaps(s))

    def test_daily_digest_flags_zero_fill_wallet_and_reasons(self):
        digest = render_daily_digest(
            status(),
            [{"date": "2026-07-07", "coverage_pct": 98.5, "gaps": [{"reason": "ws_stale"}, {"reason": "unit"}]}],
            datetime(2026, 7, 8, 13, 0, tzinfo=UTC),
        )
        self.assertIn("Daily Digest", digest)
        self.assertIn("98.5%", digest)
        self.assertIn("ws_stale", digest)
        self.assertIn("0 fills 3d+", digest)


if __name__ == "__main__":
    unittest.main()
