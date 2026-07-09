from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time as dt_time, timedelta
from typing import Any

import aiohttp
import discord
from discord import app_commands

STATUS_URL = "http://127.0.0.1:8710/api/status"
GAPS_URL = "http://127.0.0.1:8710/api/gaps?days=2"
PAPER_URL = "http://127.0.0.1:8710/api/paper"
POLL_SECONDS = 60
ALERT_COOLDOWN_SECONDS = 30 * 60
UNREACHABLE_GRACE_SECONDS = 2 * 60
WS_STALE_SECONDS = 90
FOLLOWUP_STUCK_SECONDS = 30 * 60
DISK_MB_PER_DAY_LIMIT = 100.0
DAILY_DIGEST_UTC = dt_time(hour=13, minute=0, tzinfo=UTC)

RED_UNREACHABLE = "red_service_unreachable"
RED_INACTIVE = "red_service_inactive"
ORANGE_WS = "orange_ws_stale"
ORANGE_FOLLOWUPS_STUCK = "orange_followups_stuck"
ORANGE_MISSED = "orange_followups_missed"
ORANGE_GAP = "orange_gap_rows"
ORANGE_DISK = "orange_disk_anomaly"


@dataclass
class PendingValue:
    value: int = 0
    first_seen: datetime | None = None


@dataclass
class MonitorState:
    last_status: dict[str, Any] | None = None
    last_poll_at: datetime | None = None
    unreachable_since: datetime | None = None
    active_conditions: set[str] = field(default_factory=set)
    last_alert_at: dict[str, datetime] = field(default_factory=dict)
    pending: PendingValue = field(default_factory=PendingValue)
    last_missed_count: int | None = None
    seen_gap_ids: set[str] = field(default_factory=set)
    last_digest_date: date | None = None


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), UTC)
        except Exception:
            return None
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def age_label(ts: str | None, now: datetime | None = None) -> str:
    dt = parse_ts(ts)
    if not dt:
        return "never"
    seconds = max(0, int(((now or utc_now()) - dt).total_seconds()))
    if seconds < 90:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 90:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def gap_id(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(k) or "") for k in ("start_ts", "end_ts", "reason"))


def can_send(alert_type: str, state: MonitorState, now: datetime) -> bool:
    last = state.last_alert_at.get(alert_type)
    return last is None or (now - last).total_seconds() >= ALERT_COOLDOWN_SECONDS


def mark_sent(alert_type: str, state: MonitorState, now: datetime) -> None:
    state.last_alert_at[alert_type] = now


def render_alert(condition: str, status: dict[str, Any] | None, details: str = "") -> str:
    if condition == RED_UNREACHABLE:
        return f"🔴 **Copybot status API unreachable >2m** {details}".strip()
    if condition == RED_INACTIVE:
        return "🔴 **Polymarket copybot archive service inactive** `service_active=false`"
    if condition == ORANGE_WS:
        age = (((status or {}).get("archiver") or {}).get("last_ws_message_age_s"))
        return f"🟠 **Copybot websocket stale** last_ws_message_age_s=`{age}`"
    if condition == ORANGE_FOLLOWUPS_STUCK:
        pending = (((status or {}).get("shadow") or {}).get("followups_pending"))
        return f"🟠 **Copybot followup queue stuck** pending=`{pending}` unchanged for >30m"
    if condition == ORANGE_MISSED:
        return f"🟠 **Copybot missed followups increased** {details}".strip()
    if condition == ORANGE_GAP:
        return f"🟠 **Copybot new gap row** {details}".strip()
    if condition == ORANGE_DISK:
        mb = (((status or {}).get("archiver") or {}).get("mb_per_day"))
        return f"🟠 **Copybot disk anomaly** mb_per_day=`{mb}` > 100"
    return f"🟠 **Copybot alert** `{condition}` {details}".strip()


def render_recovery(condition: str) -> str:
    labels = {
        RED_UNREACHABLE: "status API reachable again",
        RED_INACTIVE: "archive service active again",
        ORANGE_WS: "websocket fresh again",
        ORANGE_FOLLOWUPS_STUCK: "followup queue unstuck/cleared",
        ORANGE_DISK: "disk rate back under threshold",
    }
    return f"🟢 **Copybot recovered:** {labels.get(condition, condition)}"


def evaluate_poll(status: dict[str, Any] | None, state: MonitorState, now: datetime, error: str | None = None) -> list[str]:
    messages: list[str] = []
    current_conditions: set[str] = set()

    if status is None:
        if state.unreachable_since is None:
            state.unreachable_since = now
        if (now - state.unreachable_since).total_seconds() > UNREACHABLE_GRACE_SECONDS:
            current_conditions.add(RED_UNREACHABLE)
            if can_send(RED_UNREACHABLE, state, now):
                messages.append(render_alert(RED_UNREACHABLE, None, error or ""))
                mark_sent(RED_UNREACHABLE, state, now)
    else:
        state.unreachable_since = None
        archiver = status.get("archiver") if isinstance(status.get("archiver"), dict) else {}
        shadow = status.get("shadow") if isinstance(status.get("shadow"), dict) else {}

        if archiver.get("service_active") is False:
            current_conditions.add(RED_INACTIVE)
            if can_send(RED_INACTIVE, state, now):
                messages.append(render_alert(RED_INACTIVE, status))
                mark_sent(RED_INACTIVE, state, now)

        ws_age = float(archiver.get("last_ws_message_age_s") or 0)
        if ws_age > WS_STALE_SECONDS:
            current_conditions.add(ORANGE_WS)
            if can_send(ORANGE_WS, state, now):
                messages.append(render_alert(ORANGE_WS, status))
                mark_sent(ORANGE_WS, state, now)

        pending = int(shadow.get("followups_pending") or 0)
        if pending <= 0:
            state.pending = PendingValue()
        elif state.pending.value != pending:
            state.pending = PendingValue(value=pending, first_seen=now)
        elif state.pending.first_seen and (now - state.pending.first_seen).total_seconds() > FOLLOWUP_STUCK_SECONDS:
            current_conditions.add(ORANGE_FOLLOWUPS_STUCK)
            if can_send(ORANGE_FOLLOWUPS_STUCK, state, now):
                messages.append(render_alert(ORANGE_FOLLOWUPS_STUCK, status))
                mark_sent(ORANGE_FOLLOWUPS_STUCK, state, now)

        missed = int(shadow.get("followups_missed_today") or 0)
        if state.last_missed_count is not None and missed > state.last_missed_count:
            delta = missed - state.last_missed_count
            if can_send(ORANGE_MISSED, state, now):
                messages.append(render_alert(ORANGE_MISSED, status, f"+{delta} today_total=`{missed}`"))
                mark_sent(ORANGE_MISSED, state, now)
        state.last_missed_count = missed

        gaps = status.get("gaps_today") if isinstance(status.get("gaps_today"), list) else []
        for row in gaps:
            if not isinstance(row, dict):
                continue
            gid = gap_id(row)
            if gid and gid not in state.seen_gap_ids:
                reason = str(row.get("reason") or "unknown")
                label = "ws_stale" if reason == "ws_stale" else reason
                if can_send(ORANGE_GAP, state, now):
                    messages.append(render_alert(ORANGE_GAP, status, f"reason=`{label}` duration_s=`{row.get('duration_s')}`"))
                    mark_sent(ORANGE_GAP, state, now)
            state.seen_gap_ids.add(gid)

        mb_per_day = float(archiver.get("mb_per_day") or 0)
        if mb_per_day > DISK_MB_PER_DAY_LIMIT:
            current_conditions.add(ORANGE_DISK)
            if can_send(ORANGE_DISK, state, now):
                messages.append(render_alert(ORANGE_DISK, status))
                mark_sent(ORANGE_DISK, state, now)

    recoverable = {RED_UNREACHABLE, RED_INACTIVE, ORANGE_WS, ORANGE_FOLLOWUPS_STUCK, ORANGE_DISK}
    for condition in sorted((state.active_conditions - current_conditions) & recoverable):
        messages.append(render_recovery(condition))
    state.active_conditions = current_conditions
    if status is not None:
        state.last_status = status
    state.last_poll_at = now
    return messages


def render_status_card(status: dict[str, Any]) -> str:
    archiver = status.get("archiver") or {}
    shadow = status.get("shadow") or {}
    wallets = status.get("wallets") or []
    fills_today = sum(int(w.get("fills_today") or 0) for w in wallets if isinstance(w, dict)) or int(shadow.get("fills_today") or 0)
    return (
        "📊 **Polymarket Copybot Status**\n"
        f"Service: `{'active' if archiver.get('service_active') else 'inactive'}` | WS age: `{archiver.get('last_ws_message_age_s')}s`\n"
        f"Coverage today: `{status.get('coverage_pct_today')}%` | fills today: `{fills_today}`\n"
        f"Followups: pending `{shadow.get('followups_pending')}` completed `{shadow.get('followups_completed_today')}` missed `{shadow.get('followups_missed_today')}`\n"
        f"Disk: `{archiver.get('mb_per_day')}` MB/day, retention `{archiver.get('retention_gb')}` GB"
    )


def render_paper_card(paper: dict[str, Any]) -> str:
    reasons = paper.get("rejects_by_reason") if isinstance(paper.get("rejects_by_reason"), dict) else {}
    account_value = float(paper.get("account_value") or 0.0)
    open_notional = float(paper.get("open_notional") or 0.0)
    realized = float(paper.get("realized_pnl") or 0.0)
    unrealized = float(paper.get("unrealized_pnl") or 0.0)
    lines = [
        "💼 **Paper Follower Wallet**",
        f"Account value: `${account_value:.2f}` | open `${open_notional:.2f}`",
        f"PnL: realized `${realized:.2f}` | unrealized `${unrealized:.2f}`",
        f"Today: signals `{paper.get('signals_today')}` accepts `{paper.get('accepts_today')}` rejects `{paper.get('rejects_today')}`",
        f"Avg detection latency: `{paper.get('avg_detection_latency_s')}s`",
        f"Rejects: `{reasons}`",
    ]
    per_wallet = paper.get("per_wallet") if isinstance(paper.get("per_wallet"), list) else []
    if per_wallet:
        lines.append("**Wallets**")
        for row in per_wallet[:8]:
            if isinstance(row, dict):
                lines.append(f"- `{row.get('name')}` signals `{row.get('signals')}` accepts `{row.get('accepts')}` pnl `${float(row.get('pnl') or 0):.2f}`")
    return "\n".join(lines)[:1900]


def render_wallets(status: dict[str, Any], now: datetime | None = None) -> str:
    wallets = status.get("wallets") if isinstance(status.get("wallets"), list) else []
    if not wallets:
        return "👛 **Copybot wallets**\nNo wallet rows in status."
    lines = ["👛 **Copybot wallets**"]
    for row in wallets[:20]:
        if not isinstance(row, dict):
            continue
        lines.append(
            f"- `{row.get('name')}` today `{row.get('fills_today')}` | 7d `{row.get('fills_7d')}` | last fill `{age_label(row.get('last_fill_ts'), now)}` ago"
        )
    return "\n".join(lines)[:1900]


def render_gaps(status: dict[str, Any]) -> str:
    gaps = status.get("gaps_today") if isinstance(status.get("gaps_today"), list) else []
    if not gaps:
        return "🕳️ **Copybot gaps today**\nNone."
    lines = ["🕳️ **Copybot gaps today**"]
    for row in gaps[-15:]:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "unknown")
        label = "ws_stale" if reason == "ws_stale" else reason
        lines.append(f"- `{label}` duration `{row.get('duration_s')}`s ending `{row.get('end_ts')}`")
    return "\n".join(lines)[:1900]


def render_daily_digest(status: dict[str, Any], gaps_payload: list[dict[str, Any]] | None, now: datetime | None = None) -> str:
    now = now or utc_now()
    yesterday_key = (now.date() - timedelta(days=1)).isoformat()
    yesterday = next((row for row in gaps_payload or [] if row.get("date") == yesterday_key), {})
    gaps = yesterday.get("gaps") if isinstance(yesterday.get("gaps"), list) else []
    reasons: dict[str, int] = {}
    for row in gaps:
        reason = str(row.get("reason") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
    archiver = status.get("archiver") or {}
    shadow = status.get("shadow") or {}
    lines = [
        "🗞️ **Polymarket Copybot Daily Digest**",
        f"Yesterday coverage: `{yesterday.get('coverage_pct', 'n/a')}%` | gaps `{len(gaps)}` | reasons `{reasons or {}}`",
        "",
        "**Wallet fills**",
    ]
    for row in (status.get("wallets") or [])[:20]:
        if not isinstance(row, dict):
            continue
        last = parse_ts(row.get("last_fill_ts"))
        flag = " ⚠️ 0 fills 3d+" if (int(row.get("fills_today") or 0) == 0 and (not last or (now - last).total_seconds() >= 3 * 86400)) else ""
        lines.append(f"- `{row.get('name')}` today `{row.get('fills_today')}` 7d `{row.get('fills_7d')}` last `{age_label(row.get('last_fill_ts'), now)}` ago{flag}")
    lines += [
        "",
        f"Followups: completed `{shadow.get('followups_completed_today')}` missed `{shadow.get('followups_missed_today')}` pending `{shadow.get('followups_pending')}`",
        f"Disk: `{archiver.get('mb_per_day')}` MB/day | retention `{archiver.get('retention_gb')}` GB / `{archiver.get('retention_days')}`d",
    ]
    return "\n".join(lines)[:1900]


class CopybotMonitor(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.state = MonitorState()
        self.http_session: aiohttp.ClientSession | None = None
        self.channel_id = int(os.environ["DISCORD_CHANNEL_ID"])

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession()
        self.tree.add_command(app_commands.Command(name="status", description="Read-only copybot status", callback=self.cmd_status))
        self.tree.add_command(app_commands.Command(name="wallets", description="Read-only wallet fill summary", callback=self.cmd_wallets))
        self.tree.add_command(app_commands.Command(name="gaps", description="Read-only copybot gaps today", callback=self.cmd_gaps))
        self.tree.add_command(app_commands.Command(name="paper", description="Read-only paper wallet value", callback=self.cmd_paper))
        await self.tree.sync()
        self.poll_task = asyncio.create_task(self.poll_loop())

    async def close(self) -> None:
        if self.http_session:
            await self.http_session.close()
        await super().close()

    async def fetch_json(self, url: str) -> Any:
        assert self.http_session is not None
        async with self.http_session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_status(self) -> dict[str, Any]:
        return await self.fetch_json(STATUS_URL)

    async def fetch_gaps(self) -> list[dict[str, Any]]:
        return await self.fetch_json(GAPS_URL)

    async def fetch_paper(self) -> dict[str, Any]:
        return await self.fetch_json(PAPER_URL)

    async def send_channel(self, content: str) -> None:
        channel = self.get_channel(self.channel_id) or await self.fetch_channel(self.channel_id)
        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            raise RuntimeError(f"Unsupported channel type for {self.channel_id}: {type(channel)!r}")
        await channel.send(content[:1900])

    async def poll_once(self) -> None:
        now = utc_now()
        try:
            status = await self.fetch_status()
            messages = evaluate_poll(status, self.state, now)
        except Exception as exc:
            messages = evaluate_poll(None, self.state, now, repr(exc))
        for msg in messages:
            await self.send_channel(msg)
        await self.maybe_daily_digest(now)

    async def maybe_daily_digest(self, now: datetime) -> None:
        if now.timetz().replace(second=0, microsecond=0) < DAILY_DIGEST_UTC:
            return
        if self.state.last_digest_date == now.date():
            return
        status = self.state.last_status or await self.fetch_status()
        gaps = await self.fetch_gaps()
        await self.send_channel(render_daily_digest(status, gaps, now))
        self.state.last_digest_date = now.date()

    async def poll_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            await self.poll_once()
            await asyncio.sleep(POLL_SECONDS)

    async def cmd_status(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(render_status_card(await self.fetch_status()))

    async def cmd_wallets(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(render_wallets(await self.fetch_status()))

    async def cmd_gaps(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(render_gaps(await self.fetch_status()))

    async def cmd_paper(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(render_paper_card(await self.fetch_paper()))


def main() -> None:
    token = os.environ["DISCORD_BOT_TOKEN"]
    bot = CopybotMonitor()
    bot.run(token)


if __name__ == "__main__":
    main()
