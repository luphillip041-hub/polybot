"""Entry vs real-world outcome timing (paper / shadow analysis).

On-chain ConditionalTokens payout is *not* the real-world event.  This
module joins paper ledger entry timestamps to Gamma market metadata
(``endDate``, ``closedTime``) and flags positions that were opened after
the public outcome window but still settled later on-chain.

Fail closed: missing or unparseable Gamma fields are ``unknown``, never
counted as proof that entry preceded the event.  ``gameStartTime`` and
``umaResolutionStatus`` alone are not outcome timestamps.

Gamma ``endDate`` is still only a *scheduled market end*, not a news
wire timestamp.  A ``precedes`` verdict is necessary but not sufficient
to clear lookahead risk.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, Iterable

from .scorecard_slices import condition_id_from_row

VERDICT_PRECEDES = "precedes"
VERDICT_SUSPICIOUS = "suspicious"
VERDICT_UNKNOWN = "unknown"

# Fields that can stand in for a public outcome-window end.  Start-of-event
# timestamps are recorded but never selected (they are not resolution time).
_OUTCOME_END_KEYS = (
    "endDate",
    "end_date",
    "closedTime",
    "closed_time",
    "closedAt",
    "umaEndDate",
)
_START_ONLY_KEYS = ("gameStartTime", "eventStartTime", "startTime", "startDate")


def parse_ts(value: Any) -> datetime | None:
    """Parse ISO strings, unix seconds, or unix milliseconds.  None if ambiguous."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return None
        if ts >= 1e12:  # epoch milliseconds
            ts /= 1000.0
        if ts <= 0:
            return None
        try:
            return datetime.fromtimestamp(ts, UTC)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            as_num = float(text)
        except ValueError:
            as_num = None
        if as_num is not None and text.replace(".", "", 1).replace("-", "", 1).isdigit():
            return parse_ts(as_num)
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(UTC).isoformat(timespec="seconds") if dt else None


def seconds_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return (later - earlier).total_seconds()


def _first_ts(blob: dict[str, Any], keys: tuple[str, ...]) -> tuple[datetime | None, str | None]:
    for key in keys:
        ts = parse_ts(blob.get(key))
        if ts is not None:
            return ts, key
    return None, None


def extract_market_timing(
    market: dict[str, Any] | None,
    event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Copy Gamma timing fields off a market (and optional parent event).

    Does not decide a verdict.  Unknown / unparseable values stay None.
    """
    market = market if isinstance(market, dict) else {}
    event = event if isinstance(event, dict) else {}
    merged: dict[str, Any] = {}
    for key in (*_OUTCOME_END_KEYS, *_START_ONLY_KEYS, "umaResolutionStatus", "resolution"):
        if market.get(key) not in (None, ""):
            merged[key] = market.get(key)
        elif event.get(key) not in (None, ""):
            merged[key] = event.get(key)

    end_date, end_src = _first_ts(merged, ("endDate", "end_date"))
    closed_time, closed_src = _first_ts(merged, ("closedTime", "closed_time", "closedAt", "umaEndDate"))
    start_ts, start_src = _first_ts(merged, _START_ONLY_KEYS)
    uma = merged.get("umaResolutionStatus") or merged.get("resolution")

    real_end: datetime | None = None
    real_src: str | None = None
    candidates = [(end_date, end_src), (closed_time, closed_src)]
    present = [(ts, src) for ts, src in candidates if ts is not None and src]
    if len(present) == 1:
        real_end, real_src = present[0]
    elif len(present) == 2:
        real_end, real_src = min(present, key=lambda item: item[0])
        real_src = f"earliest({present[0][1]},{present[1][1]})"

    return {
        "end_date": iso(end_date),
        "end_date_field": end_src,
        "closed_time": iso(closed_time),
        "closed_time_field": closed_src,
        "game_start": iso(start_ts),
        "game_start_field": start_src,
        "uma_resolution_status": uma,
        "real_world_end_ts": iso(real_end),
        "real_world_end_source": real_src,
        "condition_id": str(
            market.get("conditionId") or market.get("condition_id") or event.get("conditionId") or ""
        ).lower(),
        "question": market.get("question") or event.get("title") or event.get("question"),
        "raw_keys": sorted(merged.keys()),
    }


def classify_pair(
    entry: dict[str, Any],
    closed: dict[str, Any],
    market_meta: dict[str, Any] | None,
) -> dict[str, Any]:
    """Classify one paper position against Gamma timing metadata."""
    entry_ts = parse_ts(entry.get("ts") or entry.get("entry_ts"))
    closed_type = str(closed.get("type") or "")
    onchain_ts = parse_ts(closed.get("ts")) if closed_type == "resolution" else None
    onchain_source = "resolution" if closed_type == "resolution" else (
        "exit" if closed_type == "exit" else None
    )
    token = str(entry.get("token") or closed.get("token") or "")
    cid = condition_id_from_row(entry) or condition_id_from_row(closed)
    pnl = float(closed.get("pnl") or 0)
    raw_meta = market_meta if isinstance(market_meta, dict) else {}
    if raw_meta and "real_world_end_ts" not in raw_meta:
        meta = extract_market_timing(raw_meta)
    else:
        meta = raw_meta

    real_end = parse_ts(meta.get("real_world_end_ts"))
    real_src = meta.get("real_world_end_source")
    unknown_reason = ""
    verdict = VERDICT_UNKNOWN

    if entry_ts is None:
        unknown_reason = "missing_entry_ts"
    elif real_end is None:
        if parse_ts(meta.get("game_start")) is not None and not real_src:
            unknown_reason = "gameStartTime_is_not_outcome_time"
        elif meta.get("uma_resolution_status") and not real_src:
            unknown_reason = "status_without_timestamp"
        elif not meta:
            unknown_reason = "no_gamma_meta"
        else:
            unknown_reason = "unparseable_or_missing_endDate_closedTime"
    elif entry_ts < real_end:
        verdict = VERDICT_PRECEDES
    else:
        # Entered at/after the public outcome window.
        if onchain_source != "resolution" or onchain_ts is None or entry_ts <= onchain_ts:
            verdict = VERDICT_SUSPICIOUS
        else:
            # Entry after the follower already journaled a resolution: data bug,
            # not a lookahead proof either way.
            unknown_reason = "entry_after_ledger_resolution"
            verdict = VERDICT_UNKNOWN

    return {
        "position_id": str(entry.get("position_id") or closed.get("position_id") or ""),
        "wallet": entry.get("wallet") or closed.get("wallet"),
        "token": token,
        "condition_id": cid,
        "question": meta.get("question"),
        "pnl": round(pnl, 4),
        "won": pnl > 0,
        "closed_type": closed_type,
        "entry_ts": iso(entry_ts),
        "onchain_resolution_ts": iso(onchain_ts),
        "onchain_source": onchain_source,
        "end_date": meta.get("end_date"),
        "closed_time": meta.get("closed_time"),
        "uma_resolution_status": meta.get("uma_resolution_status"),
        "real_world_end_ts": iso(real_end),
        "real_world_end_source": real_src,
        "lead_s": seconds_between(real_end, entry_ts),
        "hold_s": seconds_between(parse_ts(closed.get("ts")), entry_ts),
        "verdict": verdict,
        "unknown_reason": unknown_reason or None,
    }


def iter_closed_pairs(rows: Iterable[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Join resolution/exit rows to their entry by ``position_id``."""
    entries: dict[str, dict[str, Any]] = {}
    closed: list[dict[str, Any]] = []
    for row in rows:
        kind = row.get("type")
        pid = str(row.get("position_id") or "")
        if kind == "entry" and pid:
            entries[pid] = row
        elif kind in {"resolution", "exit"}:
            closed.append(row)
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in closed:
        pid = str(row.get("position_id") or "")
        entry = entries.get(pid)
        if entry is None:
            continue
        pairs.append((entry, row))
    return pairs


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((q / 100) * (len(ordered) - 1)))))
    return round(ordered[idx], 2)


def _rate(n: int, d: int) -> float | None:
    return round(n / d * 100, 2) if d else None


def timing_summary(classified: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate verdicts.  Rates are over the *known* subset, never over unknowns."""
    precedes = [r for r in classified if r.get("verdict") == VERDICT_PRECEDES]
    suspicious = [r for r in classified if r.get("verdict") == VERDICT_SUSPICIOUS]
    unknown = [r for r in classified if r.get("verdict") == VERDICT_UNKNOWN]
    known = precedes + suspicious
    wins = [r for r in classified if r.get("won")]
    losses = [r for r in classified if not r.get("won")]
    known_wins = [r for r in known if r.get("won")]
    known_losses = [r for r in known if not r.get("won")]
    leads = [float(r["lead_s"]) for r in known if r.get("lead_s") is not None]
    holds = [float(r["hold_s"]) for r in classified if r.get("hold_s") is not None]
    unknown_reasons: dict[str, int] = {}
    for row in unknown:
        reason = str(row.get("unknown_reason") or "unknown")
        unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1

    def side_stats(rows: list[dict[str, Any]], known_side: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "n": len(rows),
            "known": len(known_side),
            "precedes": sum(1 for r in known_side if r.get("verdict") == VERDICT_PRECEDES),
            "suspicious": sum(1 for r in known_side if r.get("verdict") == VERDICT_SUSPICIOUS),
            "precedes_pct_of_known": _rate(
                sum(1 for r in known_side if r.get("verdict") == VERDICT_PRECEDES),
                len(known_side),
            ),
        }

    return {
        "closed_paired": len(classified),
        "known": len(known),
        "unknown": len(unknown),
        "unknown_pct": _rate(len(unknown), len(classified)),
        "precedes": len(precedes),
        "suspicious": len(suspicious),
        "precedes_pct_of_known": _rate(len(precedes), len(known)),
        "suspicious_pct_of_known": _rate(len(suspicious), len(known)),
        "wins": side_stats(wins, known_wins),
        "losses": side_stats(losses, known_losses),
        "lead_s": {
            "n": len(leads),
            "min": round(min(leads), 2) if leads else None,
            "p5": _percentile(leads, 5),
            "p50": _percentile(leads, 50),
            "p95": _percentile(leads, 95),
            "max": round(max(leads), 2) if leads else None,
            "note": "real_world_end - entry; positive means entry before Gamma end",
        },
        "ledger_hold_s": {
            "n": len(holds),
            "min": round(min(holds), 2) if holds else None,
            "p50": _percentile(holds, 50),
            "p95": _percentile(holds, 95),
            "max": round(max(holds), 2) if holds else None,
            "note": "resolution/exit observer ts - entry; NOT real-world outcome time",
        },
        "unknown_reasons": unknown_reasons,
        "suspicious_pnl": round(sum(float(r.get("pnl") or 0) for r in suspicious), 2),
        "precedes_pnl": round(sum(float(r.get("pnl") or 0) for r in precedes), 2),
        "limitations": [
            "endDate is a scheduled market end, not a news/sports-outcome timestamp.",
            "closedTime is venue close (often UMA-lagged) and is a weaker proxy when endDate is missing.",
            "onchain_resolution_ts is the follower's ledger write time, not the ConditionResolution block time.",
            "unknown rows are excluded from precedes/suspicious rates (fail closed).",
            "A high precedes_pct does not by itself prove absence of lookahead.",
        ],
    }


class GammaTimingFetcher:
    """Look up Gamma timing metadata with optional offline maps.

    Network callables are injected so tests never need keys or live HTTP.
    Lookup errors become empty meta (caller classifies ``unknown``).
    """

    def __init__(
        self,
        *,
        event_by_token: Callable[[str], list[dict[str, Any]]] | None = None,
        markets_by_token: Callable[[str], list[dict[str, Any]]] | None = None,
        events_by_condition: Callable[[str], list[dict[str, Any]]] | None = None,
        meta_by_token: dict[str, dict[str, Any]] | None = None,
        meta_by_condition: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.event_by_token = event_by_token
        self.markets_by_token = markets_by_token
        self.events_by_condition = events_by_condition
        self.meta_by_token = {str(k).lower(): v for k, v in (meta_by_token or {}).items()}
        self.meta_by_condition = {str(k).lower(): v for k, v in (meta_by_condition or {}).items()}
        self._cache: dict[tuple[str, str], dict[str, Any]] = {}
        self.lookups = 0
        self.hits_offline = 0
        self.failures = 0

    def lookup(self, token: str, condition_id: str) -> dict[str, Any]:
        token_key = str(token or "").lower()
        cid_key = str(condition_id or "").lower()
        cache_key = (token_key, cid_key)
        if cache_key in self._cache:
            return self._cache[cache_key]
        meta = self._lookup_uncached(token_key, cid_key)
        self._cache[cache_key] = meta
        return meta

    def _normalize(self, blob: dict[str, Any], event: dict[str, Any] | None = None) -> dict[str, Any]:
        extracted = extract_market_timing(blob, event)
        if blob.get("real_world_end_ts"):
            extracted["real_world_end_ts"] = blob["real_world_end_ts"]
            extracted["real_world_end_source"] = (
                blob.get("real_world_end_source") or extracted.get("real_world_end_source")
            )
        return extracted

    def _lookup_uncached(self, token: str, condition_id: str) -> dict[str, Any]:
        if token and token in self.meta_by_token:
            self.hits_offline += 1
            return self._normalize(self.meta_by_token[token])
        if condition_id and condition_id in self.meta_by_condition:
            self.hits_offline += 1
            return self._normalize(self.meta_by_condition[condition_id])

        try:
            if token and self.event_by_token is not None:
                self.lookups += 1
                events = self.event_by_token(token) or []
                for evt in events:
                    if not isinstance(evt, dict):
                        continue
                    for market in evt.get("markets") or []:
                        if not isinstance(market, dict):
                            continue
                        ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []
                        if isinstance(ids, str):
                            ids = [ids]
                        if any(str(x).lower() == token for x in ids):
                            return extract_market_timing(market, evt)
            if token and self.markets_by_token is not None:
                self.lookups += 1
                rows = self.markets_by_token(token) or []
                for row in rows:
                    if isinstance(row, dict):
                        return extract_market_timing(row)
            if condition_id and self.events_by_condition is not None:
                self.lookups += 1
                events = self.events_by_condition(condition_id) or []
                for evt in events:
                    if not isinstance(evt, dict):
                        continue
                    for market in evt.get("markets") or []:
                        if not isinstance(market, dict):
                            continue
                        cid = str(market.get("conditionId") or market.get("condition_id") or "").lower()
                        if cid and cid == condition_id:
                            return extract_market_timing(market, evt)
        except Exception:
            self.failures += 1
            return {}
        return {}


def load_offline_meta(payload: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Parse ``--meta`` JSON: ``{by_token, by_condition_id}`` or a flat market list."""
    by_token: dict[str, dict[str, Any]] = {}
    by_cid: dict[str, dict[str, Any]] = {}
    if isinstance(payload, dict):
        token_map = payload.get("by_token") or payload.get("tokens") or {}
        cid_map = payload.get("by_condition_id") or payload.get("conditions") or {}
        if isinstance(token_map, dict):
            by_token = {str(k).lower(): v for k, v in token_map.items() if isinstance(v, dict)}
        if isinstance(cid_map, dict):
            by_cid = {str(k).lower(): v for k, v in cid_map.items() if isinstance(v, dict)}
        markets = payload.get("markets")
        if isinstance(markets, list):
            payload = markets
        elif not by_token and not by_cid and (
            payload.get("endDate") or payload.get("conditionId") or payload.get("clobTokenIds")
        ):
            payload = [payload]
    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            cid = str(row.get("conditionId") or row.get("condition_id") or "").lower()
            if cid:
                by_cid[cid] = row
            ids = row.get("clobTokenIds") or row.get("clob_token_ids") or []
            if isinstance(ids, str):
                ids = [ids]
            for token in ids:
                by_token[str(token).lower()] = row
    return by_token, by_cid


def classify_ledger(
    rows: Iterable[dict[str, Any]],
    fetcher: GammaTimingFetcher,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    pairs = iter_closed_pairs(rows)
    if limit is not None:
        pairs = pairs[: max(0, int(limit))]
    out: list[dict[str, Any]] = []
    for entry, closed in pairs:
        token = str(entry.get("token") or closed.get("token") or "")
        cid = condition_id_from_row(entry) or condition_id_from_row(closed)
        meta = fetcher.lookup(token, cid)
        out.append(classify_pair(entry, closed, meta))
    return out
