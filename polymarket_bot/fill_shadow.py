"""Fill-attainability shadow: would our paper entries really have filled?

Paper entries assume we cross the book at detection time with a haircut.
Before live money, we need evidence that assumption holds.  For every entry
this module schedules delayed re-quotes (+60s, +300s by default): it fetches
the token's live book again and re-runs the same fill simulation.  If the
restored book would still fill our stake at or below the original simulated
price, the paper fill was attainable.

Results land in the ledger as ``fill_check`` rows so attainability can be
measured offline (target: >=90% attainable before any live sizing).

Dependency-injected (book fetcher + fill simulator + clock) so it stays
deterministic in tests and carries no import cycle with paper_follower.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

DEFAULT_OFFSETS_SECONDS = (60, 300)


class FillShadow:
    def __init__(
        self,
        pending_path: Path,
        *,
        book_fetcher: Callable[[str], dict[str, Any] | None],
        fill_simulator: Callable[..., tuple[float | None, float, str | None]],
        haircut: float,
        stake_usd: float,
        offsets_seconds: tuple[int, ...] = DEFAULT_OFFSETS_SECONDS,
        enabled: bool = True,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        self.pending_path = pending_path
        self.book_fetcher = book_fetcher
        self.fill_simulator = fill_simulator
        self.haircut = haircut
        self.stake_usd = stake_usd
        self.offsets_seconds = offsets_seconds
        self.enabled = enabled
        self.now_fn = now_fn
        self._pending: list[dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.pending_path.read_text())
        except (OSError, ValueError):
            self._pending = []
            return
        self._pending = [p for p in raw if isinstance(p, dict) and p.get("trade_id")] if isinstance(raw, list) else []

    def _save(self) -> None:
        try:
            self.pending_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.pending_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._pending))
            tmp.replace(self.pending_path)
        except OSError:
            pass

    def schedule(self, entry_row: dict[str, Any]) -> None:
        """Queue delayed attainability checks for a fresh paper entry."""
        if not self.enabled:
            return
        trade_id = str(entry_row.get("trade_id") or "")
        token = str(entry_row.get("token") or "")
        sim_price = entry_row.get("sim_fill_price")
        if not trade_id or not token or sim_price is None:
            return
        if any(p["trade_id"] == trade_id for p in self._pending):
            return
        self._pending.append(
            {
                "trade_id": trade_id,
                "token": token,
                "sim_fill_price": float(sim_price),
                "entry_epoch": self.now_fn(),
                "entry_ts": entry_row.get("ts"),
                "done_offsets": [],
            }
        )
        self._save()

    def run_due(self) -> list[dict[str, Any]]:
        """Run all due checks; returns fill_check ledger rows."""
        if not self.enabled:
            return []
        now = self.now_fn()
        rows: list[dict[str, Any]] = []
        for pending in self._pending:
            done = set(pending.get("done_offsets") or [])
            for offset in self.offsets_seconds:
                if offset in done:
                    continue
                if now < float(pending["entry_epoch"]) + offset:
                    continue
                rows.append(self._check(pending, offset, now))
                done.add(offset)
            pending["done_offsets"] = sorted(done)
        before = len(self._pending)
        self._pending = [
            p
            for p in self._pending
            if len(set(p.get("done_offsets") or [])) < len(self.offsets_seconds)
        ]
        if rows or len(self._pending) != before:
            self._save()
        return rows

    def _check(self, pending: dict[str, Any], offset: int, now: float) -> dict[str, Any]:
        token = str(pending["token"])
        sim_price = float(pending["sim_fill_price"])
        row: dict[str, Any] = {
            "type": "fill_check",
            "trade_id": pending["trade_id"],
            "token": token,
            "offset_s": offset,
            "sim_fill_price": sim_price,
            "entry_ts": pending.get("entry_ts"),
            "check_epoch": now,
        }
        try:
            book = self.book_fetcher(token)
        except Exception as exc:
            row.update({"attainable": None, "error": f"book_fetch:{type(exc).__name__}"})
            return row
        if not book:
            row.update({"attainable": False, "error": "book_unavailable"})
            return row
        price_now, _shares, err = self.fill_simulator(book, "BUY", self.stake_usd, self.haircut)
        if err is not None or price_now is None:
            row.update({"attainable": False, "error": err or "no_fill", "book_snapshot_age_s": None})
            return row
        row.update(
            {
                # 1e-6 epsilon absorbs float noise when the book is unchanged
                # between entry and check (identical math, borderline repr).
                "attainable": price_now <= sim_price + 1e-6,
                "price_now": price_now,
                "price_drift": round(price_now - sim_price, 6),
            }
        )
        return row

    def __len__(self) -> int:
        return len(self._pending)
