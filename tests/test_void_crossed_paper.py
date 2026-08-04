#!/usr/bin/env python3
"""Tests for scripts/void_crossed_paper.py in-loop seen-set dedup guard.

Two cases:
1. Duplicate close rows for the same position_id must collapse to ONE void_correction.
2. Two distinct crossed positions must produce TWO void_corrections
   (guards against the seen-set being too aggressive and swallowing legitimate voids).
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCRIPT = REPO / "scripts" / "void_crossed_paper.py"


def load_module():
    spec = importlib.util.spec_from_file_location("void_crossed_paper", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["void_crossed_paper"] = mod
    spec.loader.exec_module(mod)
    return mod


def make_crossed_entry(token: str, position_id: str, wallet: str = "0xw") -> dict:
    return {
        "ts": "2026-08-01T12:00:00+00:00",
        "type": "entry",
        "token": token,
        "market": "0x" + token.rjust(64, "0"),
        "position_id": position_id,
        "wallet": wallet,
        "side": "BUY",
        "sim_fill_price": 0.006,
        "wallet_fill_price": 0.03,
        "sim_size": 100.0,
        "book_snapshot": {"best_bid": 0.011, "best_ask": 0.001, "spread": -0.01},
    }


def make_resolution(position_id: str, pnl: float) -> dict:
    return {
        "ts": "2026-08-02T12:00:00+00:00",
        "type": "resolution",
        "position_id": position_id,
        "pnl": pnl,
        "sim_fill_price": 0.0,
        "sim_size": 100.0,
    }


class VoidCrossedPaperDedupTests(unittest.TestCase):
    def test_duplicate_close_collapsed_to_one_void(self) -> None:
        mod = load_module()
        pos = "0xw:tokA"
        entry = make_crossed_entry("tokA", pos)
        # Two duplicate resolution rows for the SAME position_id.
        close_a = make_resolution(pos, -50.0)
        close_b = make_resolution(pos, -50.0)
        rows = [entry, close_a, close_b]
        corrections = mod.build_void_corrections(rows)
        self.assertEqual(
            len(corrections),
            1,
            f"expected exactly 1 void_correction, got {len(corrections)}",
        )
        self.assertEqual(corrections[0]["voided_pnl"], -50.0)
        self.assertEqual(corrections[0]["pnl"], 50.0)
        self.assertEqual(corrections[0]["position_id"], pos)

    def test_distinct_positions_emit_two_voids(self) -> None:
        mod = load_module()
        pos_a = "0xw:tokA"
        pos_b = "0xw:tokB"
        entry_a = make_crossed_entry("tokA", pos_a)
        entry_b = make_crossed_entry("tokB", pos_b)
        close_a = make_resolution(pos_a, 100.0)
        close_b = make_resolution(pos_b, -100.0)
        rows = [entry_a, close_a, entry_b, close_b]
        corrections = mod.build_void_corrections(rows)
        self.assertEqual(
            len(corrections),
            2,
            f"expected exactly 2 void_corrections (one per position), got {len(corrections)}",
        )
        pos_ids = {c["position_id"] for c in corrections}
        self.assertEqual(pos_ids, {pos_a, pos_b})
        # Verify the seen-set is keyed on position_id, not on pnl magnitude.
        by_pos = {c["position_id"]: c for c in corrections}
        self.assertEqual(by_pos[pos_a]["voided_pnl"], 100.0)
        self.assertEqual(by_pos[pos_a]["pnl"], -100.0)
        self.assertEqual(by_pos[pos_b]["voided_pnl"], -100.0)
        self.assertEqual(by_pos[pos_b]["pnl"], 100.0)

    def test_duplicate_close_does_not_swallow_distinct_position(self) -> None:
        """Mixed case: pos A has a duplicate close, pos B is distinct.

        A real in-loop seen-set should emit exactly 2 corrections
        (one for A, one for B), not 1 (over-dedup) or 3 (no dedup).
        """
        mod = load_module()
        pos_a = "0xw:tokA"
        pos_b = "0xw:tokB"
        entry_a = make_crossed_entry("tokA", pos_a)
        entry_b = make_crossed_entry("tokB", pos_b)
        rows = [
            entry_a,
            make_resolution(pos_a, -50.0),
            make_resolution(pos_a, -50.0),  # duplicate
            entry_b,
            make_resolution(pos_b, 25.0),
        ]
        corrections = mod.build_void_corrections(rows)
        self.assertEqual(
            len(corrections),
            2,
            f"expected 2 (pos A dedup'd, pos B single), got {len(corrections)}",
        )
        pos_ids = sorted(c["position_id"] for c in corrections)
        self.assertEqual(pos_ids, sorted([pos_a, pos_b]))


class VoidCrossedPaperApplyTests(unittest.TestCase):
    """Apply-path smoke: --apply must append corrections and respect dedup."""

    def test_apply_is_idempotent_across_runs(self) -> None:
        mod = load_module()
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            paper_dir = td_path / "paper"
            paper_dir.mkdir()
            ledger = paper_dir / "ledger.jsonl"
            pos = "0xw:tokA"
            entry = make_crossed_entry("tokA", pos)
            close = make_resolution(pos, -50.0)
            ledger.write_text(
                json.dumps(entry) + "\n" + json.dumps(close) + "\n"
            )
            # First apply — should append one void_correction.
            sys.argv = ["void_crossed_paper.py", "--apply", "--paper-dir", str(paper_dir)]
            mod.main()
            lines_after_first = ledger.read_text().strip().splitlines()
            self.assertEqual(
                len(lines_after_first),
                3,
                f"expected 3 ledger rows after first --apply, got {len(lines_after_first)}",
            )
            # Second apply — must NOT append another (already-corrected set).
            sys.argv = ["void_crossed_paper.py", "--apply", "--paper-dir", str(paper_dir)]
            mod.main()
            lines_after_second = ledger.read_text().strip().splitlines()
            self.assertEqual(
                len(lines_after_second),
                3,
                f"expected 3 ledger rows after second --apply (idempotent), got {len(lines_after_second)}",
            )


if __name__ == "__main__":
    unittest.main()
