"""Tests for the polybot cleanup module."""

import gzip
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


class CleanupTest(unittest.TestCase):
    def setUp(self):
        from polymarket_bot import cleanup
        self.cleanup = cleanup
        self.tmpdir = tempfile.mkdtemp()
        self.runs_dir = Path(self.tmpdir) / "runs"
        self.runs_dir.mkdir()
        # Create test files
        self._make_file("book_2026-07-01_00.jsonl.gz", age_days=10)
        self._make_file("book_2026-07-08_00.jsonl.gz", age_days=20)
        self._make_file("book_2026-07-15_12.jsonl.gz", age_days=13)
        self._make_file("book_2026-07-27_00.jsonl.gz", age_days=0)
        self._make_file("shadow_2026-07-01_00.jsonl.gz", age_days=10)
        self._make_file("shadow_2026-07-15_00.jsonl.gz", age_days=13)
        self._make_file("shadow_2026-07-27_00.jsonl.gz", age_days=0)
        # Critical state files that must never be deleted
        (self.runs_dir / "paper").mkdir()
        (self.runs_dir / "paper" / "state.json").write_text("{}")
        (self.runs_dir / "wallet_scores_latest.json").write_text("{}")
        (self.runs_dir / "decisions_latest.json").write_text("{}")
        (self.runs_dir / "scan_latest.json").write_text("{}")
        (self.runs_dir / "dashboard.html").write_text("<html></html>")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_file(self, rel_path: str, age_days: float = 0, content: bytes = b"x" * 1024) -> None:
        path = self.runs_dir / "book_archive" / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        # Set mtime to age_days ago
        mtime = time.time() - (age_days * 86400)
        import os
        os.utime(path, (mtime, mtime))

    def test_prunes_old_book_files(self):
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=30,
            max_gb=10.0,
            dry_run=False,
        )
        # 3 book files >7 days old should be deleted
        self.assertEqual(result.files_deleted, 3)
        remaining = list((self.runs_dir / "book_archive").glob("book_*.jsonl.gz"))
        self.assertEqual(len(remaining), 1)
        self.assertIn("07-27", remaining[0].name)

    def test_keeps_recent_book_files(self):
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=30,
            max_gb=10.0,
            dry_run=False,
        )
        # Recent (today) book file survives
        recent = list((self.runs_dir / "book_archive").glob("book_2026-07-27*"))
        self.assertEqual(len(recent), 1)

    def test_prunes_old_shadow_files(self):
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=12,
            max_gb=10.0,
            dry_run=False,
        )
        # 2 shadow files >12 days old
        remaining = list((self.runs_dir / "book_archive").glob("shadow_*.jsonl.gz"))
        # 07-15 was 13d old (delete), 07-27 was today (keep), 07-01 was 10d (keep)
        self.assertEqual(len(remaining), 2)
        for f in remaining:
            self.assertNotIn("07-15", f.name)

    def test_preserves_state_files(self):
        self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=7,
            max_gb=0.0001,  # Force hard cap
            dry_run=False,
        )
        # Even with hard cap, critical files survive
        self.assertTrue((self.runs_dir / "paper" / "state.json").exists())
        self.assertTrue((self.runs_dir / "wallet_scores_latest.json").exists())
        self.assertTrue((self.runs_dir / "decisions_latest.json").exists())
        self.assertTrue((self.runs_dir / "scan_latest.json").exists())
        self.assertTrue((self.runs_dir / "dashboard.html").exists())

    def test_dry_run_doesnt_delete(self):
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=7,
            max_gb=0.0001,
            dry_run=True,
        )
        self.assertGreater(result.files_deleted, 0)  # Reports what would delete
        # But nothing actually deleted
        self.assertTrue((self.runs_dir / "book_archive" / "book_2026-07-01_00.jsonl.gz").exists())

    def test_hard_cap_enforced(self):
        # Create some large files to push over the cap
        for i in range(5):
            self._make_file(f"shadow_2026-07-20_{i:02d}.jsonl.gz", age_days=0,
                            content=b"X" * (1024 * 1024))  # 1MB each
        # Set 2MB cap (in bytes, since the test expects sub-2MB)
        cap_bytes = 2 * 1024 * 1024
        # Pass cap in bytes via a tiny max_gb (0.00186 GB = 2MB)
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=999,  # Don't trigger normal retention
            shadow_retention_days=999,
            max_gb=cap_bytes / (1024 ** 3),  # Convert to GB
            dry_run=False,
        )
        # Should have deleted enough to get under cap
        self.assertGreater(result.files_deleted, 0)
        # Verify total is now under cap
        new_total = sum(p.stat().st_size for p in self.runs_dir.rglob("*") if p.is_file())
        self.assertLessEqual(new_total, cap_bytes)

    def test_trims_large_ledger(self):
        # Create a large ledger
        (self.runs_dir / "paper").mkdir(exist_ok=True)
        ledger = self.runs_dir / "paper" / "ledger.jsonl"
        # 60MB of lines (above 50MB threshold)
        with open(ledger, "w") as f:
            for i in range(500_000):
                f.write(json.dumps({"i": i, "data": "x" * 100}) + "\n")
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=999,
            shadow_retention_days=999,
            max_gb=10.0,
            dry_run=False,
        )
        self.assertTrue(result.ledger_trimmed)
        # Ledger should be smaller now
        new_size = ledger.stat().st_size
        self.assertLess(new_size, 50 * 1024 * 1024)

    def test_returns_result_dict(self):
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=30,
            max_gb=10.0,
            dry_run=True,
        )
        d = result.to_dict()
        self.assertIn("files_deleted", d)
        self.assertIn("bytes_freed", d)
        self.assertIn("duration_s", d)
        self.assertIn("total_before_mb", d)

    def test_handles_missing_runs_dir(self):
        import shutil
        shutil.rmtree(self.runs_dir)
        result = self.cleanup.run_cleanup(
            runs_dir=self.runs_dir,
            book_retention_days=7,
            shadow_retention_days=30,
            max_gb=10.0,
            dry_run=False,
        )
        # Should not crash, just report 0 deletions
        self.assertEqual(result.files_deleted, 0)


if __name__ == "__main__":
    unittest.main()