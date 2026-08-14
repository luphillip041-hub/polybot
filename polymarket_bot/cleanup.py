"""Disk cleanup for polybot runs/ directory.

The book archive grows ~25MB/day (book_*.jsonl.gz + shadow_*.jsonl.gz).
This module prunes:

1. book_2026-*.jsonl.gz: delete after 1 day (already aggregated into
   shadow, no reason to keep raw snapshots)
2. shadow_2026-*.jsonl.gz: delete after 30 days (backtest window)
3. runs/paper/ledger.jsonl: trim to last 100k lines if > 50MB
4. Hard cap: if total runs/ size > MAX_RUNS_GB, delete oldest shadow_ files

State files (state.json, scan_latest.json, decisions_latest.json,
wallet_scores_latest.json, etc.) are NEVER touched.

Run manually:
    python -m polymarket_bot.cleanup [--dry-run]

Schedule via systemd timer (deploy/cleanup.timer, daily 02:00 UTC).
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("polymarket_bot.cleanup")

# Defaults
BOOK_RETENTION_DAYS = 1
SHADOW_RETENTION_DAYS = 30
LEDGER_MAX_LINES = 100_000
LEDGER_MAX_BYTES = 50 * 1024 * 1024  # 50MB
MAX_RUNS_GB = 2.0
DEFAULT_RUNS_DIR = Path("/root/flip/projects/polymarket-copybot/runs")


# Files that should NEVER be deleted regardless of age
PROTECTED_PATTERNS = {
    "state.json",
    "scan_latest.json",
    "decisions_latest.json",
    "wallet_scores_latest.json",
    "shadow_journal_state.json",
    "heartbeat_latest.json",
    "markets_latest.json",
    "dashboard.html",
    "copyability_backtest_feasibility.md",
    "*.json",  # all current JSON snapshots
}


@dataclass
class CleanupResult:
    files_deleted: int = 0
    bytes_freed: int = 0
    ledger_trimmed: bool = False
    errors: list[str] = field(default_factory=list)
    duration_s: float = 0.0
    total_before_bytes: int = 0
    total_after_bytes: int = 0
    # Internal only: prevents dry-run retention candidates from being counted
    # a second time by hard-cap simulation.
    planned_deletions: set[Path] = field(default_factory=set, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "files_deleted": self.files_deleted,
            "bytes_freed": self.bytes_freed,
            "bytes_freed_mb": round(self.bytes_freed / 1e6, 2),
            "ledger_trimmed": self.ledger_trimmed,
            "errors": self.errors,
            "duration_s": round(self.duration_s, 2),
            "total_before_mb": round(self.total_before_bytes / 1e6, 2),
            "total_after_mb": round(self.total_after_bytes / 1e6, 2),
        }


def _file_age_days(path: Path) -> float:
    return (time.time() - path.stat().st_mtime) / 86400.0


def _total_size(paths: list[Path]) -> int:
    return sum(p.stat().st_size for p in paths if p.exists())


def _safe_delete(path: Path, dry_run: bool) -> int:
    """Delete a file. Returns bytes freed. Refuses protected files."""
    if path.name in PROTECTED_PATTERNS or path.suffix in (".html", ".md"):
        return 0
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return 0
    if not dry_run:
        try:
            path.unlink()
            logger.debug("deleted %s (%.1f KB)", path, size / 1024)
        except Exception as e:
            logger.warning("delete failed: %s: %s", path, e)
            raise
    return size


def _prune_old_files(
    runs_dir: Path,
    pattern: str,
    retention_days: int,
    dry_run: bool,
    result: CleanupResult,
) -> int:
    """Delete files matching pattern older than retention_days. Returns bytes freed."""
    freed = 0
    if not runs_dir.exists():
        return 0
    for path in runs_dir.glob(pattern):
        if not path.is_file():
            continue
        try:
            age = _file_age_days(path)
        except FileNotFoundError:
            continue
        if age > retention_days:
            try:
                deleted_bytes = _safe_delete(path, dry_run)
                freed += deleted_bytes
                if deleted_bytes:
                    result.files_deleted += 1
                    result.planned_deletions.add(path)
            except Exception as e:
                result.errors.append(f"delete {path}: {e}")
    return freed


def _trim_ledger(runs_dir: Path, dry_run: bool, result: CleanupResult) -> bool:
    """Trim ledger.jsonl if it exceeds size/count thresholds. Keeps last N lines."""
    ledger = runs_dir / "paper" / "ledger.jsonl"
    if not ledger.exists():
        return False
    try:
        size = ledger.stat().st_size
    except FileNotFoundError:
        return False
    if size < LEDGER_MAX_BYTES:
        return False
    # Count lines (read in binary mode for speed)
    logger.info("ledger.jsonl is %d MB, trimming...", size // 1_048_576)
    if dry_run:
        result.ledger_trimmed = True
        return True
    try:
        with open(ledger, "rb") as f:
            # Read all lines, keep last N
            lines = f.readlines()
        keep = lines[-LEDGER_MAX_LINES:]
        with open(ledger, "wb") as f:
            f.writelines(keep)
        new_size = ledger.stat().st_size
        logger.info("trimmed ledger: %d → %d lines, %.1f MB → %.1f MB",
                    len(lines), len(keep), size / 1e6, new_size / 1e6)
        result.ledger_trimmed = True
        return True
    except Exception as e:
        result.errors.append(f"ledger trim: {e}")
        return False


def _hard_cap(runs_dir: Path, max_gb: float, dry_run: bool, result: CleanupResult) -> int:
    """If total runs/ exceeds max_gb, delete oldest shadow_ files until under cap."""
    if not runs_dir.exists():
        return 0
    total_bytes = sum(p.stat().st_size for p in runs_dir.rglob("*") if p.is_file())
    cap_bytes = max_gb * 1024 * 1024 * 1024
    if total_bytes < cap_bytes:
        return 0
    # Delete oldest shadow files first
    shadow_files = sorted(
        [p for p in runs_dir.glob("book_archive/shadow_*.jsonl.gz") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
    )
    freed = 0
    # A dry run leaves files on disk, so filesystem recomputation cannot show
    # simulated progress. Start from the post-retention projection instead.
    projected_size = total_bytes - result.bytes_freed
    for path in shadow_files:
        if path in result.planned_deletions:
            continue
        # Actual cleanup recomputes from disk each iteration. Dry-run cleanup
        # advances a projected total so it stops after the required candidates.
        current_size = projected_size if dry_run else sum(
            p.stat().st_size for p in runs_dir.rglob("*") if p.is_file()
        )
        if current_size <= cap_bytes:
            break
        try:
            deleted_bytes = _safe_delete(path, dry_run)
            freed += deleted_bytes
            if deleted_bytes:
                result.files_deleted += 1
                result.planned_deletions.add(path)
                if dry_run:
                    projected_size -= deleted_bytes
        except Exception as e:
            result.errors.append(f"hard_cap delete {path}: {e}")
    return freed


def run_cleanup(
    runs_dir: Path = DEFAULT_RUNS_DIR,
    book_retention_days: int = BOOK_RETENTION_DAYS,
    shadow_retention_days: int = SHADOW_RETENTION_DAYS,
    max_gb: float = MAX_RUNS_GB,
    dry_run: bool = False,
) -> CleanupResult:
    """Run a single cleanup pass. Returns CleanupResult with details."""
    start = time.time()
    result = CleanupResult()
    if not runs_dir.exists():
        result.errors.append(f"runs_dir does not exist: {runs_dir}")
        return result

    # Snapshot total size before
    result.total_before_bytes = sum(
        p.stat().st_size for p in runs_dir.rglob("*") if p.is_file()
    )

    book_dir = runs_dir / "book_archive"

    # 1. Prune old book_*.jsonl.gz files
    freed = _prune_old_files(
        book_dir, "book_*.jsonl.gz", book_retention_days, dry_run, result
    )
    result.bytes_freed += freed
    logger.info("book_* retention: deleted %d files, %.1f MB freed",
                result.files_deleted, freed / 1e6)

    # 2. Prune old shadow_*.jsonl.gz files
    cnt_before = result.files_deleted
    freed = _prune_old_files(
        book_dir, "shadow_*.jsonl.gz", shadow_retention_days, dry_run, result
    )
    result.bytes_freed += freed
    logger.info("shadow_* retention: deleted %d files, %.1f MB freed",
                result.files_deleted - cnt_before, freed / 1e6)

    # 3. Trim ledger if too big
    _trim_ledger(runs_dir, dry_run, result)

    # 4. Hard cap enforcement
    cnt_before = result.files_deleted
    freed = _hard_cap(runs_dir, max_gb, dry_run, result)
    result.bytes_freed += freed
    if result.files_deleted - cnt_before > 0:
        logger.info("hard_cap: deleted %d files, %.1f MB freed",
                    result.files_deleted - cnt_before, freed / 1e6)

    # Snapshot total size after (only if not dry_run, since dry-run doesn't actually delete)
    if not dry_run:
        result.total_after_bytes = sum(
            p.stat().st_size for p in runs_dir.rglob("*") if p.is_file()
        )
    else:
        result.total_after_bytes = result.total_before_bytes - result.bytes_freed

    result.duration_s = time.time() - start
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Polybot runs/ cleanup")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be deleted without deleting")
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR))
    parser.add_argument("--book-days", type=int, default=BOOK_RETENTION_DAYS)
    parser.add_argument("--shadow-days", type=int, default=SHADOW_RETENTION_DAYS)
    parser.add_argument("--max-gb", type=float, default=MAX_RUNS_GB)
    parser.add_argument("--json", action="store_true", help="Output JSON result")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("POLYMARKET_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    runs_dir = Path(args.runs_dir)
    result = run_cleanup(
        runs_dir=runs_dir,
        book_retention_days=args.book_days,
        shadow_retention_days=args.shadow_days,
        max_gb=args.max_gb,
        dry_run=args.dry_run,
    )

    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        d = result.to_dict()
        print(f"Cleanup {'(DRY RUN) ' if args.dry_run else ''}complete in {d['duration_s']}s")
        print(f"  Files deleted: {d['files_deleted']}")
        print(f"  Bytes freed:   {d['bytes_freed_mb']} MB")
        print(f"  Total before:  {d['total_before_mb']} MB")
        print(f"  Total after:   {d['total_after_mb']} MB")
        print(f"  Ledger trimmed: {result.ledger_trimmed}")
        if d["errors"]:
            print(f"  Errors: {len(d['errors'])}")
            for e in d["errors"][:5]:
                print(f"    {e}")

    return 0 if not result.errors else 1


if __name__ == "__main__":
    sys.exit(main())