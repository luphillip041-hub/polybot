"""Read the paper ledger across rotations.

``cleanup`` used to trim ledger.jsonl to the last 100k lines, which silently
destroyed history (and made every "all-time" statistic a moving target).
It now rotates the live ledger into ``runs/paper/ledger_archive/`` as
timestamped ``.jsonl.gz`` segments instead — nothing is ever deleted.

All consumers that need history (wallet quality scores, daily counters, the
forward-test scorecard) must read through here so they see archive segments
plus the live file in oldest→newest order.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator

ARCHIVE_DIRNAME = "ledger_archive"
LIVE_NAME = "ledger.jsonl"


def ledger_segment_paths(paper_dir: Path) -> list[Path]:
    """Archive segments (oldest first) followed by the live ledger."""
    archive_dir = paper_dir / ARCHIVE_DIRNAME
    paths: list[Path] = []
    if archive_dir.exists():
        paths.extend(sorted(archive_dir.glob("ledger-*.jsonl.gz")))
    live = paper_dir / LIVE_NAME
    if live.exists():
        paths.append(live)
    return paths


def iter_ledger_rows(paper_dir: Path) -> Iterator[dict[str, Any]]:
    """Yield every ledger row across all segments, oldest → newest."""
    for path in ledger_segment_paths(paper_dir):
        opener = gzip.open if path.name.endswith(".gz") else open
        try:
            with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(row, dict):
                        yield row
        except OSError:
            continue


def read_ledger_history(paper_dir: Path) -> list[dict[str, Any]]:
    return list(iter_ledger_rows(paper_dir))
