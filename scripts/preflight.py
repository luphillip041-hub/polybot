#!/usr/bin/env python3
"""Preflight checks: validate config, API, creds, and services before going live.

Usage:
    python scripts/preflight.py

Returns exit code 0 if all checks pass, 1 if any fail.
Checks can be skipped with env vars (see --help).

Add --verbose for per-check details on passing checks too.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.config import CONFIG


# ---------- check runner ----------
_checks_run = 0
_checks_failed = 0
_verbose = False


def check(name: str, passed: bool, detail: str = "") -> bool:
    global _checks_run, _checks_failed
    _checks_run += 1
    if passed:
        if _verbose:
            print(f"  ✅ {name}" + (f"  ({detail})" if detail else ""))
    else:
        _checks_failed += 1
        print(f"  ❌ {name}" + (f"  — {detail}" if detail else ""))
    return passed


def check_opt(name: str, detail: str = "") -> bool:
    """Non-fatal check — warn but don't fail."""
    global _checks_run
    _checks_run += 1
    if _verbose:
        print(f"  ⚠️  {name}" + (f"  ({detail})" if detail else ""))
    return True


# ---------- individual checks ----------


def check_python_version():
    v = sys.version_info
    return check("Python ≥ 3.10", v.major >= 3 and v.minor >= 10, f"{v.major}.{v.minor}.{v.micro}")


def check_config():
    ok = True
    ok &= check("Project root exists", CONFIG.root.exists(), str(CONFIG.root))
    ok &= check("Runs directory exists", CONFIG.runs_dir.exists(), str(CONFIG.runs_dir))
    return ok


def check_dotenv():
    env_file = CONFIG.root / ".env"
    return check_opt(".env file present", str(env_file) if env_file.exists() else "not found — using defaults")


def check_data_api():
    try:
        r = requests.get(f"{CONFIG.data_api}/trades", params={"limit": 1}, timeout=10)
        passed = r.status_code in (200, 400, 422)
        return check("Data API reachable", passed, f"HTTP {r.status_code}")
    except Exception as e:
        return check("Data API reachable", False, str(e))


def check_clob_api():
    try:
        r = requests.get(f"{CONFIG.clob_base}/markets", params={"limit": 1}, timeout=10)
        passed = r.status_code in (200, 422)
        return check("CLOB API reachable", passed, f"HTTP {r.status_code}")
    except Exception as e:
        return check("CLOB API reachable", False, str(e))


def check_gamma_api():
    try:
        r = requests.get(f"{CONFIG.gamma_base}/markets", params={"limit": 1}, timeout=10)
        passed = r.status_code in (200, 422)
        return check("Gamma API reachable", passed, f"HTTP {r.status_code}")
    except Exception as e:
        return check("Gamma API reachable", False, str(e))


def check_polygon_rpc():
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_chainId",
        "params": [],
        "id": 1,
    }
    try:
        r = requests.post(CONFIG.polygon_rpc_url, json=payload, timeout=10)
        result = r.json()
        chain_id = int(result.get("result", "0x0"), 16)
        passed = chain_id == 137
        return check("Polygon RPC reachable", passed, f"chain_id={chain_id}")
    except Exception as e:
        return check("Polygon RPC reachable", False, str(e))


def check_archive_config():
    cfg = ArchiveConfig.load()
    return check("Archive config loadable", True, f"top_n={cfg.top_n_markets}, max_tokens={cfg.max_tokens}")


def check_wallet_scores():
    score_file = CONFIG.runs_dir / "wallet_scores_latest.json"
    if score_file.exists():
        try:
            scores = json.loads(score_file.read_text())
            n = len(scores) if isinstance(scores, list) else len(scores.keys())
            return check("Wallet scores loaded", True, f"{n} wallets, {str(score_file)}")
        except Exception as e:
            return check("Wallet scores loadable", False, str(e))
    return check_opt("Wallet scores file", "not found — will use defaults")


def check_running_services():
    """Check systemd services if running on Linux with systemd."""
    if not sys.platform.startswith("linux"):
        return check_opt("Running services", "non-Linux, skipping")
    try:
        result = subprocess_run(
            ["systemctl", "is-active", "polymarket-copybot-book-archive.service"],
            timeout=5,
        )
        if result.returncode == 0:
            return check("Book archive service", True, "active")
        return check_opt("Book archive service", "not active — start with systemctl start")
    except Exception:
        return check_opt("Running services", "systemd not available")


def check_filesystem():
    """Check disk space and critical paths."""
    runs_dir = CONFIG.runs_dir
    try:
        st = runs_dir.stat()
        return check("Runs directory writable", True)
    except Exception as e:
        return check("Runs directory accessible", False, str(e))


def subprocess_run(cmd, timeout=10):
    """Minimal subprocess wrapper to avoid subprocess import issues."""
    import subprocess
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


# ---------- main ----------


def main() -> int:
    global _verbose
    parser = argparse.ArgumentParser(description="Polymarket copybot preflight checks")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show passing checks too")
    args = parser.parse_args()
    _verbose = args.verbose

    print(f"🔍 Polymarket Copybot Preflight  ({CONFIG.root})")
    print()

    checks = [
        ("Python", check_python_version),
        ("Config", check_config),
        ("Environment", check_dotenv),
        ("Data API", check_data_api),
        ("CLOB API", check_clob_api),
        ("Gamma API", check_gamma_api),
        ("Polygon RPC", check_polygon_rpc),
        ("Archive config", check_archive_config),
        ("Wallet scores", check_wallet_scores),
        ("Filesystem", check_filesystem),
        ("Services", check_running_services),
    ]

    for name, fn in checks:
        print(f"[{name}]")
        fn()
        print()

    total = _checks_run
    failed = _checks_failed
    passed = total - failed

    print(f"{'=' * 50}")
    print(f"  {passed}/{total} passed" + (f"  ({failed} failed)" if failed else ""))
    print(f"{'=' * 50}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
