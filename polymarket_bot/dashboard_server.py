"""Thin HTTP server for the copybot ops dashboard.

Background thread rebuilds the snapshot every REFRESH_SECONDS (the streaming
ledger pass takes a few seconds; requests always serve the last good cache).
Routes: GET/HEAD / and /health.  Read-only; binds per DASHBOARD_HOST.
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .dashboard_snapshot import build_snapshot
from .dashboard_ui import render_html

REFRESH_SECONDS = float(os.getenv("POLYMARKET_DASH_REFRESH_SECONDS", "30"))
ROOT = Path(__file__).resolve().parents[1]

_cache: dict = {"html": "<h1>warming up…</h1>", "snap": {}, "built_at": 0.0}
_lock = threading.Lock()


def _refresher() -> None:
    while True:
        try:
            snap = build_snapshot(ROOT)
            page = render_html(snap)
            with _lock:
                _cache.update({"html": page, "snap": snap, "built_at": time.time()})
        except Exception:
            # keep serving the last good page; errors surface via /health staleness
            pass
        time.sleep(REFRESH_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def _send(self, body: bytes, ctype: str, head_only: bool = False, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._route(head_only=False)

    def do_HEAD(self) -> None:  # noqa: N802
        self._route(head_only=True)

    def _route(self, head_only: bool) -> None:
        if self.path.rstrip("/") in ("", "/index.html"):
            with _lock:
                body = _cache["html"].encode()
            self._send(body, "text/html; charset=utf-8", head_only)
        elif self.path.rstrip("/") == "/health":
            with _lock:
                age = time.time() - _cache["built_at"] if _cache["built_at"] else None
            body = json.dumps({"ok": age is not None and age < 3 * REFRESH_SECONDS, "snapshot_age_s": age}).encode()
            self._send(body, "application/json", head_only)
        elif self.path.rstrip("/") == "/snapshot.json":
            with _lock:
                body = json.dumps(_cache["snap"]).encode()
            self._send(body, "application/json", head_only)
        else:
            self._send(b"not found", "text/plain", head_only, status=404)

    def log_message(self, format: str, *args) -> None:  # quiet  # noqa: A002
        pass


def main() -> None:
    host = os.getenv("POLYMARKET_DASH_HOST", "0.0.0.0")
    port = int(os.getenv("POLYMARKET_DASH_PORT", "8730"))
    threading.Thread(target=_refresher, daemon=True, name="snapshot-refresh").start()
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
