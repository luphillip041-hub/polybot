from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any


class ApiError(RuntimeError):
    pass


def get_json(base: str, path: str, params: dict[str, Any] | None = None, *, user_agent: str = "Hermes", timeout: int = 20) -> Any:
    query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
    url = base.rstrip("/") + path
    if query:
        url += "?" + query
    req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            return json.loads(raw)
    except Exception as exc:
        raise ApiError(f"GET {url} failed: {exc}") from exc
