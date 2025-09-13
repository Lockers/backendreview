from __future__ import annotations

import json
import logging
import sys
from typing import Any, Mapping
from urllib.parse import urlparse
import hashlib


def setup_logging(level: int = logging.INFO) -> None:
    # Minimal, safe logger. Won't interfere with networking.
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # ensure our simple handler is used
    )


def log_json(event: str, **fields: Any) -> None:
    rec: dict[str, Any] = {"event": event, **fields}
    # basic redaction
    for key in ("authorization", "auth", "token", "api_key", "password"):
        if key in rec and isinstance(rec[key], str):
            rec[key] = "***"
    logging.getLogger("pricer").info(json.dumps(rec, ensure_ascii=False))


def _auth_fingerprint(authorization_value: str | None) -> dict[str, Any] | None:
    if not authorization_value:
        return None
    val = authorization_value.strip()
    if val.lower().startswith("basic "):
        scheme = "Basic"
        token = val[6:]
    elif val.lower().startswith("bearer "):
        scheme = "Bearer"
        token = val[7:]
    else:
        scheme = "Unknown"
        token = val
    fp = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
    return {"scheme": scheme, "token_fp": fp}


def sanitize_headers(headers: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not headers:
        return out
    for k, v in headers.items():
        if k.lower() == "authorization":
            out["authorization"] = _auth_fingerprint(str(v))
        else:
            out[k] = v
    return out


def summarize_proxy(proxy_url: str | None) -> dict[str, Any] | None:
    if not proxy_url:
        return None
    p = urlparse(proxy_url)
    return {
        "scheme": p.scheme or "",
        "host": p.hostname or "",
        "port": p.port or None,
        "username": (p.username[:3] + "***") if p.username else None,
        "has_password": bool(p.password),
    }

