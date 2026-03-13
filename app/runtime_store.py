from __future__ import annotations

from pathlib import Path
from typing import Any
import json


BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_PROFILE_PATH = BASE_DIR / "output" / "local" / "sqlserver_profile.json"
_CONNECTIONS: dict[str, dict[str, Any]] = {}


def _ensure_local_dir() -> None:
    LOCAL_PROFILE_PATH.parent.mkdir(parents=True, exist_ok=True)


def save_connection(token: str, config: dict[str, Any]) -> None:
    _CONNECTIONS[token] = dict(config)


def load_saved_profile() -> dict[str, Any] | None:
    if not LOCAL_PROFILE_PATH.exists():
        return None
    try:
        payload = json.loads(LOCAL_PROFILE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def save_saved_profile(config: dict[str, Any]) -> None:
    _ensure_local_dir()
    LOCAL_PROFILE_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")


def clear_saved_profile() -> None:
    if LOCAL_PROFILE_PATH.exists():
        LOCAL_PROFILE_PATH.unlink()


def load_connection(token: str) -> dict[str, Any] | None:
    config = _CONNECTIONS.get(token)
    if isinstance(config, dict):
        return dict(config)

    saved = load_saved_profile()
    if isinstance(saved, dict):
        _CONNECTIONS[token] = dict(saved)
        return dict(saved)
    return None


def clear_connection(token: str) -> None:
    _CONNECTIONS.pop(token, None)
