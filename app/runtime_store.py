from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any
import json


BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_PROFILE_PATH = BASE_DIR / "output" / "local" / "sqlserver_profile.ini"
LEGACY_LOCAL_PROFILE_PATH = BASE_DIR / "output" / "local" / "sqlserver_profile.json"
_CONNECTIONS: dict[str, dict[str, Any]] = {}
PROFILE_SECTION = "sqlserver"
BOOL_FIELDS = {
    "encrypt",
    "trust_server_certificate",
    "integrated_security",
}


def _ensure_local_dir() -> None:
    LOCAL_PROFILE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in config.items():
        if key in BOOL_FIELDS:
            normalized[key] = bool(value)
        else:
            normalized[key] = "" if value is None else str(value)
    return normalized


def _load_ini_profile() -> dict[str, Any] | None:
    if not LOCAL_PROFILE_PATH.exists():
        return None

    parser = ConfigParser()
    try:
        parser.read(LOCAL_PROFILE_PATH, encoding="utf-8")
    except Exception:
        return None

    if not parser.has_section(PROFILE_SECTION):
        return None

    payload: dict[str, Any] = {}
    for key, value in parser.items(PROFILE_SECTION):
        if key in BOOL_FIELDS:
            payload[key] = parser.getboolean(PROFILE_SECTION, key, fallback=False)
        else:
            payload[key] = value
    return payload


def _load_legacy_json_profile() -> dict[str, Any] | None:
    if not LEGACY_LOCAL_PROFILE_PATH.exists():
        return None
    try:
        payload = json.loads(LEGACY_LOCAL_PROFILE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def save_connection(token: str, config: dict[str, Any]) -> None:
    _CONNECTIONS[token] = dict(config)


def load_saved_profile() -> dict[str, Any] | None:
    payload = _load_ini_profile()
    if isinstance(payload, dict):
        return payload

    legacy_payload = _load_legacy_json_profile()
    if not isinstance(legacy_payload, dict):
        return None

    # Migra il vecchio profilo JSON al nuovo formato INI locale.
    save_saved_profile(legacy_payload)
    return _normalize_config(legacy_payload)


def save_saved_profile(config: dict[str, Any]) -> None:
    _ensure_local_dir()
    parser = ConfigParser()
    parser[PROFILE_SECTION] = {
        key: str(value).lower() if key in BOOL_FIELDS else str(value)
        for key, value in _normalize_config(config).items()
    }
    with LOCAL_PROFILE_PATH.open("w", encoding="utf-8") as handle:
        parser.write(handle)

    if LEGACY_LOCAL_PROFILE_PATH.exists():
        LEGACY_LOCAL_PROFILE_PATH.unlink()


def clear_saved_profile() -> None:
    if LOCAL_PROFILE_PATH.exists():
        LOCAL_PROFILE_PATH.unlink()
    if LEGACY_LOCAL_PROFILE_PATH.exists():
        LEGACY_LOCAL_PROFILE_PATH.unlink()


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
