from __future__ import annotations

from pathlib import Path
import json
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
HISTORY_DIR = OUTPUT_DIR / "history"


def ensure_history_dir() -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _record_path(record_id: str) -> Path:
    return HISTORY_DIR / f"{record_id}.json"


def save_record(record: dict[str, Any]) -> Path:
    ensure_history_dir()
    record_id = str(record.get("record_id") or "").strip()
    if not record_id:
        raise ValueError("record_id mancante")
    path = _record_path(record_id)
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def load_record(record_id: str) -> dict[str, Any]:
    path = _record_path(record_id)
    if not path.exists():
        raise FileNotFoundError(record_id)
    return json.loads(path.read_text(encoding="utf-8"))


def list_records(limit: int = 50) -> list[dict[str, Any]]:
    ensure_history_dir()
    rows: list[dict[str, Any]] = []
    for path in sorted(HISTORY_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows.append(record)
    return rows
