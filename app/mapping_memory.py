from __future__ import annotations

from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
import json
import re
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
MEMORY_PATH = BASE_DIR / "output" / "learning" / "mapping_memory.json"
TOKEN_RE = re.compile(r"[^a-z0-9]+")

SEED_ALIASES = {
    "datax0020inizio": "data_inizio",
    "datafine": "data_fine",
    "tipoassenza": "tipo_assenza",
    "motivazionerichiesta": "motivazione_richiesta",
    "saltax0020approvazione": "salta_approvazione",
    "moderationstatus": "moderation_status",
    "car": "capo_email",
    "email": "dipendente_email",
    "mail": "dipendente_email",
    "emaildipendente": "dipendente_email",
    "dipendenteemail": "dipendente_email",
    "maildipendente": "dipendente_email",
}


def _ensure_memory_dir() -> None:
    MEMORY_PATH.parent.mkdir(parents=True, exist_ok=True)


def _normalize(value: str) -> str:
    cleaned = value.strip().lower()
    cleaned = cleaned.replace("{", "").replace("}", "")
    return TOKEN_RE.sub("", cleaned)


def _load_memory() -> dict[str, Any]:
    if not MEMORY_PATH.exists():
        return {"version": 1, "table_mappings": {}, "global_mappings": {}, "updated_at": ""}
    try:
        return json.loads(MEMORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "table_mappings": {}, "global_mappings": {}, "updated_at": ""}


def _save_memory(payload: dict[str, Any]) -> None:
    _ensure_memory_dir()
    payload["updated_at"] = datetime.now(UTC).isoformat()
    MEMORY_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _score_column(source_norm: str, column_name: str, *, learned_count: int = 0) -> tuple[float, str]:
    column_norm = _normalize(column_name)
    if not column_norm:
        return 0.0, "colonna vuota"

    if learned_count > 0:
        learned_score = min(0.99, 0.9 + min(learned_count, 5) * 0.015)
        return learned_score, f"gia' confermato {learned_count} volte"

    seeded = SEED_ALIASES.get(source_norm)
    if seeded and _normalize(seeded) == column_norm:
        return 0.93, "alias noto del dominio"

    if source_norm == column_norm:
        return 0.91, "match esatto normalizzato"

    if source_norm in column_norm or column_norm in source_norm:
        return 0.78, "match parziale sul nome"

    similarity = SequenceMatcher(None, source_norm, column_norm).ratio()
    if similarity >= 0.75:
        return 0.68 + (similarity - 0.75) * 0.4, "similarita' lessicale"

    return similarity * 0.5, "similarita' debole"


def suggest_mappings(
    source_fields: list[str],
    table_columns: list[dict[str, Any]],
    *,
    table_key: str,
) -> dict[str, dict[str, Any]]:
    memory = _load_memory()
    table_memory = memory.get("table_mappings", {}).get(table_key, {})
    global_memory = memory.get("global_mappings", {})
    column_names = [str(col.get("name") or "") for col in table_columns if str(col.get("name") or "").strip()]

    suggestions: dict[str, dict[str, Any]] = {}
    for source_field in source_fields:
        source_norm = _normalize(source_field)
        ranked: list[tuple[float, str, str]] = []
        for column_name in column_names:
            learned_count = int(
                table_memory.get(source_norm, {}).get(column_name, 0)
                or global_memory.get(source_norm, {}).get(column_name, 0)
                or 0
            )
            score, reason = _score_column(source_norm, column_name, learned_count=learned_count)
            if score <= 0:
                continue
            ranked.append((score, column_name, reason))

        ranked.sort(key=lambda item: item[0], reverse=True)
        if ranked:
            top_score, top_column, top_reason = ranked[0]
            confidence = "high" if top_score >= 0.9 else "medium" if top_score >= 0.7 else "low"
            suggestions[source_field] = {
                "target_field": top_column if top_score >= 0.35 else "",
                "confidence": confidence,
                "score": round(top_score, 4),
                "note": top_reason,
                "reason": top_reason,
                "source": "mapping_memory",
                "alternatives": [
                    {
                        "target_field": column_name,
                        "score": round(score, 4),
                        "note": reason,
                        "reason": reason,
                        "source": "mapping_memory",
                    }
                    for score, column_name, reason in ranked[:3]
                ],
            }
        else:
            suggestions[source_field] = {
                "target_field": "",
                "confidence": "low",
                "score": 0.0,
                "note": "nessun candidato attendibile",
                "reason": "nessun candidato attendibile",
                "source": "mapping_memory",
                "alternatives": [],
            }
    return suggestions


def learn_from_approved_mappings(
    approved_mapping: dict[str, str],
    *,
    table_key: str,
) -> None:
    memory = _load_memory()
    table_mappings = memory.setdefault("table_mappings", {})
    global_mappings = memory.setdefault("global_mappings", {})
    table_bucket = table_mappings.setdefault(table_key, {})

    for source_field, target_field in approved_mapping.items():
        source_norm = _normalize(source_field)
        if not source_norm or not target_field:
            continue
        table_source_bucket = table_bucket.setdefault(source_norm, {})
        table_source_bucket[target_field] = int(table_source_bucket.get(target_field, 0)) + 1

        global_source_bucket = global_mappings.setdefault(source_norm, {})
        global_source_bucket[target_field] = int(global_source_bucket.get(target_field, 0)) + 1

    _save_memory(memory)
