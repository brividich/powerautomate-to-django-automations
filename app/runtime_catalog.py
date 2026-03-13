from __future__ import annotations

import json
import re
from typing import Any


TOKEN_RE = re.compile(r"[^a-z0-9]+")

RUNTIME_SOURCE_CATALOGS: dict[str, dict[str, Any]] = {
    "assenze": {
        "source_code": "assenze",
        "label": "Assenze",
        "fields": [
            {"name": "id", "label": "ID", "data_type": "int"},
            {"name": "dipendente_id", "label": "Dipendente", "data_type": "int"},
            {"name": "data_inizio", "label": "Data inizio", "data_type": "datetime"},
            {"name": "data_fine", "label": "Data fine", "data_type": "datetime"},
            {"name": "tipo_assenza", "label": "Tipo assenza", "data_type": "string"},
            {"name": "motivazione_richiesta", "label": "Motivazione", "data_type": "string"},
            {"name": "moderation_status", "label": "Stato approvazione", "data_type": "int"},
            {"name": "capo_reparto_id", "label": "Capo reparto", "data_type": "int"},
            {"name": "capo_email", "label": "Capo email", "data_type": "string"},
            {"name": "dipendente_email", "label": "Dipendente email", "data_type": "string"},
            {"name": "salta_approvazione", "label": "Salta approvazione", "data_type": "bool"},
        ],
    },
    "tasks": {
        "source_code": "tasks",
        "label": "Tasks",
        "fields": [
            {"name": "id", "label": "ID", "data_type": "int"},
            {"name": "titolo", "label": "Titolo", "data_type": "string"},
            {"name": "descrizione", "label": "Descrizione", "data_type": "string"},
            {"name": "assegnatario_id", "label": "Assegnatario", "data_type": "int"},
            {"name": "assegnatario_email", "label": "Email assegnatario", "data_type": "string"},
            {"name": "priorita", "label": "Priorita", "data_type": "string"},
            {"name": "scadenza", "label": "Scadenza", "data_type": "datetime"},
            {"name": "stato", "label": "Stato", "data_type": "string"},
            {"name": "commessa_id", "label": "Commessa", "data_type": "int"},
            {"name": "note", "label": "Note", "data_type": "string"},
        ],
    },
    "assets": {
        "source_code": "assets",
        "label": "Assets",
        "fields": [
            {"name": "id", "label": "ID", "data_type": "int"},
            {"name": "codice_asset", "label": "Codice asset", "data_type": "string"},
            {"name": "nome", "label": "Nome", "data_type": "string"},
            {"name": "categoria", "label": "Categoria", "data_type": "string"},
            {"name": "serial_number", "label": "Serial number", "data_type": "string"},
            {"name": "assegnatario_id", "label": "Assegnatario", "data_type": "int"},
            {"name": "assegnatario_email", "label": "Email assegnatario", "data_type": "string"},
            {"name": "sede_id", "label": "Sede", "data_type": "int"},
            {"name": "stato", "label": "Stato", "data_type": "string"},
            {"name": "data_acquisto", "label": "Data acquisto", "data_type": "date"},
        ],
    },
    "tickets": {
        "source_code": "tickets",
        "label": "Tickets",
        "fields": [
            {"name": "id", "label": "ID", "data_type": "int"},
            {"name": "titolo", "label": "Titolo", "data_type": "string"},
            {"name": "descrizione", "label": "Descrizione", "data_type": "string"},
            {"name": "richiedente_id", "label": "Richiedente", "data_type": "int"},
            {"name": "richiedente_email", "label": "Email richiedente", "data_type": "string"},
            {"name": "assegnatario_id", "label": "Assegnatario", "data_type": "int"},
            {"name": "assegnatario_email", "label": "Email assegnatario", "data_type": "string"},
            {"name": "priorita", "label": "Priorita", "data_type": "string"},
            {"name": "stato", "label": "Stato", "data_type": "string"},
            {"name": "categoria", "label": "Categoria", "data_type": "string"},
        ],
    },
    "anomalie": {
        "source_code": "anomalie",
        "label": "Anomalie",
        "fields": [
            {"name": "id", "label": "ID", "data_type": "int"},
            {"name": "titolo", "label": "Titolo", "data_type": "string"},
            {"name": "descrizione", "label": "Descrizione", "data_type": "string"},
            {"name": "asset_id", "label": "Asset", "data_type": "int"},
            {"name": "reporter_id", "label": "Reporter", "data_type": "int"},
            {"name": "reporter_email", "label": "Email reporter", "data_type": "string"},
            {"name": "severity", "label": "Severity", "data_type": "string"},
            {"name": "stato", "label": "Stato", "data_type": "string"},
            {"name": "data_apertura", "label": "Data apertura", "data_type": "datetime"},
            {"name": "data_chiusura", "label": "Data chiusura", "data_type": "datetime"},
        ],
    },
    "generic": {
        "source_code": "generic",
        "label": "Generic",
        "fields": [],
    },
}

RUNTIME_FIELD_ALIASES: dict[str, dict[str, str]] = {
    "assenze": {
        "datax0020inizio": "data_inizio",
        "datainizio": "data_inizio",
        "datafine": "data_fine",
        "tipoassenza": "tipo_assenza",
        "motivazionerichiesta": "motivazione_richiesta",
        "saltax0020approvazione": "salta_approvazione",
        "moderationstatus": "moderation_status",
        "car": "capo_email",
        "caporeparto": "capo_reparto_id",
        "email": "dipendente_email",
        "mail": "dipendente_email",
        "emaildipendente": "dipendente_email",
        "dipendenteemail": "dipendente_email",
        "maildipendente": "dipendente_email",
        "emaildeldipendente": "dipendente_email",
        "dipendenteemailaziendale": "dipendente_email",
    },
    "tasks": {
        "title": "titolo",
        "tasktitle": "titolo",
        "description": "descrizione",
        "assignedto": "assegnatario_email",
        "assignee": "assegnatario_email",
        "due": "scadenza",
        "duedate": "scadenza",
        "status": "stato",
        "priority": "priorita",
    },
    "assets": {
        "assetcode": "codice_asset",
        "assetname": "nome",
        "category": "categoria",
        "serialnumber": "serial_number",
        "assignedto": "assegnatario_email",
        "status": "stato",
    },
    "tickets": {
        "title": "titolo",
        "subject": "titolo",
        "description": "descrizione",
        "requester": "richiedente_email",
        "assignedto": "assegnatario_email",
        "priority": "priorita",
        "status": "stato",
        "category": "categoria",
    },
    "anomalie": {
        "title": "titolo",
        "description": "descrizione",
        "asset": "asset_id",
        "reporter": "reporter_email",
        "severity": "severity",
        "status": "stato",
        "openedat": "data_apertura",
        "closedat": "data_chiusura",
    },
}


def _copy(value: Any) -> Any:
    return json.loads(json.dumps(value))


def normalize_token(value: str) -> str:
    cleaned = value.strip().lower()
    cleaned = cleaned.replace("{", "").replace("}", "")
    return TOKEN_RE.sub("", cleaned)


def get_runtime_source_catalog(source_code: str) -> dict[str, Any]:
    catalog = RUNTIME_SOURCE_CATALOGS.get(source_code) or RUNTIME_SOURCE_CATALOGS["generic"]
    return _copy(catalog)


def supported_runtime_sources() -> list[dict[str, Any]]:
    return [_copy(row) for row in RUNTIME_SOURCE_CATALOGS.values() if row["source_code"] != "generic"]


def runtime_field_names(source_code: str) -> list[str]:
    catalog = get_runtime_source_catalog(source_code)
    return [str(field.get("name") or "") for field in catalog.get("fields", []) if str(field.get("name") or "").strip()]


def suggest_runtime_field_mapping(fields_used: list[str], *, source_code: str) -> dict[str, dict[str, Any]]:
    aliases = RUNTIME_FIELD_ALIASES.get(source_code, {})
    if not aliases:
        return {}

    mapped: dict[str, dict[str, Any]] = {}
    for source_field in fields_used:
        normalized = normalize_token(source_field)
        target_field = aliases.get(normalized)
        if not target_field:
            continue
        mapped[source_field] = {
            "target_field": target_field,
            "mapping_scope": "runtime_source",
            "confidence": "high" if target_field != "dipendente_email" and normalized != "car" else "medium",
            "note": "alias runtime supportato dal portale",
            "reason": "alias runtime supportato dal portale",
            "source": "runtime_catalog",
        }
    return mapped
