from __future__ import annotations

import json
import re
from typing import Any


TOKEN_RE = re.compile(r"[^a-z0-9]+")

NOVICROM_PORTAL_PROFILE = {
    "code": "novicrom",
    "label": "Portale Novicrom",
    "mode": "builtin_preset",
    "public_release_strategy": "keep_profile_explicit",
}

GENERIC_PORTAL_PROFILE = {
    "code": "generic",
    "label": "Generic Portal",
    "mode": "fallback",
    "public_release_strategy": "require_manual_runtime_review",
}


def _field(
    *,
    name: str,
    label: str,
    data_type: str,
    description: str,
    db_column: str | None = None,
    is_virtual: bool = False,
    aliases: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "data_type": data_type,
        "description": description,
        "db_column": None if is_virtual else (db_column or name),
        "is_virtual": is_virtual,
        "aliases": list(aliases or []),
    }


RUNTIME_SOURCE_CATALOGS: dict[str, dict[str, Any]] = {
    "assenze": {
        "source_code": "assenze",
        "label": "Assenze",
        "description": (
            "Richieste assenza gestite dal modulo Novicrom con approval nativa "
            "basata su `consenso` e `moderation_status`."
        ),
        "portal_profile": NOVICROM_PORTAL_PROFILE,
        "workflow_capabilities": {
            "approval": {
                "mode": "native_status_workflow",
                "managed_by_portal": True,
                "status_field": "moderation_status",
                "pending_value": 2,
                "approved_value": 0,
                "rejected_value": 1,
                "skip_field": "salta_approvazione",
                "approver_field": "capo_reparto_id",
                "approver_email_field": "capo_email",
                "approver_email_template": "{capo_email}",
                "owner_field": "dipendente_id",
                "owner_email_field": "dipendente_email",
                "default_subject_template": "[Assenze] richiesta #{id} da approvare",
                "default_message_template": (
                    "Richiesta di {tipo_assenza} per il periodo {data_inizio} - {data_fine}. "
                    "Dipendente: {dipendente_id}."
                ),
                "notes": [
                    "Il portale Novicrom aggiorna `moderation_status` tramite API CAR/AMMINISTRAZIONE.",
                    "Le automazioni vanno agganciate agli eventi insert/update della queue SQL Server.",
                ],
            }
        },
        "fields": [
            _field(name="id", label="ID", data_type="int", description="Chiave primaria record assenza."),
            _field(
                name="dipendente_id",
                label="Dipendente / Utente",
                data_type="int",
                description="FK interna al dipendente; equivalente conservativo di `utente_id`.",
                aliases=["utente_id", "employee_id"],
            ),
            _field(
                name="data_inizio",
                label="Data inizio",
                data_type="datetime",
                description="Data e ora di inizio della richiesta assenza.",
                aliases=["Data_x0020_inizio", "DATAINIZIO", "data", "inizio", "datainizio"],
            ),
            _field(
                name="data_fine",
                label="Data fine",
                data_type="datetime",
                description="Data e ora di fine della richiesta assenza.",
                aliases=["Datafine", "fine", "datafine"],
            ),
            _field(
                name="tipo_assenza",
                label="Tipo assenza",
                data_type="string",
                description="Categoria assenza: ferie, permesso, malattia, ecc.",
                aliases=["Tipoassenza", "tipo", "category", "tipoassenza"],
            ),
            _field(
                name="motivazione_richiesta",
                label="Motivazione",
                data_type="string",
                description="Motivazione libera inserita dall'utente.",
                aliases=["Motivazionerichiesta", "motivazione", "motivazionerichiesta"],
            ),
            _field(
                name="moderation_status",
                label="Stato approvazione",
                data_type="int",
                description="Stato tecnico del workflow approvativo nel portale.",
                aliases=["ModerationStatus", "{ModerationStatus}", "status_approvazione", "moderationstatus"],
            ),
            _field(
                name="capo_reparto_id",
                label="Capo reparto",
                data_type="int",
                description="Responsabile approvatore associato alla richiesta.",
                aliases=["capo", "approvatore_id", "capo_id", "caporeparto"],
            ),
            _field(
                name="capo_email",
                label="Caporeparto email",
                data_type="string",
                description="Email ereditata dal caporeparto selezionato nella richiesta.",
                is_virtual=True,
                aliases=["CAR", "capo_reparto_email", "responsabile_email", "capoemail"],
            ),
            _field(
                name="dipendente_email",
                label="Dipendente email",
                data_type="string",
                description="Email del dipendente collegata alla richiesta assenza.",
                db_column="email_esterna",
                aliases=[
                    "Email",
                    "email",
                    "email_esterna",
                    "richiedente_email",
                    "EmailDipendente",
                    "emaildipendente",
                    "dipendenteemail",
                    "maildipendente",
                    "emaildeldipendente",
                    "dipendenteemailaziendale",
                ],
            ),
            _field(
                name="salta_approvazione",
                label="Salta approvazione",
                data_type="bool",
                description="Flag che bypassa il normale flusso approvativo.",
                aliases=["Salta_x0020_approvazione", "SALTAAPPROVAZIONE", "skip_approval", "saltax0020approvazione"],
            ),
        ],
    },
    "tasks": {
        "source_code": "tasks",
        "label": "Tasks",
        "description": "Task ORM Django del portale. I nomi reali colonna sono in inglese.",
        "portal_profile": NOVICROM_PORTAL_PROFILE,
        "workflow_capabilities": {},
        "fields": [
            _field(name="id", label="ID", data_type="int", description="Chiave primaria del task.", aliases=["task_id"]),
            _field(
                name="title",
                label="Titolo",
                data_type="string",
                description="Titolo sintetico del task.",
                aliases=["titolo", "task_title", "subject"],
            ),
            _field(
                name="status",
                label="Stato",
                data_type="string",
                description="Stato operativo del task.",
                aliases=["stato", "task_status"],
            ),
            _field(
                name="priority",
                label="Priorita'",
                data_type="string",
                description="Priorita' del task.",
                aliases=["priorita", "task_priority"],
            ),
            _field(
                name="assigned_to_id",
                label="Assegnato a",
                data_type="int",
                description="Utente Django assegnatario del task.",
                aliases=["assigned_to", "assegnato_a", "owner_id"],
            ),
            _field(
                name="project_id",
                label="Progetto",
                data_type="int",
                description="Progetto collegato al task.",
                aliases=["project", "progetto"],
            ),
            _field(
                name="due_date",
                label="Scadenza",
                data_type="date",
                description="Data scadenza del task.",
                aliases=["deadline", "data_scadenza"],
            ),
        ],
    },
    "assets": {
        "source_code": "assets",
        "label": "Assets",
        "description": "Asset ORM Django del portale con mapping conservativo verso sede e assegnatario.",
        "portal_profile": NOVICROM_PORTAL_PROFILE,
        "workflow_capabilities": {},
        "fields": [
            _field(name="id", label="ID", data_type="int", description="Chiave primaria asset.", aliases=["asset_id"]),
            _field(
                name="asset_tag",
                label="Codice asset",
                data_type="string",
                description="Codice univoco asset visibile in inventario.",
                aliases=["codice", "code", "tag", "asset_code", "codice_asset"],
            ),
            _field(name="name", label="Nome", data_type="string", description="Nome asset.", aliases=["nome", "nome_asset"]),
            _field(
                name="asset_category_id",
                label="Categoria",
                data_type="int",
                description="Categoria asset configurata nel portale.",
                aliases=["category", "categoria"],
            ),
            _field(
                name="status",
                label="Stato",
                data_type="string",
                description="Stato ciclo di vita asset.",
                aliases=["stato", "asset_status"],
            ),
            _field(
                name="assignment_location",
                label="Sede / Posizione",
                data_type="string",
                description="Posizione o sede operativa dell'asset.",
                aliases=["sede", "ubicazione", "posizione", "location"],
            ),
            _field(
                name="assigned_legacy_user_id",
                label="Assegnato a",
                data_type="int",
                description="Legacy user assegnatario dell'asset.",
                aliases=["assegnatario_id", "utente_id", "assigned_to"],
            ),
        ],
    },
    "tickets": {
        "source_code": "tickets",
        "label": "Tickets",
        "description": "Ticket ORM Django per IT e manutenzione.",
        "portal_profile": NOVICROM_PORTAL_PROFILE,
        "workflow_capabilities": {},
        "fields": [
            _field(name="id", label="ID", data_type="int", description="Chiave primaria ticket.", aliases=["ticket_id"]),
            _field(
                name="titolo",
                label="Titolo",
                data_type="string",
                description="Titolo del ticket.",
                aliases=["title", "subject"],
            ),
            _field(
                name="stato",
                label="Stato",
                data_type="string",
                description="Stato workflow del ticket.",
                aliases=["status", "ticket_status"],
            ),
            _field(
                name="priorita",
                label="Priorita'",
                data_type="string",
                description="Priorita' del ticket.",
                aliases=["priority", "ticket_priority"],
            ),
            _field(
                name="richiedente_legacy_user_id",
                label="Richiedente",
                data_type="int",
                description="Legacy user che ha aperto il ticket.",
                aliases=["richiedente_id", "requester_id", "utente_id"],
            ),
            _field(
                name="assegnato_a",
                label="Assegnato a",
                data_type="string",
                description="Nome libero dell'assegnatario corrente.",
                aliases=["assigned_to", "assegnatario", "owner"],
            ),
            _field(
                name="categoria",
                label="Categoria",
                data_type="string",
                description="Categoria ticket.",
                aliases=["category"],
            ),
        ],
    },
    "anomalie": {
        "source_code": "anomalie",
        "label": "Anomalie",
        "description": "Anomalie legacy SQL Server con mapping conservativo sui campi OP/PN.",
        "portal_profile": NOVICROM_PORTAL_PROFILE,
        "workflow_capabilities": {},
        "fields": [
            _field(name="id", label="ID", data_type="int", description="Chiave primaria anomalia.", aliases=["anomalia_id"]),
            _field(
                name="ex_op_nominativo",
                label="OP",
                data_type="string",
                description="Ordine di produzione in formato testuale.",
                aliases=["op", "ordine_produzione", "op_id", "exopnominativo"],
            ),
            _field(
                name="op_lookup_id",
                label="OP lookup",
                data_type="int",
                description="Lookup tecnico verso ordini di produzione.",
                aliases=["op_lookup", "op_lookupid", "OP_x002d_IDLookupId"],
            ),
            _field(
                name="seriale",
                label="PN / Seriale",
                data_type="string",
                description="Riferimento tecnico disponibile a schema, usato come mapping conservativo del PN.",
                aliases=["pn", "sn", "part_number"],
            ),
            _field(
                name="avanzamento",
                label="Stato",
                data_type="string",
                description="Stato avanzamento dell'anomalia.",
                aliases=["status", "stato"],
            ),
            _field(
                name="chiudere",
                label="Da chiudere",
                data_type="bool",
                description="Flag booleano di chiusura anomalia.",
                aliases=["chiusa", "close", "da_chiudere"],
            ),
            _field(
                name="created_by",
                label="Responsabile / Autore",
                data_type="int",
                description="Utente autore disponibile nello schema corrente.",
                db_column="created_by_user_id",
                aliases=["created_by_user_id", "autore", "responsabile", "legacy_user_id"],
            ),
            _field(
                name="ordine_id",
                label="Ordine interno",
                data_type="int",
                description="Collegamento interno aggiuntivo presente nel database attivo.",
                aliases=["ordine_interno", "order_id"],
            ),
        ],
    },
    "generic": {
        "source_code": "generic",
        "label": "Generic",
        "description": "Profilo generico senza capabilities native del portale.",
        "portal_profile": GENERIC_PORTAL_PROFILE,
        "workflow_capabilities": {},
        "fields": [],
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


def get_portal_profile(source_code: str) -> dict[str, Any]:
    catalog = get_runtime_source_catalog(source_code)
    return _copy(catalog.get("portal_profile") or GENERIC_PORTAL_PROFILE)


def get_workflow_capabilities(source_code: str) -> dict[str, Any]:
    catalog = get_runtime_source_catalog(source_code)
    return _copy(catalog.get("workflow_capabilities") or {})


def has_native_approval_workflow(source_code: str) -> bool:
    approval = get_workflow_capabilities(source_code).get("approval", {})
    return bool(approval and approval.get("managed_by_portal"))


def runtime_field_alias_map(source_code: str) -> dict[str, str]:
    catalog = get_runtime_source_catalog(source_code)
    alias_map: dict[str, str] = {}
    for field in catalog.get("fields", []):
        field_name = str(field.get("name") or "").strip()
        if not field_name:
            continue
        aliases = [field_name] + [str(alias or "").strip() for alias in field.get("aliases", [])]
        for alias in aliases:
            normalized = normalize_token(alias)
            if normalized:
                alias_map[normalized] = field_name
    return alias_map


def suggest_runtime_field_mapping(fields_used: list[str], *, source_code: str) -> dict[str, dict[str, Any]]:
    alias_map = runtime_field_alias_map(source_code)
    if not alias_map:
        return {}

    portal_profile = get_portal_profile(source_code)
    mapped: dict[str, dict[str, Any]] = {}
    for source_field in fields_used:
        normalized = normalize_token(source_field)
        target_field = alias_map.get(normalized)
        if not target_field:
            continue

        confidence = "high"
        if target_field in {"dipendente_email", "capo_email", "assegnato_a"} and normalized != normalize_token(target_field):
            confidence = "medium"

        mapped[source_field] = {
            "target_field": target_field,
            "mapping_scope": "runtime_source",
            "confidence": confidence,
            "note": "alias runtime allineato al source registry del portale",
            "reason": "alias runtime allineato al source registry del portale",
            "source": f"{portal_profile['code']}_source_registry" if portal_profile.get("code") != "generic" else "runtime_catalog",
        }
    return mapped
