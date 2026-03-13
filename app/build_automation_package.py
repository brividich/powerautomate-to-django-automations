from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any

from extract_logic import extract_actions_and_fields, extract_trigger_summary
from package_mapping import normalize_package_mappings
from runtime_catalog import suggest_runtime_field_mapping


CONNECTION_KEY_RE = re.compile(r"\['([^']+)'\]")


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _iter_actions(actions: dict[str, Any], *, parent: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action_name, action_def in actions.items():
        if not isinstance(action_def, dict):
            continue
        rows.append(
            {
                "name": action_name,
                "type": str(action_def.get("type", "")),
                "kind": str(action_def.get("kind", "")),
                "parent": parent,
                "definition": action_def,
            }
        )

        nested = action_def.get("actions")
        if isinstance(nested, dict):
            rows.extend(_iter_actions(nested, parent=action_name))

        else_branch = action_def.get("else")
        if isinstance(else_branch, dict) and isinstance(else_branch.get("actions"), dict):
            rows.extend(_iter_actions(else_branch["actions"], parent=action_name))

        for case_def in (action_def.get("cases") or {}).values():
            if isinstance(case_def, dict) and isinstance(case_def.get("actions"), dict):
                rows.extend(_iter_actions(case_def["actions"], parent=action_name))

        default_branch = action_def.get("default")
        if isinstance(default_branch, dict) and isinstance(default_branch.get("actions"), dict):
            rows.extend(_iter_actions(default_branch["actions"], parent=action_name))
    return rows


def _collect_connectors(raw_flow: dict[str, Any], actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    connection_refs = (
        _safe_dict(_safe_dict(raw_flow.get("properties")).get("connectionReferences"))
        if isinstance(raw_flow.get("properties"), dict)
        else {}
    )
    usage = Counter()

    for row in actions:
        definition = _safe_dict(row.get("definition"))
        connection_name = (
            _safe_dict(_safe_dict(_safe_dict(definition.get("inputs")).get("host")).get("connection"))
            .get("name")
        )
        if not isinstance(connection_name, str):
            continue
        match = CONNECTION_KEY_RE.search(connection_name)
        if not match:
            continue
        usage[match.group(1)] += 1

    connectors: list[dict[str, Any]] = []
    for key, ref in connection_refs.items():
        ref_dict = _safe_dict(ref)
        if not ref_dict:
            continue
        connectors.append(
            {
                "key": key,
                "api_name": str(ref_dict.get("apiName", "")),
                "display_name": str(ref_dict.get("apiName", "")),
                "usage_count": int(usage.get(key, 0)),
            }
        )
    return sorted(connectors, key=lambda item: (item["api_name"], item["key"]))


def _guess_source(flow_name: str, fields_used: list[str]) -> dict[str, str]:
    lowered_name = flow_name.lower()
    field_set = set(fields_used)

    if "assenze" in lowered_name or {"Data_x0020_inizio", "Datafine", "Tipoassenza"} & field_set:
        return {
            "source_code": "assenze",
            "confidence": "high",
            "reason": "Il nome del flow e i campi principali coincidono con la sorgente assenze del portale.",
        }

    source_patterns = {
        "tasks": ("task", "attivita"),
        "assets": ("asset", "cespite"),
        "tickets": ("ticket", "helpdesk"),
        "anomalie": ("anomalia", "anomalie"),
    }
    for source_code, patterns in source_patterns.items():
        if any(pattern in lowered_name for pattern in patterns):
            return {
                "source_code": source_code,
                "confidence": "medium",
                "reason": f"Il nome del flow contiene indicatori compatibili con la sorgente runtime '{source_code}'.",
            }

    return {
        "source_code": "generic",
        "confidence": "low",
        "reason": "Nessuna sorgente del portale e' riconoscibile con sufficiente affidabilita'.",
    }


def _approved_runtime_field_mapping(
    runtime_candidates: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    approved: dict[str, dict[str, Any]] = {}
    for source_field, candidate in runtime_candidates.items():
        if not isinstance(candidate, dict):
            continue
        target_field = str(candidate.get("target_field") or "").strip()
        if not target_field:
            continue
        approved[source_field] = {
            "target_field": target_field,
            "confidence": str(candidate.get("confidence") or "medium"),
            "status": "auto",
            "mapping_scope": "runtime_source",
            "note": str(candidate.get("note") or candidate.get("reason") or "approvato automaticamente"),
            "source": str(candidate.get("source") or "runtime_catalog"),
        }
    return approved


def _build_issue(
    *,
    code: str,
    severity: str,
    category: str,
    title: str,
    detail: str,
    remediation: str,
) -> dict[str, str]:
    return {
        "code": code,
        "severity": severity,
        "category": category,
        "title": title,
        "detail": detail,
        "remediation": remediation,
    }


def _detect_issues(
    *,
    actions: list[dict[str, Any]],
    connectors: list[dict[str, Any]],
    fields_used: list[str],
    source_code: str,
) -> list[dict[str, str]]:
    action_names = {row["name"] for row in actions}
    action_types = {row["type"] for row in actions}
    connector_names = {row["api_name"] for row in connectors if row["usage_count"] > 0}
    issues: list[dict[str, str]] = []

    if "approvals" in connector_names or "ApiConnectionWebhook" in action_types:
        issues.append(
            _build_issue(
                code="unsupported-approval-runtime",
                severity="high",
                category="runtime",
                title="Approval asincrona non importabile nel motore attuale",
                detail=(
                    "Il flow usa azioni di approval con attesa asincrona (`CreateAnApproval` / `WaitForAnApproval`). "
                    "Il runtime di Brizio-CRM non supporta webhook o attese di approvazione."
                ),
                remediation=(
                    "Sostituire il ramo approval con regole che reagiscono agli update di `moderation_status`, "
                    "lasciando il processo approvativo al modulo assenze del portale."
                ),
            )
        )

    if "Until" in action_types:
        issues.append(
            _build_issue(
                code="unsupported-loop-until",
                severity="high",
                category="logic",
                title="Loop `Until` non convertibile automaticamente",
                detail=(
                    "Il flow crea elementi successivi in uno o piu' loop `Until`, tipicamente per spezzare assenze "
                    "su piu' giorni o creare record derivati."
                ),
                remediation=(
                    "Portare questa logica nel modulo assenze o aggiungere al runtime una action custom dedicata "
                    "alla generazione dei record giornalieri."
                ),
            )
        )

    if any(name.startswith("Crea_elemento") for name in action_names):
        issues.append(
            _build_issue(
                code="unsupported-sharepoint-create",
                severity="high",
                category="connector",
                title="Creazione record SharePoint non importabile",
                detail=(
                    "Il flow genera nuovi elementi SharePoint con dati derivati. Il motore automazioni del portale "
                    "non ha una action equivalente su SharePoint."
                ),
                remediation=(
                    "Sostituire con inserimenti controllati nel database del portale oppure gestire la duplicazione "
                    "dei record direttamente nel modulo assenze."
                ),
            )
        )

    if any("Imposta_stato_di_approvazione_del_contenuto" in name for name in action_names):
        issues.append(
            _build_issue(
                code="unsupported-sharepoint-approval-update",
                severity="medium",
                category="connector",
                title="Aggiornamento stato approvazione SharePoint non importabile",
                detail=(
                    "Il flow aggiorna lo stato di moderazione del contenuto SharePoint in piu' rami. "
                    "Il runtime del portale non espone update verso SharePoint."
                ),
                remediation=(
                    "Mantenere il portale come sistema master dello stato approvazione e, se necessario, "
                    "implementare una sync separata verso SharePoint."
                ),
            )
        )

    if not issues:
        issues.append(
            _build_issue(
                code="no-blockers-detected",
                severity="low",
                category="analysis",
                title="Nessun blocco evidente",
                detail="Il flow non presenta pattern chiaramente incompatibili con il motore attuale.",
                remediation="Verificare comunque il mapping dei campi e i template di notifica prima dell'import.",
            )
        )

    return issues


def _build_proposed_rules(source_code: str, flow_name: str) -> list[dict[str, Any]]:
    if source_code != "assenze":
        return []

    base_description = (
        f"Bozza generata dall'analisi del flow Power Automate '{flow_name}'. "
        "Richiede verifica manuale prima della pubblicazione."
    )

    return [
        {
            "code": "pa-assenze-insert-malattia-avviso-responsabile",
            "name": "PA import - Avviso responsabile su nuova malattia",
            "description": base_description,
            "source_code": "assenze",
            "operation_type": "insert",
            "trigger_scope": "all_inserts",
            "watched_field": "",
            "is_active": False,
            "is_draft": True,
            "stop_on_first_failure": False,
            "conditions": [
                {
                    "order": 1,
                    "field_name": "tipo_assenza",
                    "operator": "equals",
                    "expected_value": "Malattia",
                    "value_type": "string",
                    "compare_with_old": False,
                    "is_enabled": True,
                },
                {
                    "order": 2,
                    "field_name": "capo_email",
                    "operator": "is_not_empty",
                    "expected_value": "",
                    "value_type": "string",
                    "compare_with_old": False,
                    "is_enabled": True,
                },
            ],
            "actions": [
                {
                    "order": 1,
                    "action_type": "send_email",
                    "description": "Avvisa il responsabile su nuova malattia",
                    "is_enabled": True,
                    "config_json": {
                        "from_email": "",
                        "to": "{capo_email}",
                        "cc": "",
                        "bcc": "",
                        "reply_to": "",
                        "subject_template": "[Assenze] nuova malattia #{id}",
                        "body_text_template": (
                            "Nuova assenza per malattia.\n"
                            "Dipendente: {dipendente_id}\n"
                            "Periodo: {data_inizio} - {data_fine}\n"
                            "Tipo: {tipo_assenza}"
                        ),
                        "body_html_template": (
                            "<p>Nuova assenza per malattia.</p>"
                            "<p>Dipendente: {dipendente_id}</p>"
                            "<p>Periodo: {data_inizio} - {data_fine}</p>"
                            "<p>Tipo: {tipo_assenza}</p>"
                        ),
                        "fail_silently": False,
                    },
                }
            ],
        },
        {
            "code": "pa-assenze-update-approvata-notifica-dipendente",
            "name": "PA import - Esito approvazione assenza",
            "description": base_description,
            "source_code": "assenze",
            "operation_type": "update",
            "trigger_scope": "specific_field",
            "watched_field": "moderation_status",
            "is_active": False,
            "is_draft": True,
            "stop_on_first_failure": False,
            "conditions": [
                {
                    "order": 1,
                    "field_name": "moderation_status",
                    "operator": "changed_to",
                    "expected_value": "0",
                    "value_type": "int",
                    "compare_with_old": False,
                    "is_enabled": True,
                }
            ],
            "actions": [
                {
                    "order": 1,
                    "action_type": "send_email",
                    "description": "Invia email al dipendente dopo approvazione",
                    "is_enabled": True,
                    "config_json": {
                        "from_email": "",
                        "to": "{dipendente_email}",
                        "cc": "",
                        "bcc": "",
                        "reply_to": "",
                        "subject_template": "[Assenze] richiesta #{id} approvata",
                        "body_text_template": (
                            "La tua richiesta di {tipo_assenza} dal {data_inizio} al {data_fine} "
                            "e' stata approvata."
                        ),
                        "body_html_template": (
                            "<p>La tua richiesta di <strong>{tipo_assenza}</strong> "
                            "dal {data_inizio} al {data_fine} e' stata approvata.</p>"
                        ),
                        "fail_silently": False,
                    },
                }
            ],
        },
        {
            "code": "pa-assenze-update-rifiutata-notifica-dipendente",
            "name": "PA import - Esito rifiuto assenza",
            "description": base_description,
            "source_code": "assenze",
            "operation_type": "update",
            "trigger_scope": "specific_field",
            "watched_field": "moderation_status",
            "is_active": False,
            "is_draft": True,
            "stop_on_first_failure": False,
            "conditions": [
                {
                    "order": 1,
                    "field_name": "moderation_status",
                    "operator": "changed_to",
                    "expected_value": "1",
                    "value_type": "int",
                    "compare_with_old": False,
                    "is_enabled": True,
                }
            ],
            "actions": [
                {
                    "order": 1,
                    "action_type": "send_email",
                    "description": "Invia email al dipendente dopo rifiuto",
                    "is_enabled": True,
                    "config_json": {
                        "from_email": "",
                        "to": "{dipendente_email}",
                        "cc": "",
                        "bcc": "",
                        "reply_to": "",
                        "subject_template": "[Assenze] richiesta #{id} non approvata",
                        "body_text_template": (
                            "La tua richiesta di {tipo_assenza} dal {data_inizio} al {data_fine} "
                            "non e' stata approvata."
                        ),
                        "body_html_template": (
                            "<p>La tua richiesta di <strong>{tipo_assenza}</strong> "
                            "dal {data_inizio} al {data_fine} non e' stata approvata.</p>"
                        ),
                        "fail_silently": False,
                    },
                }
            ],
        },
    ]


def build_automation_package(flow: dict[str, Any], *, input_path: Path | None = None) -> dict[str, Any]:
    triggers = extract_trigger_summary(flow["triggers"])
    actions, fields_used = extract_actions_and_fields(flow["actions"])
    raw_actions = _iter_actions(flow["actions"])
    connectors = _collect_connectors(flow["raw"], raw_actions)
    source_candidate = _guess_source(flow["flow_name"], fields_used)
    runtime_candidates = suggest_runtime_field_mapping(fields_used, source_code=source_candidate["source_code"])
    issues = _detect_issues(
        actions=raw_actions,
        connectors=connectors,
        fields_used=fields_used,
        source_code=source_candidate["source_code"],
    )
    severity_rank = {"low": 0, "medium": 1, "high": 2}
    compatibility_status = "full"
    if any(issue["severity"] == "high" for issue in issues):
        compatibility_status = "partial"
    elif any(issue["severity"] == "medium" for issue in issues):
        compatibility_status = "partial"

    package = {
        "package_version": "1.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "input": {
            "filename": input_path.name if input_path else "",
            "flow_name": flow["flow_name"],
            "flow_slug": flow["flow_slug"],
        },
        "source_candidate": source_candidate,
        "compatibility": {
            "status": compatibility_status,
            "issue_count": len(issues),
            "highest_severity": max((issue["severity"] for issue in issues), key=severity_rank.get, default="low"),
        },
        "trigger_summary": triggers,
        "action_summary": {
            "top_level_action_count": len(flow["actions"]),
            "flattened_action_count": len(raw_actions),
            "action_type_counts": dict(sorted(Counter(row["type"] for row in raw_actions).items())),
        },
        "connectors": connectors,
        "field_mapping_candidates": runtime_candidates,
        "runtime_field_mapping_candidates": runtime_candidates,
        "approved_runtime_field_mapping": _approved_runtime_field_mapping(runtime_candidates),
        "target_field_mapping_candidates": {},
        "approved_target_field_mapping": {},
        "fields_used": fields_used,
        "issues": issues,
        "proposed_rules": _build_proposed_rules(source_candidate["source_code"], flow["flow_name"]),
        "normalized_flow": {
            "triggers": triggers,
            "actions": actions,
        },
    }
    return normalize_package_mappings(package)
