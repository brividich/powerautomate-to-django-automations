from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
import json
import re
from typing import Any

from extract_logic import extract_actions_and_fields, extract_trigger_summary
from package_mapping import normalize_package_mappings
from runtime_catalog import (
    get_workflow_capabilities,
    has_native_approval_workflow,
    runtime_field_names,
    suggest_runtime_field_mapping,
)


CONNECTION_KEY_RE = re.compile(r"\['([^']+)'\]")
APPROVAL_LOOP_HINTS = (
    "approval",
    "approv",
    "moderationstatus",
    "moderation_status",
    "consenso",
    "waitforanapproval",
    "createanapproval",
)
APPROVAL_TEMPLATE_DELIVERY_MODES = {"mail_reply", "hybrid"}
APPROVAL_ACTION_HINTS = ("createanapproval", "waitforanapproval", "approval")
APPROVAL_APPROVER_FIELD_HINTS = ("capo_email", "approver_email", "responsabile_email", "manager_email")


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


def _action_blob(row: dict[str, Any]) -> str:
    definition = _safe_dict(row.get("definition"))
    action_name = str(row.get("name") or "")
    try:
        serialized = json.dumps(definition, ensure_ascii=False).lower()
    except Exception:
        serialized = ""
    return f"{action_name.lower()} {serialized}".strip()


def _classify_until_rows(actions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    approval_related: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []
    for row in actions:
        if row.get("type") != "Until":
            continue
        haystack = _action_blob(row)
        if any(token in haystack for token in APPROVAL_LOOP_HINTS):
            approval_related.append(row)
        else:
            other.append(row)
    return approval_related, other


def _has_power_automate_approval(*, connectors: list[dict[str, Any]], actions: list[dict[str, Any]]) -> bool:
    connector_names = {str(row.get("api_name") or "").lower() for row in connectors if row.get("usage_count")}
    if "approvals" in connector_names:
        return True
    return any(any(hint in _action_blob(row) for hint in APPROVAL_ACTION_HINTS) for row in actions)


def _select_template_metadata(approval_template: dict[str, Any] | None) -> dict[str, str]:
    template = approval_template if isinstance(approval_template, dict) else {}
    return {
        "code": str(template.get("code") or "").strip(),
        "name": str(template.get("name") or "").strip(),
        "delivery_mode": str(template.get("delivery_mode") or "").strip(),
    }


def _guess_rule_trigger_shape(triggers: list[dict[str, Any]] | dict[str, Any]) -> tuple[str, str]:
    try:
        serialized = json.dumps(triggers, ensure_ascii=False).lower()
    except Exception:
        serialized = ""
    if any(token in serialized for token in ("creato", "created", "new item", "all_inserts", "insert")):
        return "insert", "all_inserts"
    if any(token in serialized for token in ("modific", "updated", "changed", "all_updates", "update")):
        return "update", "all_updates"
    return "insert", "all_inserts"


def _resolve_approval_recipient_template(
    *,
    workflow_capabilities: dict[str, Any],
    runtime_candidates: dict[str, dict[str, Any]],
) -> str:
    approval_cfg = workflow_capabilities.get("approval", {}) if isinstance(workflow_capabilities, dict) else {}
    explicit_template = str(approval_cfg.get("approver_email_template") or "").strip()
    if explicit_template:
        return explicit_template

    explicit_field = str(approval_cfg.get("approver_email_field") or "").strip()
    if explicit_field:
        return f"{{{explicit_field}}}"

    normalized_candidates = []
    for candidate in runtime_candidates.values():
        if not isinstance(candidate, dict):
            continue
        target_field = str(candidate.get("target_field") or "").strip()
        if not target_field:
            continue
        normalized_candidates.append(target_field)
        if target_field in APPROVAL_APPROVER_FIELD_HINTS:
            return f"{{{target_field}}}"

    for target_field in normalized_candidates:
        lowered = target_field.lower()
        if lowered.endswith("_email") and any(hint.split("_")[0] in lowered for hint in APPROVAL_APPROVER_FIELD_HINTS):
            return f"{{{target_field}}}"
    return ""


def _build_approval_subject_template(
    *,
    source_code: str,
    flow_name: str,
    workflow_capabilities: dict[str, Any],
) -> str:
    approval_cfg = workflow_capabilities.get("approval", {}) if isinstance(workflow_capabilities, dict) else {}
    explicit = str(approval_cfg.get("default_subject_template") or "").strip()
    if explicit:
        return explicit
    label = source_code.replace("_", " ").title() if source_code else flow_name
    return f"[{label}] richiesta #{{id}} da approvare"


def _build_approval_message_template(
    *,
    source_code: str,
    flow_name: str,
    workflow_capabilities: dict[str, Any],
) -> str:
    approval_cfg = workflow_capabilities.get("approval", {}) if isinstance(workflow_capabilities, dict) else {}
    explicit = str(approval_cfg.get("default_message_template") or "").strip()
    if explicit:
        return explicit

    available_fields = set(runtime_field_names(source_code))
    lines = [f"Flow Power Automate convertito: {flow_name}."]
    if "title" in available_fields:
        lines.append("Titolo: {title}")
    elif "tipo_assenza" in available_fields:
        lines.append("Tipo: {tipo_assenza}")
    if "data_inizio" in available_fields and "data_fine" in available_fields:
        lines.append("Periodo: {data_inizio} - {data_fine}")
    elif "due_date" in available_fields:
        lines.append("Scadenza: {due_date}")
    lines.append("Record: #{id}")
    return "\n".join(lines)


def _build_approval_branch_actions(
    *,
    source_code: str,
    workflow_capabilities: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    approval_cfg = workflow_capabilities.get("approval", {}) if isinstance(workflow_capabilities, dict) else {}
    status_field = str(approval_cfg.get("status_field") or "").strip()
    approved_value = approval_cfg.get("approved_value")
    rejected_value = approval_cfg.get("rejected_value")
    available_fields = set(runtime_field_names(source_code))

    approved_actions: list[dict[str, Any]] = []
    rejected_actions: list[dict[str, Any]] = []
    if status_field and status_field in available_fields and approved_value not in (None, ""):
        approved_actions.append(
            {
                "action_type": "update_trigger_record",
                "description": "Aggiorna stato approvazione (approvato)",
                "config_json": {"update_fields": {status_field: approved_value}},
            }
        )
    if status_field and status_field in available_fields and rejected_value not in (None, ""):
        rejected_actions.append(
            {
                "action_type": "update_trigger_record",
                "description": "Aggiorna stato approvazione (rifiutato)",
                "config_json": {"update_fields": {status_field: rejected_value}},
            }
        )
    return approved_actions, rejected_actions


def _build_approval_unsupported_actions(raw_actions: list[dict[str, Any]]) -> list[dict[str, str]]:
    unsupported: list[dict[str, str]] = []
    for row in raw_actions:
        if any(hint in _action_blob(row) for hint in APPROVAL_ACTION_HINTS):
            continue
        unsupported.append(
            {
                "name": str(row.get("name") or ""),
                "type": str(row.get("type") or ""),
                "parent": str(row.get("parent") or ""),
            }
        )
    return unsupported


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
    approval_until_rows, generic_until_rows = _classify_until_rows(actions)
    issues: list[dict[str, str]] = []

    if generic_until_rows:
        issues.append(
            _build_issue(
                code="unsupported-loop-until",
                severity="high",
                category="logic",
                title="Loop `Until` non convertibile automaticamente",
                detail=(
                    "Il flow crea elementi successivi in uno o piu' loop `Until` non riconducibili al solo workflow "
                    "approvativo, tipicamente per spezzare assenze su piu' giorni o creare record derivati."
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


def _build_legacy_assenze_rules(source_code: str, flow_name: str) -> list[dict[str, Any]]:
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


def _build_approval_conversion(
    *,
    flow_name: str,
    source_code: str,
    triggers: list[dict[str, Any]] | dict[str, Any],
    raw_actions: list[dict[str, Any]],
    connectors: list[dict[str, Any]],
    workflow_capabilities: dict[str, Any],
    runtime_candidates: dict[str, dict[str, Any]],
    approval_template: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, list[dict[str, str]], list[dict[str, Any]]]:
    if not _has_power_automate_approval(connectors=connectors, actions=raw_actions):
        return None, [], []

    template_meta = _select_template_metadata(approval_template)
    recipient_template = _resolve_approval_recipient_template(
        workflow_capabilities=workflow_capabilities,
        runtime_candidates=runtime_candidates,
    )
    subject_template = _build_approval_subject_template(
        source_code=source_code,
        flow_name=flow_name,
        workflow_capabilities=workflow_capabilities,
    )
    message_template = _build_approval_message_template(
        source_code=source_code,
        flow_name=flow_name,
        workflow_capabilities=workflow_capabilities,
    )
    approved_actions, rejected_actions = _build_approval_branch_actions(
        source_code=source_code,
        workflow_capabilities=workflow_capabilities,
    )
    unsupported_branch_actions = _build_approval_unsupported_actions(raw_actions)
    conversion = {
        "detected": True,
        "strategy": "send_approval",
        "template_code": template_meta["code"],
        "template_delivery_mode": template_meta["delivery_mode"],
        "approver_template": recipient_template,
        "subject_template": subject_template,
        "message_template": message_template,
        "approved_branch_supported_count": len(approved_actions),
        "rejected_branch_supported_count": len(rejected_actions),
        "unsupported_branch_actions": unsupported_branch_actions,
    }
    issues: list[dict[str, str]] = []

    if source_code == "generic":
        issues.append(
            _build_issue(
                code="approval-source-unresolved",
                severity="high",
                category="workflow",
                title="Approval rilevata ma sorgente portale non riconosciuta",
                detail=(
                    "Il flow contiene un ramo approval Power Automate, ma il converter non ha riconosciuto "
                    "una sorgente runtime specifica del portale."
                ),
                remediation=(
                    "Rieseguire l'analisi su un flow con naming/campi piu' riconoscibili oppure rifinire "
                    "manualmente la regola nel designer dopo aver scelto la sorgente corretta."
                ),
            )
        )
        return conversion, issues, []

    if not template_meta["code"]:
        issues.append(
            _build_issue(
                code="approval-template-required",
                severity="high",
                category="workflow",
                title="Template approvazione obbligatorio per la conversione approval",
                detail=(
                    "Il flow contiene un ramo approval e il converter e' configurato per generare `send_approval`, "
                    "ma non e' stato selezionato alcun template email compatibile."
                ),
                remediation=(
                    "Selezionare un template attivo `hybrid` o `mail_reply` nel converter e rieseguire l'analisi."
                ),
            )
        )
        return conversion, issues, []

    if template_meta["delivery_mode"] not in APPROVAL_TEMPLATE_DELIVERY_MODES:
        issues.append(
            _build_issue(
                code="approval-template-mail-required",
                severity="high",
                category="workflow",
                title="Template approvazione non compatibile con la conversione via mail",
                detail=(
                    "Per generare `send_approval` serve un template email approval con delivery_mode "
                    "`mail_reply` o `hybrid`."
                ),
                remediation=(
                    "Selezionare nel converter un template attivo `hybrid` o `mail_reply` prima di rigenerare il package."
                ),
            )
        )
        return conversion, issues, []

    if not recipient_template:
        issues.append(
            _build_issue(
                code="approval-recipient-unresolved",
                severity="high",
                category="workflow",
                title="Impossibile risolvere un approvatore email affidabile",
                detail=(
                    "Il converter non ha trovato un campo email approvatore coerente con la sorgente runtime "
                    f"`{source_code}`."
                ),
                remediation=(
                    "Aggiungere un hint approver email nel catalogo runtime oppure completare manualmente "
                    "la regola nel designer."
                ),
            )
        )
        return conversion, issues, []

    operation_type, trigger_scope = _guess_rule_trigger_shape(triggers)
    approval_cfg = workflow_capabilities.get("approval", {}) if isinstance(workflow_capabilities, dict) else {}
    skip_field = str(approval_cfg.get("skip_field") or "").strip()
    available_fields = set(runtime_field_names(source_code))
    conditions: list[dict[str, Any]] = []
    if skip_field and skip_field in available_fields:
        conditions.append(
            {
                "order": 1,
                "field_name": skip_field,
                "operator": "is_false",
                "expected_value": "",
                "value_type": "bool",
                "compare_with_old": False,
                "is_enabled": True,
            }
        )

    rule_code = f"pa-{source_code}-send-approval-mail"
    rules = [
        {
            "code": rule_code,
            "name": f"PA import - Approval via mail ({source_code})",
            "description": (
                f"Bozza generata dall'analisi del flow Power Automate '{flow_name}'. "
                "Richiede verifica manuale prima della pubblicazione."
            ),
            "source_code": source_code,
            "operation_type": operation_type,
            "trigger_scope": trigger_scope,
            "watched_field": "",
            "is_active": False,
            "is_draft": True,
            "stop_on_first_failure": False,
            "conditions": conditions,
            "actions": [
                {
                    "order": 1,
                    "action_type": "send_approval",
                    "description": "Richiedi approvazione via mail",
                    "is_enabled": True,
                    "config_json": {
                        "delivery_mode": "email",
                        "to_template": recipient_template,
                        "subject_template": subject_template,
                        "message_template": message_template,
                        "expiry_days": 7,
                        "approve_label": "Approva",
                        "reject_label": "Rifiuta",
                        "approval_email_template_code": template_meta["code"],
                        "approved_actions": approved_actions,
                        "rejected_actions": rejected_actions,
                    },
                }
            ],
        }
    ]
    if unsupported_branch_actions:
        issues.append(
            _build_issue(
                code="approval-branch-manual-review",
                severity="medium",
                category="workflow",
                title="Parte del ramo approval richiede rifinitura manuale",
                detail=(
                    "Il converter ha generato la base `send_approval`, ma alcune azioni del flow originale "
                    "non sono state tradotte automaticamente nei branch approvato/rifiutato."
                ),
                remediation="Aprire la bozza nel designer e completare solo le azioni mancanti realmente necessarie.",
            )
        )
    return conversion, issues, rules


def _build_proposed_rules(
    source_code: str,
    flow_name: str,
    *,
    triggers: list[dict[str, Any]] | dict[str, Any],
    raw_actions: list[dict[str, Any]],
    connectors: list[dict[str, Any]],
    workflow_capabilities: dict[str, Any],
    runtime_candidates: dict[str, dict[str, Any]],
    approval_template: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, list[dict[str, str]]]:
    approval_conversion, approval_issues, approval_rules = _build_approval_conversion(
        flow_name=flow_name,
        source_code=source_code,
        triggers=triggers,
        raw_actions=raw_actions,
        connectors=connectors,
        workflow_capabilities=workflow_capabilities,
        runtime_candidates=runtime_candidates,
        approval_template=approval_template,
    )
    if approval_conversion is not None:
        return approval_rules, approval_conversion, approval_issues
    return _build_legacy_assenze_rules(source_code, flow_name), None, []


def build_automation_package(
    flow: dict[str, Any],
    *,
    input_path: Path | None = None,
    approval_template: dict[str, Any] | None = None,
) -> dict[str, Any]:
    triggers = extract_trigger_summary(flow["triggers"])
    actions, fields_used = extract_actions_and_fields(flow["actions"])
    raw_actions = _iter_actions(flow["actions"])
    connectors = _collect_connectors(flow["raw"], raw_actions)
    source_candidate = _guess_source(flow["flow_name"], fields_used)
    workflow_capabilities = get_workflow_capabilities(source_candidate["source_code"])
    runtime_candidates = suggest_runtime_field_mapping(fields_used, source_code=source_candidate["source_code"])
    issues = _detect_issues(
        actions=raw_actions,
        connectors=connectors,
        fields_used=fields_used,
        source_code=source_candidate["source_code"],
    )
    proposed_rules, approval_conversion, approval_issues = _build_proposed_rules(
        source_code=source_candidate["source_code"],
        flow_name=flow["flow_name"],
        triggers=triggers,
        raw_actions=raw_actions,
        connectors=connectors,
        workflow_capabilities=workflow_capabilities,
        runtime_candidates=runtime_candidates,
        approval_template=approval_template,
    )
    issues.extend(approval_issues)
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
        "proposed_rules": proposed_rules,
        "normalized_flow": {
            "triggers": triggers,
            "actions": actions,
        },
    }
    if approval_conversion is not None:
        package["approval_conversion"] = approval_conversion
    return normalize_package_mappings(package)
