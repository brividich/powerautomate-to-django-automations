from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
from typing import Any
from uuid import uuid4

from build_automation_package import build_automation_package
from build_flow_diagram import build_flow_diagram
from build_preview import build_preview
from compare_schema import compare_fields_to_schema
from extract_logic import extract_actions_and_fields, extract_trigger_summary
from mapping_memory import suggest_mappings
from package_mapping import TARGET_SCOPE, normalize_package_mappings
from parse_flow import load_flow_definition_from_bytes


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"


def _table_key(target_context: dict[str, Any]) -> str:
    database = str(target_context.get("database") or "").strip()
    schema = str(target_context.get("schema") or "").strip()
    table = str(target_context.get("table") or "").strip()
    return ".".join(part for part in [database, schema, table] if part)


def _public_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for column in columns:
        if not isinstance(column, dict):
            continue
        cleaned.append(
            {
                "name": str(column.get("name") or ""),
                "data_type": str(column.get("data_type") or ""),
                "is_nullable": bool(column.get("is_nullable")),
                "ordinal_position": int(column.get("ordinal_position") or 0),
                "is_primary_key": bool(column.get("is_primary_key")),
            }
        )
    return cleaned


def _copy_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(mapping)) if isinstance(mapping, dict) else {}


def _with_mapping_scope(mapping: dict[str, Any], *, scope: str, source: str, note: str) -> dict[str, Any]:
    scoped = _copy_mapping(mapping)
    for candidate in scoped.values():
        if isinstance(candidate, dict):
            candidate["mapping_scope"] = scope
            candidate.setdefault("source", source)
            candidate.setdefault("note", str(candidate.get("reason") or note))
            candidate.setdefault("reason", str(candidate.get("note") or note))
    return scoped


def analyze_flow_upload(
    filename: str,
    payload: bytes,
    *,
    target_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flow = load_flow_definition_from_bytes(filename, payload)
    triggers = extract_trigger_summary(flow["triggers"])
    actions, fields_used = extract_actions_and_fields(flow["actions"])
    schema_match = compare_fields_to_schema(fields_used, INPUT_DIR / "schema_pack")

    normalized = {
        "flow_name": flow["flow_name"],
        "flow_slug": flow["flow_slug"],
        "triggers": triggers,
        "actions": actions,
        "diagram": build_flow_diagram(triggers, actions),
        "fields_used": fields_used,
        "direct_matches": schema_match["direct_matches"],
        "unmatched_fields": schema_match["unmatched_fields"],
    }
    preview_markdown = build_preview(normalized)
    package = build_automation_package(flow, input_path=Path(filename))
    safe_target_context: dict[str, Any] | None = None

    if target_context:
        columns = _public_columns(list(target_context.get("columns") or []))
        safe_target_context = {
            "db_type": str(target_context.get("db_type") or "sqlserver"),
            "server": str(target_context.get("server") or ""),
            "database": str(target_context.get("database") or ""),
            "schema": str(target_context.get("schema") or ""),
            "table": str(target_context.get("table") or ""),
            "full_name": str(target_context.get("full_name") or ""),
            "columns": columns,
        }
        package["target_context"] = {
            key: value for key, value in safe_target_context.items() if key != "columns"
        } | {"column_count": len(columns)}
        package["table_columns"] = columns
        package["target_field_mapping_candidates"] = _with_mapping_scope(
            suggest_mappings(
                fields_used,
                columns,
                table_key=_table_key(safe_target_context),
            ),
            scope=TARGET_SCOPE,
            source="target_context",
            note="suggerimento derivato dalle colonne reali della tabella target",
        )
        package["target_table_field_mapping_candidates"] = _copy_mapping(package["target_field_mapping_candidates"])

    normalize_package_mappings(package)

    _sync_selected_rules(package)

    return {
        "record_id": uuid4().hex[:12],
        "created_at": datetime.now(UTC).isoformat(),
        "filename": filename,
        "normalized": normalized,
        "preview_markdown": preview_markdown,
        "package": package,
        "remediations_applied": [],
        "target_context": safe_target_context or {},
    }


def _append_unique_issue(issues: list[dict[str, Any]], issue: dict[str, Any]) -> None:
    existing_codes = {str(item.get("code") or "") for item in issues if isinstance(item, dict)}
    if str(issue.get("code") or "") not in existing_codes:
        issues.append(issue)


def _sync_selected_rules(package: dict[str, Any]) -> None:
    proposed_rules = [row for row in package.get("proposed_rules", []) if isinstance(row, dict)]
    selected_codes = {
        str(code or "").strip()
        for code in package.get("selected_proposed_rule_codes", [])
        if str(code or "").strip()
    }
    available_codes = {str(rule.get("code") or "").strip() for rule in proposed_rules}

    if not selected_codes:
        selected_codes = available_codes
    else:
        selected_codes = {code for code in selected_codes if code in available_codes}

    package["selected_proposed_rule_codes"] = sorted(selected_codes)
    package["selected_proposed_rules"] = [
        rule for rule in proposed_rules if str(rule.get("code") or "").strip() in selected_codes
    ]


def apply_recommended_remediation(record: dict[str, Any]) -> dict[str, Any]:
    updated = json.loads(json.dumps(record))
    package = updated.get("package", {})
    if not isinstance(package, dict):
        return updated

    source_code = str(package.get("source_candidate", {}).get("source_code") or "")
    if source_code != "assenze":
        return updated

    applied = updated.setdefault("remediations_applied", [])
    already = {str(item.get("code") or "") for item in applied if isinstance(item, dict)}

    if "assenze-status-based-remediation" not in already:
        applied.append(
            {
                "code": "assenze-status-based-remediation",
                "label": "Conversione approval -> regole su moderation_status",
                "applied_at": datetime.now(UTC).isoformat(),
                "detail": (
                    "Il package conferma come remediation standard la sostituzione dei rami approval Power Automate "
                    "con regole assenze basate su insert/update e `moderation_status`."
                ),
            }
        )

    if "assenze-skip-approval-rule" not in already:
        proposed_rules = package.setdefault("proposed_rules", [])
        selected_codes = package.setdefault("selected_proposed_rule_codes", [])
        proposed_rules.append(
            {
                "code": "pa-assenze-insert-skip-approval-audit",
                "name": "PA import - Audit richieste con salta approvazione",
                "description": (
                    "Regola aggiunta dalla remediation automatica per tracciare richieste create con `salta_approvazione`."
                ),
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
                        "field_name": "salta_approvazione",
                        "operator": "is_true",
                        "expected_value": "",
                        "value_type": "bool",
                        "compare_with_old": False,
                        "is_enabled": True,
                    }
                ],
                "actions": [
                    {
                        "order": 1,
                        "action_type": "write_log",
                        "description": "Traccia l'inserimento con skip approval",
                        "is_enabled": True,
                        "config_json": {
                            "message_template": (
                                "Richiesta assenza #{id} inserita con salta_approvazione={salta_approvazione} "
                                "per dipendente {dipendente_id}."
                            )
                        },
                    }
                ],
            }
        )
        if "pa-assenze-insert-skip-approval-audit" not in selected_codes:
            selected_codes.append("pa-assenze-insert-skip-approval-audit")
        applied.append(
            {
                "code": "assenze-skip-approval-rule",
                "label": "Aggiunta regola audit su salta_approvazione",
                "applied_at": datetime.now(UTC).isoformat(),
                "detail": "Aggiunta una regola draft dedicata alle richieste create saltando l'approvazione.",
            }
        )

    issues = package.setdefault("issues", [])
    _append_unique_issue(
        issues,
        {
            "code": "remediation-applied-status-rules",
            "severity": "low",
            "category": "remediation",
            "title": "Remediation automatica applicata",
            "detail": (
                "La conversione ha aggiunto una regola audit per `salta_approvazione` e ha consolidato "
                "la strategia di conversione delle approval su `moderation_status`."
            ),
            "remediation": "Rivedi le nuove regole draft e rifinisci i template prima dell'import nel portale target.",
        },
    )

    package["compatibility"] = {
        **package.get("compatibility", {}),
        "status": "partial",
        "issue_count": len(issues),
    }
    _sync_selected_rules(package)
    return updated
