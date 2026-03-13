from __future__ import annotations

import json
from typing import Any

from runtime_catalog import get_runtime_source_catalog, runtime_field_names


TARGET_SCOPE = "target_table"
RUNTIME_SCOPE = "runtime_source"
LEGACY_RUNTIME_SCOPE = "runtime"


def _copy(value: Any) -> Any:
    return json.loads(json.dumps(value))


def _normalize_mapping_block(mapping: Any, *, scope: str) -> dict[str, dict[str, Any]]:
    if not isinstance(mapping, dict):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for source_field, raw_candidate in mapping.items():
        if not isinstance(raw_candidate, dict):
            continue
        target_field = str(raw_candidate.get("target_field") or "").strip()
        candidate = _copy(raw_candidate)
        candidate["target_field"] = target_field
        candidate["mapping_scope"] = scope
        if "note" not in candidate and candidate.get("reason"):
            candidate["note"] = candidate["reason"]
        if "reason" not in candidate and candidate.get("note"):
            candidate["reason"] = candidate["note"]
        normalized[str(source_field)] = candidate
    return normalized


def _mapping_warning_rows(
    approved_target: dict[str, dict[str, Any]],
    approved_runtime: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    runtime_mapped_fields = {
        source_field
        for source_field, candidate in approved_runtime.items()
        if str(candidate.get("target_field") or "").strip()
    }
    warnings: list[dict[str, str]] = []
    for source_field, candidate in approved_target.items():
        target_field = str(candidate.get("target_field") or "").strip()
        if not target_field or source_field in runtime_mapped_fields:
            continue
        warnings.append(
            {
                "code": "target-mapped-without-runtime",
                "severity": "warning",
                "source_field": str(source_field),
                "target_field": target_field,
                "message": (
                    f"Il campo flow '{source_field}' e' mappato verso la tabella target "
                    f"('{target_field}') ma non verso la sorgente runtime del portale."
                ),
            }
        )
    return warnings


def normalize_package_mappings(package: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(package, dict):
        return {}

    runtime_candidates_seed = (
        package.get("runtime_field_mapping_candidates")
        or package.get("field_mapping_candidates")
        or {}
    )
    approved_runtime_seed = package.get("approved_runtime_field_mapping") or {}
    target_candidates_seed = (
        package.get("target_field_mapping_candidates")
        or package.get("target_table_field_mapping_candidates")
        or {}
    )
    approved_target_seed = (
        package.get("approved_target_field_mapping")
        or package.get("approved_field_mapping")
        or {}
    )

    runtime_candidates = _normalize_mapping_block(runtime_candidates_seed, scope=RUNTIME_SCOPE)
    approved_runtime = _normalize_mapping_block(approved_runtime_seed, scope=RUNTIME_SCOPE)
    target_candidates = _normalize_mapping_block(target_candidates_seed, scope=TARGET_SCOPE)
    approved_target = _normalize_mapping_block(approved_target_seed, scope=TARGET_SCOPE)

    source_candidate = package.get("source_candidate", {})
    runtime_source_code = str(
        package.get("runtime_source_catalog", {}).get("source_code")
        or source_candidate.get("source_code")
        or "generic"
    ).strip() or "generic"
    runtime_catalog = get_runtime_source_catalog(runtime_source_code)

    package["runtime_source_catalog"] = runtime_catalog
    package["runtime_supported_fields"] = runtime_field_names(runtime_source_code)
    package["field_mapping_candidates"] = _copy(runtime_candidates)
    package["runtime_field_mapping_candidates"] = _copy(runtime_candidates)
    package["approved_runtime_field_mapping"] = _copy(approved_runtime)
    package["target_field_mapping_candidates"] = _copy(target_candidates)
    package["target_table_field_mapping_candidates"] = _copy(target_candidates)
    package["approved_target_field_mapping"] = _copy(approved_target)
    package["approved_field_mapping"] = _copy(approved_target)
    package["mapping_scope"] = {
        "field_mapping_candidates": RUNTIME_SCOPE,
        "runtime_field_mapping_candidates": RUNTIME_SCOPE,
        "approved_runtime_field_mapping": RUNTIME_SCOPE,
        "approved_field_mapping": TARGET_SCOPE,
        "approved_target_field_mapping": TARGET_SCOPE,
        "target_field_mapping_candidates": TARGET_SCOPE,
        "target_table_field_mapping_candidates": TARGET_SCOPE,
    }
    package["mapping_warnings"] = _mapping_warning_rows(approved_target, approved_runtime)
    return package
