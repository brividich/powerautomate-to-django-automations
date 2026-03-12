from __future__ import annotations

import re
from typing import Any


FIELD_RE = re.compile(r"\[['\"]([^'\"]+)['\"]\]")
IGNORED_FIELDS = {
    "connectionId",
    "name",
    "outcome",
}


def _walk_fields(obj: Any, found_fields: set[str]) -> None:
    if isinstance(obj, dict):
        for value in obj.values():
            _walk_fields(value, found_fields)

    elif isinstance(obj, list):
        for item in obj:
            _walk_fields(item, found_fields)

    elif isinstance(obj, str):
        for match in FIELD_RE.findall(obj):
            if match in IGNORED_FIELDS or match.startswith("shared_") or match.startswith("{"):
                continue
            found_fields.add(match)


def _collect_actions(
    actions: dict[str, Any],
    action_rows: list[dict],
    found_fields: set[str],
    *,
    parent: str | None = None,
    branch: str = "main",
    depth: int = 0,
) -> None:
    for action_name, action_def in actions.items():
        if not isinstance(action_def, dict):
            continue

        row = {
            "name": action_name,
            "type": action_def.get("type", ""),
            "parent": parent,
            "branch": branch,
            "depth": depth,
            "run_after": action_def.get("runAfter", {}),
        }

        kind = action_def.get("kind")
        if kind:
            row["kind"] = kind

        action_rows.append(row)
        _walk_fields(action_def, found_fields)

        nested_actions = action_def.get("actions")
        if isinstance(nested_actions, dict):
            _collect_actions(
                nested_actions,
                action_rows,
                found_fields,
                parent=action_name,
                branch="main",
                depth=depth + 1,
            )

        else_branch = action_def.get("else")
        if isinstance(else_branch, dict) and isinstance(else_branch.get("actions"), dict):
            _collect_actions(
                else_branch["actions"],
                action_rows,
                found_fields,
                parent=action_name,
                branch="else",
                depth=depth + 1,
            )

        for case_name, case_def in (action_def.get("cases") or {}).items():
            if isinstance(case_def, dict) and isinstance(case_def.get("actions"), dict):
                _collect_actions(
                    case_def["actions"],
                    action_rows,
                    found_fields,
                    parent=action_name,
                    branch=f"case:{case_name}",
                    depth=depth + 1,
                )

        default_branch = action_def.get("default")
        if isinstance(default_branch, dict) and isinstance(default_branch.get("actions"), dict):
            _collect_actions(
                default_branch["actions"],
                action_rows,
                found_fields,
                parent=action_name,
                branch="default",
                depth=depth + 1,
            )


def extract_trigger_summary(triggers: dict) -> list[dict]:
    rows = []
    for name, trigger in triggers.items():
        rows.append(
            {
                "name": name,
                "type": trigger.get("type", ""),
                "kind": trigger.get("kind", ""),
                "inputs": trigger.get("inputs", {}),
            }
        )
    return rows


def extract_actions_and_fields(actions: dict) -> tuple[list[dict], list[str]]:
    found_fields: set[str] = set()
    action_rows: list[dict] = []

    _collect_actions(actions, action_rows, found_fields)

    return action_rows, sorted(found_fields)
