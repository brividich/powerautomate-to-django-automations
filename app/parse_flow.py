from __future__ import annotations

from pathlib import Path
import io
import json
import zipfile
from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _find_workflow_json_in_zip(zip_path: Path) -> dict:
    with zipfile.ZipFile(zip_path, "r") as zf:
        candidates = [n for n in zf.namelist() if n.lower().endswith(".json")]

        for name in candidates:
            raw = zf.read(name)
            try:
                data = json.loads(raw.decode("utf-8"))
            except Exception:
                continue

            if not isinstance(data, dict):
                continue

            if "definition" in data:
                return data

            if isinstance(data.get("properties"), dict) and "definition" in data["properties"]:
                return data

            if "resources" in data:
                resources = data.get("resources", {})
                if isinstance(resources, list):
                    for res in resources:
                        if isinstance(res, dict) and res.get("type") == "Microsoft.Logic/workflows":
                            return data
                elif isinstance(resources, dict):
                    for res in resources.values():
                        if isinstance(res, dict) and res.get("type") in {"Microsoft.Logic/workflows", "Microsoft.Flow/flows"}:
                            return data

    raise ValueError(f"Nessun workflow JSON valido trovato in {zip_path.name}")


def _normalize_arm_or_workflow(data: dict) -> dict:
    if "definition" in data:
        return {
            "name": data.get("name", "flow_sconosciuto"),
            "definition": _as_dict(data.get("definition")),
            "raw": data,
        }

    if isinstance(data.get("properties"), dict) and "definition" in data["properties"]:
        props = data["properties"]
        return {
            "name": props.get("displayName") or data.get("name", "flow_sconosciuto"),
            "definition": _as_dict(props.get("definition")),
            "raw": data,
        }

    resources = data.get("resources", {})
    iterable = resources if isinstance(resources, list) else resources.values()

    for res in iterable:
        if not isinstance(res, dict):
            continue
        if res.get("type") in {"Microsoft.Logic/workflows", "Microsoft.Flow/flows"}:
            props = _as_dict(res.get("properties"))
            details = _as_dict(res.get("details"))
            return {
                "name": details.get("displayName") or res.get("name", "flow_sconosciuto"),
                "definition": _as_dict(props.get("definition")),
                "raw": data,
            }

    raise ValueError("Workflow definition non trovata")


def _build_flow_payload(wf: dict[str, Any]) -> dict[str, Any]:
    definition = _as_dict(wf.get("definition"))
    flow_name = str(wf.get("name") or "flow_sconosciuto")

    return {
        "flow_name": flow_name,
        "flow_slug": (
            flow_name
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
        ),
        "triggers": _as_dict(definition.get("triggers")),
        "actions": _as_dict(definition.get("actions")),
        "raw": _as_dict(wf.get("raw")),
    }


def load_flow_definition(path: Path) -> dict:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
    elif path.suffix.lower() == ".zip":
        data = _find_workflow_json_in_zip(path)
    else:
        raise ValueError(f"Formato non supportato: {path.suffix}")

    wf = _normalize_arm_or_workflow(data)
    return _build_flow_payload(wf)


def load_flow_definition_from_bytes(filename: str, payload: bytes) -> dict:
    suffix = Path(filename).suffix.lower()
    if suffix == ".json":
        data = json.loads(payload.decode("utf-8"))
    elif suffix == ".zip":
        with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
            candidates = [n for n in zf.namelist() if n.lower().endswith(".json")]
            data = None
            for name in candidates:
                raw = zf.read(name)
                try:
                    candidate = json.loads(raw.decode("utf-8"))
                except Exception:
                    continue
                if not isinstance(candidate, dict):
                    continue
                if "definition" in candidate:
                    data = candidate
                    break
                if isinstance(candidate.get("properties"), dict) and "definition" in candidate["properties"]:
                    data = candidate
                    break
                if "resources" in candidate:
                    resources = candidate.get("resources", {})
                    iterable = resources if isinstance(resources, list) else resources.values()
                    for res in iterable:
                        if isinstance(res, dict) and res.get("type") in {"Microsoft.Logic/workflows", "Microsoft.Flow/flows"}:
                            data = candidate
                            break
                    if data is not None:
                        break
            if data is None:
                raise ValueError(f"Nessun workflow JSON valido trovato in {filename}")
    else:
        raise ValueError(f"Formato non supportato: {suffix}")

    wf = _normalize_arm_or_workflow(data)
    return _build_flow_payload(wf)
