from __future__ import annotations

from pathlib import Path
import json

from parse_flow import load_flow_definition
from extract_logic import extract_trigger_summary, extract_actions_and_fields
from compare_schema import compare_fields_to_schema
from build_preview import build_preview
from build_automation_package import build_automation_package


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"


def ensure_dirs() -> None:
    (OUTPUT_DIR / "normalized").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "previews").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "packages").mkdir(parents=True, exist_ok=True)


def process_flow_file(flow_file: Path) -> None:
    flow = load_flow_definition(flow_file)

    triggers = extract_trigger_summary(flow["triggers"])
    actions, fields_used = extract_actions_and_fields(flow["actions"])
    schema_match = compare_fields_to_schema(fields_used, INPUT_DIR / "schema_pack")

    result = {
        "flow_name": flow["flow_name"],
        "flow_slug": flow["flow_slug"],
        "triggers": triggers,
        "actions": actions,
        "fields_used": fields_used,
        "direct_matches": schema_match["direct_matches"],
        "unmatched_fields": schema_match["unmatched_fields"],
    }

    normalized_path = OUTPUT_DIR / "normalized" / f"{flow['flow_slug']}.json"
    preview_path = OUTPUT_DIR / "previews" / f"{flow['flow_slug']}.md"
    package_path = OUTPUT_DIR / "packages" / f"{flow['flow_slug']}.automation_package.json"
    automation_package = build_automation_package(flow, input_path=flow_file)

    normalized_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    preview_path.write_text(
        build_preview(result),
        encoding="utf-8",
    )
    package_path.write_text(
        json.dumps(automation_package, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"[OK] {flow_file.name}")
    print(f"     JSON: {normalized_path}")
    print(f"     MD  : {preview_path}")
    print(f"     PKG : {package_path}")


def main() -> None:
    ensure_dirs()

    flows_dir = INPUT_DIR / "flows"
    if not flows_dir.exists():
        raise FileNotFoundError(f"Cartella non trovata: {flows_dir}")

    files = [p for p in flows_dir.iterdir() if p.suffix.lower() in {".json", ".zip"}]
    if not files:
        print("Nessun file .json o .zip trovato in input/flows")
        return

    for flow_file in sorted(files):
        try:
            process_flow_file(flow_file)
        except Exception as exc:
            print(f"[ERRORE] {flow_file.name}: {exc}")


if __name__ == "__main__":
    main()
