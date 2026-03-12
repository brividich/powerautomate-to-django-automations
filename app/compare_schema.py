from __future__ import annotations

from pathlib import Path
import csv


def load_schema_columns(schema_dir: Path) -> set[str]:
    csv_path = schema_dir / "columns.csv"
    if not csv_path.exists():
        return set()

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return {row["column_name"] for row in reader if row.get("column_name")}


def compare_fields_to_schema(fields_used: list[str], schema_dir: Path) -> dict:
    known_columns = load_schema_columns(schema_dir)

    direct_matches = []
    unmatched = []

    for field in fields_used:
        if field in known_columns:
            direct_matches.append(field)
        else:
            unmatched.append(field)

    return {
        "direct_matches": sorted(direct_matches),
        "unmatched_fields": sorted(unmatched),
    }