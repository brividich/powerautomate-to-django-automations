from __future__ import annotations


def build_preview(result: dict) -> str:
    lines = []
    lines.append(f"# Preview flow: {result['flow_name']}")
    lines.append("")

    lines.append("## Trigger trovati")
    if result["triggers"]:
        for row in result["triggers"]:
            lines.append(f"- **{row['name']}** - type: `{row['type']}` kind: `{row['kind']}`")
    else:
        lines.append("- Nessun trigger trovato")
    lines.append("")

    lines.append("## Azioni trovate")
    if result["actions"]:
        for row in result["actions"]:
            indent = "  " * row.get("depth", 0)
            branch = row.get("branch", "main")
            parent = row.get("parent")
            extras = [f"branch: `{branch}`"]
            if parent:
                extras.append(f"parent: `{parent}`")
            if row.get("kind"):
                extras.append(f"kind: `{row['kind']}`")
            lines.append(
                f"- {indent}**{row['name']}** - type: `{row['type']}` ({', '.join(extras)})"
            )
    else:
        lines.append("- Nessuna azione trovata")
    lines.append("")

    lines.append("## Campi usati")
    if result["fields_used"]:
        for field in result["fields_used"]:
            lines.append(f"- `{field}`")
    else:
        lines.append("- Nessun campo rilevato")
    lines.append("")

    lines.append("## Match con schema pack")
    lines.append("")
    lines.append("### Match diretti")
    if result["direct_matches"]:
        for field in result["direct_matches"]:
            lines.append(f"- `{field}`")
    else:
        lines.append("- Nessun match diretto")
    lines.append("")

    lines.append("### Campi non trovati")
    if result["unmatched_fields"]:
        for field in result["unmatched_fields"]:
            lines.append(f"- `{field}`")
    else:
        lines.append("- Nessun campo non trovato")
    lines.append("")

    return "\n".join(lines)
