from __future__ import annotations

from collections import defaultdict
from typing import Any


NODE_WIDTH = 228
NODE_HEIGHT = 84
X_GAP = 278
Y_GAP = 124
LEFT_PAD = 40
TOP_PAD = 36
LANE_PAD = 18


TYPE_STYLES = {
    "trigger": {"fill": "#dff7f1", "stroke": "#0f766e", "icon": "TR"},
    "ApiConnectionWebhook": {"fill": "#fee2e2", "stroke": "#991b1b", "icon": "AP"},
    "Until": {"fill": "#fef3c7", "stroke": "#92400e", "icon": "UL"},
    "If": {"fill": "#ede9fe", "stroke": "#5b21b6", "icon": "IF"},
    "Switch": {"fill": "#dbeafe", "stroke": "#1d4ed8", "icon": "SW"},
    "ApiConnection": {"fill": "#e0f2fe", "stroke": "#0369a1", "icon": "API"},
    "OpenApiConnection": {"fill": "#dbeafe", "stroke": "#1d4ed8", "icon": "API"},
}

BRANCH_LANE_STYLES = {
    "main": {"fill": "#fffdf8", "stroke": "#d9d1c2", "label": "Main"},
    "else": {"fill": "#fef2f2", "stroke": "#fecaca", "label": "Else"},
    "default": {"fill": "#eff6ff", "stroke": "#bfdbfe", "label": "Default"},
}


def _node_style(node_type: str) -> dict[str, str]:
    return TYPE_STYLES.get(node_type, {"fill": "#fffdf8", "stroke": "#4b5563", "icon": "AC"})


def _branch_style(branch: str) -> dict[str, str]:
    if branch.startswith("case:"):
        return {"fill": "#eef2ff", "stroke": "#c7d2fe", "label": branch}
    return BRANCH_LANE_STYLES.get(branch, {"fill": "#fffdf8", "stroke": "#d9d1c2", "label": branch})


def _shorten(value: str, *, max_len: int = 28) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "..."


def _split_lines(name: str, node_type: str) -> list[str]:
    primary = _shorten(name.replace("_", " "))
    secondary = _shorten(node_type or "Azione", max_len=24)
    return [primary, secondary]


def _issue_badge(name: str, node_type: str) -> str:
    if node_type == "ApiConnectionWebhook":
        return "approval"
    if node_type == "Until":
        return "loop"
    if name.startswith("Crea_elemento"):
        return "create"
    if "Imposta_stato_di_approvazione_del_contenuto" in name:
        return "sync"
    return ""


def build_flow_diagram(triggers: list[dict[str, Any]], actions: list[dict[str, Any]]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    lanes: list[dict[str, Any]] = []
    node_lookup: dict[str, dict[str, Any]] = {}
    branch_rows: dict[str, int] = {}
    lane_nodes: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    trigger_ids: list[str] = []

    row_counter = 0
    for trigger in triggers:
        branch = "main"
        if branch not in branch_rows:
            branch_rows[branch] = row_counter
            row_counter += 1
        node_id = f"trigger:{trigger.get('name')}"
        x = LEFT_PAD
        y = TOP_PAD + branch_rows[branch] * Y_GAP
        style = _node_style("trigger")
        node = {
            "id": node_id,
            "name": str(trigger.get("name") or "Trigger"),
            "type": "trigger",
            "kind": str(trigger.get("kind") or ""),
            "branch": branch,
            "depth": 0,
            "x": x,
            "y": y,
            "width": NODE_WIDTH,
            "height": NODE_HEIGHT,
            "lines": _split_lines(str(trigger.get("name") or "Trigger"), "Trigger"),
            "issue_badge": "",
            **style,
        }
        nodes.append(node)
        lane_nodes[branch].append(node)
        node_lookup[node_id] = node
        trigger_ids.append(node_id)

    for action in actions:
        depth = int(action.get("depth") or 0)
        branch = str(action.get("branch") or "main")
        if branch not in branch_rows:
            branch_rows[branch] = row_counter
            row_counter += 1

        x = LEFT_PAD + (depth + 1) * X_GAP
        y = TOP_PAD + branch_rows[branch] * Y_GAP
        style = _node_style(str(action.get("type") or "Azione"))

        node_id = f"action:{action.get('name')}"
        node = {
            "id": node_id,
            "name": str(action.get("name") or "Azione"),
            "type": str(action.get("type") or "Azione"),
            "kind": str(action.get("kind") or ""),
            "branch": branch,
            "depth": depth + 1,
            "x": x,
            "y": y,
            "width": NODE_WIDTH,
            "height": NODE_HEIGHT,
            "lines": _split_lines(str(action.get("name") or "Azione"), str(action.get("type") or "Azione")),
            "issue_badge": _issue_badge(str(action.get("name") or ""), str(action.get("type") or "")),
            **style,
        }
        nodes.append(node)
        lane_nodes[branch].append(node)
        node_lookup[node_id] = node

        parent_name = str(action.get("parent") or "")
        if parent_name:
            source_id = f"action:{parent_name}"
            if source_id in node_lookup:
                edges.append({"from": source_id, "to": node_id, "label": "" if branch == "main" else branch})
                continue

        run_after = action.get("run_after")
        if isinstance(run_after, dict) and run_after:
            for predecessor in run_after.keys():
                source_id = f"action:{predecessor}"
                if source_id in node_lookup:
                    edges.append({"from": source_id, "to": node_id, "label": ""})
        elif trigger_ids:
            edges.append({"from": trigger_ids[0], "to": node_id, "label": ""})

    for branch, branch_index in branch_rows.items():
        branch_style = _branch_style(branch)
        branch_y = TOP_PAD + branch_index * Y_GAP - LANE_PAD
        lanes.append(
            {
                "branch": branch,
                "label": branch_style["label"],
                "x": LEFT_PAD - 18,
                "y": branch_y,
                "width": max((node["x"] + node["width"] for node in lane_nodes[branch]), default=LEFT_PAD + NODE_WIDTH),
                "height": NODE_HEIGHT + LANE_PAD * 2,
                "fill": branch_style["fill"],
                "stroke": branch_style["stroke"],
            }
        )

    for edge in edges:
        source = node_lookup.get(edge["from"])
        target = node_lookup.get(edge["to"])
        if not source or not target:
            continue
        edge["x1"] = int(source["x"] + source["width"])
        edge["y1"] = int(source["y"] + source["height"] / 2)
        edge["x2"] = int(target["x"])
        edge["y2"] = int(target["y"] + target["height"] / 2)
        edge["label_x"] = int((edge["x1"] + edge["x2"]) / 2)
        edge["label_y"] = int((edge["y1"] + edge["y2"]) / 2) - 8

    max_x = max((node["x"] + node["width"] for node in nodes), default=LEFT_PAD + NODE_WIDTH)
    max_y = max((node["y"] + node["height"] for node in nodes), default=TOP_PAD + NODE_HEIGHT)

    for lane in lanes:
        lane["width"] = int(max_x - lane["x"] + LEFT_PAD)

    return {
        "nodes": nodes,
        "edges": edges,
        "lanes": lanes,
        "width": int(max_x + LEFT_PAD + 24),
        "height": int(max_y + TOP_PAD + 24),
    }
