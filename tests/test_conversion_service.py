from __future__ import annotations

from io import BytesIO
from pathlib import Path
import json
import sys
import unittest
import zipfile


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from conversion_service import analyze_flow_upload, apply_recommended_remediation
from fixtures import sample_power_automate_payload


class ConversionServiceTests(unittest.TestCase):
    def test_analyze_flow_upload_tolerates_non_dict_blocks(self):
        payload_buffer = BytesIO()
        malformed_workflow = {
            "name": "newOP",
            "properties": {
                "displayName": "newOP",
                "connectionReferences": {
                    "shared_sharepointonline": "unexpected-string-reference",
                },
                "definition": {
                    "triggers": {
                        "manual": "unexpected-string-trigger",
                    },
                    "actions": {
                        "Compose_1": "unexpected-string-action",
                    },
                },
            },
        }

        with zipfile.ZipFile(payload_buffer, "w") as zf:
            zf.writestr("workflow.json", json.dumps(malformed_workflow))

        record = analyze_flow_upload("newOP.zip", payload_buffer.getvalue())

        self.assertEqual(record["normalized"]["flow_name"], "newOP")
        self.assertEqual(record["package"]["action_summary"]["flattened_action_count"], 0)
        self.assertEqual(record["package"]["connectors"], [])

    def test_apply_recommended_remediation_adds_audit_rule(self):
        record = analyze_flow_upload("sample.zip", sample_power_automate_payload())

        updated = apply_recommended_remediation(record)

        self.assertGreaterEqual(len(updated["remediations_applied"]), 2)
        rule_codes = [row["code"] for row in updated["package"]["proposed_rules"]]
        self.assertIn("pa-assenze-insert-skip-approval-audit", rule_codes)
        self.assertIn("pa-assenze-insert-skip-approval-audit", updated["package"]["selected_proposed_rule_codes"])

    def test_analyze_flow_upload_builds_diagram_model(self):
        record = analyze_flow_upload("sample.zip", sample_power_automate_payload())

        diagram = record["normalized"]["diagram"]

        self.assertGreaterEqual(len(diagram["nodes"]), 2)
        self.assertGreaterEqual(len(diagram["edges"]), 1)
        self.assertGreaterEqual(len(diagram["lanes"]), 1)
        self.assertTrue(any(node["issue_badge"] for node in diagram["nodes"] if node["type"] != "trigger"))
        self.assertGreater(diagram["width"], 0)
        self.assertGreater(diagram["height"], 0)

    def test_analyze_flow_upload_selects_all_rules_by_default(self):
        record = analyze_flow_upload("sample.zip", sample_power_automate_payload())

        self.assertEqual(
            sorted(record["package"]["selected_proposed_rule_codes"]),
            sorted(rule["code"] for rule in record["package"]["proposed_rules"]),
        )
        self.assertEqual(len(record["package"]["selected_proposed_rules"]), len(record["package"]["proposed_rules"]))

    def test_analyze_flow_upload_with_target_context_adds_visual_mapping(self):
        target_context = {
            "db_type": "sqlserver",
            "server": "sql01",
            "database": "hr",
            "schema": "staging",
            "table": "richieste_import",
            "full_name": "staging.richieste_import",
            "columns": [
                {"name": "start_date", "data_type": "date", "is_nullable": False, "ordinal_position": 1, "is_primary_key": False},
                {"name": "end_date", "data_type": "date", "is_nullable": False, "ordinal_position": 2, "is_primary_key": False},
                {"name": "request_email", "data_type": "nvarchar", "is_nullable": False, "ordinal_position": 3, "is_primary_key": False},
            ],
        }

        record = analyze_flow_upload("sample.zip", sample_power_automate_payload(), target_context=target_context)

        self.assertEqual(record["package"]["target_context"]["full_name"], "staging.richieste_import")
        self.assertEqual(record["package"]["runtime_source_catalog"]["source_code"], "assenze")
        self.assertEqual(record["package"]["field_mapping_candidates"]["Data_x0020_inizio"]["target_field"], "data_inizio")
        self.assertEqual(record["package"]["runtime_field_mapping_candidates"]["EmailDipendente"]["target_field"], "dipendente_email")
        self.assertEqual(record["package"]["approved_runtime_field_mapping"]["Salta_x0020_approvazione"]["target_field"], "salta_approvazione")
        self.assertEqual(record["package"]["runtime_field_mapping_candidates"]["EmailDipendente"]["mapping_scope"], "runtime_source")
        self.assertEqual(record["package"]["target_field_mapping_candidates"]["Data_x0020_inizio"]["mapping_scope"], "target_table")
        self.assertIn("target_table_field_mapping_candidates", record["package"])
        self.assertIn("approved_target_field_mapping", record["package"])
        self.assertNotEqual(
            record["package"]["target_field_mapping_candidates"]["Data_x0020_inizio"]["target_field"],
            record["package"]["field_mapping_candidates"]["Data_x0020_inizio"]["target_field"],
        )
        self.assertEqual(len(record["package"]["table_columns"]), 3)


if __name__ == "__main__":
    unittest.main()
