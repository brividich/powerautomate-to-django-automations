from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from build_automation_package import build_automation_package
from parse_flow import load_flow_definition
from fixtures import sample_power_automate_payload


class BuildAutomationPackageTests(unittest.TestCase):
    def test_real_power_automate_zip_produces_partial_package_with_rules(self):
        with NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp.write(sample_power_automate_payload())
            flow_path = Path(tmp.name)

        try:
            flow = load_flow_definition(flow_path)
            package = build_automation_package(
                flow,
                input_path=flow_path,
                approval_template={
                    "code": "tpl-approval-hybrid",
                    "name": "Template Approval Hybrid",
                    "delivery_mode": "hybrid",
                },
            )
        finally:
            flow_path.unlink(missing_ok=True)

        self.assertEqual(package["source_candidate"]["source_code"], "assenze")
        self.assertEqual(package["compatibility"]["status"], "partial")
        self.assertGreaterEqual(len(package["issues"]), 4)
        self.assertEqual(len(package["proposed_rules"]), 1)
        self.assertIn("runtime_source_catalog", package)
        self.assertEqual(package["portal_profile"]["code"], "novicrom")
        self.assertTrue(package["workflow_capabilities"]["approval"]["managed_by_portal"])
        self.assertIn("field_mapping_candidates", package)
        self.assertIn("runtime_field_mapping_candidates", package)
        self.assertIn("approved_runtime_field_mapping", package)
        self.assertIn("approved_target_field_mapping", package)
        self.assertEqual(package["field_mapping_candidates"]["Data_x0020_inizio"]["target_field"], "data_inizio")
        self.assertEqual(package["field_mapping_candidates"]["Data_x0020_inizio"]["mapping_scope"], "runtime_source")
        self.assertEqual(package["runtime_field_mapping_candidates"]["EmailDipendente"]["target_field"], "dipendente_email")
        self.assertEqual(package["runtime_source_catalog"]["source_code"], "assenze")
        self.assertIn("salta_approvazione", package["runtime_supported_fields"])
        self.assertIn("approval_conversion", package)
        self.assertEqual(package["approval_conversion"]["strategy"], "send_approval")
        self.assertEqual(package["approval_conversion"]["template_code"], "tpl-approval-hybrid")
        self.assertEqual(package["approval_conversion"]["template_delivery_mode"], "hybrid")
        self.assertEqual(package["approval_conversion"]["approver_template"], "{capo_email}")
        self.assertEqual(package["proposed_rules"][0]["actions"][0]["action_type"], "send_approval")
        self.assertEqual(
            package["proposed_rules"][0]["actions"][0]["config_json"]["approval_email_template_code"],
            "tpl-approval-hybrid",
        )
        issue_codes = {issue["code"] for issue in package["issues"]}
        self.assertIn("approval-branch-manual-review", issue_codes)
        self.assertNotIn("approval-delegated-to-portal", issue_codes)


if __name__ == "__main__":
    unittest.main()
