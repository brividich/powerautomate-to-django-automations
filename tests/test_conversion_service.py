from __future__ import annotations

from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from conversion_service import analyze_flow_upload, apply_recommended_remediation


class ConversionServiceTests(unittest.TestCase):
    def test_apply_recommended_remediation_adds_audit_rule(self):
        flow_path = (
            Path(__file__).resolve().parents[1]
            / "input"
            / "flows"
            / "inserimentoassenze_20260311180852.zip"
        )
        record = analyze_flow_upload(flow_path.name, flow_path.read_bytes())

        updated = apply_recommended_remediation(record)

        self.assertGreaterEqual(len(updated["remediations_applied"]), 2)
        rule_codes = [row["code"] for row in updated["package"]["proposed_rules"]]
        self.assertIn("pa-assenze-insert-skip-approval-audit", rule_codes)


if __name__ == "__main__":
    unittest.main()
