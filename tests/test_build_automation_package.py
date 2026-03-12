from __future__ import annotations

from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from build_automation_package import build_automation_package
from parse_flow import load_flow_definition


class BuildAutomationPackageTests(unittest.TestCase):
    def test_real_power_automate_zip_produces_partial_package_with_rules(self):
        flow_path = (
            Path(__file__).resolve().parents[1]
            / "input"
            / "flows"
            / "inserimentoassenze_20260311180852.zip"
        )

        flow = load_flow_definition(flow_path)
        package = build_automation_package(flow, input_path=flow_path)

        self.assertEqual(package["source_candidate"]["source_code"], "assenze")
        self.assertEqual(package["compatibility"]["status"], "partial")
        self.assertGreaterEqual(len(package["issues"]), 4)
        self.assertEqual(len(package["proposed_rules"]), 3)


if __name__ == "__main__":
    unittest.main()
