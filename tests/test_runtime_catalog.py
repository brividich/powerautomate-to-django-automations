from __future__ import annotations

from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from runtime_catalog import get_runtime_source_catalog, has_native_approval_workflow, supported_runtime_sources


class RuntimeCatalogTests(unittest.TestCase):
    def test_supported_runtime_sources_include_required_portal_catalogs(self):
        supported_codes = {row["source_code"] for row in supported_runtime_sources()}
        self.assertTrue({"assenze", "tasks", "assets", "tickets", "anomalie"}.issubset(supported_codes))

    def test_catalog_exposes_runtime_fields_for_assenze(self):
        catalog = get_runtime_source_catalog("assenze")
        field_names = {field["name"] for field in catalog["fields"]}

        self.assertEqual(catalog["label"], "Assenze")
        self.assertEqual(catalog["portal_profile"]["code"], "novicrom")
        self.assertIn("dipendente_email", field_names)
        self.assertIn("salta_approvazione", field_names)
        self.assertTrue(has_native_approval_workflow("assenze"))

    def test_catalog_aligns_tasks_fields_with_novicrom_source_registry(self):
        catalog = get_runtime_source_catalog("tasks")
        field_names = {field["name"] for field in catalog["fields"]}

        self.assertIn("title", field_names)
        self.assertIn("assigned_to_id", field_names)
        self.assertIn("due_date", field_names)
        self.assertFalse(has_native_approval_workflow("tasks"))


if __name__ == "__main__":
    unittest.main()
