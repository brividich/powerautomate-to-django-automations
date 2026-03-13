from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from fixtures import sample_power_automate_payload
from webapp import create_app


class WebAppTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.testing = True
        self.client = self.app.test_client()

    def test_index_renders(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Conversion Studio", response.data)
        self.assertIn(b"Configura SQL Server", response.data)

    def test_sqlserver_connect_rejects_instance_and_port_together(self):
        response = self.client.post(
            "/wizard/sqlserver/connect",
            data={
                "driver": "SQL Server",
                "server": r"localhost\SQLEXPRESS",
                "port": "1433",
                "database": "PORTALE_NOVICROM",
                "auth_mode": "integrated",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn(b"SQLEXPRESS", response.data)

    @patch("webapp.sqlserver_service.list_columns")
    @patch("webapp.sqlserver_service.list_tables")
    @patch("webapp.sqlserver_service.test_connection")
    def test_guided_sqlserver_flow_enables_targeted_conversion(
        self,
        mock_test_connection,
        mock_list_tables,
        mock_list_columns,
    ):
        mock_test_connection.return_value = {"server_name": "sql01", "database_name": "hr"}
        mock_list_tables.return_value = [
            {"schema": "dbo", "table": "assenze", "column_count": 5, "full_name": "dbo.assenze", "table_type": "BASE TABLE"}
        ]
        mock_list_columns.return_value = [
            {"name": "data_inizio", "data_type": "date", "is_nullable": False, "ordinal_position": 1, "is_primary_key": False},
            {"name": "data_fine", "data_type": "date", "is_nullable": False, "ordinal_position": 2, "is_primary_key": False},
            {"name": "tipo_assenza", "data_type": "nvarchar", "is_nullable": False, "ordinal_position": 3, "is_primary_key": False},
        ]

        response = self.client.post(
            "/wizard/sqlserver/connect",
            data={
                "driver": "ODBC Driver 18 for SQL Server",
                "server": "sql01",
                "database": "hr",
                "auth_mode": "sql",
                "username": "sa",
                "password": "secret",
                "encrypt": "on",
                "trust_server_certificate": "on",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/wizard/sqlserver/tables", response.headers["Location"])

        response = self.client.get("/wizard/sqlserver/tables")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"assenze", response.data)

        response = self.client.post(
            "/wizard/sqlserver/select-table",
            data={"table_name": "dbo|assenze"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/wizard/convert", response.headers["Location"])

        response = self.client.post(
            "/convert",
            data={"flow_file": (BytesIO(sample_power_automate_payload()), "sample.zip")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/conversions/", response.headers["Location"])

    @patch("webapp.learn_from_approved_mappings")
    def test_mapping_save_persists_user_choices(self, mock_learn):
        response = self.client.post(
            "/convert",
            data={"flow_file": (BytesIO(sample_power_automate_payload()), "sample.zip")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        detail_url = response.headers["Location"]
        record_id = detail_url.rstrip("/").split("/")[-1]

        # Inject a target table so the visual mapping form is enabled.
        from conversion_store import load_record, save_record

        record = load_record(record_id)
        record["package"]["target_context"] = {
            "database": "hr",
            "schema": "dbo",
            "table": "assenze",
            "full_name": "dbo.assenze",
            "column_count": 2,
        }
        record["package"]["table_columns"] = [
            {"name": "data_inizio", "data_type": "date", "is_nullable": False, "ordinal_position": 1, "is_primary_key": False},
            {"name": "tipo_assenza", "data_type": "nvarchar", "is_nullable": False, "ordinal_position": 2, "is_primary_key": False},
        ]
        save_record(record)

        response = self.client.post(
            f"/conversions/{record_id}/mapping",
            data={
                "source_fields": ["Data_x0020_inizio", "Tipoassenza"],
                "target_mapping__Data_x0020_inizio": "data_inizio",
                "target_mapping__Tipoassenza": "tipo_assenza",
                "runtime_mapping__Data_x0020_inizio": "data_inizio",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        updated = load_record(record_id)
        self.assertEqual(updated["package"]["approved_field_mapping"]["Data_x0020_inizio"]["target_field"], "data_inizio")
        self.assertEqual(updated["package"]["approved_target_field_mapping"]["Tipoassenza"]["target_field"], "tipo_assenza")
        self.assertEqual(updated["package"]["approved_runtime_field_mapping"]["Data_x0020_inizio"]["target_field"], "data_inizio")
        self.assertEqual(len(updated["package"]["mapping_warnings"]), 1)
        mock_learn.assert_called_once()

    def test_conversion_detail_renders_flow_diagram(self):
        response = self.client.post(
            "/convert",
            data={"flow_file": (BytesIO(sample_power_automate_payload()), "sample.zip")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        detail_url = response.headers["Location"]

        response = self.client.get(detail_url)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Diagramma del Flusso", response.data)
        self.assertIn(b"<svg", response.data)

    def test_rule_selection_persists_and_is_exported(self):
        response = self.client.post(
            "/convert",
            data={"flow_file": (BytesIO(sample_power_automate_payload()), "sample.zip")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        record_id = response.headers["Location"].rstrip("/").split("/")[-1]

        from conversion_store import load_record

        record = load_record(record_id)
        rule_codes = [rule["code"] for rule in record["package"]["proposed_rules"]]
        selected_code = rule_codes[0]

        response = self.client.post(
            f"/conversions/{record_id}/rules",
            data={"selected_rule_codes": [selected_code]},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        updated = load_record(record_id)
        self.assertEqual(updated["package"]["selected_proposed_rule_codes"], [selected_code])
        self.assertEqual(len(updated["package"]["selected_proposed_rules"]), 1)
        self.assertEqual(updated["package"]["selected_proposed_rules"][0]["code"], selected_code)

        response = self.client.get(f"/conversions/{record_id}/package.json")
        self.assertEqual(response.status_code, 200)
        _ = response.get_data()
        response.close()

        refreshed = load_record(record_id)
        self.assertEqual(refreshed["package"]["selected_proposed_rule_codes"], [selected_code])


if __name__ == "__main__":
    unittest.main()
