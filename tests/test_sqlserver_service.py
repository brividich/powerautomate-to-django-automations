from __future__ import annotations

from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from sqlserver_service import SqlServerConfig, build_connection_string


class SqlServerServiceTests(unittest.TestCase):
    def test_legacy_sql_server_driver_omits_modern_ssl_attributes(self):
        config = SqlServerConfig(
            driver="SQL Server",
            server=r"localhost\SQLEXPRESS",
            database="PORTALE_NOVICROM",
            integrated_security=True,
            encrypt=True,
            trust_server_certificate=True,
        )

        conn_str = build_connection_string(config)

        self.assertIn("DRIVER={SQL Server}", conn_str)
        self.assertNotIn("Encrypt=", conn_str)
        self.assertNotIn("TrustServerCertificate=", conn_str)


if __name__ == "__main__":
    unittest.main()
