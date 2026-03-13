from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import runtime_store


class RuntimeStoreTests(unittest.TestCase):
    def test_saved_profile_ini_is_loaded_when_runtime_cache_is_empty(self):
        with TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "sqlserver_profile.ini"
            legacy_profile_path = Path(tmp_dir) / "sqlserver_profile.json"
            config = {
                "driver": "ODBC Driver 18 for SQL Server",
                "server": r"localhost\SQLEXPRESS",
                "database": "PORTALE_NOVICROM",
                "username": "",
                "password": "",
                "port": "",
                "encrypt": True,
                "trust_server_certificate": True,
                "integrated_security": True,
            }

            with (
                patch.object(runtime_store, "LOCAL_PROFILE_PATH", profile_path),
                patch.object(runtime_store, "LEGACY_LOCAL_PROFILE_PATH", legacy_profile_path),
            ):
                runtime_store.clear_connection("abc")
                runtime_store.save_saved_profile(config)

                loaded = runtime_store.load_connection("abc")

                self.assertEqual(loaded["server"], r"localhost\SQLEXPRESS")
                self.assertTrue(loaded["integrated_security"])
                self.assertTrue(profile_path.exists())
                self.assertFalse(legacy_profile_path.exists())

    def test_legacy_json_profile_is_migrated_to_ini(self):
        with TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "sqlserver_profile.ini"
            legacy_profile_path = Path(tmp_dir) / "sqlserver_profile.json"
            config = {
                "driver": "ODBC Driver 18 for SQL Server",
                "server": "sql01",
                "database": "hr",
                "username": "sa",
                "password": "secret",
                "port": "1433",
                "encrypt": True,
                "trust_server_certificate": False,
                "integrated_security": False,
            }

            legacy_profile_path.write_text(runtime_store.json.dumps(config, indent=2), encoding="utf-8")

            with (
                patch.object(runtime_store, "LOCAL_PROFILE_PATH", profile_path),
                patch.object(runtime_store, "LEGACY_LOCAL_PROFILE_PATH", legacy_profile_path),
            ):
                runtime_store.clear_connection("legacy")
                loaded = runtime_store.load_connection("legacy")

                self.assertEqual(loaded["server"], "sql01")
                self.assertEqual(loaded["password"], "secret")
                self.assertFalse(loaded["integrated_security"])
                self.assertTrue(profile_path.exists())
                self.assertFalse(legacy_profile_path.exists())


if __name__ == "__main__":
    unittest.main()
