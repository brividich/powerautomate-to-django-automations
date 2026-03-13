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
    def test_saved_profile_is_loaded_when_runtime_cache_is_empty(self):
        with TemporaryDirectory() as tmp_dir:
            profile_path = Path(tmp_dir) / "sqlserver_profile.json"
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

            with patch.object(runtime_store, "LOCAL_PROFILE_PATH", profile_path):
                runtime_store.clear_connection("abc")
                runtime_store.save_saved_profile(config)

                loaded = runtime_store.load_connection("abc")

                self.assertEqual(loaded["server"], r"localhost\SQLEXPRESS")
                self.assertTrue(profile_path.exists())


if __name__ == "__main__":
    unittest.main()
