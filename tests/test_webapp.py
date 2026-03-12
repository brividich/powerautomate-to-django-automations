from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
import unittest


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

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

    def test_convert_creates_history_and_redirects_to_detail(self):
        flow_path = (
            Path(__file__).resolve().parents[1]
            / "input"
            / "flows"
            / "inserimentoassenze_20260311180852.zip"
        )

        response = self.client.post(
            "/convert",
            data={"flow_file": (BytesIO(flow_path.read_bytes()), flow_path.name)},
            content_type="multipart/form-data",
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/conversions/", response.headers["Location"])


if __name__ == "__main__":
    unittest.main()
