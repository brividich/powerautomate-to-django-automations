from __future__ import annotations

from pathlib import Path
import json

from flask import Flask, abort, flash, redirect, render_template, request, send_file, url_for

from conversion_service import analyze_flow_upload, apply_recommended_remediation
from conversion_store import list_records, load_record, save_record


BASE_DIR = Path(__file__).resolve().parent


def create_app() -> Flask:
    app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
    app.config["SECRET_KEY"] = "powerautomate-to-django-automations-dev"

    @app.get("/")
    def index():
        return render_template("index.html", records=list_records())

    @app.post("/convert")
    def convert():
        uploaded = request.files.get("flow_file")
        if uploaded is None or not uploaded.filename:
            flash("Seleziona un file .zip o .json da analizzare.", "error")
            return redirect(url_for("index"))

        filename = str(uploaded.filename)
        if not filename.lower().endswith((".zip", ".json")):
            flash("Formato non supportato. Usa un export .zip o .json.", "error")
            return redirect(url_for("index"))

        payload = uploaded.read()
        try:
            record = analyze_flow_upload(filename, payload)
            save_record(record)
        except Exception as exc:
            flash(f"Analisi fallita: {exc}", "error")
            return redirect(url_for("index"))

        flash("Flow analizzato e conversione salvata nello storico.", "success")
        return redirect(url_for("conversion_detail", record_id=record["record_id"]))

    @app.get("/conversions/<record_id>")
    def conversion_detail(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)
        return render_template("detail.html", record=record)

    @app.post("/conversions/<record_id>/apply-remediation")
    def conversion_apply_remediation(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        updated = apply_recommended_remediation(record)
        save_record(updated)
        flash("Remediation automatica applicata al package di conversione.", "success")
        return redirect(url_for("conversion_detail", record_id=record_id))

    @app.get("/conversions/<record_id>/package.json")
    def conversion_package_download(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        package_path = BASE_DIR.parent / "output" / "history" / f"{record_id}.package.json"
        package_path.write_text(
            json.dumps(record.get("package", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return send_file(package_path, as_attachment=True, download_name=f"{record_id}.automation_package.json")

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, port=8787)
