from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import json
from uuid import uuid4

from flask import Flask, abort, flash, redirect, render_template, request, send_file, session, url_for

from conversion_service import analyze_flow_upload, apply_recommended_remediation
from conversion_store import list_records, load_record, save_record
from mapping_memory import learn_from_approved_mappings
from package_mapping import RUNTIME_SCOPE, TARGET_SCOPE, normalize_package_mappings
from runtime_store import clear_connection, clear_saved_profile, load_connection, save_connection, save_saved_profile
from sqlserver_service import SqlServerConfig
import sqlserver_service


BASE_DIR = Path(__file__).resolve().parent


def _session_token() -> str:
    token = str(session.get("runtime_token") or "").strip()
    if not token:
        token = uuid4().hex
        session["runtime_token"] = token
    return token


def _current_connection() -> dict[str, str] | None:
    return load_connection(_session_token())


def _selected_table() -> dict[str, str]:
    selected = session.get("selected_table")
    return selected if isinstance(selected, dict) else {}


def _wizard_state() -> dict[str, object]:
    connection = _current_connection() or {}
    selected_table = _selected_table()
    return {
        "has_connection": bool(connection),
        "connection": {
            "driver": str(connection.get("driver") or ""),
            "server": str(connection.get("server") or ""),
            "database": str(connection.get("database") or ""),
            "integrated_security": bool(connection.get("integrated_security")),
            "username": str(connection.get("username") or ""),
        },
        "selected_table": selected_table,
        "ready_for_upload": bool(connection and selected_table),
    }


def _build_target_context() -> dict[str, object] | None:
    connection = _current_connection()
    selected_table = _selected_table()
    if not connection or not selected_table:
        return None

    config = SqlServerConfig(**connection)
    columns = sqlserver_service.list_columns(
        config,
        schema=str(selected_table.get("schema") or ""),
        table=str(selected_table.get("table") or ""),
    )
    return {
        "db_type": "sqlserver",
        "server": str(connection.get("server") or ""),
        "database": str(connection.get("database") or ""),
        "schema": str(selected_table.get("schema") or ""),
        "table": str(selected_table.get("table") or ""),
        "full_name": str(selected_table.get("full_name") or ""),
        "columns": columns,
    }


def _table_key_from_record(record: dict) -> str:
    target_context = record.get("package", {}).get("target_context", {})
    if not isinstance(target_context, dict):
        return ""
    return ".".join(
        part
        for part in [
            str(target_context.get("database") or "").strip(),
            str(target_context.get("schema") or "").strip(),
            str(target_context.get("table") or "").strip(),
        ]
        if part
    )


def _render_sqlserver_form(*, form_data: dict[str, object], wizard: dict[str, object], status_code: int = 200):
    selected_driver = str(form_data.get("driver") or "")
    return (
        render_template(
            "wizard_sqlserver.html",
            drivers=sqlserver_service.available_drivers(),
            form_data=form_data,
            wizard=wizard,
            show_modern_security_options=sqlserver_service.supports_modern_security_options(selected_driver),
        ),
        status_code,
    )


def _refresh_selected_rules(package: dict[str, object]) -> None:
    proposed_rules = [row for row in package.get("proposed_rules", []) if isinstance(row, dict)]
    selected_codes = {
        str(code or "").strip()
        for code in package.get("selected_proposed_rule_codes", [])
        if str(code or "").strip()
    }
    available_codes = {str(rule.get("code") or "").strip() for rule in proposed_rules}

    if not selected_codes:
        selected_codes = available_codes
    else:
        selected_codes = {code for code in selected_codes if code in available_codes}

    package["selected_proposed_rule_codes"] = sorted(selected_codes)
    package["selected_proposed_rules"] = [
        rule for rule in proposed_rules if str(rule.get("code") or "").strip() in selected_codes
    ]


def _normalize_record_package(record: dict[str, object]) -> bool:
    package = record.get("package", {})
    if not isinstance(package, dict):
        return False

    before = json.dumps(package, sort_keys=True, ensure_ascii=False)
    normalize_package_mappings(package)
    after = json.dumps(package, sort_keys=True, ensure_ascii=False)
    return before != after


def create_app() -> Flask:
    app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
    app.config["SECRET_KEY"] = "powerautomate-to-django-automations-dev"

    @app.get("/")
    def index():
        return render_template("index.html", records=list_records(), wizard=_wizard_state())

    @app.post("/wizard/reset")
    def wizard_reset():
        clear_connection(_session_token())
        clear_saved_profile()
        session.pop("selected_table", None)
        flash("Procedura guidata azzerata e profilo locale rimosso.", "success")
        return redirect(url_for("index"))

    @app.get("/wizard/sqlserver")
    def wizard_sqlserver():
        current = _current_connection() or {}
        drivers = sqlserver_service.available_drivers()
        form_data = {
            "driver": str(current.get("driver") or (drivers[0] if drivers else "")),
            "server": str(current.get("server") or ""),
            "database": str(current.get("database") or ""),
            "username": str(current.get("username") or ""),
            "password": str(current.get("password") or ""),
            "port": str(current.get("port") or ""),
            "encrypt": bool(current.get("encrypt", True)),
            "trust_server_certificate": bool(current.get("trust_server_certificate", True)),
            "integrated_security": bool(current.get("integrated_security", False)),
            "remember_connection": True,
        }
        return _render_sqlserver_form(form_data=form_data, wizard=_wizard_state())

    @app.post("/wizard/sqlserver/connect")
    def wizard_sqlserver_connect():
        auth_mode = str(request.form.get("auth_mode") or "sql")
        form_data = {
            "driver": str(request.form.get("driver") or ""),
            "server": str(request.form.get("server") or "").strip(),
            "database": str(request.form.get("database") or "").strip(),
            "username": str(request.form.get("username") or "").strip(),
            "password": str(request.form.get("password") or ""),
            "port": str(request.form.get("port") or "").strip(),
            "encrypt": bool(request.form.get("encrypt")),
            "trust_server_certificate": bool(request.form.get("trust_server_certificate")),
            "integrated_security": auth_mode == "integrated",
            "remember_connection": bool(request.form.get("remember_connection")),
        }
        password = str(form_data["password"] or "")

        if not form_data["driver"] or not form_data["server"] or not form_data["database"]:
            flash("Compila driver, server e database prima di testare la connessione.", "error")
            return _render_sqlserver_form(form_data=form_data, wizard=_wizard_state(), status_code=400)

        if "\\" in form_data["server"] and form_data["port"]:
            flash(
                "Usa o un'istanza nominata come `localhost\\SQLEXPRESS` oppure una porta come `1433`, non entrambe insieme.",
                "error",
            )
            return _render_sqlserver_form(form_data=form_data, wizard=_wizard_state(), status_code=400)

        if not form_data["integrated_security"] and (not form_data["username"] or not password):
            flash("Per autenticazione SQL Server servono username e password.", "error")
            return _render_sqlserver_form(form_data=form_data, wizard=_wizard_state(), status_code=400)

        config = SqlServerConfig(
            driver=form_data["driver"],
            server=form_data["server"],
            database=form_data["database"],
            username=form_data["username"],
            password=password,
            port=form_data["port"],
            encrypt=form_data["encrypt"],
            trust_server_certificate=form_data["trust_server_certificate"],
            integrated_security=form_data["integrated_security"],
        )

        try:
            connection_info = sqlserver_service.test_connection(config)
        except Exception as exc:
            app.logger.exception("Errore connessione SQL Server")
            flash(f"Connessione fallita: {exc}", "error")
            return _render_sqlserver_form(form_data=form_data, wizard=_wizard_state(), status_code=400)

        save_connection(
            _session_token(),
            {
                "driver": config.driver,
                "server": config.server,
                "database": config.database,
                "username": config.username,
                "password": config.password,
                "port": config.port,
                "encrypt": config.encrypt,
                "trust_server_certificate": config.trust_server_certificate,
                "integrated_security": config.integrated_security,
            },
        )
        if form_data["remember_connection"]:
            save_saved_profile(
                {
                    "driver": config.driver,
                    "server": config.server,
                    "database": config.database,
                    "username": config.username,
                    "password": config.password,
                    "port": config.port,
                    "encrypt": config.encrypt,
                    "trust_server_certificate": config.trust_server_certificate,
                    "integrated_security": config.integrated_security,
                }
            )
        else:
            clear_saved_profile()
        session.pop("selected_table", None)
        flash(
            f"Connessione riuscita a {connection_info['server_name']} / {connection_info['database_name']}. Ora scegli la tabella target.",
            "success",
        )
        return redirect(url_for("wizard_sqlserver_tables"))

    @app.get("/wizard/sqlserver/tables")
    def wizard_sqlserver_tables():
        connection = _current_connection()
        if not connection:
            flash("Prima configura la connessione SQL Server.", "error")
            return redirect(url_for("wizard_sqlserver"))

        config = SqlServerConfig(**connection)
        try:
            tables = sqlserver_service.list_tables(config)
        except Exception as exc:
            app.logger.exception("Errore lettura tabelle SQL Server")
            flash(f"Impossibile leggere le tabelle: {exc}", "error")
            return redirect(url_for("wizard_sqlserver"))

        query = str(request.args.get("q") or "").strip().lower()
        if query:
            tables = [
                row for row in tables
                if query in str(row.get("schema") or "").lower() or query in str(row.get("table") or "").lower()
            ]

        return render_template(
            "wizard_tables.html",
            tables=tables,
            wizard=_wizard_state(),
            query=query,
        )

    @app.post("/wizard/sqlserver/select-table")
    def wizard_sqlserver_select_table():
        connection = _current_connection()
        if not connection:
            flash("Prima configura la connessione SQL Server.", "error")
            return redirect(url_for("wizard_sqlserver"))

        selection = str(request.form.get("table_name") or "")
        if "|" not in selection:
            flash("Seleziona una tabella valida.", "error")
            return redirect(url_for("wizard_sqlserver_tables"))

        schema_name, table_name = selection.split("|", 1)
        selected = {
            "schema": schema_name,
            "table": table_name,
            "full_name": f"{schema_name}.{table_name}",
        }
        session["selected_table"] = selected
        flash(f"Tabella target selezionata: {selected['full_name']}. Ora carica il flow Power Automate.", "success")
        return redirect(url_for("wizard_convert"))

    @app.get("/wizard/convert")
    def wizard_convert():
        try:
            target_context = _build_target_context()
        except Exception as exc:
            app.logger.exception("Errore preparazione target context")
            flash(f"Impossibile leggere le colonne della tabella selezionata: {exc}", "error")
            return redirect(url_for("wizard_sqlserver_tables"))
        if target_context is None:
            if not _current_connection():
                flash("Prima configura la connessione SQL Server.", "error")
                return redirect(url_for("wizard_sqlserver"))
            flash("Prima scegli la tabella target.", "error")
            return redirect(url_for("wizard_sqlserver_tables"))

        return render_template(
            "wizard_convert.html",
            wizard=_wizard_state(),
            target_context=target_context,
        )

    @app.post("/convert")
    def convert():
        uploaded = request.files.get("flow_file")
        if uploaded is None or not uploaded.filename:
            flash("Seleziona un file .zip o .json da analizzare.", "error")
            return redirect(url_for("wizard_convert" if _wizard_state()["ready_for_upload"] else "index"))

        filename = str(uploaded.filename)
        if not filename.lower().endswith((".zip", ".json")):
            flash("Formato non supportato. Usa un export .zip o .json.", "error")
            return redirect(url_for("wizard_convert" if _wizard_state()["ready_for_upload"] else "index"))

        payload = uploaded.read()
        try:
            target_context = _build_target_context()
            record = analyze_flow_upload(filename, payload, target_context=target_context)
            save_record(record)
        except Exception as exc:
            app.logger.exception("Errore durante l'analisi del flow '%s'", filename)
            flash(f"Analisi fallita: {exc}", "error")
            return redirect(url_for("wizard_convert" if _wizard_state()["ready_for_upload"] else "index"))

        flash("Flow analizzato e conversione salvata nello storico.", "success")
        return redirect(url_for("conversion_detail", record_id=record["record_id"]))

    @app.get("/conversions/<record_id>")
    def conversion_detail(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)
        if _normalize_record_package(record):
            save_record(record)
        return render_template("detail.html", record=record, wizard=_wizard_state())

    @app.post("/conversions/<record_id>/rules")
    def conversion_select_rules(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        package = record.setdefault("package", {})
        normalize_package_mappings(package)
        package["selected_proposed_rule_codes"] = [
            str(code or "").strip()
            for code in request.form.getlist("selected_rule_codes")
            if str(code or "").strip()
        ]
        _refresh_selected_rules(package)
        save_record(record)
        flash("Selezione regole aggiornata.", "success")
        return redirect(url_for("conversion_detail", record_id=record_id))

    @app.post("/conversions/<record_id>/mapping")
    def conversion_save_mapping(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        package = record.setdefault("package", {})
        normalize_package_mappings(package)

        target_submitted = any(
            key.startswith("target_mapping__") or key.startswith("mapping__")
            for key in request.form.keys()
        )
        runtime_submitted = any(key.startswith("runtime_mapping__") for key in request.form.keys())
        source_fields: list[str] = []
        seen_fields: set[str] = set()
        for raw_source_field in request.form.getlist("source_fields"):
            source_field = str(raw_source_field or "").strip()
            if not source_field or source_field in seen_fields:
                continue
            seen_fields.add(source_field)
            source_fields.append(source_field)
        for key in request.form.keys():
            if "__" not in key:
                continue
            _, source_field = key.split("__", 1)
            source_field = str(source_field or "").strip()
            if source_field and source_field not in seen_fields:
                seen_fields.add(source_field)
                source_fields.append(source_field)

        target_candidates = package.setdefault("target_field_mapping_candidates", {})
        runtime_candidates = package.setdefault("runtime_field_mapping_candidates", {})
        approved_target = json.loads(json.dumps(package.get("approved_target_field_mapping", {})))
        approved_runtime = json.loads(json.dumps(package.get("approved_runtime_field_mapping", {})))
        approved_at = datetime.now(UTC).isoformat()

        if target_submitted:
            for source_field in source_fields:
                target_field = str(
                    request.form.get(f"target_mapping__{source_field}")
                    or request.form.get(f"mapping__{source_field}")
                    or ""
                ).strip()
                if target_field:
                    approved_target[source_field] = {
                        "target_field": target_field,
                        "approved_at": approved_at,
                        "status": "approved",
                        "confidence": "approved",
                        "mapping_scope": TARGET_SCOPE,
                        "note": "confermato manualmente dall'utente",
                        "reason": "confermato manualmente dall'utente",
                        "source": "user",
                    }
                    candidate = target_candidates.setdefault(source_field, {})
                    if isinstance(candidate, dict):
                        candidate["target_field"] = target_field
                        candidate["confidence"] = "approved"
                        candidate["mapping_scope"] = TARGET_SCOPE
                        candidate["note"] = "confermato manualmente dall'utente"
                        candidate["reason"] = "confermato manualmente dall'utente"
                        candidate["source"] = "user"
                else:
                    approved_target.pop(source_field, None)

        if runtime_submitted:
            for source_field in source_fields:
                runtime_field = str(request.form.get(f"runtime_mapping__{source_field}") or "").strip()
                if runtime_field:
                    approved_runtime[source_field] = {
                        "target_field": runtime_field,
                        "approved_at": approved_at,
                        "status": "approved",
                        "confidence": "approved",
                        "mapping_scope": RUNTIME_SCOPE,
                        "note": "confermato manualmente dall'utente",
                        "reason": "confermato manualmente dall'utente",
                        "source": "user",
                    }
                    candidate = runtime_candidates.setdefault(source_field, {})
                    if isinstance(candidate, dict):
                        candidate["target_field"] = runtime_field
                        candidate["confidence"] = "approved"
                        candidate["mapping_scope"] = RUNTIME_SCOPE
                        candidate["note"] = "confermato manualmente dall'utente"
                        candidate["reason"] = "confermato manualmente dall'utente"
                        candidate["source"] = "user"
                else:
                    approved_runtime.pop(source_field, None)

        package["target_field_mapping_candidates"] = target_candidates
        package["runtime_field_mapping_candidates"] = runtime_candidates
        package["approved_target_field_mapping"] = approved_target
        package["approved_runtime_field_mapping"] = approved_runtime
        normalize_package_mappings(package)

        table_key = _table_key_from_record(record)
        if table_key and target_submitted:
            approved = {
                source_field: str(candidate.get("target_field") or "").strip()
                for source_field, candidate in package.get("approved_target_field_mapping", {}).items()
                if isinstance(candidate, dict) and str(candidate.get("target_field") or "").strip()
            }
            if approved:
                learn_from_approved_mappings(approved, table_key=table_key)

        save_record(record)
        warning_count = len(package.get("mapping_warnings", []))
        flash(
            (
                "Pairing salvato. I suggerimenti futuri terranno conto delle conferme sulla tabella target."
                if warning_count == 0
                else f"Pairing salvato con {warning_count} warning: alcune colonne target non hanno ancora un mapping runtime."
            ),
            "success",
        )
        return redirect(url_for("conversion_detail", record_id=record_id))

    @app.post("/conversions/<record_id>/apply-remediation")
    def conversion_apply_remediation(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        updated = apply_recommended_remediation(record)
        _normalize_record_package(updated)
        save_record(updated)
        flash("Remediation automatica applicata al package di conversione.", "success")
        return redirect(url_for("conversion_detail", record_id=record_id))

    @app.get("/conversions/<record_id>/package.json")
    def conversion_package_download(record_id: str):
        try:
            record = load_record(record_id)
        except FileNotFoundError:
            abort(404)

        package = record.get("package", {})
        if isinstance(package, dict):
            normalize_package_mappings(package)
            _refresh_selected_rules(package)
            save_record(record)

        package_path = BASE_DIR.parent / "output" / "history" / f"{record_id}.package.json"
        package_path.write_text(
            json.dumps(record.get("package", {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return send_file(package_path, as_attachment=True, download_name=f"{record_id}.automation_package.json")

    return app


app = create_app()


if __name__ == "__main__":
    host = os.environ.get("PA_CONVERTER_HOST", "0.0.0.0")
    port = int(os.environ.get("PA_CONVERTER_PORT", "8787"))
    app.run(debug=True, host=host, port=port)
