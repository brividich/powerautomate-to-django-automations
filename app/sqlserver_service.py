from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    import pyodbc
except ImportError:  # pragma: no cover - covered by behavior tests instead
    pyodbc = None


DEFAULT_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
]


@dataclass(slots=True)
class SqlServerConfig:
    driver: str
    server: str
    database: str
    username: str = ""
    password: str = ""
    port: str = ""
    encrypt: bool = True
    trust_server_certificate: bool = True
    integrated_security: bool = False


def _require_pyodbc() -> None:
    if pyodbc is None:
        raise RuntimeError(
            "pyodbc non e' installato. Installa le dipendenze con `pip install -r requirements.txt` "
            "e verifica di avere un driver ODBC SQL Server disponibile."
        )


def available_drivers() -> list[str]:
    if pyodbc is None:
        return DEFAULT_DRIVERS

    drivers = [name for name in pyodbc.drivers() if "sql server" in name.lower()]
    return drivers or DEFAULT_DRIVERS


def supports_modern_security_options(driver_name: str) -> bool:
    lowered = driver_name.strip().lower()
    return lowered.startswith("odbc driver ")


def build_connection_string(config: SqlServerConfig) -> str:
    server = config.server.strip()
    if config.port.strip():
        server = f"{server},{config.port.strip()}"

    parts = [
        f"DRIVER={{{config.driver}}}",
        f"SERVER={server}",
        f"DATABASE={config.database.strip()}",
    ]

    if supports_modern_security_options(config.driver):
        parts.append(f"Encrypt={'yes' if config.encrypt else 'no'}")
        parts.append(f"TrustServerCertificate={'yes' if config.trust_server_certificate else 'no'}")

    if config.integrated_security:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={config.username}")
        parts.append(f"PWD={config.password}")

    return ";".join(parts)


def _connect(config: SqlServerConfig):
    _require_pyodbc()
    conn_str = build_connection_string(config)
    return pyodbc.connect(conn_str, timeout=5)


def test_connection(config: SqlServerConfig) -> dict[str, str]:
    with _connect(config) as connection:
        cursor = connection.cursor()
        row = cursor.execute("SELECT @@SERVERNAME AS server_name, DB_NAME() AS database_name").fetchone()
        return {
            "server_name": str(getattr(row, "server_name", "") or row[0] or config.server),
            "database_name": str(getattr(row, "database_name", "") or row[1] or config.database),
        }


def list_tables(config: SqlServerConfig) -> list[dict[str, Any]]:
    query = """
    SELECT
        t.TABLE_SCHEMA,
        t.TABLE_NAME,
        t.TABLE_TYPE,
        COUNT(c.COLUMN_NAME) AS column_count
    FROM INFORMATION_SCHEMA.TABLES t
    LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
       AND c.TABLE_NAME = t.TABLE_NAME
    WHERE t.TABLE_TYPE = 'BASE TABLE'
    GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE
    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    """
    with _connect(config) as connection:
        rows = connection.cursor().execute(query).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        schema_name = str(row[0])
        table_name = str(row[1])
        result.append(
            {
                "schema": schema_name,
                "table": table_name,
                "table_type": str(row[2]),
                "column_count": int(row[3] or 0),
                "full_name": f"{schema_name}.{table_name}",
            }
        )
    return result


def list_columns(config: SqlServerConfig, *, schema: str, table: str) -> list[dict[str, Any]]:
    query = """
    SELECT
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.IS_NULLABLE,
        c.ORDINAL_POSITION,
        CASE WHEN pk.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS is_primary_key
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN (
        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
            ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA
           AND ku.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    ) pk
        ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA
       AND pk.TABLE_NAME = c.TABLE_NAME
       AND pk.COLUMN_NAME = c.COLUMN_NAME
    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION
    """
    with _connect(config) as connection:
        rows = connection.cursor().execute(query, schema, table).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "name": str(row[0]),
                "data_type": str(row[1]),
                "is_nullable": str(row[2]).upper() == "YES",
                "ordinal_position": int(row[3] or 0),
                "is_primary_key": bool(row[4]),
            }
        )
    return result
