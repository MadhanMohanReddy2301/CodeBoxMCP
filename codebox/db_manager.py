"""
"""
#------------------------------------------------------------------------------------------------------------------
"""
Database Manager — Azure SQL query execution with two-level safety.

Level 1: Application-level validation (keyword blocking, SELECT/WITH only).
Level 2: Database-level safety (read-only transaction, always ROLLBACK).

Supports per-client credential injection via HTTP headers
(X-Db-Connection-String, X-Db-Password) or server-level env vars.
"""

import os
import re
import uuid
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from codebox.config import (
    DB_CONNECTION_STRING,
    DB_PASSWORD,
    SQL_MAX_INLINE_ROWS,
    logger,
)


# ===========================================================================
#  QUERY VALIDATOR (Level 1 — application-level)
# ===========================================================================
# Keywords that indicate a write / DDL / admin operation
_BLOCKED_KEYWORDS: set[str] = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "EXEC", "EXECUTE", "MERGE", "GRANT", "REVOKE",
    "CALL", "SET", "BACKUP", "RESTORE", "KILL", "SHUTDOWN",
    "DBCC", "BULK", "OPENROWSET", "OPENQUERY", "XP_",
}

# Compiled pattern that matches any blocked keyword at a word boundary
_BLOCKED_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _BLOCKED_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Pattern to strip SQL comments   -- line comments and /* block comments */
_COMMENT_RE = re.compile(
    r"(--[^\r\n]*)|(/\*[\s\S]*?\*/)",
    re.MULTILINE,
)


def validate_query(query: str) -> str | None:
    """
    Validate that *query* is a read-only SELECT statement.

    Returns None if the query is safe, or an error message string
    explaining why it was rejected.
    """
    if not query or not query.strip():
        return "Query is empty."

    # Strip comments so blocked keywords inside comments don't cause
    # false positives (and hidden keywords don't bypass checks).
    cleaned = _COMMENT_RE.sub(" ", query).strip()

    if not cleaned:
        return "Query is empty after removing comments."

    # Reject semicolons — no multi-statement batches
    if ";" in cleaned:
        # Allow a trailing semicolon only
        parts = [p.strip() for p in cleaned.split(";") if p.strip()]
        if len(parts) > 1:
            return "Multiple statements detected (semicolons). Only single SELECT queries are allowed."

    # First meaningful keyword must be SELECT or WITH (CTEs)
    first_word = cleaned.split()[0].upper()
    if first_word not in ("SELECT", "WITH"):
        return (
            f"Query must start with SELECT or WITH. "
            f"Found: '{first_word}'. Only read queries are allowed."
        )

    # Scan for blocked keywords
    match = _BLOCKED_RE.search(cleaned)
    if match:
        return (
            f"Blocked keyword '{match.group()}' found in query. "
            f"Only SELECT queries are allowed — no INSERT, UPDATE, DELETE, "
            f"DROP, ALTER, CREATE, EXEC, or other write/DDL/admin operations."
        )

    return None  # Safe


# ===========================================================================
#  DATABASE MANAGER
# ===========================================================================
class DbManager:
    """
    Manages SQLAlchemy engine connections for Azure SQL databases.

    Similar to BlobStorageManager: supports server-default credentials
    (from env vars) and per-client credentials (from HTTP headers),
    with a class-level cache for per-connection-string engine reuse.
    """

    # Class-level cache: connection_key → DbManager instance
    _cache: dict[str, "DbManager"] = {}

    def __init__(
        self,
        connection_string: str = "",
        password: str = "",
    ) -> None:
        self._enabled = False
        self._engine: Engine | None = None

        if connection_string and password:
            try:
                # Inject password into the template
                odbc_str = connection_string.replace("{your_password_here}", password)
                quoted = quote_plus(odbc_str)
                conn_url = f"mssql+pyodbc:///?odbc_connect={quoted}"
                self._engine = create_engine(conn_url, fast_executemany=True)
                self._enabled = True
                logger.info("DbManager initialized (engine created)")
            except Exception as exc:
                logger.warning("DbManager init failed: %s", exc)
        else:
            logger.info("DbManager not configured (connection string or password empty)")

    # ---- Factory ----

    @classmethod
    def get_instance(
        cls,
        connection_string: str,
        password: str,
    ) -> "DbManager":
        """Return a cached DbManager for the given credentials."""
        if not connection_string or not password:
            return _default_db_mgr

        cache_key = f"{hash(connection_string)}|{hash(password)}"
        if cache_key not in cls._cache:
            cls._cache[cache_key] = cls(connection_string, password)
        return cls._cache[cache_key]

    # ---- Properties ----

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ---- Query execution (Level 2 — DB-level safety) ----

    def execute_query(self, query: str) -> dict:
        """
        Execute a read-only SELECT query and return the results.

        Level 2 safety: the query runs inside a transaction that is
        always rolled back — even if the application-level check missed
        something, no writes will persist.

        Returns:
            dict with keys: columns, rows (list of dicts), row_count
        """
        if not self._enabled:
            raise RuntimeError(
                "Database not configured. Provide X-Db-Connection-String "
                "and X-Db-Password headers, or set AZURE_DATABASE_CONNECTION_STRING "
                "and AZURE_DATABASE_PASSWORD env vars."
            )

        with self._engine.connect() as conn:
            # Begin explicit transaction
            trans = conn.begin()
            try:
                result = conn.execute(text(query))
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                }
            finally:
                # ALWAYS rollback — never commit. Defense in depth.
                trans.rollback()


# ---------------------------------------------------------------------------
# Server-default DbManager (from env vars)
# ---------------------------------------------------------------------------
_default_db_mgr = DbManager(DB_CONNECTION_STRING, DB_PASSWORD)


def get_default_db_manager() -> DbManager:
    """Return the server-default DbManager (from env vars)."""
    return _default_db_mgr
