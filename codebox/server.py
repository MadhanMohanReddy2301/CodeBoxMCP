"""
"""
#------------------------------------------------------------------------------------------------------------------
"""
MCP Code Interpreter Server
============================
FastMCP instance, MCP resource, all tool definitions, and the server entrypoint.

Tools are registered as standalone functions (FastMCP convention).
Blob storage credentials can be injected per-client via HTTP headers
(X-Blob-Connection-String, X-Blob-Container-Name) or fall back to
server-level environment variables.

Database credentials for exec_sql can be injected via HTTP headers
(X-Db-Connection-String, X-Db-Password) or fall back to env vars.
"""

import os
import threading
import uuid
from pathlib import Path

from mcp.server.fastmcp import FastMCP, Context

from codebox.config import (
    BASE_WORKDIR,
    BLOB_CONNECTION_STRING,
    BLOB_CONTAINER_NAME,
    BLOB_SAS_EXPIRY_HOURS,
    CLEANUP_INTERVAL,
    DB_CONNECTION_STRING,
    DB_PASSWORD,
    DOWNLOAD_TIMEOUT,
    EXEC_TIMEOUT,
    HOST,
    IDLE_TIMEOUT,
    MAX_DOWNLOAD_SIZE,
    MCP_TRANSPORT,
    PORT,
    SERVER_NAME,
    SESSION_TTL,
    SQL_MAX_INLINE_ROWS,
    logger,
)
from codebox.db_manager import (
    DbManager,
    get_default_db_manager,
    validate_query,
)
from codebox.helpers import (
    BlobStorageManager,
    collect_artifacts,
    download_from_url,
    extract_text_output,
    filename_from_url,
    get_default_blob_manager,
    move_new_artifacts_to_output,
    results_to_csv,
)
from codebox.resources import USAGE_GUIDE
from codebox.session_manager import (
    session_cleanup_loop,
    session_mgr,
)

# ---------------------------------------------------------------------------
# FastMCP Server instance
# ---------------------------------------------------------------------------
# stateless_http=True disables MCP protocol-level session tracking so
# each HTTP request is processed independently — required for Azure Web
# App where the reverse proxy / ARR can break Mcp-Session-Id continuity.
mcp = FastMCP(SERVER_NAME, host=HOST, port=PORT, stateless_http=True)


# ===========================================================================
#  MCP RESOURCES — context automatically available to connected agents
# ===========================================================================
@mcp.resource("code-interpreter://usage-guide")
def get_usage_guide() -> str:
    """Usage guide and best practices for the Code Interpreter MCP server."""
    return USAGE_GUIDE


# ===========================================================================
#  CLIENT CONFIG RESOLUTION
# ===========================================================================
def _resolve_client_config(ctx: Context) -> dict:
    """
    Read client-injected configuration from HTTP headers and clamp each
    value to the server-level maximum defined in .env / config.py.

    Clients can override via headers:
      X-Session-TTL, X-Idle-Timeout, X-Exec-Timeout,
      X-Download-Timeout, X-Max-Download-Size,
      X-Blob-SAS-Expiry-Hours

    Values are clamped:  effective = min(client_value, server_max).
    If a header is absent, the server default is used.
    """
    defaults = {
        "session_ttl": SESSION_TTL,
        "idle_timeout": IDLE_TIMEOUT,
        "exec_timeout": EXEC_TIMEOUT,
        "download_timeout": DOWNLOAD_TIMEOUT,
        "max_download_size": MAX_DOWNLOAD_SIZE,
        "blob_sas_expiry_hours": BLOB_SAS_EXPIRY_HOURS,
    }
    try:
        headers = ctx.request_context.request.headers
    except Exception:
        return defaults

    def _clamp(header: str, server_max: int) -> int:
        raw = headers.get(header, "")
        if raw:
            try:
                return min(int(raw), server_max)
            except (ValueError, TypeError):
                pass
        return server_max

    return {
        "session_ttl": _clamp("x-session-ttl", SESSION_TTL),
        "idle_timeout": _clamp("x-idle-timeout", IDLE_TIMEOUT),
        "exec_timeout": _clamp("x-exec-timeout", EXEC_TIMEOUT),
        "download_timeout": _clamp("x-download-timeout", DOWNLOAD_TIMEOUT),
        "max_download_size": _clamp("x-max-download-size", MAX_DOWNLOAD_SIZE),
        "blob_sas_expiry_hours": _clamp("x-blob-sas-expiry-hours", BLOB_SAS_EXPIRY_HOURS),
        "sql_max_inline_rows": _clamp("x-sql-max-inline-rows", SQL_MAX_INLINE_ROWS),
    }


def _resolve_blob_manager(ctx: Context) -> BlobStorageManager:
    """
    Resolve the BlobStorageManager to use for this request.

    Priority:
      1. Client HTTP headers (X-Blob-Connection-String, X-Blob-Container-Name)
      2. Server environment variables (AZURE_BLOB_CONNECTION_STRING)
      3. Disabled (no blob upload — artifacts stay local)
    """
    try:
        request = ctx.request_context.request
        conn_str = request.headers.get("x-blob-connection-string", "")
        container = request.headers.get("x-blob-container-name", "")
        if conn_str:
            return BlobStorageManager.get_instance(
                conn_str,
                container or BLOB_CONTAINER_NAME,
            )
    except Exception:
        # No request context (e.g. stdio transport) — fall through
        pass
    return get_default_blob_manager()


def _resolve_db_manager(ctx: Context) -> DbManager:
    """
    Resolve the DbManager to use for this request.

    Priority:
      1. Client HTTP headers (X-Db-Connection-String, X-Db-Password)
      2. Server environment variables (AZURE_DATABASE_CONNECTION_STRING)
      3. Disabled (no database — exec_sql will return an error)
    """
    try:
        request = ctx.request_context.request
        conn_str = request.headers.get("x-db-connection-string", "")
        password = request.headers.get("x-db-password", "")
        if conn_str and password:
            return DbManager.get_instance(conn_str, password)
    except Exception:
        pass
    return get_default_db_manager()


# ===========================================================================
#  MCP TOOLS
# ===========================================================================
@mcp.tool()
def exec_code(session_id: str, language: str, code: str, ctx: Context) -> dict:
    """
    Execute code in a stateful interpreter session. No LLM is involved.

    Sessions are created automatically on first use — just pass any
    session_id and start running code. State (variables, imports, files,
    cwd) is preserved across calls with the same session_id.

    Any output files (plots, Excel, CSV, HTML, etc.) saved to the session's
    output directory are automatically detected as artifacts. If Azure Blob
    Storage is configured (via server env vars or client HTTP headers),
    each new artifact is uploaded and a SAS URL is included in the response
    under new_artifacts[].sas_url — no extra steps needed.

    Returns:
        dict with keys:
          - session_id, status, output (stdout/stderr text)
          - new_artifacts: list of files created in this call, each with
            filename, path, extension, size_bytes, and sas_url (if blob enabled)
          - all_artifacts: every file in the session's output directory

    Args:
        session_id: Unique conversation/session identifier (auto-created if new).
        language:   Programming language (e.g. "python").
        code:       The code to execute.
    """
    logger.info("[exec_code] session=%s language=%s", session_id, language)

    try:
        # Resolve client config (headers → clamped to server max)
        cfg = _resolve_client_config(ctx)

        sess = session_mgr.get_or_create(
            session_id,
            session_ttl=cfg["session_ttl"],
            idle_timeout=cfg["idle_timeout"],
        )
        interp = sess["interpreter"]
        workdir = sess["workdir"]

        # Basic validation
        if not code or not code.strip():
            return {
                "session_id": session_id,
                "status": "error",
                "error": "Code is empty",
            }

        if interp.computer.terminal.get_language(language) is None:
            return {
                "session_id": session_id,
                "status": "error",
                "error": f"Language '{language}' is not enabled or supported",
            }

        # Snapshot existing artifacts BEFORE execution
        existing_artifacts = set()
        output_dir = os.path.join(workdir, "output")
        if os.path.isdir(output_dir):
            for f in os.listdir(output_dir):
                existing_artifacts.add(os.path.join(output_dir, f))

        # Run code directly; no LLM involved
        response: list[dict] = []
        for line in interp.computer.run(language, code, stream=True):
            response.append({"role": "computer", **line})

        # Move any newly created artifact files from workdir root into output/
        moved = move_new_artifacts_to_output(workdir)

        # Collect all artifacts
        artifacts = collect_artifacts(workdir)

        # Determine NEW artifacts = (all artifacts now) - (what existed before)
        new_artifacts = []
        for art in artifacts:
            if art["path"] not in existing_artifacts:
                new_artifacts.append(art)
        # Also include anything moved from workdir root
        for p in moved:
            basename = os.path.basename(p)
            if not any(a["filename"] == basename for a in new_artifacts):
                new_artifacts.append({
                    "filename": basename,
                    "path": p,
                    "extension": Path(basename).suffix.lower(),
                    "size_bytes": os.path.getsize(p),
                })

        # Resolve blob manager (client headers → env vars → disabled)
        blob_mgr = _resolve_blob_manager(ctx)

        # Upload NEW artifacts to Azure Blob Storage & get SAS URLs
        new_artifacts = blob_mgr.upload_artifacts(
            new_artifacts, session_id, sas_expiry_hours=cfg["blob_sas_expiry_hours"]
        )

        # Extract readable text output (stdout/stderr)
        text_output = extract_text_output(response)

        return {
            "session_id": session_id,
            "status": "success",
            "output": text_output,
            "new_artifacts": new_artifacts,
            "all_artifacts": artifacts,
        }

    except Exception as exc:
        logger.error("[exec_code] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


@mcp.tool()
def upload_file(
    session_id: str,
    ctx: Context,
    blob_url: str = "",
    filename: str = "",
) -> dict:
    """
    Upload a file (Excel, CSV, etc.) into the session's input directory
    by downloading it from a URL. Session is auto-created if it doesn't exist.

    Args:
        session_id: Unique conversation/session identifier (auto-created if new).
        blob_url:   Public URL or Azure Blob SAS URL to the file.
        filename:   Name for the saved file (e.g. "sales.xlsx").
                    If omitted, inferred from the URL.

    Returns:
        dict with keys: session_id, status, saved_path, size_bytes, message
    """
    logger.info(
        "[upload_file] session=%s blob_url=%s filename=%s",
        session_id,
        blob_url[:120] if blob_url else "(none)",
        filename,
    )

    if not blob_url:
        return {
            "session_id": session_id,
            "status": "error",
            "error": "Provide 'blob_url' (a public or SAS URL to the file).",
        }

    try:
        cfg = _resolve_client_config(ctx)

        sess = session_mgr.get_or_create(
            session_id,
            session_ttl=cfg["session_ttl"],
            idle_timeout=cfg["idle_timeout"],
        )
        workdir = sess["workdir"]
        input_dir = os.path.join(workdir, "input")

        if not filename:
            filename = filename_from_url(blob_url)
        save_path = os.path.join(input_dir, filename)
        file_size = download_from_url(
            blob_url, save_path,
            timeout=cfg["download_timeout"],
            max_size=cfg["max_download_size"],
        )

        logger.info("[upload_file] Saved %s (%d bytes)", save_path, file_size)

        return {
            "session_id": session_id,
            "status": "success",
            "saved_path": save_path,
            "size_bytes": file_size,
            "message": (
                f"File '{filename}' uploaded to session. "
                f"Use exec_code to work with it. "
                f"File path: {save_path}"
            ),
        }

    except Exception as exc:
        logger.error("[upload_file] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


@mcp.tool()
def exec_sql(session_id: str, query: str, ctx: Context) -> dict:
    """
    Execute a read-only SQL SELECT query against an Azure SQL database.

    Two-level safety:
      1. Application-level: validates the query is SELECT/WITH only,
         blocks INSERT/UPDATE/DELETE/DROP/ALTER and other write operations.
      2. Database-level: runs inside a transaction that is always rolled back.

    Result delivery:
      - Small results (≤ SQL_MAX_INLINE_ROWS, default 30): returned inline
        as JSON with columns and rows.
      - Large results (> threshold): saved to a CSV file in the session's
        **input/** directory so the agent can immediately load it via
        `exec_code` (e.g. `pd.read_csv(path)`) — no download or
        re-upload needed. Optionally also uploaded to blob for external access.

    Database credentials can be injected per-client via HTTP headers
    (X-Db-Connection-String, X-Db-Password) or set as server env vars.

    Args:
        session_id: Unique session identifier (used for file output paths).
        query:      SQL SELECT query to execute.
    """
    logger.info("[exec_sql] session=%s query_len=%d", session_id, len(query))

    try:
        # --- Level 1: Application-level validation ---
        validation_error = validate_query(query)
        if validation_error:
            return {
                "session_id": session_id,
                "status": "error",
                "error": validation_error,
            }

        cfg = _resolve_client_config(ctx)
        max_inline = cfg["sql_max_inline_rows"]

        # Resolve DB manager (client headers → env vars → disabled)
        db_mgr = _resolve_db_manager(ctx)
        if not db_mgr.enabled:
            return {
                "session_id": session_id,
                "status": "error",
                "error": (
                    "Database not configured. Provide X-Db-Connection-String "
                    "and X-Db-Password headers, or set AZURE_DATABASE_CONNECTION_STRING "
                    "and AZURE_DATABASE_PASSWORD env vars."
                ),
            }

        # --- Level 2: Execute in read-only transaction ---
        result = db_mgr.execute_query(query)
        columns = result["columns"]
        rows = result["rows"]
        row_count = result["row_count"]

        logger.info("[exec_sql] query returned %d rows, %d columns", row_count, len(columns))

        # --- Inline delivery (small result sets) ---
        if row_count <= max_inline:
            return {
                "session_id": session_id,
                "status": "success",
                "delivery": "inline",
                "columns": columns,
                "rows": rows,
                "row_count": row_count,
            }

        # --- CSV delivery (large result sets) ---
        # Save to input/ so the agent can load it directly via exec_code
        # (e.g. pd.read_csv(path)) — no blob download round-trip needed.
        # CSV chosen over Excel: ~5-10x faster I/O, no openpyxl overhead.
        sess = session_mgr.get_or_create(
            session_id,
            session_ttl=cfg["session_ttl"],
            idle_timeout=cfg["idle_timeout"],
        )
        workdir = sess["workdir"]
        input_dir = os.path.join(workdir, "input")
        os.makedirs(input_dir, exist_ok=True)

        filename = f"sql_result_{uuid.uuid4().hex[:8]}.csv"
        csv_path = os.path.join(input_dir, filename)
        results_to_csv(columns, rows, csv_path)

        # Optionally upload to blob for external access (download link)
        blob_mgr = _resolve_blob_manager(ctx)
        sas_url = None
        if blob_mgr.enabled:
            sas_url = blob_mgr.upload_and_get_sas_url(
                csv_path, session_id,
                sas_expiry_hours=cfg["blob_sas_expiry_hours"],
            )

        response = {
            "session_id": session_id,
            "status": "success",
            "delivery": "file",
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "file_path": csv_path,
            "file_name": filename,
            "message": (
                f"Query returned {row_count} rows (exceeds inline threshold "
                f"of {max_inline}). Results saved to session input folder. "
                f"Load it in exec_code with: "
                f"pd.read_csv(r'{csv_path}')"
            ),
        }
        if sas_url:
            response["sas_url"] = sas_url
        return response

    except Exception as exc:
        logger.error("[exec_sql] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


@mcp.tool()
def list_artifacts(session_id: str) -> dict:
    """
    List all generated artifacts (plots, CSVs, HTMLs) in the session.

    Args:
        session_id: Unique conversation/session identifier.

    Returns:
        dict with keys: session_id, status, artifacts
    """
    logger.info("[list_artifacts] session=%s", session_id)

    try:
        sess = session_mgr.get(session_id)
        if not sess:
            return {
                "session_id": session_id,
                "status": "error",
                "error": f"Session '{session_id}' not found.",
            }

        artifacts = collect_artifacts(sess["workdir"])

        return {
            "session_id": session_id,
            "status": "success",
            "artifacts": artifacts,
            "count": len(artifacts),
        }

    except Exception as exc:
        logger.error("[list_artifacts] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


@mcp.tool()
def destroy_session(session_id: str) -> dict:
    """
    Explicitly destroy a session, killing its interpreter and wiping its files.

    Args:
        session_id: Unique conversation/session identifier.

    Returns:
        dict with keys: session_id, status
    """
    logger.info("[destroy_session] session=%s", session_id)

    try:
        session_mgr.destroy(session_id)
        return {
            "session_id": session_id,
            "status": "success",
            "message": f"Session '{session_id}' destroyed.",
        }
    except Exception as exc:
        logger.error("[destroy_session] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


@mcp.tool()
def list_sessions() -> dict:
    """
    List all active interpreter sessions with their metadata.

    Returns:
        dict with keys: status, sessions, count
    """
    logger.info("[list_sessions]")
    try:
        sessions = session_mgr.list_sessions()
        return {
            "status": "success",
            "sessions": sessions,
            "count": len(sessions),
        }
    except Exception as exc:
        logger.error("[list_sessions] error: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def session_info(session_id: str, ctx: Context) -> dict:
    """
    Return session paths and storage capabilities for the given session.
    Session is auto-created if it doesn't exist.

    Call this first to discover the absolute paths for input/output
    directories before writing or reading files.

    Args:
        session_id: Unique conversation/session identifier (auto-created if new).
    """
    logger.info("[session_info] session=%s", session_id)
    try:
        cfg = _resolve_client_config(ctx)

        sess = session_mgr.get_or_create(
            session_id,
            session_ttl=cfg["session_ttl"],
            idle_timeout=cfg["idle_timeout"],
        )

        workdir = sess["workdir"]
        input_dir = os.path.join(workdir, "input")
        output_dir = os.path.join(workdir, "output")

        # Resolve blob manager (client headers → env vars → disabled)
        blob_mgr = _resolve_blob_manager(ctx)

        return {
            "session_id": session_id,
            "status": "success",
            "workdir": workdir,
            "input_dir": input_dir,
            "output_dir": output_dir,
            "blob_upload_enabled": blob_mgr.enabled,
            "blob_container": blob_mgr.container_name if blob_mgr.enabled else None,
            "effective_config": {
                "session_ttl": sess.get("session_ttl", cfg["session_ttl"]),
                "idle_timeout": sess.get("idle_timeout", cfg["idle_timeout"]),
                "exec_timeout": cfg["exec_timeout"],
                "download_timeout": cfg["download_timeout"],
                "max_download_size": cfg["max_download_size"],
                "blob_sas_expiry_hours": cfg["blob_sas_expiry_hours"],
            },
        }

    except Exception as exc:
        logger.error("[session_info] error: %s", exc, exc_info=True)
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(exc),
        }


# ===========================================================================
#  DISPLAY RUNTIME INFO
# ===========================================================================
def display_runtime_info(transport: str) -> None:
    """Log server configuration at startup."""
    # streamable-http → /mcp, legacy sse → /sse
    endpoint = "/mcp" if transport == "streamable-http" else "/sse"
    logger.info("=" * 60)
    logger.info("  %s", SERVER_NAME)
    logger.info("=" * 60)
    logger.info("  Transport        : %s", transport)
    if HOST == "0.0.0.0":
        logger.info("  Listening on: http://localhost:%s%s", PORT, endpoint)
        logger.info("  Listening on: http://127.0.0.1:%s%s", PORT, endpoint)
    else:
        logger.info("  Listening on: http://%s:%s%s", HOST, PORT, endpoint)
    logger.info("  Session TTL      : %ds", SESSION_TTL)
    logger.info("  Idle Timeout     : %ds", IDLE_TIMEOUT)
    logger.info("  Cleanup Interval : %ds", CLEANUP_INTERVAL)
    logger.info("  Base Workdir     : %s", BASE_WORKDIR)
    logger.info(
        "  Tools exposed    : exec_code, exec_sql, upload_file, "
        "list_artifacts, destroy_session, list_sessions, session_info"
    )
    logger.info("  Blob (server)    : %s", "enabled" if get_default_blob_manager().enabled else "disabled (env)")
    logger.info("  DB   (server)    : %s", "enabled" if get_default_db_manager().enabled else "disabled (env)")
    logger.info("  Client headers   : X-Blob-Connection-String, X-Blob-Container-Name,")
    logger.info("                     X-Db-Connection-String, X-Db-Password,")
    logger.info("                     X-Session-TTL, X-Idle-Timeout, X-Exec-Timeout,")
    logger.info("                     X-Download-Timeout, X-Max-Download-Size,")
    logger.info("                     X-Blob-SAS-Expiry-Hours, X-Sql-Max-Inline-Rows")
    logger.info("  Clamping         : min(client_value, server_max)")
    logger.info("=" * 60)


# ===========================================================================
#  SERVER ENTRYPOINT
# ===========================================================================
def main(transport: str = MCP_TRANSPORT) -> None:
    """Start the Code Interpreter MCP server."""

    # Ensure base workdir exists
    os.makedirs(BASE_WORKDIR, exist_ok=True)

    # Start background cleanup
    cleanup_thread = threading.Thread(target=session_cleanup_loop, daemon=True)
    cleanup_thread.start()

    # Display info
    display_runtime_info(transport)

    # Start MCP server
    try:
        logger.info("Starting FastMCP server (transport=%s) ...", transport)
        mcp.run(transport=transport)
    except Exception as exc:
        logger.error("Failed to start MCP server: %s", exc, exc_info=True)
        raise


if __name__ == "__main__":
    main()
