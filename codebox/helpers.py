"""
"""
#------------------------------------------------------------------------------------------------------------------

"""
Helpers — Blob storage, artifact management, file downloads, text extraction.
"""

import os
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, unquote

import requests

from codebox.config import (
    ARTIFACT_EXTENSIONS,
    BLOB_CONNECTION_STRING,
    BLOB_CONTAINER_NAME,
    BLOB_SAS_EXPIRY_HOURS,
    DOWNLOAD_TIMEOUT,
    MAX_DOWNLOAD_SIZE,
    logger,
)


# ===========================================================================
#  BLOB STORAGE MANAGER
# ===========================================================================
class BlobStorageManager:
    """
    Uploads artifacts to Azure Blob Storage and generates SAS URLs.

    Supports two modes:
      1. Server-default:  uses AZURE_BLOB_CONNECTION_STRING env var.
      2. Client-injected: client sends credentials via HTTP headers
         (X-Blob-Connection-String, X-Blob-Container-Name).
         Cached per unique (connection_string, container) pair.
    """

    # Class-level cache for per-client instances
    _cache: dict[str, "BlobStorageManager"] = {}

    def __init__(
        self,
        connection_string: str = "",
        container_name: str = "code-interpreter-artifacts",
    ) -> None:
        self._enabled = False
        self._blob_service_client = None
        self._container_client = None
        self._account_name = ""
        self._account_key = ""
        self._container_name = container_name

        if connection_string:
            try:
                from azure.storage.blob import BlobServiceClient

                self._blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
                self._container_client = self._blob_service_client.get_container_client(
                    container_name
                )
                # Ensure container exists
                try:
                    self._container_client.get_container_properties()
                except Exception:
                    self._container_client.create_container()
                    logger.info("Created blob container: %s", container_name)

                # Extract account name and key for SAS generation
                for part in connection_string.split(";"):
                    if part.startswith("AccountName="):
                        self._account_name = part.split("=", 1)[1]
                    elif part.startswith("AccountKey="):
                        self._account_key = part.split("=", 1)[1]

                self._enabled = True
                logger.info(
                    "Blob storage enabled (account=%s, container=%s)",
                    self._account_name,
                    container_name,
                )
            except Exception as exc:
                logger.warning("Blob storage init failed: %s", exc)
        else:
            logger.info("Blob storage not configured (connection string empty)")

    # ---- Factory for per-client instances ----

    @classmethod
    def get_instance(
        cls,
        connection_string: str,
        container_name: str,
    ) -> "BlobStorageManager":
        """
        Return a cached BlobStorageManager for the given credentials.
        If connection_string is empty, returns the server-default instance.
        """
        if not connection_string:
            return _default_blob_mgr

        cache_key = f"{hash(connection_string)}|{container_name}"
        if cache_key not in cls._cache:
            cls._cache[cache_key] = cls(connection_string, container_name)
        return cls._cache[cache_key]

    # ---- Properties ----

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def container_name(self) -> str:
        return self._container_name

    # ---- Upload logic ----

    def upload_and_get_sas_url(
        self,
        local_path: str,
        session_id: str,
        sas_expiry_hours: int | None = None,
    ) -> str | None:
        """
        Upload a local file to blob storage and return a SAS URL.
        Blob path: <session_id>/<filename>
        """
        if not self._enabled:
            return None

        expiry_hours = sas_expiry_hours if sas_expiry_hours is not None else BLOB_SAS_EXPIRY_HOURS

        try:
            from azure.storage.blob import BlobSasPermissions, generate_blob_sas

            filename = os.path.basename(local_path)
            blob_name = f"{session_id}/{filename}"

            # Upload
            blob_client = self._container_client.get_blob_client(blob_name)
            with open(local_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
            logger.info("Uploaded to blob: %s", blob_name)

            # Generate SAS URL
            sas_token = generate_blob_sas(
                account_name=self._account_name,
                account_key=self._account_key,
                container_name=self._container_name,
                blob_name=blob_name,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
            )

            sas_url = (
                f"https://{self._account_name}.blob.core.windows.net/"
                f"{self._container_name}/{blob_name}?{sas_token}"
            )
            logger.info(
                "Generated SAS URL for %s (expires in %dh)",
                blob_name,
                expiry_hours,
            )
            return sas_url

        except Exception as exc:
            logger.error("Blob upload failed for %s: %s", local_path, exc)
            return None

    def upload_artifacts(
        self,
        artifacts: list[dict],
        session_id: str,
        sas_expiry_hours: int | None = None,
    ) -> list[dict]:
        """
        Upload a list of artifact dicts and enrich each with a 'sas_url' key.
        Returns the enriched list.
        """
        if not self._enabled:
            return artifacts

        for art in artifacts:
            sas_url = self.upload_and_get_sas_url(art["path"], session_id, sas_expiry_hours)
            if sas_url:
                art["sas_url"] = sas_url
        return artifacts


# ---------------------------------------------------------------------------
# Server-default Blob Manager (from env vars)
# ---------------------------------------------------------------------------
_default_blob_mgr = BlobStorageManager(BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME)


def get_default_blob_manager() -> BlobStorageManager:
    """Return the server-default BlobStorageManager (from env vars)."""
    return _default_blob_mgr


# ===========================================================================
#  ARTIFACT HELPERS
# ===========================================================================
def collect_artifacts(workdir: str) -> list[dict]:
    """Walk the session workdir and return info about generated artifacts."""
    artifacts: list[dict] = []
    output_dir = os.path.join(workdir, "output")
    if not os.path.isdir(output_dir):
        return artifacts
    for root, _dirs, files in os.walk(output_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            ext = Path(fname).suffix.lower()
            artifacts.append({
                "filename": fname,
                "path": fpath,
                "extension": ext,
                "size_bytes": os.path.getsize(fpath),
            })
    return artifacts


def move_new_artifacts_to_output(workdir: str) -> list[str]:
    """
    Scan workdir root for newly generated artifact files and move them
    into output/ so they are easy to find.
    """
    moved: list[str] = []
    output_dir = os.path.join(workdir, "output")
    for item in os.listdir(workdir):
        item_path = os.path.join(workdir, item)
        if os.path.isfile(item_path):
            ext = Path(item).suffix.lower()
            if ext in ARTIFACT_EXTENSIONS:
                dest = os.path.join(output_dir, item)
                # avoid overwrite — add uuid suffix
                if os.path.exists(dest):
                    stem = Path(item).stem
                    dest = os.path.join(output_dir, f"{stem}_{uuid.uuid4().hex[:6]}{ext}")
                shutil.move(item_path, dest)
                moved.append(dest)
                logger.info("Moved artifact → %s", dest)
    return moved


# ===========================================================================
#  TEXT OUTPUT EXTRACTION
# ===========================================================================
def extract_text_output(response: list) -> str:
    """
    Extract human-readable text from Open Interpreter's response list.
    Each item is a dict like {"role": ..., "type": ..., "content": ...}.
    """
    parts: list[str] = []
    for chunk in response:
        if not isinstance(chunk, dict):
            continue
        role = chunk.get("role", "")
        msg_type = chunk.get("type", "")
        content = chunk.get("content", "")

        if msg_type == "message" and role == "assistant":
            parts.append(content)
        elif msg_type == "console" and role in ("computer", "system"):
            # stdout/stderr from code execution
            # OI yields these as role="computer", type="console", format="output"
            fmt = chunk.get("format", "output")
            if fmt == "active_line":
                continue  # skip line-highlight markers
            if isinstance(content, str) and content.strip():
                parts.append(f"[{fmt}]\n{content}")

    return "\n\n".join(parts) if parts else "(no text output)"


# ===========================================================================
#  BLOB / URL DOWNLOAD HELPERS
# ===========================================================================
def download_from_url(
    url: str,
    dest_path: str,
    timeout: int | None = None,
    max_size: int | None = None,
) -> int:
    """
    Stream-download a file from a public URL or SAS URL to dest_path.
    Returns the number of bytes written.

    Args:
        timeout:  Download timeout in seconds (defaults to DOWNLOAD_TIMEOUT).
        max_size: Max file size in bytes (defaults to MAX_DOWNLOAD_SIZE).
    """
    effective_timeout = timeout if timeout is not None else DOWNLOAD_TIMEOUT
    effective_max_size = max_size if max_size is not None else MAX_DOWNLOAD_SIZE

    logger.info("Downloading from URL → %s", dest_path)
    resp = requests.get(url, stream=True, timeout=effective_timeout)
    resp.raise_for_status()

    # Check Content-Length if available
    content_length = resp.headers.get("Content-Length")
    if content_length and int(content_length) > effective_max_size:
        raise ValueError(
            f"File too large: {int(content_length)} bytes "
            f"(max {effective_max_size} bytes)"
        )

    total = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            total += len(chunk)
            if total > effective_max_size:
                raise ValueError(
                    f"Download exceeded max size ({effective_max_size} bytes)"
                )
            f.write(chunk)

    logger.info("Downloaded %d bytes → %s", total, dest_path)
    return total


def filename_from_url(url: str) -> str:
    """Extract a reasonable filename from a URL (strip query/SAS params)."""
    parsed = urlparse(url)
    path = unquote(parsed.path)
    name = os.path.basename(path)
    return name if name else "Uploaded_file"


# ===========================================================================
#  SQL RESULT → CSV HELPER
# ===========================================================================
def results_to_csv(
    columns: list[str],
    rows: list[dict],
    output_path: str,
) -> str:
    """
    Write SQL query results to a CSV file.

    CSV is preferred over Excel for in-session use because:
      - pd.read_csv() is ~5-10x faster than pd.read_excel()
      - No openpyxl/XML overhead
      - Works natively with all Python data libraries

    Args:
        columns:    Column names.
        rows:       List of row-dicts (column → value).
        output_path: Destination .csv file path.

    Returns:
        Absolute path to the created file.
    """
    import pandas as pd

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %d rows to %s", len(rows), output_path)
    return os.path.abspath(output_path)
