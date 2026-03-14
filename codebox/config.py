"""
"""
#------------------------------------------------------------------------------------------------------------------
"""
Configuration — single source of truth for all settings.

All values are overridable via environment variables or a .env file.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("CodeBoxMCP")

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
SERVER_NAME = os.getenv("MCP_SERVER_NAME", "Code Interpreter MCP")
HOST = os.getenv("MCP_HOST", "0.0.0.0")
# Azure Web App injects PORT; fall back to MCP_PORT or 8000
PORT = int(os.getenv("PORT", os.getenv("MCP_PORT", "8000")))
# "streamable-http" is robust behind reverse proxies (Azure, nginx, etc.)
MCP_TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http")

# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------
SESSION_TTL = int(os.getenv("SESSION_TTL", "3600"))           # seconds
IDLE_TIMEOUT = int(os.getenv("IDLE_TIMEOUT", "1800"))         # seconds
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))   # seconds
EXEC_TIMEOUT = int(os.getenv("EXEC_TIMEOUT", "300"))          # seconds

# Base working directory for all sessions
# .parent.parent because config.py lives inside codebox/
DEFAULT_BASE_WORKDIR = Path(__file__).resolve().parent.parent / "sessions"
BASE_WORKDIR = os.getenv("BASE_WORKDIR", str(DEFAULT_BASE_WORKDIR))

# ---------------------------------------------------------------------------
# Azure Blob Storage (for artifact upload)
# ---------------------------------------------------------------------------
BLOB_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONNECTION_STRING", "")
BLOB_CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME", "code-interpreter-artifacts")
BLOB_SAS_EXPIRY_HOURS = int(os.getenv("BLOB_SAS_EXPIRY_HOURS", "24"))

# ---------------------------------------------------------------------------
# Azure SQL Database (for exec_sql tool)
# ---------------------------------------------------------------------------
DB_CONNECTION_STRING = os.getenv("AZURE_DATABASE_CONNECTION_STRING", "")
DB_PASSWORD = os.getenv("AZURE_DATABASE_PASSWORD", "")
SQL_MAX_INLINE_ROWS = int(os.getenv("SQL_MAX_INLINE_ROWS", "30"))

# ---------------------------------------------------------------------------
# File download limits
# ---------------------------------------------------------------------------
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))               # seconds
MAX_DOWNLOAD_SIZE = int(os.getenv("MAX_DOWNLOAD_SIZE", str(500 * 1024 * 1024)))  # 500 MB

# ---------------------------------------------------------------------------
# Artifact detection
# ---------------------------------------------------------------------------
ARTIFACT_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".html", ".csv",
    ".xlsx", ".json", ".svg", ".pdf",
}
