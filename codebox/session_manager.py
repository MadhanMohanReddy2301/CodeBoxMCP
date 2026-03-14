"""
"""
#------------------------------------------------------------------------------------------------------------------

"""
Session Manager — lifecycle management for Open Interpreter sessions.

Rules (Azure CI-equivalent):
  1. One interpreter process = one conversation
  2. Interpreter persists across tool calls
  3. Interpreter dies after TTL
  4. No interpreter sharing across sessions
  5. Interpreter is infra, NOT agent logic
"""

import os
import shutil
import threading
import time
from typing import Any

from interpreter import OpenInterpreter

from codebox.config import (
    BASE_WORKDIR,
    CLEANUP_INTERVAL,
    IDLE_TIMEOUT,
    SESSION_TTL,
    logger,
)


# ===========================================================================
#  SESSION MANAGER
# ===========================================================================
class SessionManager:
    """Manages Open Interpreter sessions with thread-safe CRUD."""

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    # ----- public API -----

    def get_or_create(
        self,
        session_id: str,
        session_ttl: int | None = None,
        idle_timeout: int | None = None,
    ) -> dict[str, Any]:
        """Return existing session or spin up a new one."""
        with self._lock:
            if session_id in self._sessions:
                sess = self._sessions[session_id]
                sess["last_used"] = time.time()
                logger.info("Reusing session %s", session_id)
                return sess
            return self._create_session(session_id, session_ttl, idle_timeout)

    def get(self, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            sess = self._sessions.get(session_id)
            if sess:
                sess["last_used"] = time.time()
            return sess

    def destroy(self, session_id: str) -> None:
        """Kill interpreter and wipe filesystem for a session."""
        with self._lock:
            sess = self._sessions.pop(session_id, None)
        if sess:
            self._cleanup_session(sess, session_id)

    def cleanup_expired(self) -> None:
        """Remove sessions that exceed their per-session TTL or idle timeout."""
        now = time.time()
        expired_ids: list[str] = []
        with self._lock:
            for sid, sess in self._sessions.items():
                age = now - sess["created_at"]
                idle = now - sess["last_used"]
                ttl = sess.get("session_ttl", SESSION_TTL)
                idle_max = sess.get("idle_timeout", IDLE_TIMEOUT)
                if age > ttl or idle > idle_max:
                    expired_ids.append(sid)
        for sid in expired_ids:
            logger.info("Expiring session %s", sid)
            self.destroy(sid)

    def list_sessions(self) -> list[dict]:
        """Return metadata for all active sessions."""
        with self._lock:
            result = []
            now = time.time()
            for sid, sess in self._sessions.items():
                result.append({
                    "session_id": sid,
                    "age_seconds": round(now - sess["created_at"]),
                    "idle_seconds": round(now - sess["last_used"]),
                    "workdir": sess["workdir"],
                })
            return result

    # ----- internal -----

    def _create_session(
        self,
        session_id: str,
        session_ttl: int | None = None,
        idle_timeout: int | None = None,
    ) -> dict[str, Any]:
        """Create a new session with its own interpreter and filesystem."""
        workdir = os.path.join(BASE_WORKDIR, f"session_{session_id}")
        os.makedirs(os.path.join(workdir, "input"), exist_ok=True)
        os.makedirs(os.path.join(workdir, "output"), exist_ok=True)
        os.makedirs(os.path.join(workdir, "logs"), exist_ok=True)

        interpreter = self._create_interpreter(workdir)

        sess = {
            "interpreter": interpreter,
            "created_at": time.time(),
            "last_used": time.time(),
            "workdir": workdir,
            "session_ttl": session_ttl if session_ttl is not None else SESSION_TTL,
            "idle_timeout": idle_timeout if idle_timeout is not None else IDLE_TIMEOUT,
        }
        self._sessions[session_id] = sess
        logger.info("Created new session %s → %s", session_id, workdir)
        return sess

    @staticmethod
    def _create_interpreter(workdir: str):
        """Spin up a configured Open Interpreter instance."""

        interp = OpenInterpreter()

        # ---- LLM disabled ----
        # We use Open Interpreter purely as a stateful execution sandbox.
        # No LLM calls are made; configuration is intentionally skipped.

        # ---- Behaviour ----
        interp.auto_run = True            # execute code without confirmation
        interp.verbose = False
        interp.os = False                 # disable OS-level control

        # ---- Safety ----
        interp.safe_mode = "off"          # we rely on container-level isolation

        # ---- Working directory ----
        interp.computer.cwd = workdir     # all relative paths resolve here

        # ---- Bootstrap: set cwd inside the Python kernel (no LLM needed) ----
        try:
            list(interp.computer.run(
                "python",
                f"import os; os.chdir(r'{workdir}')",
                stream=True,
            ))
        except Exception as exc:
            logger.warning("Bootstrap os.chdir failed: %s", exc)

        return interp

    @staticmethod
    def _cleanup_session(sess: dict[str, Any], session_id: str) -> None:
        """Kill interpreter process and remove session directory."""
        try:
            interp = sess.get("interpreter")
            if interp:
                try:
                    interp.computer.terminate()
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Error terminating interpreter for %s: %s", session_id, exc)

        workdir = sess.get("workdir", "")
        if workdir and os.path.isdir(workdir):
            try:
                shutil.rmtree(workdir, ignore_errors=True)
                logger.info("Cleaned up workdir for session %s", session_id)
            except Exception as exc:
                logger.warning("Failed to clean workdir for %s: %s", session_id, exc)


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------
session_mgr = SessionManager()


# ===========================================================================
#  BACKGROUND CLEANUP THREAD
# ===========================================================================
def session_cleanup_loop() -> None:
    """Periodically removes expired / idle sessions."""
    logger.info("Session cleanup thread started (interval=%ds)", CLEANUP_INTERVAL)
    while True:
        try:
            session_mgr.cleanup_expired()
        except Exception as exc:
            logger.error("Cleanup error: %s", exc, exc_info=True)
        time.sleep(CLEANUP_INTERVAL)
