"""
"""
# ------------------------------------------------------------------------------------------------------------------
"""
MCP Resources — prompts and guides exposed to connected agents.
"""

USAGE_GUIDE = """\
# Usage Guide

## Tools at a Glance
| Tool | Purpose |
|------|---------|
| `exec_code` | Run Python/JS in a **stateful** kernel (variables persist across calls) |
| `exec_sql` | Run **read-only SELECT** queries against Azure SQL |
| `upload_file` | Download a file from URL into the session's `input/` folder |
| `list_artifacts` | List all generated files in `output/` |
| `session_info` | Get session paths (`input_dir`, `output_dir`) and effective config |
| `list_sessions` | Show all active sessions |
| `destroy_session` | Kill a session and free resources |

## Key Rules

### Stateful Kernel (`exec_code`)
- State **persists** within the same `session_id` — variables, imports, DataFrames all survive across calls.
- **Don't** re-import or re-load what's already in memory. Reference existing variables directly.
- On error, fix only the broken line — don't re-run everything.

### Sessions
- **Auto-created** on first use — just pass any `session_id` and go.
- Call `session_info` to discover `input_dir` and `output_dir` absolute paths.

### Files & Artifacts
- `upload_file` saves to `input/` → use the returned path in `exec_code`.
- Save outputs (plots, CSV, Excel, HTML) to `output/` — they become artifacts automatically.
- If blob storage is configured, artifacts get a **SAS URL** in the response (`new_artifacts[].sas_url`).

### SQL Queries (`exec_sql`)
- **Read-only**: two-level safety — (1) app blocks write keywords, (2) DB transaction always rolls back.
- **Small results** (≤ `SQL_MAX_INLINE_ROWS`, default 30) → returned inline as JSON.
- **Large results** → saved as CSV in `input/` folder. Load it directly:
  ```python
  df = pd.read_csv(r'<file_path from response>')
  ```
  No re-upload needed — the file is already in the session.

## Client Headers (all optional)
All numeric values are **clamped**: `effective = min(client_value, server_max)`.

## Best Practices
1. **First call**: import libraries + load data.
2. **Next calls**: reuse variables already in memory.
3. Save outputs to `output/` using absolute paths.
4. Check `new_artifacts` in the response for paths and SAS URLs.
5. Use `exec_sql` for SQL queries — no code execution needed.
6. Call `destroy_session` when done.
7. If a session is not found (expired/cleaned up), create a new one with the same `session_id` and re-upload files / re-run setup — do not assume prior state survived.
"""
