"""
Code Interpreter MCP — Python package.

A shared, stateful Code Interpreter service that:
  - Uses Open Interpreter for stateful Python kernel execution
  - Exposes tools via MCP (streamable-http transport)
  - Supports Excel → Pandas → Matplotlib / Plotly pipelines
  - Preserves multi-step Python kernel state per session
  - Mirrors Azure Assistants Code Interpreter semantics 1:1
  - Can be consumed by any MCP-compatible agent framework (SK, LangGraph, CrewAI, AutoGen)
"""

__version__ = "1.0.0"
