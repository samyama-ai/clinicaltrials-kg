"""Clinical Trials Knowledge Graph -- MCP Server

FastMCP server exposing clinical trial, drug, disease, and analytics
tools backed by a Samyama embedded graph database.
"""

from fastmcp import FastMCP
from samyama import SamyamaClient
from mcp_server.tools.trial_tools import register_trial_tools
from mcp_server.tools.drug_tools import register_drug_tools
from mcp_server.tools.disease_tools import register_disease_tools
from mcp_server.tools.analytics_tools import register_analytics_tools

mcp = FastMCP("Clinical Trials KG")
client: SamyamaClient = SamyamaClient.embedded()


GRAPH = "default"


def _escape(value: str) -> str:
    """Strip double quotes and normalize whitespace for Cypher string literals."""
    if value is None:
        return ""
    return value.replace('"', '').replace('\n', ' ').replace('\r', '')


def _q(val: str) -> str:
    """Quote and escape a value for inline Cypher."""
    return f'"{_escape(val)}"'


def _to_dicts(result) -> list[dict]:
    """Convert a QueryResult (columns + records) to a list of dicts."""
    return [dict(zip(result.columns, row)) for row in result.records]


# Register all tool groups
register_trial_tools(mcp)
register_drug_tools(mcp)
register_disease_tools(mcp)
register_analytics_tools(mcp)


def run():
    """Entry point for ``python -m mcp_server.server``."""
    mcp.run()


if __name__ == "__main__":
    run()
