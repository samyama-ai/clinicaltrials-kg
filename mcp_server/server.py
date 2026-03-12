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
client: SamyamaClient | None = None


@mcp.on_startup
async def startup():
    """Initialise the embedded Samyama graph client on server start."""
    global client
    client = SamyamaClient.embedded()


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
