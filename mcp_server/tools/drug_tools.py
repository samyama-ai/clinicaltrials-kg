"""MCP tools for drug queries."""

import json
from fastmcp import FastMCP


def register_drug_tools(mcp: FastMCP):
    """Register drug-related MCP tools."""

    @mcp.tool()
    def drug_trials(drug_name: str) -> str:
        """Find all clinical trials testing a specific drug.

        Args:
            drug_name: Drug name (generic or brand, e.g. "Metformin").

        Returns:
            JSON array of trials with NCT ID, title, phase, and status.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:TESTS]->(d:Drug) "
            "WHERE toLower(d.name) CONTAINS toLower($drug_name) "
            "   OR toLower(d.generic_name) CONTAINS toLower($drug_name) "
            "RETURN t.nct_id AS nct_id, t.brief_title AS title, "
            "t.phase AS phase, t.overall_status AS status, "
            "d.name AS drug "
            "ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly("default", cypher, {"drug_name": drug_name})
        return json.dumps(results, default=str)

    @mcp.tool()
    def drug_adverse_events(drug_name: str, limit: int = 20) -> str:
        """Retrieve known adverse events for a drug from OpenFDA data.

        Args:
            drug_name: Drug name to look up.
            limit: Maximum number of adverse events to return (default 20).

        Returns:
            JSON array of adverse events with term, severity, and frequency count.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (d:Drug)-[:HAS_ADVERSE_EVENT]->(ae:AdverseEvent) "
            "WHERE toLower(d.name) CONTAINS toLower($drug_name) "
            "   OR toLower(d.generic_name) CONTAINS toLower($drug_name) "
            "RETURN ae.term AS event, ae.severity AS severity, "
            "ae.count AS frequency "
            "ORDER BY ae.count DESC LIMIT $limit"
        )
        results = client.query_readonly(
            "default", cypher, {"drug_name": drug_name, "limit": limit}
        )
        return json.dumps(results, default=str)

    @mcp.tool()
    def drug_interactions(drug_name: str) -> str:
        """Find known drug-drug interactions for a given drug.

        Interactions are sourced from trial co-administration data and
        pharmacological databases linked during ETL.

        Args:
            drug_name: Drug name to check for interactions.

        Returns:
            JSON array of interacting drugs with interaction type and severity.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (d1:Drug)-[i:INTERACTS_WITH]->(d2:Drug) "
            "WHERE toLower(d1.name) CONTAINS toLower($drug_name) "
            "   OR toLower(d1.generic_name) CONTAINS toLower($drug_name) "
            "RETURN d2.name AS interacting_drug, "
            "i.interaction_type AS interaction_type, "
            "i.severity AS severity, "
            "i.description AS description "
            "ORDER BY i.severity DESC"
        )
        results = client.query_readonly("default", cypher, {"drug_name": drug_name})
        return json.dumps(results, default=str)

    @mcp.tool()
    def drug_class(drug_name: str) -> str:
        """Get the ATC (Anatomical Therapeutic Chemical) classification hierarchy
        for a drug.

        Returns the full ATC path from anatomical main group down to the
        chemical substance level.

        Args:
            drug_name: Drug name to classify.

        Returns:
            JSON object with ATC levels (L1 through L5) and their descriptions.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (d:Drug)-[:BELONGS_TO]->(l5:ATCLevel5)"
            "-[:CHILD_OF]->(l4:ATCLevel4)"
            "-[:CHILD_OF]->(l3:ATCLevel3)"
            "-[:CHILD_OF]->(l2:ATCLevel2)"
            "-[:CHILD_OF]->(l1:ATCLevel1) "
            "WHERE toLower(d.name) CONTAINS toLower($drug_name) "
            "   OR toLower(d.generic_name) CONTAINS toLower($drug_name) "
            "RETURN d.name AS drug, "
            "l1.code AS atc_l1_code, l1.name AS atc_l1_name, "
            "l2.code AS atc_l2_code, l2.name AS atc_l2_name, "
            "l3.code AS atc_l3_code, l3.name AS atc_l3_name, "
            "l4.code AS atc_l4_code, l4.name AS atc_l4_name, "
            "l5.code AS atc_l5_code, l5.name AS atc_l5_name"
        )
        results = client.query_readonly("default", cypher, {"drug_name": drug_name})
        if not results:
            return json.dumps({"error": f"No ATC classification found for '{drug_name}'"})
        return json.dumps(results[0], default=str)
