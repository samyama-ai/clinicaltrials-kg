"""MCP tools for disease queries."""

import json
from fastmcp import FastMCP


def register_disease_tools(mcp: FastMCP):
    """Register disease-related MCP tools."""

    @mcp.tool()
    def disease_trials(condition: str, phase: str = "") -> str:
        """Find clinical trials studying a specific disease or condition.

        Args:
            condition: Disease or condition name (e.g. "Alzheimer Disease").
            phase: Optional phase filter (e.g. "Phase 2", "Phase 3").

        Returns:
            JSON array of trials with NCT ID, title, phase, status, and enrollment.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        where_parts = [f'c.name CONTAINS "{safe_cond}"']
        if phase:
            where_parts.append(f't.phase = "{_escape(phase)}"')

        where = " AND ".join(where_parts)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
            f"WHERE {where} "
            f"RETURN t.nct_id AS nct_id, t.title AS title, "
            f"t.phase AS phase, t.overall_status AS status, "
            f"t.enrollment AS enrollment "
            f"ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def treatment_landscape(condition: str) -> str:
        """Get all interventions being tested for a condition, grouped by type.

        Provides a high-level view of the therapeutic landscape: how many
        trials are testing each intervention for this condition.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of interventions with name, type, trial count, and phases.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), "
            f"      (t)-[:TESTS]->(i:Intervention) "
            f'WHERE c.name CONTAINS "{safe_cond}" '
            f"RETURN i.name AS intervention, i.type AS type, "
            f"count(t) AS trial_count, "
            f"collect(DISTINCT t.phase) AS phases "
            f"ORDER BY trial_count DESC LIMIT 30"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def related_conditions(condition: str) -> str:
        """Find conditions related via MeSH hierarchy.

        Traverses the MeSH descriptor tree to find conditions that share a
        common MeSH ancestor via CODED_AS_MESH and BROADER_THAN edges.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of related conditions with shared MeSH descriptor.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)

        # Find siblings: conditions sharing the same parent MeSH descriptor
        cypher = (
            f"MATCH (c1:Condition)-[:CODED_AS_MESH]->(m1:MeSHDescriptor)"
            f"<-[:BROADER_THAN]-(parent:MeSHDescriptor)"
            f"-[:BROADER_THAN]->(m2:MeSHDescriptor)"
            f"<-[:CODED_AS_MESH]-(c2:Condition) "
            f'WHERE c1.name CONTAINS "{safe_cond}" '
            f"RETURN DISTINCT c2.name AS related_condition, "
            f"parent.name AS shared_ancestor "
            f"ORDER BY c2.name LIMIT 20"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)
