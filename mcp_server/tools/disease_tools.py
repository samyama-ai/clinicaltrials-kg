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
        from mcp_server.server import client

        where_clauses = ["toLower(c.name) CONTAINS toLower($condition)"]
        params: dict = {"condition": condition}

        if phase:
            where_clauses.append("t.phase = $phase")
            params["phase"] = phase

        where = " AND ".join(where_clauses)
        cypher = (
            f"MATCH (t:Trial)-[:STUDIES]->(c:Condition) "
            f"WHERE {where} "
            f"RETURN t.nct_id AS nct_id, t.brief_title AS title, "
            f"t.phase AS phase, t.overall_status AS status, "
            f"t.enrollment AS enrollment "
            f"ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly("default", cypher, params)
        return json.dumps(results, default=str)

    @mcp.tool()
    def treatment_landscape(condition: str) -> str:
        """Get all interventions being tested for a condition, grouped by type.

        Provides a high-level view of the therapeutic landscape: how many
        trials are testing each drug or intervention for this condition.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of interventions with name, type, trial count, and phases.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:STUDIES]->(c:Condition), "
            "      (t)-[:TESTS]->(d:Drug) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "RETURN d.name AS intervention, d.intervention_type AS type, "
            "count(t) AS trial_count, "
            "collect(DISTINCT t.phase) AS phases "
            "ORDER BY trial_count DESC LIMIT 30"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})
        return json.dumps(results, default=str)

    @mcp.tool()
    def related_conditions(condition: str) -> str:
        """Find conditions related via MeSH hierarchy.

        Traverses the MeSH tree to find parent, sibling, and child conditions
        that share a common MeSH ancestor.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of related conditions with MeSH relationship type and
            shared ancestor.
        """
        from mcp_server.server import client

        # Find siblings: conditions sharing the same MeSH parent
        cypher_siblings = (
            "MATCH (c1:Condition)-[:MAPPED_TO]->(m1:MeSHTerm)"
            "-[:CHILD_OF]->(parent:MeSHTerm)"
            "<-[:CHILD_OF]-(m2:MeSHTerm)<-[:MAPPED_TO]-(c2:Condition) "
            "WHERE toLower(c1.name) CONTAINS toLower($condition) "
            "  AND c1 <> c2 "
            "RETURN DISTINCT c2.name AS related_condition, "
            "'sibling' AS relationship, "
            "parent.name AS shared_ancestor "
            "ORDER BY c2.name LIMIT 20"
        )

        # Find children: conditions under the same MeSH term
        cypher_children = (
            "MATCH (c1:Condition)-[:MAPPED_TO]->(m:MeSHTerm)"
            "<-[:CHILD_OF]-(child:MeSHTerm)<-[:MAPPED_TO]-(c2:Condition) "
            "WHERE toLower(c1.name) CONTAINS toLower($condition) "
            "  AND c1 <> c2 "
            "RETURN DISTINCT c2.name AS related_condition, "
            "'child' AS relationship, "
            "m.name AS shared_ancestor "
            "ORDER BY c2.name LIMIT 20"
        )

        params = {"condition": condition}
        siblings = client.query_readonly("default", cypher_siblings, params)
        children = client.query_readonly("default", cypher_children, params)

        combined = siblings + children
        # Deduplicate by condition name, keeping the first occurrence
        seen = set()
        unique = []
        for row in combined:
            name = row["related_condition"]
            if name not in seen:
                seen.add(name)
                unique.append(row)

        return json.dumps(unique, default=str)

    @mcp.tool()
    def disease_genes(condition: str) -> str:
        """Find genes associated with a disease or condition.

        Gene associations come from trial biomarker data and linked
        genomic databases.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of genes with name, association type, and supporting
            trial count.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (c:Condition)<-[:STUDIES]-(t:Trial)-[:HAS_BIOMARKER]->(g:Gene) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "RETURN g.name AS gene, g.symbol AS symbol, "
            "g.association_type AS association_type, "
            "count(DISTINCT t) AS trial_count "
            "ORDER BY trial_count DESC LIMIT 30"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})
        return json.dumps(results, default=str)
