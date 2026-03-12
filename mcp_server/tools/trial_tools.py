"""MCP tools for clinical trial queries."""

import json
from fastmcp import FastMCP


def register_trial_tools(mcp: FastMCP):
    """Register trial-related MCP tools."""

    @mcp.tool()
    def search_trials(
        condition: str, phase: str = "", status: str = ""
    ) -> str:
        """Search clinical trials by condition, phase, and/or recruitment status.

        Args:
            condition: Disease or condition name (e.g. "Breast Cancer").
            phase: Optional trial phase filter (e.g. "Phase 3").
            status: Optional recruitment status (e.g. "Recruiting", "Completed").

        Returns:
            JSON array of matching trials with NCT ID, title, phase, and status.
        """
        from mcp_server.server import client

        where_clauses = ["c.name CONTAINS $condition"]
        params = {"condition": condition}

        if phase:
            where_clauses.append("t.phase = $phase")
            params["phase"] = phase
        if status:
            where_clauses.append("t.overall_status = $status")
            params["status"] = status

        where = " AND ".join(where_clauses)
        cypher = (
            f"MATCH (t:Trial)-[:STUDIES]->(c:Condition) "
            f"WHERE {where} "
            f"RETURN t.nct_id AS nct_id, t.brief_title AS title, "
            f"t.phase AS phase, t.overall_status AS status "
            f"ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly("default", cypher, params)
        return json.dumps(results, default=str)

    @mcp.tool()
    def get_trial(nct_id: str) -> str:
        """Get full details for a specific clinical trial by NCT ID.

        Args:
            nct_id: ClinicalTrials.gov identifier (e.g. "NCT04280705").

        Returns:
            JSON object with trial details including title, phase, status,
            interventions, conditions, sponsors, and eligibility criteria.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial {nct_id: $nct_id}) "
            "OPTIONAL MATCH (t)-[:STUDIES]->(c:Condition) "
            "OPTIONAL MATCH (t)-[:TESTS]->(d:Drug) "
            "OPTIONAL MATCH (t)-[:SPONSORED_BY]->(s:Sponsor) "
            "RETURN t.nct_id AS nct_id, t.brief_title AS title, "
            "t.phase AS phase, t.overall_status AS status, "
            "t.start_date AS start_date, t.enrollment AS enrollment, "
            "t.eligibility_criteria AS eligibility, "
            "collect(DISTINCT c.name) AS conditions, "
            "collect(DISTINCT d.name) AS drugs, "
            "collect(DISTINCT s.name) AS sponsors"
        )
        results = client.query_readonly("default", cypher, {"nct_id": nct_id})
        if not results:
            return json.dumps({"error": f"Trial {nct_id} not found"})
        return json.dumps(results[0], default=str)

    @mcp.tool()
    def find_similar_trials(nct_id: str, k: int = 10) -> str:
        """Find semantically similar trials using vector search.

        Uses embeddings generated from trial titles and descriptions to find
        trials with similar research objectives.

        Args:
            nct_id: NCT ID of the reference trial.
            k: Number of similar trials to return (default 10).

        Returns:
            JSON array of similar trials ranked by similarity score.
        """
        from mcp_server.server import client

        # Retrieve the embedding for the reference trial
        ref = client.query_readonly(
            "default",
            "MATCH (t:Trial {nct_id: $nct_id}) RETURN id(t) AS node_id",
            {"nct_id": nct_id},
        )
        if not ref:
            return json.dumps({"error": f"Trial {nct_id} not found"})

        node_id = ref[0]["node_id"]
        neighbours = client.vector_search("default", "trial_embedding", node_id, k + 1)

        # Filter out the query trial itself and format results
        similar = []
        for nbr in neighbours:
            if nbr["nct_id"] == nct_id:
                continue
            similar.append(
                {
                    "nct_id": nbr["nct_id"],
                    "title": nbr.get("title", ""),
                    "score": round(nbr["score"], 4),
                }
            )
        return json.dumps(similar[:k], default=str)

    @mcp.tool()
    def trial_sites(nct_id: str) -> str:
        """List all study sites for a clinical trial with their locations.

        Args:
            nct_id: ClinicalTrials.gov identifier.

        Returns:
            JSON array of sites with facility name, city, state, and country.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial {nct_id: $nct_id})-[:CONDUCTED_AT]->(s:Site) "
            "RETURN s.facility AS facility, s.city AS city, "
            "s.state AS state, s.country AS country "
            "ORDER BY s.country, s.city"
        )
        results = client.query_readonly("default", cypher, {"nct_id": nct_id})
        return json.dumps(results, default=str)
