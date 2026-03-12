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
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        where_parts = [f'c.name CONTAINS "{_escape(condition)}"']
        if phase:
            where_parts.append(f't.phase = "{_escape(phase)}"')
        if status:
            where_parts.append(f't.overall_status = "{_escape(status)}"')

        where = " AND ".join(where_parts)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
            f"WHERE {where} "
            f"RETURN t.nct_id AS nct_id, t.title AS title, "
            f"t.phase AS phase, t.overall_status AS status "
            f"ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def get_trial(nct_id: str) -> str:
        """Get full details for a specific clinical trial by NCT ID.

        Args:
            nct_id: ClinicalTrials.gov identifier (e.g. "NCT04280705").

        Returns:
            JSON object with trial details including title, phase, status,
            interventions, conditions, sponsors, and eligibility criteria.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_id = _escape(nct_id)
        cypher = (
            f'MATCH (t:ClinicalTrial {{nct_id: "{safe_id}"}}) '
            f"OPTIONAL MATCH (t)-[:STUDIES]->(c:Condition) "
            f"OPTIONAL MATCH (t)-[:TESTS]->(i:Intervention) "
            f"OPTIONAL MATCH (t)-[:SPONSORED_BY]->(s:Sponsor) "
            f"RETURN t.nct_id AS nct_id, t.title AS title, "
            f"t.phase AS phase, t.overall_status AS status, "
            f"t.start_date AS start_date, t.enrollment AS enrollment, "
            f"t.brief_summary AS summary, "
            f"collect(DISTINCT c.name) AS conditions, "
            f"collect(DISTINCT i.name) AS interventions, "
            f"collect(DISTINCT s.name) AS sponsors"
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        if not results:
            return json.dumps({"error": f"Trial {nct_id} not found"})
        return json.dumps(results[0], default=str)

    @mcp.tool()
    def find_similar_trials(nct_id: str, k: int = 10) -> str:
        """Find semantically similar trials using vector search.

        Uses embeddings generated from trial descriptions to find
        trials with similar research objectives.

        Args:
            nct_id: NCT ID of the reference trial.
            k: Number of similar trials to return (default 10).

        Returns:
            JSON array of similar trials ranked by similarity score.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_id = _escape(nct_id)

        # Get the reference trial's summary to encode
        ref = _to_dicts(client.query_readonly(
            f'MATCH (t:ClinicalTrial {{nct_id: "{safe_id}"}}) '
            f"RETURN t.brief_summary AS summary",
            GRAPH,
        ))
        if not ref or not ref[0].get("summary"):
            return json.dumps({"error": f"Trial {nct_id} not found or has no summary"})

        # Encode the summary to get a query vector
        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer("all-MiniLM-L6-v2")
            query_vec = model.encode([ref[0]["summary"]]).tolist()[0]
        except ImportError:
            return json.dumps({"error": "sentence-transformers not installed"})

        # Vector search for similar trials
        neighbours = client.vector_search("ClinicalTrial", "embedding", query_vec, k + 1)

        # Look up trial details for each result
        similar = []
        for node_id, distance in neighbours:
            rows = _to_dicts(client.query_readonly(
                f"MATCH (t:ClinicalTrial) WHERE id(t) = {node_id} "
                f"RETURN t.nct_id AS nct_id, t.title AS title",
                GRAPH,
            ))
            if rows and rows[0].get("nct_id") != nct_id:
                similar.append({
                    "nct_id": rows[0]["nct_id"],
                    "title": rows[0].get("title", ""),
                    "score": round(1.0 - distance, 4),
                })
        return json.dumps(similar[:k], default=str)

    @mcp.tool()
    def trial_sites(nct_id: str) -> str:
        """List all study sites for a clinical trial with their locations.

        Args:
            nct_id: ClinicalTrials.gov identifier.

        Returns:
            JSON array of sites with facility name, city, state, and country.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_id = _escape(nct_id)
        cypher = (
            f'MATCH (t:ClinicalTrial {{nct_id: "{safe_id}"}})-[:CONDUCTED_AT]->(s:Site) '
            f"RETURN s.facility AS facility, s.city AS city, "
            f"s.state AS state, s.country AS country "
            f"ORDER BY s.country, s.city"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)
