"""MCP tools for drug queries."""

import json
from fastmcp import FastMCP


def register_drug_tools(mcp: FastMCP):
    """Register drug-related MCP tools."""

    @mcp.tool()
    def drug_trials(drug_name: str) -> str:
        """Find all clinical trials testing a specific drug.

        Traces from Drug nodes through Intervention nodes to ClinicalTrial nodes.

        Args:
            drug_name: Drug name (generic or brand, e.g. "Metformin").

        Returns:
            JSON array of trials with NCT ID, title, phase, and status.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_name = _escape(drug_name)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:TESTS]->(i:Intervention)"
            f"-[:CODED_AS_DRUG]->(d:Drug) "
            f'WHERE d.name CONTAINS "{safe_name}" '
            f"RETURN t.nct_id AS nct_id, t.title AS title, "
            f"t.phase AS phase, t.overall_status AS status, "
            f"d.name AS drug "
            f"ORDER BY t.start_date DESC LIMIT 50"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def drug_adverse_events(drug_name: str, limit: int = 20) -> str:
        """Retrieve known adverse events for a drug from OpenFDA data.

        Args:
            drug_name: Drug name to look up.
            limit: Maximum number of adverse events to return (default 20).

        Returns:
            JSON array of adverse events with term and source vocabulary.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_name = _escape(drug_name)
        cypher = (
            f"MATCH (d:Drug)-[:HAS_ADVERSE_EFFECT]->(ae:AdverseEvent) "
            f'WHERE d.name CONTAINS "{safe_name}" '
            f"RETURN ae.term AS event, ae.source_vocabulary AS source "
            f"LIMIT {int(limit)}"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def drug_class(drug_name: str) -> str:
        """Get the ATC (Anatomical Therapeutic Chemical) classification hierarchy
        for a drug.

        Returns the DrugClass nodes linked via CLASSIFIED_AS and PARENT_CLASS
        edges, showing the full ATC hierarchy from substance to anatomical group.

        Args:
            drug_name: Drug name to classify.

        Returns:
            JSON object with ATC code, class name, and hierarchy levels.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_name = _escape(drug_name)

        # Get the drug's immediate class (most specific ATC level)
        cypher = (
            f"MATCH (d:Drug)-[:CLASSIFIED_AS]->(dc:DrugClass) "
            f'WHERE d.name CONTAINS "{safe_name}" '
            f"RETURN d.name AS drug, d.rxnorm_cui AS rxnorm_cui, "
            f"dc.atc_code AS atc_code, dc.name AS class_name, dc.level AS level"
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        if not results:
            return json.dumps({"error": f"No ATC classification found for '{drug_name}'"})

        # Walk up the hierarchy via PARENT_CLASS
        drug_info = results[0]
        hierarchy = [{"atc_code": drug_info["atc_code"],
                      "name": drug_info["class_name"],
                      "level": drug_info["level"]}]

        current_code = _escape(str(drug_info["atc_code"]))
        for _ in range(5):
            parent_cypher = (
                f'MATCH (child:DrugClass {{atc_code: "{current_code}"}})'
                f"-[:PARENT_CLASS]->(parent:DrugClass) "
                f"RETURN parent.atc_code AS atc_code, "
                f"parent.name AS name, parent.level AS level"
            )
            parents = _to_dicts(client.query_readonly(parent_cypher, GRAPH))
            if not parents:
                break
            hierarchy.append(parents[0])
            current_code = _escape(str(parents[0]["atc_code"]))

        return json.dumps({
            "drug": drug_info["drug"],
            "rxnorm_cui": drug_info.get("rxnorm_cui"),
            "hierarchy": hierarchy,
        }, default=str)
