"""MCP tools for clinical trial analytics."""

import json
from fastmcp import FastMCP


def register_analytics_tools(mcp: FastMCP):
    """Register analytics-related MCP tools."""

    @mcp.tool()
    def enrollment_by_phase(condition: str) -> str:
        """Get enrollment counts grouped by trial phase for a condition.

        Useful for understanding how much patient exposure exists at each
        stage of clinical development.

        Args:
            condition: Disease or condition name (e.g. "Type 2 Diabetes").

        Returns:
            JSON array with phase, trial count, total enrollment, and
            average enrollment per trial.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
            f'WHERE c.name CONTAINS "{safe_cond}" '
            f"RETURN t.phase AS phase, "
            f"count(t) AS trial_count, "
            f"sum(t.enrollment) AS total_enrollment, "
            f"avg(t.enrollment) AS avg_enrollment "
            f"ORDER BY phase"
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        for row in results:
            if row.get("avg_enrollment") is not None:
                row["avg_enrollment"] = round(row["avg_enrollment"], 1)
        return json.dumps(results, default=str)

    @mcp.tool()
    def sponsor_landscape(condition: str) -> str:
        """Show who is funding clinical research for a condition.

        Returns sponsors ranked by the number of trials they are running,
        along with their class (Industry, NIH, etc.).

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of sponsors with name, class, trial count, and phases.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), "
            f"      (t)-[:SPONSORED_BY]->(s:Sponsor) "
            f'WHERE c.name CONTAINS "{safe_cond}" '
            f"RETURN s.name AS sponsor, s.class AS sponsor_class, "
            f"count(t) AS trial_count, "
            f"collect(DISTINCT t.phase) AS phases "
            f"ORDER BY trial_count DESC LIMIT 25"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def geographic_distribution(condition: str) -> str:
        """Show where clinical trials for a condition are being conducted.

        Aggregates trial site locations by country.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of countries with trial count and site count.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), "
            f"      (t)-[:CONDUCTED_AT]->(s:Site) "
            f'WHERE c.name CONTAINS "{safe_cond}" '
            f"RETURN s.country AS country, "
            f"count(DISTINCT t) AS trial_count, "
            f"count(s) AS site_count "
            f"ORDER BY trial_count DESC"
        )
        results = client.query_readonly(cypher, GRAPH)
        return json.dumps(_to_dicts(results), default=str)

    @mcp.tool()
    def trial_timeline(condition: str) -> str:
        """Show clinical trial activity over time for a condition.

        Groups trials by start year to reveal trends in research activity.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of years with trial count and total enrollment.
        """
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe_cond = _escape(condition)
        cypher = (
            f"MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
            f'WHERE c.name CONTAINS "{safe_cond}" '
            f"RETURN t.start_date AS start_date, "
            f"t.phase AS phase, t.enrollment AS enrollment, "
            f"t.nct_id AS nct_id"
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))

        # Group by year in Python (avoids substring function compatibility)
        from collections import defaultdict
        by_year: dict[str, list] = defaultdict(list)
        for row in results:
            date_str = str(row.get("start_date", "") or "")
            year = date_str[:4] if len(date_str) >= 4 else "Unknown"
            by_year[year].append(row)

        timeline = []
        for year in sorted(by_year.keys()):
            rows = by_year[year]
            total_enroll = sum(r.get("enrollment") or 0 for r in rows)
            phases = [r.get("phase") for r in rows if r.get("phase")]
            from collections import Counter
            counts = Counter(phases)
            dominant = counts.most_common(1)[0][0] if counts else None
            timeline.append({
                "year": year,
                "trial_count": len(rows),
                "total_enrollment": total_enroll,
                "dominant_phase": dominant,
            })
        return json.dumps(timeline, default=str)
