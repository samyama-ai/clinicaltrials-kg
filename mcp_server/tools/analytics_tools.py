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
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:STUDIES]->(c:Condition) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "  AND t.enrollment IS NOT NULL "
            "RETURN t.phase AS phase, "
            "count(t) AS trial_count, "
            "sum(t.enrollment) AS total_enrollment, "
            "avg(t.enrollment) AS avg_enrollment "
            "ORDER BY phase"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})
        # Round averages for readability
        for row in results:
            if row.get("avg_enrollment") is not None:
                row["avg_enrollment"] = round(row["avg_enrollment"], 1)
        return json.dumps(results, default=str)

    @mcp.tool()
    def sponsor_landscape(condition: str) -> str:
        """Show who is funding clinical research for a condition.

        Returns sponsors ranked by the number of trials they are running,
        along with their type (Industry, NIH, Academic, etc.).

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of sponsors with name, type, trial count, and phases.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:STUDIES]->(c:Condition), "
            "      (t)-[:SPONSORED_BY]->(s:Sponsor) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "RETURN s.name AS sponsor, s.sponsor_type AS type, "
            "count(t) AS trial_count, "
            "collect(DISTINCT t.phase) AS phases "
            "ORDER BY trial_count DESC LIMIT 25"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})
        return json.dumps(results, default=str)

    @mcp.tool()
    def geographic_distribution(condition: str) -> str:
        """Show where clinical trials for a condition are being conducted.

        Aggregates trial site locations by country.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of countries with trial count and site count.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:STUDIES]->(c:Condition), "
            "      (t)-[:CONDUCTED_AT]->(s:Site) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "RETURN s.country AS country, "
            "count(DISTINCT t) AS trial_count, "
            "count(s) AS site_count "
            "ORDER BY trial_count DESC"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})
        return json.dumps(results, default=str)

    @mcp.tool()
    def trial_timeline(condition: str) -> str:
        """Show clinical trial activity over time for a condition.

        Groups trials by start year to reveal trends in research activity.

        Args:
            condition: Disease or condition name.

        Returns:
            JSON array of years with trial count, total enrollment, and
            dominant phase.
        """
        from mcp_server.server import client

        cypher = (
            "MATCH (t:Trial)-[:STUDIES]->(c:Condition) "
            "WHERE toLower(c.name) CONTAINS toLower($condition) "
            "  AND t.start_date IS NOT NULL "
            "WITH t, substring(toString(t.start_date), 0, 4) AS year "
            "RETURN year, "
            "count(t) AS trial_count, "
            "sum(t.enrollment) AS total_enrollment, "
            "collect(t.phase) AS phases "
            "ORDER BY year"
        )
        results = client.query_readonly("default", cypher, {"condition": condition})

        # Compute dominant phase per year
        for row in results:
            phase_list = row.pop("phases", [])
            if phase_list:
                from collections import Counter
                counts = Counter(p for p in phase_list if p)
                row["dominant_phase"] = counts.most_common(1)[0][0] if counts else None
            else:
                row["dominant_phase"] = None

        return json.dumps(results, default=str)
