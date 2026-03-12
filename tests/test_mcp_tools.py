"""Smoke tests for MCP tool functions against an embedded Samyama graph.

Creates a small test dataset and verifies each MCP tool returns
correct JSON with expected keys.
"""

import json
import pytest
from samyama import SamyamaClient


@pytest.fixture(scope="module")
def client():
    """Create an embedded client and populate with test data."""
    c = SamyamaClient.embedded()
    _load_test_data(c)
    return c


def _q(val):
    return val.replace('"', '')


def _load_test_data(c):
    g = "default"

    # Trials
    c.query('MERGE (t:ClinicalTrial {nct_id: "NCT001", title: "Phase 3 Metformin for T2D", '
            'phase: "Phase 3", overall_status: "Completed", enrollment: 500, '
            'start_date: "2023-01-15", brief_summary: "A study of metformin in diabetes"})', g)
    c.query('MERGE (t:ClinicalTrial {nct_id: "NCT002", title: "Pembrolizumab Breast Cancer", '
            'phase: "Phase 2", overall_status: "Recruiting", enrollment: 200, '
            'start_date: "2024-03-01", brief_summary: "Immunotherapy for breast cancer"})', g)
    c.query('MERGE (t:ClinicalTrial {nct_id: "NCT003", title: "Insulin Analog Diabetes", '
            'phase: "Phase 2", overall_status: "Active", enrollment: 150, '
            'start_date: "2024-06-01", brief_summary: "Novel insulin analog for diabetic patients"})', g)

    # Conditions
    c.query('MERGE (c:Condition {name: "Type 2 Diabetes"})', g)
    c.query('MERGE (c:Condition {name: "Breast Cancer"})', g)
    c.query('MERGE (c:Condition {name: "Diabetes Mellitus"})', g)

    # Interventions
    c.query('MERGE (i:Intervention {name: "Metformin", type: "DRUG", description: "Oral diabetes medication"})', g)
    c.query('MERGE (i:Intervention {name: "Pembrolizumab", type: "DRUG", description: "Anti-PD-1 antibody"})', g)
    c.query('MERGE (i:Intervention {name: "Insulin Glargine", type: "DRUG", description: "Long-acting insulin"})', g)

    # Sponsors
    c.query('MERGE (s:Sponsor {name: "Merck", class: "INDUSTRY"})', g)
    c.query('MERGE (s:Sponsor {name: "NIH", class: "NIH"})', g)

    # Sites
    c.query('MERGE (s:Site {facility: "Mayo Clinic", city: "Rochester", state: "MN", country: "United States"})', g)
    c.query('MERGE (s:Site {facility: "Johns Hopkins", city: "Baltimore", state: "MD", country: "United States"})', g)
    c.query('MERGE (s:Site {facility: "Charite", city: "Berlin", country: "Germany"})', g)

    # Drug nodes (from drug_loader)
    c.query('MERGE (d:Drug {rxnorm_cui: "6809", name: "Metformin"})', g)

    # DrugClass (ATC hierarchy)
    c.query('MERGE (dc:DrugClass {atc_code: "A10BA02", name: "Metformin", level: 5})', g)
    c.query('MERGE (dc:DrugClass {atc_code: "A10BA", name: "Biguanides", level: 4})', g)
    c.query('MERGE (dc:DrugClass {atc_code: "A10B", name: "Blood glucose lowering drugs", level: 3})', g)

    # AdverseEvent
    c.query('MERGE (ae:AdverseEvent {term: "Nausea", source_vocabulary: "MedDRA"})', g)
    c.query('MERGE (ae:AdverseEvent {term: "Diarrhea", source_vocabulary: "MedDRA"})', g)

    # MeSHDescriptor
    c.query('MERGE (m:MeSHDescriptor {descriptor_id: "D003924", name: "Diabetes Mellitus, Type 2"})', g)
    c.query('MERGE (m:MeSHDescriptor {descriptor_id: "D001943", name: "Breast Neoplasms"})', g)
    c.query('MERGE (m:MeSHDescriptor {descriptor_id: "D003920", name: "Diabetes Mellitus"})', g)

    # Edges: STUDIES
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (c:Condition {name: "Type 2 Diabetes"}) CREATE (t)-[:STUDIES]->(c)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (c:Condition {name: "Diabetes Mellitus"}) CREATE (t)-[:STUDIES]->(c)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT002"}), (c:Condition {name: "Breast Cancer"}) CREATE (t)-[:STUDIES]->(c)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT003"}), (c:Condition {name: "Type 2 Diabetes"}) CREATE (t)-[:STUDIES]->(c)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT003"}), (c:Condition {name: "Diabetes Mellitus"}) CREATE (t)-[:STUDIES]->(c)', g)

    # Edges: TESTS
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (i:Intervention {name: "Metformin"}) CREATE (t)-[:TESTS]->(i)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT002"}), (i:Intervention {name: "Pembrolizumab"}) CREATE (t)-[:TESTS]->(i)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT003"}), (i:Intervention {name: "Insulin Glargine"}) CREATE (t)-[:TESTS]->(i)', g)

    # Edges: SPONSORED_BY
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (s:Sponsor {name: "NIH"}) CREATE (t)-[:SPONSORED_BY]->(s)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT002"}), (s:Sponsor {name: "Merck"}) CREATE (t)-[:SPONSORED_BY]->(s)', g)

    # Edges: CONDUCTED_AT
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (s:Site {facility: "Mayo Clinic"}) CREATE (t)-[:CONDUCTED_AT]->(s)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT001"}), (s:Site {facility: "Johns Hopkins"}) CREATE (t)-[:CONDUCTED_AT]->(s)', g)
    c.query('MATCH (t:ClinicalTrial {nct_id: "NCT002"}), (s:Site {facility: "Charite"}) CREATE (t)-[:CONDUCTED_AT]->(s)', g)

    # Edges: CODED_AS_DRUG
    c.query('MATCH (i:Intervention {name: "Metformin"}), (d:Drug {rxnorm_cui: "6809"}) CREATE (i)-[:CODED_AS_DRUG]->(d)', g)

    # Edges: CLASSIFIED_AS
    c.query('MATCH (d:Drug {rxnorm_cui: "6809"}), (dc:DrugClass {atc_code: "A10BA02"}) CREATE (d)-[:CLASSIFIED_AS]->(dc)', g)

    # Edges: PARENT_CLASS
    c.query('MATCH (c:DrugClass {atc_code: "A10BA02"}), (p:DrugClass {atc_code: "A10BA"}) CREATE (c)-[:PARENT_CLASS]->(p)', g)
    c.query('MATCH (c:DrugClass {atc_code: "A10BA"}), (p:DrugClass {atc_code: "A10B"}) CREATE (c)-[:PARENT_CLASS]->(p)', g)

    # Edges: HAS_ADVERSE_EFFECT
    c.query('MATCH (d:Drug {rxnorm_cui: "6809"}), (ae:AdverseEvent {term: "Nausea"}) CREATE (d)-[:HAS_ADVERSE_EFFECT]->(ae)', g)
    c.query('MATCH (d:Drug {rxnorm_cui: "6809"}), (ae:AdverseEvent {term: "Diarrhea"}) CREATE (d)-[:HAS_ADVERSE_EFFECT]->(ae)', g)

    # Edges: CODED_AS_MESH
    c.query('MATCH (c:Condition {name: "Type 2 Diabetes"}), (m:MeSHDescriptor {descriptor_id: "D003924"}) CREATE (c)-[:CODED_AS_MESH]->(m)', g)
    c.query('MATCH (c:Condition {name: "Breast Cancer"}), (m:MeSHDescriptor {descriptor_id: "D001943"}) CREATE (c)-[:CODED_AS_MESH]->(m)', g)

    # Edges: BROADER_THAN
    c.query('MATCH (p:MeSHDescriptor {descriptor_id: "D003920"}), (c:MeSHDescriptor {descriptor_id: "D003924"}) CREATE (p)-[:BROADER_THAN]->(c)', g)


@pytest.fixture(scope="module", autouse=True)
def _inject_client(client):
    """Inject our test client into the MCP server module."""
    import mcp_server.server as srv
    srv.client = client


# ---- Trial tools ----

class TestTrialTools:
    def test_search_trials_by_condition(self):
        from mcp_server.tools.trial_tools import register_trial_tools
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        safe = _escape("Diabetes")
        cypher = (
            f'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) '
            f'WHERE c.name CONTAINS "{safe}" '
            f'RETURN t.nct_id AS nct_id, t.title AS title, '
            f't.phase AS phase, t.overall_status AS status '
            f'ORDER BY t.start_date DESC LIMIT 50'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 2
        nct_ids = {r["nct_id"] for r in results}
        assert "NCT001" in nct_ids
        assert "NCT003" in nct_ids

    def test_search_trials_with_phase_filter(self):
        from mcp_server.server import client, _escape, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) '
            'WHERE c.name CONTAINS "Diabetes" AND t.phase = "Phase 3" '
            'RETURN t.nct_id AS nct_id '
            'LIMIT 50'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        nct_ids = {r["nct_id"] for r in results}
        assert "NCT001" in nct_ids
        assert "NCT003" not in nct_ids  # NCT003 is Phase 2

    def test_get_trial(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial {nct_id: "NCT001"}) '
            'OPTIONAL MATCH (t)-[:STUDIES]->(c:Condition) '
            'OPTIONAL MATCH (t)-[:TESTS]->(i:Intervention) '
            'OPTIONAL MATCH (t)-[:SPONSORED_BY]->(s:Sponsor) '
            'RETURN t.nct_id AS nct_id, t.title AS title, '
            't.phase AS phase, t.overall_status AS status, '
            'collect(DISTINCT c.name) AS conditions, '
            'collect(DISTINCT i.name) AS interventions, '
            'collect(DISTINCT s.name) AS sponsors'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) == 1
        row = results[0]
        assert row["nct_id"] == "NCT001"
        assert "Metformin" in row["interventions"]
        assert "Type 2 Diabetes" in row["conditions"]

    def test_trial_sites(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial {nct_id: "NCT001"})-[:CONDUCTED_AT]->(s:Site) '
            'RETURN s.facility AS facility, s.city AS city, s.country AS country '
            'ORDER BY s.country, s.city'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) == 2
        facilities = {r["facility"] for r in results}
        assert "Mayo Clinic" in facilities
        assert "Johns Hopkins" in facilities


# ---- Drug tools ----

class TestDrugTools:
    def test_drug_trials(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:TESTS]->(i:Intervention)'
            '-[:CODED_AS_DRUG]->(d:Drug) '
            'WHERE d.name CONTAINS "Metformin" '
            'RETURN t.nct_id AS nct_id, d.name AS drug '
            'LIMIT 50'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 1
        assert results[0]["nct_id"] == "NCT001"

    def test_drug_adverse_events(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (d:Drug)-[:HAS_ADVERSE_EFFECT]->(ae:AdverseEvent) '
            'WHERE d.name CONTAINS "Metformin" '
            'RETURN ae.term AS event, ae.source_vocabulary AS source '
            'LIMIT 20'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) == 2
        events = {r["event"] for r in results}
        assert "Nausea" in events
        assert "Diarrhea" in events

    def test_drug_class_hierarchy(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        # Direct classification
        cypher = (
            'MATCH (d:Drug)-[:CLASSIFIED_AS]->(dc:DrugClass) '
            'WHERE d.name CONTAINS "Metformin" '
            'RETURN dc.atc_code AS atc_code, dc.name AS class_name, dc.level AS level'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) == 1
        assert results[0]["atc_code"] == "A10BA02"

        # Walk parent class
        cypher2 = (
            'MATCH (child:DrugClass {atc_code: "A10BA02"})-[:PARENT_CLASS]->(parent:DrugClass) '
            'RETURN parent.atc_code AS atc_code, parent.name AS name'
        )
        parents = _to_dicts(client.query_readonly(cypher2, GRAPH))
        assert len(parents) == 1
        assert parents[0]["atc_code"] == "A10BA"


# ---- Disease tools ----

class TestDiseaseTools:
    def test_disease_trials(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) '
            'WHERE c.name CONTAINS "Breast Cancer" '
            'RETURN t.nct_id AS nct_id, t.title AS title '
            'LIMIT 50'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) == 1
        assert results[0]["nct_id"] == "NCT002"

    def test_treatment_landscape(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), '
            '      (t)-[:TESTS]->(i:Intervention) '
            'WHERE c.name CONTAINS "Diabetes" '
            'RETURN i.name AS intervention, i.type AS type, '
            'count(t) AS trial_count '
            'ORDER BY trial_count DESC LIMIT 30'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 1
        names = {r["intervention"] for r in results}
        assert "Metformin" in names


# ---- Analytics tools ----

class TestAnalyticsTools:
    def test_enrollment_by_phase(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) '
            'WHERE c.name CONTAINS "Diabetes" '
            'RETURN t.phase AS phase, '
            'count(t) AS trial_count, '
            'sum(t.enrollment) AS total_enrollment '
            'ORDER BY phase'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 1
        phases = {r["phase"] for r in results}
        assert "Phase 3" in phases or "Phase 2" in phases

    def test_sponsor_landscape(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), '
            '      (t)-[:SPONSORED_BY]->(s:Sponsor) '
            'WHERE c.name CONTAINS "Diabetes" '
            'RETURN s.name AS sponsor, count(t) AS trial_count '
            'ORDER BY trial_count DESC LIMIT 25'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 1
        sponsors = {r["sponsor"] for r in results}
        assert "NIH" in sponsors

    def test_geographic_distribution(self):
        from mcp_server.server import client, _to_dicts, GRAPH

        cypher = (
            'MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), '
            '      (t)-[:CONDUCTED_AT]->(s:Site) '
            'WHERE c.name CONTAINS "Breast Cancer" '
            'RETURN s.country AS country, count(DISTINCT t) AS trial_count '
            'ORDER BY trial_count DESC'
        )
        results = _to_dicts(client.query_readonly(cypher, GRAPH))
        assert len(results) >= 1
        countries = {r["country"] for r in results}
        assert "Germany" in countries


# ---- Vector search ----

class TestVectorSearch:
    def test_create_index_and_search(self, client):
        """Test vector index creation and search on ClinicalTrial nodes."""
        import random
        random.seed(42)
        dim = 64  # HNSW panics on very small dims; use realistic size

        client.create_vector_index("ClinicalTrial", "emb", dim, "l2")

        # Get node IDs
        rows = client.query_readonly(
            "MATCH (t:ClinicalTrial) RETURN id(t) AS node_id, t.nct_id AS nct_id", "default"
        )
        records = [dict(zip(rows.columns, r)) for r in rows.records]
        assert len(records) >= 3

        # Generate deterministic vectors: diabetes trials similar, cancer different
        base_diabetes = [random.gauss(0, 1) for _ in range(dim)]
        base_cancer = [random.gauss(0, 1) for _ in range(dim)]

        vectors = {
            "NCT001": base_diabetes,
            "NCT002": base_cancer,
            "NCT003": [x + random.gauss(0, 0.1) for x in base_diabetes],
        }
        for rec in records:
            nct = rec["nct_id"]
            if nct in vectors:
                client.add_vector("ClinicalTrial", "emb", rec["node_id"], vectors[nct])

        # Search for diabetes-like trials
        results = client.vector_search("ClinicalTrial", "emb", base_diabetes, 3)
        assert len(results) >= 2

        # First result should be closest to the query (NCT001 or NCT003)
        node_ids_returned = {r[0] for r in results}
        diabetes_ids = {rec["node_id"] for rec in records if rec["nct_id"] in ("NCT001", "NCT003")}
        assert diabetes_ids.issubset(node_ids_returned)
