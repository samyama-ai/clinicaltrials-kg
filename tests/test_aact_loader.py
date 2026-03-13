"""Tests for the AACT flat files loader.

Uses small sample pipe-delimited files to verify loading logic,
deduplication, edge creation, and progress reporting.
"""

import os
import tempfile
from pathlib import Path

import pytest

from etl.aact_loader import (
    _col,
    _esc,
    _prop_str,
    _read_pipe_file,
    load_aact,
)


# -- Helper: create sample pipe-delimited files ------------------------------

def _write_pipe_file(directory: Path, filename: str, header: str, rows: list[str]):
    """Write a pipe-delimited file with header and rows."""
    path = directory / filename
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def _create_sample_data(tmpdir: Path):
    """Create a minimal set of AACT-format pipe-delimited files."""
    _write_pipe_file(tmpdir, "studies.txt",
        "nct_id|brief_title|official_title|study_type|phase|overall_status|enrollment|start_date|completion_date|primary_completion_date|last_update_submitted_date|results_first_submitted_date|why_stopped",
        [
            'NCT00000001|Diabetes Drug Trial|Official Title A|Interventional|Phase 2|Completed|100|2020-01-01|2022-01-01|2021-06-01|2022-03-01|2022-04-01|',
            'NCT00000002|Cancer Immunotherapy|Official Title B|Interventional|Phase 3|Recruiting|500|2021-06-01||||',
            'NCT00000003|Heart Failure Study|Official Title C|Observational||Active, not recruiting|200|2019-01-01||||',
        ],
    )

    _write_pipe_file(tmpdir, "brief_summaries.txt",
        "id|nct_id|description",
        [
            '1|NCT00000001|This study evaluates a new diabetes drug in adults.',
            '2|NCT00000002|A phase 3 trial of immunotherapy for breast cancer.',
            '3|NCT00000003|Observational study of heart failure patients.',
        ],
    )

    _write_pipe_file(tmpdir, "conditions.txt",
        "id|nct_id|name|downcase_name",
        [
            '1|NCT00000001|Type 2 Diabetes|type 2 diabetes',
            '2|NCT00000001|Insulin Resistance|insulin resistance',
            '3|NCT00000002|Breast Cancer|breast cancer',
            '4|NCT00000003|Heart Failure|heart failure',
            # Duplicate condition name (different study)
            '5|NCT00000003|Type 2 Diabetes|type 2 diabetes',
        ],
    )

    _write_pipe_file(tmpdir, "interventions.txt",
        "id|nct_id|intervention_type|name|description",
        [
            '10|NCT00000001|DRUG|Metformin|Oral diabetes medication',
            '11|NCT00000001|DRUG|Placebo|Matching placebo',
            '12|NCT00000002|BIOLOGICAL|Pembrolizumab|Anti-PD-1 antibody',
        ],
    )

    _write_pipe_file(tmpdir, "design_groups.txt",
        "id|nct_id|group_type|title|description",
        [
            '20|NCT00000001|Experimental|Treatment Arm|Receives Metformin',
            '21|NCT00000001|Placebo Comparator|Control Arm|Receives Placebo',
            '22|NCT00000002|Experimental|Pembro Arm|Receives Pembrolizumab',
        ],
    )

    _write_pipe_file(tmpdir, "design_group_interventions.txt",
        "id|design_group_id|intervention_id",
        [
            '30|20|10',
            '31|21|11',
            '32|22|12',
        ],
    )

    _write_pipe_file(tmpdir, "sponsors.txt",
        "id|nct_id|agency_class|lead_or_collaborator|name",
        [
            '40|NCT00000001|NIH|lead|National Institute of Diabetes',
            '41|NCT00000001|OTHER|collaborator|Johns Hopkins University',
            '42|NCT00000002|INDUSTRY|lead|Merck Sharp & Dohme',
            '43|NCT00000003|NIH|lead|National Heart, Lung, and Blood Institute',
        ],
    )

    _write_pipe_file(tmpdir, "design_outcomes.txt",
        "id|nct_id|outcome_type|measure|time_frame|description",
        [
            '50|NCT00000001|primary|HbA1c Change from Baseline|12 weeks|Primary efficacy',
            '51|NCT00000001|secondary|Fasting Glucose|12 weeks|Secondary endpoint',
            '52|NCT00000002|primary|Overall Survival|24 months|',
        ],
    )

    _write_pipe_file(tmpdir, "facilities.txt",
        "id|nct_id|status|name|city|state|zip|country",
        [
            '60|NCT00000001|Completed|Johns Hopkins Hospital|Baltimore|Maryland|21287|United States',
            '61|NCT00000001|Completed|Mayo Clinic|Rochester|Minnesota|55905|United States',
            '62|NCT00000002||Memorial Sloan Kettering|New York|New York|10065|United States',
            # Duplicate facility
            '63|NCT00000003||Johns Hopkins Hospital|Baltimore|Maryland|21287|United States',
        ],
    )

    _write_pipe_file(tmpdir, "browse_conditions.txt",
        "id|nct_id|mesh_term|downcase_mesh_term|mesh_type",
        [
            '70|NCT00000001|Diabetes Mellitus, Type 2|diabetes mellitus, type 2|mesh-ancestor',
            '71|NCT00000002|Breast Neoplasms|breast neoplasms|mesh-ancestor',
        ],
    )

    _write_pipe_file(tmpdir, "study_references.txt",
        "id|nct_id|pmid|reference_type|citation",
        [
            '80|NCT00000001|12345678|result|Smith J et al. Diabetes results. J Med. 2022.',
            '81|NCT00000002|87654321|background|Jones A et al. Immunotherapy review. Lancet. 2021.',
        ],
    )


# -- Unit tests for helpers ---------------------------------------------------

class TestHelpers:
    def test_esc_strips_quotes(self):
        assert _esc('"hello"') == "hello"

    def test_esc_strips_newlines(self):
        assert _esc("line1\nline2") == "line1 line2"
        assert "\r" not in _esc("line1\rline2")
        assert "\n" not in _esc("line1\nline2")

    def test_esc_strips_backslash(self):
        assert _esc("a\\b") == "ab"

    def test_esc_none(self):
        assert _esc(None) == ""

    def test_esc_strips_whitespace(self):
        assert _esc("  hello  ") == "hello"

    def test_prop_str_skips_none(self):
        result = _prop_str({"a": "x", "b": None, "c": ""})
        assert "a:" in result
        assert "b:" not in result
        assert "c:" not in result

    def test_prop_str_handles_int(self):
        result = _prop_str({"count": 42})
        assert "count: 42" in result

    def test_prop_str_handles_bool(self):
        result = _prop_str({"flag": True})
        assert '"true"' in result

    def test_col_strips(self):
        assert _col({"name": "  hello  "}, "name") == "hello"

    def test_col_missing_key(self):
        assert _col({}, "missing") == ""

    def test_col_none_value(self):
        assert _col({"x": None}, "x") == ""


class TestReadPipeFile:
    def test_reads_pipe_delimited(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text("a|b|c\n1|2|3\n4|5|6\n")
        rows = list(_read_pipe_file(path))
        assert len(rows) == 2
        assert rows[0]["a"] == "1"
        assert rows[0]["b"] == "2"
        assert rows[1]["c"] == "6"

    def test_handles_empty_fields(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text("x|y|z\n1||3\n")
        rows = list(_read_pipe_file(path))
        assert rows[0]["y"] == ""

    def test_handles_quoted_fields(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text('a|b\n"value|with|pipes"|normal\n')
        rows = list(_read_pipe_file(path))
        assert rows[0]["a"] == "value|with|pipes"
        assert rows[0]["b"] == "normal"


# -- Integration test with mock Samyama client --------------------------------

class FakeQueryResult:
    def __init__(self, records=None, columns=None):
        self.records = records or []
        self.columns = columns or []


class FakeClient:
    """Minimal mock SamyamaClient that records Cypher queries."""

    def __init__(self):
        self.queries = []
        self.readonly_queries = []

    def query(self, cypher, graph="default"):
        self.queries.append(cypher)
        return FakeQueryResult()

    def query_readonly(self, cypher, graph="default"):
        self.readonly_queries.append(cypher)
        return FakeQueryResult()


@pytest.fixture
def sample_data(tmp_path):
    """Create sample AACT data files and return the directory path."""
    _create_sample_data(tmp_path)
    return tmp_path


@pytest.fixture
def client():
    return FakeClient()


class TestLoadAACT:
    def test_load_returns_counts(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["studies"] == 3
        assert "conditions" in counts
        assert "interventions" in counts
        assert "sponsors" in counts
        assert "elapsed_seconds" in counts

    def test_creates_study_nodes(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        create_queries = [q for q in client.queries if "CREATE (n:ClinicalTrial" in q]
        assert len(create_queries) == 3

    def test_study_has_brief_summary(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        create_queries = [q for q in client.queries if "CREATE (n:ClinicalTrial" in q]
        # First study should have brief_summary from brief_summaries.txt
        assert "diabetes drug" in create_queries[0].lower()

    def test_deduplicates_conditions(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        merge_queries = [q for q in client.queries if "MERGE (n:Condition" in q]
        # "Type 2 Diabetes" appears twice in data but should be MERGEd once
        diabetes_merges = [q for q in merge_queries if "Type 2 Diabetes" in q]
        assert len(diabetes_merges) == 1

    def test_condition_count(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        # 4 unique conditions: Type 2 Diabetes, Insulin Resistance, Breast Cancer, Heart Failure
        assert counts["conditions"] == 4

    def test_creates_studies_edges(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        edge_queries = [q for q in client.queries if "CREATE (t)-[:STUDIES]->" in q]
        # 5 rows in conditions.txt, all valid
        assert len(edge_queries) == 5

    def test_deduplicates_interventions(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        counts = load_aact.__wrapped__ if hasattr(load_aact, '__wrapped__') else None
        merge_queries = [q for q in client.queries if "MERGE (n:Intervention" in q]
        # 3 unique interventions
        assert len(merge_queries) >= 3

    def test_intervention_has_type(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        merge_queries = [q for q in client.queries if "MERGE (n:Intervention" in q and "Metformin" in q]
        assert len(merge_queries) >= 1
        assert "DRUG" in merge_queries[0]

    def test_arm_groups_created(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["arm_groups"] == 3

    def test_uses_edges_created(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["uses_edges"] == 3

    def test_lead_sponsors_only(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        # 4 sponsor rows, but only 3 are lead sponsors
        assert counts["sponsored_by_edges"] == 3

    def test_deduplicates_sites(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        # "Johns Hopkins Hospital|Baltimore" appears twice → 1 unique
        # + Mayo Clinic + Memorial Sloan Kettering = 3 unique sites
        assert counts["sites"] == 3

    def test_outcomes_created(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["outcomes"] == 3

    def test_mesh_terms_created(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["mesh_terms"] == 2

    def test_publications_created(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data))
        assert counts["publications"] == 2

    def test_max_studies_limit(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data), max_studies=2)
        assert counts["studies"] == 2

    def test_skip_sites(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data), include_sites=False)
        assert "sites" not in counts

    def test_skip_outcomes(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data), include_outcomes=False)
        assert "outcomes" not in counts

    def test_skip_adverse_events(self, client, sample_data):
        counts = load_aact(client, data_dir=str(sample_data), include_adverse_events=False)
        assert "adverse_events" not in counts

    def test_missing_studies_file_raises(self, client, tmp_path):
        with pytest.raises(FileNotFoundError, match="studies.txt not found"):
            load_aact(client, data_dir=str(tmp_path))

    def test_creates_indexes(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        index_queries = [q for q in client.queries if "CREATE INDEX" in q]
        assert len(index_queries) == 10

    def test_has_results_derived(self, client, sample_data):
        load_aact(client, data_dir=str(sample_data))
        create_queries = [q for q in client.queries if "CREATE (n:ClinicalTrial" in q]
        # NCT00000001 has results_first_submitted_date → has_results: "true"
        assert '"true"' in create_queries[0]
        # NCT00000002 has no results_first_submitted_date → has_results: "false"
        assert '"false"' in create_queries[1]


class TestDownloadAACT:
    """Tests for download_aact module (no actual downloads)."""

    def test_import(self):
        from etl.download_aact import find_flat_files_url, download_aact
        assert callable(find_flat_files_url)
        assert callable(download_aact)
