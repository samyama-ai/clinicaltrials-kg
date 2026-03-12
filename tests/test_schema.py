"""Tests for the graph schema definition in schema/clinicaltrials_kg.cypher."""

import re
import pytest
from pathlib import Path

SCHEMA_FILE = Path(__file__).parent.parent / "schema" / "clinicaltrials_kg.cypher"

EXPECTED_LABELS = {
    "ClinicalTrial", "Condition", "Intervention", "ArmGroup", "Outcome",
    "Sponsor", "Site", "AdverseEvent", "MeSHDescriptor", "Drug",
    "DrugClass", "Publication", "Gene", "Protein", "LabTest",
}

EXPECTED_RELATIONSHIPS = {
    "STUDIES", "TESTS", "HAS_ARM", "USES", "MEASURES", "SPONSORED_BY",
    "CONDUCTED_AT", "REPORTED", "PUBLISHED_IN", "CODED_AS_MESH",
    "CODED_AS_DRUG", "BROADER_THAN", "CLASSIFIED_AS", "PARENT_CLASS",
    "TARGETS", "TREATS", "INTERACTS_WITH", "HAS_ADVERSE_EFFECT",
    "ENCODES", "ASSOCIATED_WITH", "MEASURED_BY", "DESCRIBES",
    "TAGGED_WITH", "LOCATED_IN",
}

EXPECTED_STANDARDS = {"ICH", "CDISC", "ICD-10", "MeSH", "SNOMED", "RxNorm", "LOINC", "UMLS"}


@pytest.fixture(scope="module")
def schema_text():
    """Read the schema file once for the entire module."""
    assert SCHEMA_FILE.exists(), f"Schema file not found: {SCHEMA_FILE}"
    return SCHEMA_FILE.read_text()


def extract_labels(text):
    """Extract all node labels from CREATE (...:Label ...) statements."""
    # Matches labels in patterns like (var:Label ...) or (:Label ...)
    return set(re.findall(r"\(\w*:(\w+)", text))


def extract_relationships(text):
    """Extract all relationship types from [:TYPE] patterns."""
    return set(re.findall(r"\[:(\w+)", text))


# ---- File-level tests ----

def test_schema_file_exists():
    """The schema Cypher file must exist."""
    assert SCHEMA_FILE.exists(), f"Schema file missing at {SCHEMA_FILE}"


def test_schema_not_empty(schema_text):
    """Schema file must have meaningful content."""
    assert len(schema_text.strip()) > 100, "Schema file appears too short"


# ---- Node label tests ----

def test_all_expected_labels_present(schema_text):
    """All 15 expected node labels must appear in the schema."""
    found_labels = extract_labels(schema_text)
    missing = EXPECTED_LABELS - found_labels
    assert not missing, f"Missing node labels: {missing}"


def test_no_unexpected_labels(schema_text):
    """No extra labels beyond the expected 15 should be defined."""
    found_labels = extract_labels(schema_text)
    extra = found_labels - EXPECTED_LABELS
    assert not extra, f"Unexpected node labels found: {extra}"


def test_label_count(schema_text):
    """There should be exactly 15 distinct node labels."""
    found_labels = extract_labels(schema_text)
    assert len(found_labels) == 15, (
        f"Expected 15 node labels, found {len(found_labels)}: {found_labels}"
    )


# ---- Relationship type tests ----

def test_all_expected_relationships_present(schema_text):
    """All 25 expected relationship types must appear in the schema."""
    found_rels = extract_relationships(schema_text)
    missing = EXPECTED_RELATIONSHIPS - found_rels
    assert not missing, f"Missing relationship types: {missing}"


def test_no_unexpected_relationships(schema_text):
    """No extra relationship types beyond the expected set should be defined."""
    found_rels = extract_relationships(schema_text)
    extra = found_rels - EXPECTED_RELATIONSHIPS
    assert not extra, f"Unexpected relationship types found: {extra}"


def test_relationship_count(schema_text):
    """There should be exactly 24 distinct relationship types."""
    found_rels = extract_relationships(schema_text)
    assert len(found_rels) == 24, (
        f"Expected 24 relationship types, found {len(found_rels)}: {found_rels}"
    )


# ---- Standards reference tests ----

@pytest.mark.parametrize("standard", sorted(EXPECTED_STANDARDS))
def test_standard_referenced(schema_text, standard):
    """Each expected medical/regulatory standard must be mentioned in the schema."""
    assert standard in schema_text, (
        f"Standard '{standard}' not referenced in schema comments"
    )


# ---- Structural quality tests ----

def test_header_comment_present(schema_text):
    """Schema should start with a descriptive header comment block."""
    assert schema_text.lstrip().startswith("//"), "Schema should begin with a comment block"


def test_relationship_comments_present(schema_text):
    """Every relationship CREATE should be preceded by a comment."""
    lines = schema_text.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("CREATE") and "]->" in stripped:
            # Look back for a comment within the preceding 3 lines
            preceding = [lines[j].strip() for j in range(max(0, i - 3), i)]
            has_comment = any(p.startswith("//") for p in preceding)
            assert has_comment, (
                f"Line {i + 1}: relationship CREATE has no preceding comment: {stripped}"
            )
