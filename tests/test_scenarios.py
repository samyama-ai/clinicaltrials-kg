"""Validate all scenario JSON files in the scenarios/ directory."""

import json
import pytest
from pathlib import Path

SCENARIO_DIR = Path(__file__).parent.parent / "scenarios"

KNOWN_TOOLS = {
    "search_trials", "get_trial", "find_similar_trials", "trial_sites",
    "drug_trials", "drug_adverse_events", "drug_interactions", "drug_class",
    "disease_trials", "treatment_landscape", "related_conditions", "disease_genes",
    "enrollment_by_phase", "sponsor_landscape", "geographic_distribution", "trial_timeline",
    "vector_search", "cypher_query",
}

EXPECTED_CATEGORIES = {
    "drug_repurposing", "adverse_event_analysis", "trial_landscape",
    "eligibility_matching", "multi_hop_reasoning", "outcome_analysis",
    "geographic_analysis",
}

REQUIRED_FIELDS = {
    "id", "category", "description", "expected_tools",
    "expected_output_contains", "difficulty", "requires_graph",
}

VALID_DIFFICULTIES = {"easy", "medium", "hard"}


def load_all_scenarios():
    """Load and return all scenario dicts from every JSON file in scenarios/."""
    scenarios = []
    for path in sorted(SCENARIO_DIR.glob("*.json")):
        with open(path) as f:
            data = json.load(f)
        # Support both a single dict and a list of dicts per file
        if isinstance(data, list):
            for item in data:
                scenarios.append((path.name, item))
        else:
            scenarios.append((path.name, data))
    return scenarios


ALL_SCENARIOS = load_all_scenarios()


@pytest.fixture
def all_scenarios():
    return ALL_SCENARIOS


# ---- Structural / field-presence tests ----

@pytest.mark.parametrize("filename,scenario", ALL_SCENARIOS, ids=[f"{f}:{s.get('id','?')}" for f, s in ALL_SCENARIOS])
def test_required_fields_present(filename, scenario):
    """Every scenario must contain all required fields."""
    missing = REQUIRED_FIELDS - set(scenario.keys())
    assert not missing, f"{filename}: scenario {scenario.get('id','?')} missing fields: {missing}"


@pytest.mark.parametrize("filename,scenario", ALL_SCENARIOS, ids=[f"{f}:{s.get('id','?')}" for f, s in ALL_SCENARIOS])
def test_field_types(filename, scenario):
    """Verify the types of each required field."""
    sid = scenario.get("id", "?")
    assert isinstance(scenario.get("id"), str), f"{filename}:{sid} 'id' must be str"
    assert isinstance(scenario.get("category"), str), f"{filename}:{sid} 'category' must be str"
    assert isinstance(scenario.get("description"), str), f"{filename}:{sid} 'description' must be str"
    assert isinstance(scenario.get("expected_tools"), list), f"{filename}:{sid} 'expected_tools' must be list"
    assert isinstance(scenario.get("expected_output_contains"), list), f"{filename}:{sid} 'expected_output_contains' must be list"
    assert isinstance(scenario.get("difficulty"), str), f"{filename}:{sid} 'difficulty' must be str"
    assert isinstance(scenario.get("requires_graph"), bool), f"{filename}:{sid} 'requires_graph' must be bool"


# ---- Value-domain tests ----

@pytest.mark.parametrize("filename,scenario", ALL_SCENARIOS, ids=[f"{f}:{s.get('id','?')}" for f, s in ALL_SCENARIOS])
def test_difficulty_values(filename, scenario):
    """difficulty must be one of easy, medium, hard."""
    assert scenario.get("difficulty") in VALID_DIFFICULTIES, (
        f"{filename}: scenario {scenario.get('id','?')} has invalid difficulty "
        f"'{scenario.get('difficulty')}'; expected one of {VALID_DIFFICULTIES}"
    )


@pytest.mark.parametrize("filename,scenario", ALL_SCENARIOS, ids=[f"{f}:{s.get('id','?')}" for f, s in ALL_SCENARIOS])
def test_expected_tools_known(filename, scenario):
    """Every tool listed in expected_tools must be in the known tool set."""
    unknown = set(scenario.get("expected_tools", [])) - KNOWN_TOOLS
    assert not unknown, (
        f"{filename}: scenario {scenario.get('id','?')} references unknown tools: {unknown}"
    )


@pytest.mark.parametrize("filename,scenario", ALL_SCENARIOS, ids=[f"{f}:{s.get('id','?')}" for f, s in ALL_SCENARIOS])
def test_category_values(filename, scenario):
    """category must be one of the expected categories."""
    assert scenario.get("category") in EXPECTED_CATEGORIES, (
        f"{filename}: scenario {scenario.get('id','?')} has unexpected category "
        f"'{scenario.get('category')}'; expected one of {EXPECTED_CATEGORIES}"
    )


# ---- Cross-file uniqueness and count tests ----

def test_ids_unique(all_scenarios):
    """All scenario IDs must be unique across every file."""
    ids = [s.get("id") for _, s in all_scenarios]
    duplicates = {x for x in ids if ids.count(x) > 1}
    assert not duplicates, f"Duplicate scenario IDs found: {duplicates}"


def test_total_scenario_count(all_scenarios):
    """There must be exactly 40 scenarios in total."""
    assert len(all_scenarios) == 40, (
        f"Expected 40 scenarios, found {len(all_scenarios)}"
    )


def test_expected_output_contains_non_empty(all_scenarios):
    """expected_output_contains must have at least one entry per scenario."""
    for filename, scenario in all_scenarios:
        assert len(scenario.get("expected_output_contains", [])) > 0, (
            f"{filename}: scenario {scenario.get('id','?')} has empty expected_output_contains"
        )


def test_expected_tools_non_empty(all_scenarios):
    """expected_tools must have at least one entry per scenario."""
    for filename, scenario in all_scenarios:
        assert len(scenario.get("expected_tools", [])) > 0, (
            f"{filename}: scenario {scenario.get('id','?')} has empty expected_tools"
        )
