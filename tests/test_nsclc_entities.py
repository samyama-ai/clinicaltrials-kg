"""Tests for the NSCLC entity vocabulary loader (`nsclc.entities`).

These tests cover the deterministic vocabulary contract — MeSH codes,
condition aliases, and the five modality definitions that the rest of the
NSCLC pipeline relies on.  No network, no filesystem beyond the packaged
``entities.yaml``.
"""

from __future__ import annotations

import pytest

from nsclc.entities import (
    get_condition_aliases,
    get_mesh_codes,
    get_modalities,
    load_entities,
)


EXPECTED_MODALITIES = {
    "targeted_therapy",
    "immunotherapy",
    "chemotherapy",
    "radiotherapy",
    "antiangiogenic",
}


def test_load_entities_returns_conditions_and_modalities():
    """``load_entities`` surfaces both top-level YAML sections."""
    data = load_entities()
    assert isinstance(data, dict)
    assert "conditions" in data
    assert "modalities" in data
    # conditions carries the two primary lookup keys
    conditions = data["conditions"]
    assert isinstance(conditions, dict)
    assert "mesh_codes" in conditions
    assert "aliases" in conditions


def test_mesh_codes_includes_d002289():
    """D002289 (Carcinoma, Non-Small-Cell Lung) must be present."""
    codes = get_mesh_codes()
    assert isinstance(codes, list)
    assert "D002289" in codes


def test_condition_aliases_are_nonempty_strings():
    """Aliases must be clean, non-empty, unique strings with no stray whitespace."""
    aliases = get_condition_aliases()
    assert isinstance(aliases, list)
    assert aliases, "condition aliases must not be empty"

    for alias in aliases:
        assert isinstance(alias, str), f"alias must be a str, got {type(alias)}"
        assert alias, "alias must be non-empty"
        assert alias == alias.strip(), (
            f"alias {alias!r} has leading/trailing whitespace"
        )

    # No duplicates (case-sensitive — the modality matcher lowercases at use time).
    assert len(aliases) == len(set(aliases)), "aliases contain duplicates"


def test_modalities_include_all_five():
    """All five expected modality families are present and non-empty dicts."""
    modalities = get_modalities()
    assert isinstance(modalities, dict)
    assert set(modalities.keys()) == EXPECTED_MODALITIES, (
        f"unexpected modality set: {sorted(modalities.keys())}"
    )
    for name, cfg in modalities.items():
        assert isinstance(cfg, dict), f"{name} config must be a dict"


def test_every_modality_has_atc_or_aliases():
    """Each modality must carry at least one ATC prefix OR one drug alias.

    Radiotherapy is the documented exception: no ATC codes, aliases only.
    """
    modalities = get_modalities()
    for name, cfg in modalities.items():
        atc_prefixes = cfg.get("atc_prefixes") or []
        drug_aliases = cfg.get("drug_aliases") or []
        assert atc_prefixes or drug_aliases, (
            f"modality {name!r} has neither atc_prefixes nor drug_aliases"
        )

    # Radiotherapy-specific: aliases only, no ATC codes.
    radio = modalities["radiotherapy"]
    assert not (radio.get("atc_prefixes") or []), (
        "radiotherapy should not carry ATC prefixes"
    )
    assert radio.get("drug_aliases"), (
        "radiotherapy must have drug_aliases (alias-only matching)"
    )


def test_atc_prefixes_are_uppercase_strings_starting_with_L01():
    """All ATC prefixes live under the L01 (antineoplastics) tree."""
    modalities = get_modalities()
    for name, cfg in modalities.items():
        for prefix in cfg.get("atc_prefixes") or []:
            assert isinstance(prefix, str), (
                f"{name}: ATC prefix {prefix!r} is not a string"
            )
            assert prefix == prefix.upper(), (
                f"{name}: ATC prefix {prefix!r} must be uppercase"
            )
            assert prefix.startswith("L01"), (
                f"{name}: ATC prefix {prefix!r} does not start with L01"
            )
