"""Tests for the deterministic modality tagger (`nsclc.modality`).

Covers the ATC-first / alias-fallback rules documented in
``nsclc/modality.py``.  Synthetic trial records only — no live graph or
network access.
"""

from __future__ import annotations

import pytest

from nsclc.entities import get_modalities
from nsclc.modality import tag_modalities


@pytest.fixture(scope="module")
def modalities_cfg():
    """Load the real modality vocabulary once for all tests in this module."""
    return get_modalities()


def _trial(nct_id: str, drugs=None, interventions=None):
    """Minimal trial record shaped the way the tagger expects."""
    return {
        "nct_id": nct_id,
        "drugs": list(drugs or []),
        "interventions": list(interventions or []),
    }


def test_tag_modalities_atc_match(modalities_cfg):
    """A drug with ATC L01EB04 tags targeted_therapy via ATC rule."""
    trial = _trial(
        "NCT-ATC-1",
        drugs=[
            {"name": "Osimertinib", "rxcui": "1721560", "atc_code": "L01EB04"},
        ],
        interventions=[],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["targeted_therapy"]
    assert len(evidence) == 1
    assert evidence[0]["modality"] == "targeted_therapy"
    assert evidence[0]["matched_by"] == "atc"
    assert "L01E" in evidence[0]["evidence"]
    assert "Osimertinib" in evidence[0]["evidence"]


def test_tag_modalities_alias_match(modalities_cfg):
    """Intervention name 'Pembrolizumab' tags immunotherapy via alias rule."""
    trial = _trial(
        "NCT-ALIAS-1",
        drugs=[],
        interventions=[{"name": "Pembrolizumab", "type": "Biological"}],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["immunotherapy"]
    assert len(evidence) == 1
    assert evidence[0]["modality"] == "immunotherapy"
    assert evidence[0]["matched_by"] == "alias"
    assert "pembrolizumab" in evidence[0]["evidence"].lower()


def test_tag_modalities_combo_therapy(modalities_cfg):
    """Cisplatin + Pembrolizumab tags both chemotherapy and immunotherapy."""
    trial = _trial(
        "NCT-COMBO-1",
        drugs=[],
        interventions=[
            {"name": "Cisplatin", "type": "Drug"},
            {"name": "Pembrolizumab", "type": "Biological"},
        ],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["chemotherapy", "immunotherapy"]
    by_modality = {row["modality"] for row in evidence}
    assert by_modality == {"chemotherapy", "immunotherapy"}
    # Both are aliased (no ATC codes on the synthetic interventions).
    assert all(row["matched_by"] == "alias" for row in evidence)


def test_tag_modalities_radiotherapy_aliases_only(modalities_cfg):
    """A Radiation intervention tags radiotherapy with no ATC dependency."""
    trial = _trial(
        "NCT-RT-1",
        drugs=[],
        interventions=[{"name": "Radiation Therapy", "type": "Radiation"}],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["radiotherapy"]
    assert len(evidence) == 1
    assert evidence[0]["modality"] == "radiotherapy"
    assert evidence[0]["matched_by"] == "alias"


def test_tag_modalities_conservative_no_guess(modalities_cfg):
    """Unknown interventions produce no tags and no evidence — we don't guess."""
    trial = _trial(
        "NCT-UNKNOWN-1",
        drugs=[],
        interventions=[
            {"name": "Best Supportive Care", "type": "Other"},
            {"name": "Quality of Life Questionnaire", "type": "Behavioral"},
        ],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == []
    assert evidence == []


def test_tag_modalities_deduplicated_and_sorted(modalities_cfg):
    """Two drugs mapping to the same modality produce one tag, two evidence rows."""
    trial = _trial(
        "NCT-DEDUP-1",
        drugs=[],
        interventions=[
            {"name": "Cisplatin", "type": "Drug"},
            {"name": "Pemetrexed", "type": "Drug"},
        ],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["chemotherapy"]  # deduplicated to one entry
    chemo_rows = [row for row in evidence if row["modality"] == "chemotherapy"]
    assert len(chemo_rows) == 2, evidence
    # Sanity: the evidence mentions both drug names.
    joined = " ".join(row["evidence"] for row in chemo_rows).lower()
    assert "cisplatin" in joined
    assert "pemetrexed" in joined


def test_tag_modalities_atc_preferred_over_alias(modalities_cfg):
    """When a drug's ATC *and* name both match, ATC wins (single evidence row)."""
    trial = _trial(
        "NCT-ATC-OVER-ALIAS",
        drugs=[
            # Osimertinib's ATC L01EB04 matches targeted_therapy's L01E prefix,
            # AND the name "Osimertinib" is also in targeted_therapy's aliases.
            {"name": "Osimertinib", "rxcui": "1721560", "atc_code": "L01EB04"},
        ],
        interventions=[
            # Repeating the name in interventions would otherwise produce a
            # second alias hit — the "ATC wins" rule should suppress it.
            {"name": "Osimertinib", "type": "Drug"},
        ],
    )
    mods, evidence = tag_modalities(trial, modalities_cfg)

    assert mods == ["targeted_therapy"]
    targeted = [row for row in evidence if row["modality"] == "targeted_therapy"]
    assert len(targeted) == 1
    assert targeted[0]["matched_by"] == "atc"
