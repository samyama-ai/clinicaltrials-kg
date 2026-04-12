"""Deterministic modality tagger for NSCLC trial records.

A *modality* is a treatment family (chemotherapy, immunotherapy, targeted
therapy, radiotherapy, antiangiogenic).  Given a trial record that already
carries drugs ``[{name, rxcui, atc_code}]`` and interventions
``[{name, type}]``, :func:`tag_modalities` decides which modalities apply
and records the rule that fired for each match.

Matching rules (per modality in ``entities.yaml``)
--------------------------------------------------
1. **ATC-first** -- for every drug attached to the trial, if its
   ``atc_code`` starts with any of the modality's ``atc_prefixes``, the
   modality matches and we record ``matched_by="atc"``.
2. **Alias fallback** -- if no ATC match was made for this modality, we
   check each modality ``drug_alias`` as a case-insensitive substring
   against (a) each drug's ``name`` and (b) each intervention's ``name``.
   A hit records ``matched_by="alias"``.
3. **Radiotherapy** -- has no ATC prefixes, so we only do alias matching
   against intervention names (and drug names, for the rare case where a
   chemoradiation protocol is ingested as a "drug").
4. **Conservative** -- if neither rule fires we do not guess.  The trial
   simply gets no tag for that modality.

Output per trial
----------------
``tag_modalities`` returns ``(modalities, evidence)`` where:

* ``modalities``  -- deduplicated sorted list of modality names.
* ``evidence``    -- list of dicts ``{"modality", "matched_by", "evidence"}``
  describing every rule that fired.  Useful for debugging and for
  transparency in downstream UIs.
"""

from __future__ import annotations

from typing import Any


_EvidenceList = list[dict[str, str]]


def _drug_atc_matches(
    modality_name: str,
    atc_prefixes: list[str],
    drugs: list[dict[str, Any]],
) -> _EvidenceList:
    """Return evidence rows for every drug whose ATC code starts with a prefix."""
    out: _EvidenceList = []
    if not atc_prefixes:
        return out
    for drug in drugs:
        code = (drug.get("atc_code") or "").strip()
        if not code:
            continue
        for prefix in atc_prefixes:
            if code.startswith(prefix):
                name = drug.get("name") or "?"
                out.append(
                    {
                        "modality": modality_name,
                        "matched_by": "atc",
                        "evidence": (
                            f"drug '{name}' ATC {code} matches prefix {prefix}"
                        ),
                    }
                )
                break  # one prefix match per drug is enough
    return out


def _alias_hits(
    modality_name: str,
    aliases: list[str],
    drugs: list[dict[str, Any]],
    interventions: list[dict[str, Any]],
) -> _EvidenceList:
    """Return evidence rows for every alias substring hit on a drug/intervention name."""
    out: _EvidenceList = []
    if not aliases:
        return out
    lowered = [(a, a.lower()) for a in aliases if a]

    for drug in drugs:
        name = drug.get("name") or ""
        lname = name.lower()
        for alias, lalias in lowered:
            if lalias and lalias in lname:
                out.append(
                    {
                        "modality": modality_name,
                        "matched_by": "alias",
                        "evidence": (
                            f"drug '{name}' matches alias '{alias}'"
                        ),
                    }
                )
                break  # one alias match per drug is plenty

    for iv in interventions:
        name = iv.get("name") or ""
        lname = name.lower()
        for alias, lalias in lowered:
            if lalias and lalias in lname:
                out.append(
                    {
                        "modality": modality_name,
                        "matched_by": "alias",
                        "evidence": (
                            f"intervention '{name}' matches alias '{alias}'"
                        ),
                    }
                )
                break

    return out


def tag_modalities(
    trial_record: dict[str, Any],
    modalities_config: dict[str, dict[str, Any]],
) -> tuple[list[str], _EvidenceList]:
    """Return (modalities, evidence) for one trial record.

    ATC-first: we try ATC prefix matching before alias matching, and only
    fall back to aliases if ATC matching produced no rows for this
    modality.  Radiotherapy (empty ``atc_prefixes``) is alias-only.
    """
    drugs = trial_record.get("drugs") or []
    interventions = trial_record.get("interventions") or []

    evidence: _EvidenceList = []
    hit_modalities: set[str] = set()

    for modality_name, cfg in modalities_config.items():
        atc_prefixes = list(cfg.get("atc_prefixes") or [])
        aliases = list(cfg.get("drug_aliases") or [])

        atc_rows = _drug_atc_matches(modality_name, atc_prefixes, drugs)
        if atc_rows:
            evidence.extend(atc_rows)
            hit_modalities.add(modality_name)
            continue  # ATC won -- do not add alias-based duplicates

        alias_rows = _alias_hits(modality_name, aliases, drugs, interventions)
        if alias_rows:
            evidence.extend(alias_rows)
            hit_modalities.add(modality_name)

    return sorted(hit_modalities), evidence


# ---------------------------------------------------------------------------
# Inline sanity checks (run as `python -m nsclc.modality`)
# ---------------------------------------------------------------------------

def _selftest() -> None:
    """Small hand-rolled checks; full pytest coverage is deferred to Step 12."""
    # Lazy import so the module stays importable without a YAML file present.
    from nsclc.entities import get_modalities

    cfg = get_modalities()

    # Case 1: chemo + immuno combo, alias-only (no ATC codes on drugs).
    rec1 = {
        "nct_id": "NCT-TEST-1",
        "drugs": [],
        "interventions": [
            {"name": "Pembrolizumab", "type": "Biological"},
            {"name": "Cisplatin", "type": "Drug"},
            {"name": "Pemetrexed", "type": "Drug"},
        ],
    }
    mods1, ev1 = tag_modalities(rec1, cfg)
    assert mods1 == ["chemotherapy", "immunotherapy"], mods1
    assert all(e["matched_by"] == "alias" for e in ev1), ev1

    # Case 2: targeted therapy via ATC code (Osimertinib L01EB04 -> L01E).
    rec2 = {
        "nct_id": "NCT-TEST-2",
        "drugs": [
            {"name": "Osimertinib", "rxcui": "1721560", "atc_code": "L01EB04"},
        ],
        "interventions": [
            {"name": "Osimertinib", "type": "Drug"},
        ],
    }
    mods2, ev2 = tag_modalities(rec2, cfg)
    assert mods2 == ["targeted_therapy"], mods2
    assert ev2 and ev2[0]["matched_by"] == "atc", ev2
    assert "L01E" in ev2[0]["evidence"]

    # Case 3: radiotherapy alias-only; no drugs.
    rec3 = {
        "nct_id": "NCT-TEST-3",
        "drugs": [],
        "interventions": [
            {"name": "Stereotactic Body Radiation Therapy", "type": "Radiation"},
        ],
    }
    mods3, ev3 = tag_modalities(rec3, cfg)
    assert mods3 == ["radiotherapy"], mods3
    assert ev3[0]["matched_by"] == "alias", ev3

    # Case 4: unknown intervention -> no tags (conservative).
    rec4 = {
        "nct_id": "NCT-TEST-4",
        "drugs": [],
        "interventions": [{"name": "Best Supportive Care", "type": "Other"}],
    }
    mods4, ev4 = tag_modalities(rec4, cfg)
    assert mods4 == [], mods4
    assert ev4 == [], ev4

    print("modality self-tests OK")


if __name__ == "__main__":
    _selftest()
