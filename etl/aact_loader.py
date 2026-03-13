"""AACT Flat Files Loader for Samyama Graph Database.

Loads the full ClinicalTrials.gov dataset from AACT pipe-delimited flat files
into a Samyama property graph.  Table-by-table processing with in-memory
deduplication and index-accelerated edge creation.

Expected data directory layout (after extracting the AACT zip):
    data/aact/
      studies.txt
      brief_summaries.txt
      conditions.txt
      interventions.txt
      design_groups.txt
      design_group_interventions.txt
      design_outcomes.txt
      sponsors.txt
      facilities.txt
      reported_events.txt
      browse_conditions.txt
      study_references.txt
      ...

Usage:
    from samyama import SamyamaClient
    from etl.aact_loader import load_aact

    client = SamyamaClient.embedded()
    stats = load_aact(client, data_dir="data/aact")
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

from samyama import SamyamaClient

GRAPH = "default"
PROGRESS_INTERVAL = 10_000

# Increase csv field size limit for large text fields (brief_summaries, etc.)
csv.field_size_limit(10 * 1024 * 1024)  # 10 MB


# -- Helpers ------------------------------------------------------------------

def _esc(value: str | None) -> str:
    """Sanitize a string for Cypher double-quoted literals."""
    if value is None:
        return ""
    return str(value).replace('"', "").replace("\\", "").replace("\n", " ").replace("\r", "").strip()


def _prop_str(props: dict) -> str:
    """Build a Cypher property map literal, skipping empty values."""
    parts = []
    for key, val in props.items():
        if val is None or val == "":
            continue
        if isinstance(val, bool):
            parts.append(f'{key}: "{str(val).lower()}"')
        elif isinstance(val, (int, float)):
            parts.append(f"{key}: {val}")
        else:
            parts.append(f'{key}: "{_esc(str(val))}"')
    return "{" + ", ".join(parts) + "}"


def _read_pipe_file(path: Path):
    """Yield rows as dicts from a pipe-delimited AACT file."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            yield row


def _col(row: dict, name: str) -> str:
    """Get a trimmed column value from a row, returning '' if missing."""
    return (row.get(name) or "").strip()


def _q(client: SamyamaClient, cypher: str) -> None:
    """Execute a Cypher write query, silently skipping failures."""
    try:
        client.query(cypher, GRAPH)
    except Exception:
        pass


# -- Index creation -----------------------------------------------------------

def _create_indexes(client: SamyamaClient) -> None:
    """Create indexes for efficient MATCH lookups during edge creation."""
    indexes = [
        ("ClinicalTrial", "nct_id"),
        ("Condition", "name"),
        ("Intervention", "name"),
        ("Sponsor", "name"),
        ("Site", "facility"),
        ("AdverseEvent", "term"),
        ("Outcome", "measure"),
        ("ArmGroup", "label"),
        ("MeSHDescriptor", "name"),
        ("Publication", "pmid"),
    ]
    for label, prop in indexes:
        try:
            client.query(f"CREATE INDEX FOR (n:{label}) ON (n.{prop})", GRAPH)
        except Exception:
            pass  # index may already exist
    print(f"  Created {len(indexes)} indexes")


# -- Table loaders ------------------------------------------------------------

def _load_studies(
    client: SamyamaClient,
    data_dir: Path,
    max_studies: int,
) -> tuple[set[str], dict]:
    """Load studies.txt + brief_summaries.txt → ClinicalTrial nodes."""
    counts: dict[str, int] = {}

    # Pre-load brief summaries (nct_id → description)
    summaries: dict[str, str] = {}
    bs_path = data_dir / "brief_summaries.txt"
    if bs_path.exists():
        for row in _read_pipe_file(bs_path):
            nct_id = _col(row, "nct_id")
            desc = _col(row, "description")
            if nct_id and desc:
                summaries[nct_id] = desc[:500]
        print(f"    Pre-loaded {len(summaries)} brief summaries")

    path = data_dir / "studies.txt"
    if not path.exists():
        print("    ERROR: studies.txt not found")
        return set(), counts

    loaded: set[str] = set()
    n = 0
    for row in _read_pipe_file(path):
        nct_id = _col(row, "nct_id")
        if not nct_id:
            continue
        if max_studies and n >= max_studies:
            break

        enrollment = _col(row, "enrollment")
        try:
            enrollment_val: int | None = int(enrollment) if enrollment else None
        except ValueError:
            enrollment_val = None

        phase = _col(row, "phase")
        has_results = "true" if _col(row, "results_first_submitted_date") else "false"

        props = {
            "nct_id": nct_id,
            "title": _col(row, "brief_title") or None,
            "official_title": _col(row, "official_title") or None,
            "brief_summary": summaries.get(nct_id),
            "study_type": _col(row, "study_type") or None,
            "phase": phase or None,
            "overall_status": _col(row, "overall_status") or None,
            "enrollment": enrollment_val,
            "start_date": _col(row, "start_date") or None,
            "completion_date": _col(row, "completion_date") or None,
            "primary_completion_date": _col(row, "primary_completion_date") or None,
            "last_updated": _col(row, "last_update_submitted_date") or None,
            "has_results": has_results,
            "why_stopped": _col(row, "why_stopped") or None,
        }
        _q(client, f"CREATE (n:ClinicalTrial {_prop_str(props)})")
        loaded.add(nct_id)
        n += 1

        if n % PROGRESS_INTERVAL == 0:
            print(f"    ... {n:,} studies")

    counts["studies"] = n
    print(f"  Studies: {n:,}")
    return loaded, counts


def _load_conditions(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load conditions.txt → Condition nodes + STUDIES edges."""
    path = data_dir / "conditions.txt"
    if not path.exists():
        print("    conditions.txt not found, skipping")
        return {}

    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        name = _col(row, "name")
        nct_id = _col(row, "nct_id")
        if not name or not nct_id or nct_id not in nct_ids:
            continue

        key = name.lower()
        if key not in seen:
            _q(client, f'MERGE (n:Condition {{name: "{_esc(name)}"}})')
            seen.add(key)

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(c:Condition {{name: "{_esc(name)}"}}) '
            f"CREATE (t)-[:STUDIES]->(c)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} unique, {edges:,} edges)")

    print(f"  Conditions: {len(seen):,} unique, {edges:,} STUDIES edges")
    return {"conditions": len(seen), "studies_edges": edges}


def _load_interventions(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> tuple[dict[str, str], dict]:
    """Load interventions.txt → Intervention nodes + TESTS edges.

    Returns (id_to_name_map, counts) — the map is needed for
    design_group_interventions.
    """
    path = data_dir / "interventions.txt"
    if not path.exists():
        print("    interventions.txt not found, skipping")
        return {}, {}

    id_to_name: dict[str, str] = {}
    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        name = _col(row, "name")
        nct_id = _col(row, "nct_id")
        row_id = _col(row, "id")
        itype = _col(row, "intervention_type") or None
        desc = (_col(row, "description") or "")[:300] or None
        if not name or not nct_id or nct_id not in nct_ids:
            continue

        if row_id:
            id_to_name[row_id] = name

        key = name.lower()
        if key not in seen:
            props = {"name": name, "type": itype, "description": desc}
            _q(client, f"MERGE (n:Intervention {_prop_str(props)})")
            seen.add(key)

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(i:Intervention {{name: "{_esc(name)}"}}) '
            f"CREATE (t)-[:TESTS]->(i)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} unique, {edges:,} edges)")

    print(f"  Interventions: {len(seen):,} unique, {edges:,} TESTS edges")
    return id_to_name, {"interventions": len(seen), "tests_edges": edges}


def _load_design_groups(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> tuple[dict[str, tuple[str, str]], dict]:
    """Load design_groups.txt → ArmGroup nodes + HAS_ARM edges.

    Returns (id_to_info_map, counts) — map is {id: (nct_id, title)}.
    """
    path = data_dir / "design_groups.txt"
    if not path.exists():
        print("    design_groups.txt not found, skipping")
        return {}, {}

    id_to_info: dict[str, tuple[str, str]] = {}
    count = 0
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        title = _col(row, "title")
        row_id = _col(row, "id")
        gtype = _col(row, "group_type") or None
        desc = (_col(row, "description") or "")[:300] or None
        if not nct_id or not title or nct_id not in nct_ids:
            continue

        if row_id:
            id_to_info[row_id] = (nct_id, title)

        props = {
            "label": title,
            "type": gtype,
            "description": desc,
            "trial_nct_id": nct_id,
        }
        _q(client, f"CREATE (n:ArmGroup {_prop_str(props)})")
        count += 1

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(a:ArmGroup {{label: "{_esc(title)}", trial_nct_id: "{nct_id}"}}) '
            f"CREATE (t)-[:HAS_ARM]->(a)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({count:,} groups, {edges:,} edges)")

    print(f"  ArmGroups: {count:,}, {edges:,} HAS_ARM edges")
    return id_to_info, {"arm_groups": count, "has_arm_edges": edges}


def _load_design_group_interventions(
    client: SamyamaClient,
    data_dir: Path,
    intervention_id_map: dict[str, str],
    group_id_map: dict[str, tuple[str, str]],
) -> dict:
    """Load design_group_interventions.txt → USES edges (ArmGroup → Intervention)."""
    path = data_dir / "design_group_interventions.txt"
    if not path.exists():
        print("    design_group_interventions.txt not found, skipping")
        return {}

    edges = 0
    skipped = 0
    for i, row in enumerate(_read_pipe_file(path)):
        dg_id = _col(row, "design_group_id")
        interv_id = _col(row, "intervention_id")
        if not dg_id or not interv_id:
            continue

        group_info = group_id_map.get(dg_id)
        interv_name = intervention_id_map.get(interv_id)
        if not group_info or not interv_name:
            skipped += 1
            continue

        nct_id, group_title = group_info
        _q(
            client,
            f'MATCH (a:ArmGroup {{label: "{_esc(group_title)}", trial_nct_id: "{nct_id}"}}), '
            f'(i:Intervention {{name: "{_esc(interv_name)}"}}) '
            f"CREATE (a)-[:USES]->(i)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({edges:,} edges)")

    print(f"  USES edges: {edges:,} (skipped {skipped:,} unresolved)")
    return {"uses_edges": edges}


def _load_sponsors(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load sponsors.txt → Sponsor nodes + SPONSORED_BY edges (lead sponsors only)."""
    path = data_dir / "sponsors.txt"
    if not path.exists():
        print("    sponsors.txt not found, skipping")
        return {}

    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        name = _col(row, "name")
        nct_id = _col(row, "nct_id")
        role = _col(row, "lead_or_collaborator").lower()
        agency_class = _col(row, "agency_class") or None
        if not name or not nct_id or nct_id not in nct_ids:
            continue
        # Only create SPONSORED_BY for lead sponsors
        if role and role != "lead":
            continue

        key = name.lower()
        if key not in seen:
            props = {"name": name, "class": agency_class}
            _q(client, f"MERGE (n:Sponsor {_prop_str(props)})")
            seen.add(key)

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(s:Sponsor {{name: "{_esc(name)}"}}) '
            f"CREATE (t)-[:SPONSORED_BY]->(s)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} unique, {edges:,} edges)")

    print(f"  Sponsors: {len(seen):,} unique, {edges:,} SPONSORED_BY edges")
    return {"sponsors": len(seen), "sponsored_by_edges": edges}


def _load_outcomes(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load design_outcomes.txt → Outcome nodes + MEASURES edges."""
    path = data_dir / "design_outcomes.txt"
    if not path.exists():
        print("    design_outcomes.txt not found, skipping")
        return {}

    count = 0
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        measure = _col(row, "measure")
        otype = _col(row, "outcome_type") or None
        time_frame = _col(row, "time_frame") or None
        desc = (_col(row, "description") or "")[:300] or None
        if not nct_id or not measure or nct_id not in nct_ids:
            continue

        props = {
            "measure": measure,
            "type": otype,
            "time_frame": time_frame,
            "description": desc,
            "trial_nct_id": nct_id,
        }
        _q(client, f"CREATE (n:Outcome {_prop_str(props)})")
        count += 1

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(o:Outcome {{measure: "{_esc(measure)}", trial_nct_id: "{nct_id}"}}) '
            f"CREATE (t)-[:MEASURES]->(o)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({count:,} outcomes, {edges:,} edges)")

    print(f"  Outcomes: {count:,}, {edges:,} MEASURES edges")
    return {"outcomes": count, "measures_edges": edges}


def _load_facilities(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load facilities.txt → Site nodes + CONDUCTED_AT edges."""
    path = data_dir / "facilities.txt"
    if not path.exists():
        print("    facilities.txt not found, skipping")
        return {}

    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        facility = _col(row, "name")
        city = _col(row, "city")
        state = _col(row, "state") or None
        country = _col(row, "country") or None
        zipcode = _col(row, "zip") or None
        if not nct_id or nct_id not in nct_ids:
            continue
        if not facility and not city:
            continue

        # Dedup by facility + city
        key = f"{facility}|{city}".strip("|").lower()
        if key not in seen:
            props = {
                "facility": facility or None,
                "city": city or None,
                "state": state,
                "country": country,
                "zip": zipcode,
            }
            _q(client, f"MERGE (n:Site {_prop_str(props)})")
            seen.add(key)

        match_prop = f'facility: "{_esc(facility)}"' if facility else f'city: "{_esc(city)}"'
        _q(
            client,
            f"MATCH (t:ClinicalTrial {{nct_id: \"{nct_id}\"}}), "
            f"(s:Site {{{match_prop}}}) "
            f"CREATE (t)-[:CONDUCTED_AT]->(s)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} unique sites, {edges:,} edges)")

    print(f"  Sites: {len(seen):,} unique, {edges:,} CONDUCTED_AT edges")
    return {"sites": len(seen), "conducted_at_edges": edges}


def _load_reported_events(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load reported_events.txt → AdverseEvent nodes + REPORTED edges."""
    path = data_dir / "reported_events.txt"
    if not path.exists():
        # Also try reported_event_totals.txt
        path = data_dir / "reported_event_totals.txt"
        if not path.exists():
            print("    reported_events.txt not found, skipping")
            return {}

    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        # Column name varies across AACT versions
        term = _col(row, "event_term") or _col(row, "adverse_event_term") or _col(row, "term")
        organ_system = (
            _col(row, "organ_system")
            or _col(row, "classification")
            or _col(row, "category")
            or None
        )
        event_type = _col(row, "event_type") or None
        if not nct_id or not term or nct_id not in nct_ids:
            continue

        key = term.lower()
        if key not in seen:
            props = {
                "term": term,
                "organ_system": organ_system,
                "source_vocabulary": "MedDRA",
                "is_serious": "true" if event_type and "serious" in event_type.lower() else None,
            }
            _q(client, f"MERGE (n:AdverseEvent {_prop_str(props)})")
            seen.add(key)

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(ae:AdverseEvent {{term: "{_esc(term)}"}}) '
            f"CREATE (t)-[:REPORTED]->(ae)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} unique, {edges:,} edges)")

    print(f"  AdverseEvents: {len(seen):,} unique, {edges:,} REPORTED edges")
    return {"adverse_events": len(seen), "reported_edges": edges}


def _load_browse_conditions(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load browse_conditions.txt → MeSH cross-references on Condition nodes."""
    path = data_dir / "browse_conditions.txt"
    if not path.exists():
        print("    browse_conditions.txt not found, skipping")
        return {}

    mesh_terms: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        mesh_term = _col(row, "mesh_term")
        if not nct_id or not mesh_term or nct_id not in nct_ids:
            continue

        key = mesh_term.lower()
        if key not in mesh_terms:
            _q(client, f'MERGE (m:MeSHDescriptor {{name: "{_esc(mesh_term)}"}})')
            mesh_terms.add(key)

        # Link trial's conditions to MeSH terms via the trial
        # First find conditions for this trial and link them
        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}})-[:STUDIES]->(c:Condition), '
            f'(m:MeSHDescriptor {{name: "{_esc(mesh_term)}"}}) '
            f"MERGE (c)-[:CODED_AS_MESH]->(m)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(mesh_terms):,} MeSH terms)")

    print(f"  MeSH terms: {len(mesh_terms):,}, {edges:,} CODED_AS_MESH edges")
    return {"mesh_terms": len(mesh_terms), "coded_as_mesh_edges": edges}


def _load_study_references(
    client: SamyamaClient,
    data_dir: Path,
    nct_ids: set[str],
) -> dict:
    """Load study_references.txt → Publication nodes + PUBLISHED_IN edges."""
    path = data_dir / "study_references.txt"
    if not path.exists():
        print("    study_references.txt not found, skipping")
        return {}

    seen: set[str] = set()
    edges = 0
    for i, row in enumerate(_read_pipe_file(path)):
        nct_id = _col(row, "nct_id")
        pmid = _col(row, "pmid")
        citation = _col(row, "citation") or None
        ref_type = _col(row, "reference_type") or None
        if not nct_id or not pmid or nct_id not in nct_ids:
            continue

        if pmid not in seen:
            # Extract title from citation if possible (first sentence before period)
            title = None
            if citation:
                parts = citation.split(". ", 1)
                title = parts[0][:200] if parts else None
            props = {"pmid": pmid, "title": title, "reference_type": ref_type}
            _q(client, f"MERGE (p:Publication {_prop_str(props)})")
            seen.add(pmid)

        _q(
            client,
            f'MATCH (t:ClinicalTrial {{nct_id: "{nct_id}"}}), '
            f'(p:Publication {{pmid: "{pmid}"}}) '
            f"CREATE (t)-[:PUBLISHED_IN]->(p)",
        )
        edges += 1

        if (i + 1) % PROGRESS_INTERVAL == 0:
            print(f"    ... {i+1:,} rows ({len(seen):,} pubs, {edges:,} edges)")

    print(f"  Publications: {len(seen):,}, {edges:,} PUBLISHED_IN edges")
    return {"publications": len(seen), "published_in_edges": edges}


# -- Public entry point -------------------------------------------------------

def load_aact(
    client: SamyamaClient,
    data_dir: str = "data/aact",
    max_studies: int = 0,
    include_sites: bool = True,
    include_outcomes: bool = True,
    include_adverse_events: bool = True,
) -> dict:
    """Load AACT pipe-delimited flat files into the Samyama graph.

    Args:
        client: SamyamaClient instance (embedded or remote).
        data_dir: Path to directory containing extracted AACT .txt files.
        max_studies: Maximum studies to load (0 = all).
        include_sites: Whether to load facilities/sites (largest table).
        include_outcomes: Whether to load design outcomes.
        include_adverse_events: Whether to load reported adverse events.

    Returns:
        Dict of entity and relationship counts.
    """
    ddir = Path(data_dir)

    # Handle case where files are in a subdirectory
    if not (ddir / "studies.txt").exists():
        subdirs = [d for d in ddir.iterdir() if d.is_dir()]
        for sd in subdirs:
            if (sd / "studies.txt").exists():
                ddir = sd
                print(f"  Found data files in {sd}")
                break

    if not (ddir / "studies.txt").exists():
        raise FileNotFoundError(
            f"studies.txt not found in {data_dir}. "
            "Download AACT flat files first: python -m etl.download_aact"
        )

    t0 = time.time()
    all_counts: dict = {}

    # Step 0: Create indexes
    print("\n--- Step 0: Creating indexes ---")
    _create_indexes(client)

    # Step 1: Load studies
    print("\n--- Step 1: Loading studies ---")
    nct_ids, sc = _load_studies(client, ddir, max_studies)
    all_counts.update(sc)

    if not nct_ids:
        print("  No studies loaded, aborting.")
        return all_counts

    # Step 2: Load conditions
    print("\n--- Step 2: Loading conditions ---")
    all_counts.update(_load_conditions(client, ddir, nct_ids))

    # Step 3: Load interventions
    print("\n--- Step 3: Loading interventions ---")
    interv_map, ic = _load_interventions(client, ddir, nct_ids)
    all_counts.update(ic)

    # Step 4: Load design groups (arm groups)
    print("\n--- Step 4: Loading arm groups ---")
    group_map, gc = _load_design_groups(client, ddir, nct_ids)
    all_counts.update(gc)

    # Step 5: Load design group → intervention links
    print("\n--- Step 5: Loading arm-intervention links ---")
    all_counts.update(
        _load_design_group_interventions(client, ddir, interv_map, group_map)
    )

    # Step 6: Load sponsors
    print("\n--- Step 6: Loading sponsors ---")
    all_counts.update(_load_sponsors(client, ddir, nct_ids))

    # Step 7: Load outcomes (optional)
    if include_outcomes:
        print("\n--- Step 7: Loading outcomes ---")
        all_counts.update(_load_outcomes(client, ddir, nct_ids))
    else:
        print("\n--- Step 7: Skipping outcomes (--skip-outcomes) ---")

    # Step 8: Load facilities/sites (optional)
    if include_sites:
        print("\n--- Step 8: Loading facilities ---")
        all_counts.update(_load_facilities(client, ddir, nct_ids))
    else:
        print("\n--- Step 8: Skipping sites (--skip-sites) ---")

    # Step 9: Load adverse events (optional)
    if include_adverse_events:
        print("\n--- Step 9: Loading adverse events ---")
        all_counts.update(_load_reported_events(client, ddir, nct_ids))
    else:
        print("\n--- Step 9: Skipping adverse events (--skip-adverse-events) ---")

    # Step 10: Load MeSH cross-references
    print("\n--- Step 10: Loading MeSH cross-references ---")
    all_counts.update(_load_browse_conditions(client, ddir, nct_ids))

    # Step 11: Load publication references
    print("\n--- Step 11: Loading publication references ---")
    all_counts.update(_load_study_references(client, ddir, nct_ids))

    elapsed = time.time() - t0
    all_counts["elapsed_seconds"] = round(elapsed, 1)

    print(f"\n{'='*60}")
    print(f"AACT load complete in {elapsed:.1f}s")
    for key, val in sorted(all_counts.items()):
        if key != "elapsed_seconds":
            print(f"  {key:<30s} {val:>10,}")
    print(f"{'='*60}")

    return all_counts
