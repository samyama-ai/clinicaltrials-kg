"""Extract the NSCLC trial subset from the clinical-trials knowledge graph.

This module loads trials for one or more search conditions into an embedded
Samyama graph (via :func:`etl.clinicaltrials_loader.load_trials`), then walks
the graph to find the trials that are actually about non-small-cell lung
cancer.

Two-pass identification (MeSH-first, determined and boring):

1. MeSH pass  -- any trial whose ``STUDIES`` edge points at a ``Condition``
   that is ``CODED_AS_MESH`` to a descriptor listed in ``entities.yaml``.
   This is the primary signal and the most trustworthy.  It may yield zero
   matches if MeSH enrichment has not been run yet -- that is fine.

2. Alias pass -- trials that were *not* already matched by MeSH, but whose
   ``Condition.name`` contains any of the case-insensitive alias strings
   from ``entities.yaml``.  This is a text fallback for un-enriched graphs.

Every matched trial is then flattened into a JSON-serialisable record with
its conditions, interventions, drugs, sponsor and site countries.  Records
are sorted by ``nct_id`` so the output is reproducible.

CLI
---
    python -m nsclc build-subset                     # defaults
    python -m nsclc build-subset --max-trials 500
    python -m nsclc build-subset --output out.json
    python -m nsclc build-subset --conditions "Lung Cancer" "Lung Neoplasms"
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from typing import Any

import click

from etl.clinicaltrials_loader import load_trials
from nsclc.entities import get_condition_aliases, get_mesh_codes, get_modalities
from nsclc.modality import tag_modalities
from samyama import SamyamaClient

GRAPH = "default"


# ---------------------------------------------------------------------------
# Cypher helpers
# ---------------------------------------------------------------------------

def _sanitize(value: str) -> str:
    """Strip characters that would break a double-quoted Cypher literal.

    Samyama's parser has no escape sequences inside string literals, so we
    simply remove the characters that would terminate or mangle one.
    """
    return value.replace('"', "").replace("\n", " ").replace("\r", "").strip()


def _quoted_list(values: list[str]) -> str:
    """Render a Python list as a Cypher list literal of quoted strings."""
    return "[" + ", ".join(f'"{_sanitize(v)}"' for v in values) + "]"


def _records(client: SamyamaClient, cypher: str) -> list[list[Any]]:
    """Run a read-only Cypher query and return the records as a plain list."""
    return client.query_readonly(cypher, graph=GRAPH).records


# ---------------------------------------------------------------------------
# Pass 1 / Pass 2 -- NSCLC trial identification
# ---------------------------------------------------------------------------

def find_trials_by_mesh(
    client: SamyamaClient, mesh_codes: list[str]
) -> set[str]:
    """Return NCT ids of trials linked to any of the given MeSH descriptors.

    Empty result (mesh_codes unset, or MeSH enrichment not run) is fine --
    the caller will fall back to alias matching.
    """
    if not mesh_codes:
        return set()
    cypher = (
        "MATCH (t:ClinicalTrial)-[:STUDIES]->(:Condition)"
        "-[:CODED_AS_MESH]->(m:MeSHDescriptor) "
        f"WHERE m.descriptor_id IN {_quoted_list(mesh_codes)} "
        "RETURN DISTINCT t.nct_id"
    )
    return {row[0] for row in _records(client, cypher) if row and row[0]}


def find_trials_by_alias(
    client: SamyamaClient, aliases: list[str], exclude: set[str]
) -> set[str]:
    """Return NCT ids of trials whose Condition.name contains any alias.

    Trials already present in *exclude* (the MeSH-pass result) are filtered
    out in Python -- simpler and more portable than a big NOT-IN clause.
    """
    if not aliases:
        return set()
    clauses = " OR ".join(
        f'c.name CONTAINS "{_sanitize(a)}"' for a in aliases
    )
    cypher = (
        "MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
        f"WHERE {clauses} "
        "RETURN DISTINCT t.nct_id"
    )
    found = {row[0] for row in _records(client, cypher) if row and row[0]}
    return found - exclude


# ---------------------------------------------------------------------------
# Per-trial detail extraction
# ---------------------------------------------------------------------------

_TRIAL_CORE_FIELDS = [
    "nct_id",
    "title",
    "overall_status",
    "phase",
    "enrollment",
    "start_date",
    "completion_date",
    "study_type",
    "brief_summary",
]


def _fetch_trial_core(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Return {nct_id: {core-field: value}} for the given trials."""
    if not nct_ids:
        return {}
    proj = ", ".join(f"t.{f} AS {f}" for f in _TRIAL_CORE_FIELDS)
    cypher = (
        "MATCH (t:ClinicalTrial) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        f"RETURN {proj}"
    )
    result = client.query_readonly(cypher, graph=GRAPH)
    columns = result.columns
    out: dict[str, dict[str, Any]] = {}
    for row in result.records:
        rec = dict(zip(columns, row))
        nct = rec.get("nct_id")
        if nct:
            # normalise: surface 'status' as an alias for overall_status
            rec["status"] = rec.get("overall_status")
            out[nct] = rec
    return out


def _fetch_conditions(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, list[str]]:
    """Return {nct_id: [condition_name, ...]}."""
    if not nct_ids:
        return {}
    cypher = (
        "MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        "RETURN t.nct_id, c.name"
    )
    out: dict[str, list[str]] = {}
    for nct, name in _records(client, cypher):
        if not nct or not name:
            continue
        out.setdefault(nct, []).append(name)
    for lst in out.values():
        lst.sort()
    return out


def _fetch_interventions(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, list[dict[str, str]]]:
    """Return {nct_id: [{name, type}, ...]}."""
    if not nct_ids:
        return {}
    cypher = (
        "MATCH (t:ClinicalTrial)-[:TESTS]->(i:Intervention) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        "RETURN t.nct_id, i.name, i.type"
    )
    out: dict[str, list[dict[str, str]]] = {}
    for nct, name, itype in _records(client, cypher):
        if not nct or not name:
            continue
        out.setdefault(nct, []).append({"name": name, "type": itype or ""})
    for lst in out.values():
        lst.sort(key=lambda d: d["name"].lower())
    return out


def _fetch_drugs(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, list[dict[str, str]]]:
    """Return {nct_id: [{name, rxcui, atc_code}, ...]}.

    Drug nodes only exist when drug enrichment has run; the query safely
    returns no rows if the label is empty.
    """
    if not nct_ids:
        return {}
    cypher = (
        "MATCH (t:ClinicalTrial)-[:TESTS]->(:Intervention)"
        "-[:CODED_AS_DRUG]->(d:Drug) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        "RETURN t.nct_id, d.name, d.rxcui, d.atc_code"
    )
    out: dict[str, list[dict[str, str]]] = {}
    for nct, name, rxcui, atc in _records(client, cypher):
        if not nct or not name:
            continue
        out.setdefault(nct, []).append(
            {"name": name, "rxcui": rxcui or "", "atc_code": atc or ""}
        )
    for lst in out.values():
        lst.sort(key=lambda d: d["name"].lower())
    return out


def _fetch_sponsors(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, dict[str, str]]:
    """Return {nct_id: {name, type}}.

    A trial may have multiple SPONSORED_BY edges in principle; we keep the
    first lead sponsor encountered to match the ClinicalTrials.gov schema.
    """
    if not nct_ids:
        return {}
    cypher = (
        "MATCH (t:ClinicalTrial)-[:SPONSORED_BY]->(s:Sponsor) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        "RETURN t.nct_id, s.name, s.class"
    )
    out: dict[str, dict[str, str]] = {}
    for nct, name, cls in _records(client, cypher):
        if not nct or not name or nct in out:
            continue
        out[nct] = {"name": name, "type": cls or ""}
    return out


def _fetch_sites(
    client: SamyamaClient, nct_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Return {nct_id: {site_count, countries: [...]}}."""
    if not nct_ids:
        return {}
    cypher = (
        "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) "
        f"WHERE t.nct_id IN {_quoted_list(nct_ids)} "
        "RETURN t.nct_id, s.country"
    )
    by_trial: dict[str, list[str]] = {}
    for nct, country in _records(client, cypher):
        if not nct:
            continue
        by_trial.setdefault(nct, []).append(country or "")
    out: dict[str, dict[str, Any]] = {}
    for nct, countries in by_trial.items():
        unique = sorted({c for c in countries if c})
        out[nct] = {"site_count": len(countries), "countries": unique}
    return out


# ---------------------------------------------------------------------------
# Record assembly
# ---------------------------------------------------------------------------

def assemble_records(
    client: SamyamaClient,
    mesh_hits: set[str],
    alias_hits: set[str],
) -> list[dict[str, Any]]:
    """Build the flat per-trial JSON records, sorted by nct_id.

    Each record is annotated with its treatment modalities using the
    deterministic tagger in :mod:`nsclc.modality`.
    """
    all_ids = sorted(mesh_hits | alias_hits)
    if not all_ids:
        return []

    cores = _fetch_trial_core(client, all_ids)
    conds = _fetch_conditions(client, all_ids)
    ivs = _fetch_interventions(client, all_ids)
    drugs = _fetch_drugs(client, all_ids)
    sponsors = _fetch_sponsors(client, all_ids)
    sites = _fetch_sites(client, all_ids)

    modalities_cfg = get_modalities()

    records: list[dict[str, Any]] = []
    for nct in all_ids:
        core = cores.get(nct, {})
        site_info = sites.get(nct, {"site_count": 0, "countries": []})
        record: dict[str, Any] = {
            "nct_id": nct,
            "title": core.get("title"),
            "status": core.get("status"),
            "phase": core.get("phase"),
            "enrollment": core.get("enrollment"),
            "start_date": core.get("start_date"),
            "completion_date": core.get("completion_date"),
            "study_type": core.get("study_type"),
            "brief_summary": core.get("brief_summary"),
            "conditions": conds.get(nct, []),
            "interventions": ivs.get(nct, []),
            "drugs": drugs.get(nct, []),
            "sponsor": sponsors.get(nct, {"name": "", "type": ""}),
            "sites": site_info,
            "match_method": "mesh" if nct in mesh_hits else "alias",
        }
        mods, evidence = tag_modalities(record, modalities_cfg)
        record["modalities"] = mods
        record["modality_evidence"] = evidence
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def build_subset(
    conditions: list[str],
    max_trials: int,
    include_results: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load trials, identify NSCLC ones, and return (records, summary)."""
    client = SamyamaClient.embedded()
    load_stats = load_trials(
        client,
        conditions=conditions,
        max_trials=max_trials,
        include_results=include_results,
    )

    mesh_codes = get_mesh_codes()
    aliases = get_condition_aliases()

    mesh_hits = find_trials_by_mesh(client, mesh_codes)
    alias_hits = find_trials_by_alias(client, aliases, exclude=mesh_hits)

    records = assemble_records(client, mesh_hits, alias_hits)

    # Top-level summary stats
    cond_counter: Counter[str] = Counter()
    iv_counter: Counter[str] = Counter()
    modality_counter: Counter[str] = Counter()
    trials_with_modality = 0
    for rec in records:
        cond_counter.update(rec["conditions"])
        iv_counter.update(iv["name"] for iv in rec["interventions"])
        if rec["modalities"]:
            trials_with_modality += 1
            modality_counter.update(rec["modalities"])

    summary = {
        "search_conditions": conditions,
        "max_trials": max_trials,
        "load_stats": load_stats,
        "mesh_codes": mesh_codes,
        "aliases": aliases,
        "total_nsclc_trials": len(records),
        "matched_by_mesh": len(mesh_hits),
        "matched_by_alias": len(alias_hits),
        "top_conditions": cond_counter.most_common(10),
        "top_interventions": iv_counter.most_common(10),
        "modality_distribution": modality_counter.most_common(),
        "trials_with_modality": trials_with_modality,
    }
    return records, summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_summary(summary: dict[str, Any], *, stream=sys.stderr) -> None:
    """Human-readable summary — goes to stderr so stdout stays JSON-clean."""
    print("", file=stream)
    print("=" * 60, file=stream)
    print("NSCLC subset summary", file=stream)
    print("=" * 60, file=stream)
    print(f"  search conditions      {summary['search_conditions']}", file=stream)
    print(f"  max_trials             {summary['max_trials']}", file=stream)
    print(f"  total NSCLC trials     {summary['total_nsclc_trials']}", file=stream)
    print(f"  matched by MeSH        {summary['matched_by_mesh']}", file=stream)
    print(f"  matched by alias       {summary['matched_by_alias']}", file=stream)
    print(f"  mesh codes             {summary['mesh_codes']}", file=stream)
    print(f"  aliases                {summary['aliases']}", file=stream)
    print("  top conditions:", file=stream)
    for name, n in summary["top_conditions"]:
        print(f"    {n:>4d}  {name}", file=stream)
    print("  top interventions:", file=stream)
    for name, n in summary["top_interventions"]:
        print(f"    {n:>4d}  {name}", file=stream)
    print(
        f"  trials with >=1 modality  {summary['trials_with_modality']}",
        file=stream,
    )
    print("  modality distribution:", file=stream)
    if summary["modality_distribution"]:
        for name, n in summary["modality_distribution"]:
            print(f"    {n:>4d}  {name}", file=stream)
    else:
        print("    (none matched)", file=stream)


@click.command("build-subset")
@click.option(
    "--conditions",
    multiple=True,
    default=("Lung Cancer",),
    show_default=True,
    help="Search term(s) to feed ClinicalTrials.gov. May be repeated.",
)
@click.option(
    "--max-trials",
    type=int,
    default=200,
    show_default=True,
    help="Maximum trials to fetch per search condition.",
)
@click.option(
    "--include-results",
    is_flag=True,
    default=False,
    help="Fetch resultsSection (adverse events) too.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help="Write JSON records to this file instead of stdout.",
)
@click.option(
    "--no-summary",
    is_flag=True,
    default=False,
    help="Suppress the human-readable summary on stderr.",
)
def main(
    conditions: tuple[str, ...],
    max_trials: int,
    include_results: bool,
    output: str | None,
    no_summary: bool,
) -> None:
    """Identify NSCLC trials in the knowledge graph and emit JSON."""
    records, summary = build_subset(
        conditions=list(conditions),
        max_trials=max_trials,
        include_results=include_results,
    )

    payload = json.dumps(records, indent=2, ensure_ascii=False, default=str)
    if output:
        with open(output, "w", encoding="utf-8") as fh:
            fh.write(payload)
        print(f"wrote {len(records)} records to {output}", file=sys.stderr)
    else:
        sys.stdout.write(payload)
        sys.stdout.write("\n")

    if not no_summary:
        _print_summary(summary)


if __name__ == "__main__":
    main()
