"""End-to-end NSCLC Evidence Radar run.

``python -m nsclc run`` ties the deterministic pipeline together:

1. Load trials into an embedded Samyama graph.
2. Identify the NSCLC subset (MeSH pass + alias fallback).
3. Create a timestamped snapshot directory under ``data/nsclc_runs/``.
4. Write ``subset.jsonl`` + ``subset_summary.json``.
5. Run all five workflows and write ``workflow_<name>.json`` / ``.md``.
6. Capture graph stats + run metadata and write ``run_metadata.json``.
7. Print a human-readable summary on stderr (snapshot path, counts).

No LLM calls, no network randomness; given the same inputs the same
artifacts fall out.
"""

from __future__ import annotations

import datetime as _dt
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import click

from etl.clinicaltrials_loader import load_trials
from nsclc.brief import write_brief
from nsclc.build_subset import (
    assemble_records,
    find_trials_by_alias,
    find_trials_by_mesh,
)
from nsclc.build_subset import _print_summary as _print_subset_summary
from nsclc.entities import get_condition_aliases, get_mesh_codes
from nsclc.snapshot import (
    capture_graph_stats,
    capture_run_metadata,
    create_snapshot,
    write_metadata,
    write_subset,
    write_workflows,
)
from nsclc.workflows import (
    list_workflow_names,
    parse_today,
    render_markdown,
    run_workflow,
)
from samyama import SamyamaClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_latest_prior_snapshot(
    base_dir: str | Path, current_snapshot: Path
) -> Path | None:
    """Return the most recent snapshot dir under ``base_dir`` that sorts
    lexicographically before ``current_snapshot`` and has a ``subset.jsonl``.

    ``None`` if no such dir exists.  Name-based comparison is deliberate:
    snapshot dir names start with ``YYYY-MM-DD`` so lexicographic order
    matches chronological order.
    """
    base = Path(base_dir)
    if not base.exists():
        return None
    current_name = current_snapshot.name
    candidates = [
        p for p in base.iterdir()
        if p.is_dir()
        and (p / "subset.jsonl").exists()
        and p.name < current_name
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.name)
    return candidates[-1]


def _subset_summary(
    conditions: list[str],
    max_trials: int,
    load_stats: dict[str, Any],
    mesh_codes: list[str],
    aliases: list[str],
    mesh_hits: set[str],
    alias_hits: set[str],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Re-derive the subset summary (same shape build_subset produces)."""
    cond_counter: Counter[str] = Counter()
    iv_counter: Counter[str] = Counter()
    modality_counter: Counter[str] = Counter()
    trials_with_modality = 0
    for rec in records:
        cond_counter.update(rec.get("conditions") or [])
        iv_counter.update(
            iv["name"] for iv in rec.get("interventions") or [] if iv.get("name")
        )
        if rec.get("modalities"):
            trials_with_modality += 1
            modality_counter.update(rec["modalities"])

    return {
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


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    today: _dt.date,
    label: str | None,
    conditions: list[str],
    max_trials: int,
    include_results: bool,
    base_dir: str | Path,
) -> dict[str, Any]:
    """Execute the full NSCLC radar pipeline and return a summary dict."""
    # 1. Load the graph.
    client = SamyamaClient.embedded()
    load_stats = load_trials(
        client,
        conditions=conditions,
        max_trials=max_trials,
        include_results=include_results,
    )

    # 2. Identify the NSCLC subset.
    mesh_codes = get_mesh_codes()
    aliases = get_condition_aliases()
    mesh_hits = find_trials_by_mesh(client, mesh_codes)
    alias_hits = find_trials_by_alias(client, aliases, exclude=mesh_hits)
    records = assemble_records(client, mesh_hits, alias_hits)

    summary = _subset_summary(
        conditions, max_trials, load_stats,
        mesh_codes, aliases, mesh_hits, alias_hits, records,
    )

    # 3. Create the snapshot dir.
    snapshot_dir = create_snapshot(today, label=label, base_dir=base_dir)

    # 4. Persist the subset.
    write_subset(snapshot_dir, records, summary)

    # 5. Run workflows.
    workflow_outputs: dict[str, dict[str, Any]] = {}
    workflow_summary: dict[str, dict[str, Any]] = {}
    for name in list_workflow_names():
        result = run_workflow(name, records, today=today)
        markdown = render_markdown(name, result)
        workflow_outputs[name] = {"result": result, "markdown": markdown}
        workflow_summary[name] = {
            "total_matched": result.get(
                "total_matched", sum(result.get("counts", {}).values())
            ),
            "counts": result.get("counts", {}),
        }
    write_workflows(snapshot_dir, workflow_outputs)

    # 6. Capture graph stats + metadata.
    graph_stats = capture_graph_stats(client)
    args_record = {
        "today": today.isoformat(),
        "label": label,
        "conditions": conditions,
        "max_trials": max_trials,
        "include_results": include_results,
        "base_dir": str(base_dir),
    }
    metadata = capture_run_metadata(args_record, graph_stats)
    write_metadata(snapshot_dir, metadata)

    # 7. Generate consolidated brief (auto-detect latest prior snapshot).
    prior_snapshot = _find_latest_prior_snapshot(base_dir, snapshot_dir)
    brief_path = write_brief(snapshot_dir, prior_snapshot_dir=prior_snapshot)

    return {
        "snapshot_dir": snapshot_dir,
        "subset_summary": summary,
        "workflow_summary": workflow_summary,
        "graph_stats": graph_stats,
        "metadata": metadata,
        "brief_path": brief_path,
        "prior_snapshot_dir": prior_snapshot,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command("run")
@click.option(
    "--today",
    "today_str",
    type=str,
    default=None,
    help="Override today's date (YYYY-MM-DD) for reproducible runs.",
)
@click.option(
    "--label",
    type=str,
    default=None,
    help="Optional suffix for the snapshot directory (disambiguate same-day runs).",
)
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
    "--base-dir",
    type=click.Path(file_okay=False, writable=True),
    default="data/nsclc_runs",
    show_default=True,
    help="Root directory for snapshot folders.",
)
def main(
    today_str: str | None,
    label: str | None,
    conditions: tuple[str, ...],
    max_trials: int,
    include_results: bool,
    base_dir: str,
) -> None:
    """Run the full NSCLC Evidence Radar pipeline end-to-end."""
    today = parse_today(today_str)
    conditions_list = list(conditions)

    result = run_pipeline(
        today=today,
        label=label,
        conditions=conditions_list,
        max_trials=max_trials,
        include_results=include_results,
        base_dir=base_dir,
    )

    snapshot_dir = result["snapshot_dir"]
    subset = result["subset_summary"]
    wf = result["workflow_summary"]

    _print_subset_summary(subset, stream=sys.stderr)

    print("", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print("NSCLC radar run summary", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"  snapshot dir    {snapshot_dir}", file=sys.stderr)
    print(f"  today           {today.isoformat()}", file=sys.stderr)
    print(f"  total trials    {subset['total_nsclc_trials']}", file=sys.stderr)
    print("  workflow counts:", file=sys.stderr)
    for name, info in wf.items():
        print(
            f"    {name}: matched={info.get('total_matched', 0)}",
            file=sys.stderr,
        )
    prior = result.get("prior_snapshot_dir")
    brief_path = result.get("brief_path")
    if brief_path:
        tag = f"(prior: {prior.name})" if prior else "(no prior snapshot)"
        print(f"  brief           {brief_path} {tag}", file=sys.stderr)
    print("", file=sys.stderr)

    # Final stdout line: the snapshot path, so downstream tools can capture it.
    sys.stdout.write(str(snapshot_dir) + "\n")


if __name__ == "__main__":
    main()
