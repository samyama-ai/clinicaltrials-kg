"""Snapshot persistence for NSCLC Evidence Radar runs.

A "radar run" produces a timestamped snapshot directory under
``data/nsclc_runs/`` containing:

- ``subset.jsonl``           -- one NSCLC trial record per line (diff-friendly)
- ``subset_summary.json``    -- summary stats from build_subset
- ``workflow_<name>.json``   -- structured result per workflow
- ``workflow_<name>.md``     -- human-readable report per workflow
- ``run_metadata.json``      -- run provenance (args, graph stats, versions)

Everything written here is deterministic: given the same graph state and
arguments, the same files fall out.  That makes the snapshot directory
useful for (a) reproducibility and (b) diffing successive runs.
"""

from __future__ import annotations

import datetime as _dt
import importlib.metadata as _md
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from samyama import SamyamaClient

GRAPH = "default"

_NODE_LABELS = [
    "ClinicalTrial",
    "Condition",
    "Intervention",
    "Drug",
    "Sponsor",
    "Site",
    "MeSHDescriptor",
]


# ---------------------------------------------------------------------------
# Directory creation
# ---------------------------------------------------------------------------

def create_snapshot(
    today: _dt.date,
    label: str | None = None,
    base_dir: str | Path = "data/nsclc_runs",
) -> Path:
    """Create and return the snapshot directory for this run.

    The name is ``<YYYY-MM-DD>`` or ``<YYYY-MM-DD>_<label>`` when a label
    is supplied.  Existing directories are reused (idempotent), which makes
    reruns on the same day overwrite prior artifacts.
    """
    date_part = today.isoformat()
    name = f"{date_part}_{label}" if label else date_part
    path = Path(base_dir) / name
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# Subset persistence
# ---------------------------------------------------------------------------

def write_subset(
    snapshot_dir: Path,
    trials: list[dict[str, Any]],
    summary: dict[str, Any],
) -> tuple[Path, Path]:
    """Write subset.jsonl (one record per line, sorted by nct_id) and
    subset_summary.json.

    Returns the two paths written.
    """
    snapshot_dir = Path(snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    # Sort again defensively — build_subset already sorts, but cheap insurance.
    ordered = sorted(trials, key=lambda r: r.get("nct_id") or "")

    jsonl_path = snapshot_dir / "subset.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as fh:
        for rec in ordered:
            fh.write(
                json.dumps(rec, ensure_ascii=False, default=str, sort_keys=True)
            )
            fh.write("\n")

    summary_path = snapshot_dir / "subset_summary.json"
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False, default=str)
        fh.write("\n")

    return jsonl_path, summary_path


# ---------------------------------------------------------------------------
# Workflow persistence
# ---------------------------------------------------------------------------

def write_workflows(
    snapshot_dir: Path,
    workflow_outputs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Path]]:
    """Write every workflow's JSON + MD to the snapshot dir.

    ``workflow_outputs`` maps workflow name to a dict with keys ``result``
    (the structured output) and ``markdown`` (the rendered text).  Returns
    ``{name: {"json": Path, "md": Path}}`` for logging.
    """
    snapshot_dir = Path(snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, dict[str, Path]] = {}
    for name, bundle in workflow_outputs.items():
        json_path = snapshot_dir / f"workflow_{name}.json"
        md_path = snapshot_dir / f"workflow_{name}.md"
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(
                bundle["result"], fh, indent=2, ensure_ascii=False, default=str
            )
            fh.write("\n")
        with open(md_path, "w", encoding="utf-8") as fh:
            fh.write(bundle["markdown"])
        paths[name] = {"json": json_path, "md": md_path}
    return paths


# ---------------------------------------------------------------------------
# Metadata persistence
# ---------------------------------------------------------------------------

def write_metadata(snapshot_dir: Path, metadata: dict[str, Any]) -> Path:
    """Serialise run metadata to ``run_metadata.json``."""
    snapshot_dir = Path(snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / "run_metadata.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, ensure_ascii=False, default=str)
        fh.write("\n")
    return path


# ---------------------------------------------------------------------------
# Graph stats at query time
# ---------------------------------------------------------------------------

def _count(client: SamyamaClient, cypher: str) -> int:
    """Run a COUNT query and return the integer (0 on empty result)."""
    try:
        records = client.query_readonly(cypher, graph=GRAPH).records
    except Exception:
        return 0
    if not records or not records[0]:
        return 0
    try:
        return int(records[0][0])
    except (TypeError, ValueError):
        return 0


def capture_graph_stats(client: SamyamaClient) -> dict[str, Any]:
    """Snapshot node/edge counts for the graph.

    Per-label node counts are bundled in ``nodes_by_label``; a total node
    count and a total edge count round out the picture.  Missing labels
    simply report 0 and never raise.
    """
    nodes_by_label: dict[str, int] = {}
    for label in _NODE_LABELS:
        nodes_by_label[label] = _count(
            client, f"MATCH (n:{label}) RETURN count(n)"
        )
    total_nodes = _count(client, "MATCH (n) RETURN count(n)")
    total_edges = _count(client, "MATCH ()-[r]->() RETURN count(r)")

    return {
        "graph": GRAPH,
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "nodes_by_label": nodes_by_label,
    }


# ---------------------------------------------------------------------------
# Run metadata assembly
# ---------------------------------------------------------------------------

def _git_commit_sha() -> str:
    """Return the HEAD commit SHA, or ``"unknown"`` if git isn't available."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent.parent,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip() or "unknown"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _package_version(name: str) -> str:
    """Best-effort package version; returns ``"unknown"`` if not installed."""
    try:
        return _md.version(name)
    except _md.PackageNotFoundError:
        return "unknown"


def capture_run_metadata(
    args: dict[str, Any],
    graph_stats: dict[str, Any],
) -> dict[str, Any]:
    """Assemble the run_metadata.json payload.

    ``args`` is the dict of user-facing arguments (today, label, conditions,
    max_trials, ...); we record it verbatim so reruns can be reproduced.
    """
    now = _dt.datetime.now(_dt.timezone.utc)
    return {
        "timestamp_utc": now.isoformat(),
        "today": args.get("today"),
        "args": args,
        "graph_stats": graph_stats,
        "commit_sha": _git_commit_sha(),
        "python_version": sys.version.split()[0],
        "samyama_version": _package_version("samyama"),
    }
