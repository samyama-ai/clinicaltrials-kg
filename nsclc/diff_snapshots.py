"""Diff two NSCLC Evidence Radar snapshot directories.

Given snapshot A (earlier) and snapshot B (later), emit a structured,
human-readable description of what changed in the NSCLC trial landscape
between runs.  Deterministic, no LLM, no network.

Dimensions covered
------------------
* Trials added   -- nct_ids in B but not in A
* Trials removed -- nct_ids in A but not in B
* Trials changed -- same nct_id, different values for one of the six
  important fields: ``status``, ``phase``, ``enrollment``,
  ``completion_date``, ``modalities``, ``sponsor.name``
* Summary delta  -- total_trials, mesh vs alias mix, modality distribution
* Workflow delta -- for each workflow present in both snapshots, which
  trials entered or left each group

Outputs
-------
Two files in the output directory (default
``data/nsclc_runs/diff_<a>_vs_<b>/``):

* ``diff.json`` -- fully structured machine-readable diff
* ``diff.md``   -- human-readable report

CLI
---
    python -m nsclc diff <snapshot_a> <snapshot_b>
    python -m nsclc diff <snapshot_a> <snapshot_b> --output-dir <dir>
    python -m nsclc diff --shortcut latest

Self-test
---------
    python -m nsclc.diff_snapshots --selftest

builds two synthetic snapshots in a tempdir, runs the differ, and asserts
that the expected changes show up.  No real data is touched.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Iterable, Iterator

import click

# ---------------------------------------------------------------------------
# What we diff at the trial level
# ---------------------------------------------------------------------------

# Six fields we care about at the per-trial level.  Scalar fields are
# compared directly; ``modalities`` is compared as a set for added/removed;
# ``sponsor`` is compared via its ``name`` subfield.
SCALAR_FIELDS: tuple[str, ...] = (
    "status",
    "phase",
    "enrollment",
    "completion_date",
)
LIST_FIELDS: tuple[str, ...] = ("modalities",)
SPONSOR_SUBFIELD = "sponsor.name"


# ---------------------------------------------------------------------------
# Streaming loaders
# ---------------------------------------------------------------------------

def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield one dict per line from a JSONL file.  Skips blank lines."""
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_subset(snapshot_dir: Path) -> dict[str, dict[str, Any]]:
    """Stream ``subset.jsonl`` and return ``{nct_id: record}``.

    Streaming keeps memory bounded for large snapshots; we only hold the
    final dict in memory (one entry per nct_id).
    """
    subset_path = snapshot_dir / "subset.jsonl"
    if not subset_path.exists():
        raise FileNotFoundError(f"missing subset.jsonl at {subset_path}")
    out: dict[str, dict[str, Any]] = {}
    for rec in _iter_jsonl(subset_path):
        nct_id = rec.get("nct_id")
        if not nct_id:
            continue
        out[nct_id] = rec
    return out


def load_summary(snapshot_dir: Path) -> dict[str, Any]:
    """Read ``subset_summary.json``; return ``{}`` if missing."""
    summary_path = snapshot_dir / "subset_summary.json"
    if not summary_path.exists():
        return {}
    with open(summary_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def list_workflow_files(snapshot_dir: Path) -> dict[str, Path]:
    """Return ``{workflow_name: path}`` for every ``workflow_<name>.json``."""
    out: dict[str, Path] = {}
    for p in sorted(snapshot_dir.glob("workflow_*.json")):
        name = p.stem[len("workflow_"):]
        out[name] = p
    return out


def load_workflow(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Trial-level diff
# ---------------------------------------------------------------------------

def _sponsor_name(rec: dict[str, Any]) -> str | None:
    sponsor = rec.get("sponsor") or {}
    if isinstance(sponsor, dict):
        return sponsor.get("name")
    return None


def _diff_one_trial(
    before: dict[str, Any],
    after: dict[str, Any],
) -> list[dict[str, Any]]:
    """Compare one trial record before/after and return a list of changes."""
    changes: list[dict[str, Any]] = []

    for field in SCALAR_FIELDS:
        b_val = before.get(field)
        a_val = after.get(field)
        if b_val != a_val:
            changes.append({"field": field, "before": b_val, "after": a_val})

    for field in LIST_FIELDS:
        b_list = before.get(field) or []
        a_list = after.get(field) or []
        b_set = set(b_list)
        a_set = set(a_list)
        added = sorted(a_set - b_set)
        removed = sorted(b_set - a_set)
        if added or removed:
            changes.append(
                {"field": field, "added": added, "removed": removed}
            )

    b_sp = _sponsor_name(before)
    a_sp = _sponsor_name(after)
    if b_sp != a_sp:
        changes.append(
            {"field": SPONSOR_SUBFIELD, "before": b_sp, "after": a_sp}
        )

    return changes


def diff_trials(
    subset_a: dict[str, dict[str, Any]],
    subset_b: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Return ``{added: [...], removed: [...], changed: [...]}``.

    Each entry is a small dict with ``nct_id``, ``title`` and (for
    ``changed``) a ``changes`` list.  Results are sorted by nct_id so the
    output is deterministic.
    """
    ids_a = set(subset_a)
    ids_b = set(subset_b)

    added_ids = sorted(ids_b - ids_a)
    removed_ids = sorted(ids_a - ids_b)
    common_ids = sorted(ids_a & ids_b)

    added = [
        {"nct_id": nct_id, "title": subset_b[nct_id].get("title")}
        for nct_id in added_ids
    ]
    removed = [
        {"nct_id": nct_id, "title": subset_a[nct_id].get("title")}
        for nct_id in removed_ids
    ]
    changed: list[dict[str, Any]] = []
    for nct_id in common_ids:
        ch = _diff_one_trial(subset_a[nct_id], subset_b[nct_id])
        if ch:
            changed.append(
                {
                    "nct_id": nct_id,
                    "title": subset_b[nct_id].get("title"),
                    "changes": ch,
                }
            )

    return {"added": added, "removed": removed, "changed": changed}


# ---------------------------------------------------------------------------
# Summary-level diff
# ---------------------------------------------------------------------------

def _pairs_to_dict(pairs: Any) -> dict[str, int]:
    """Turn ``[[key, n], ...]`` into ``{key: n}``.  Pass dicts through."""
    if isinstance(pairs, dict):
        return {str(k): int(v) for k, v in pairs.items()}
    out: dict[str, int] = {}
    if not isinstance(pairs, list):
        return out
    for item in pairs:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            out[str(item[0])] = int(item[1])
    return out


def _modality_delta(
    before: dict[str, int], after: dict[str, int]
) -> dict[str, dict[str, int]]:
    """Per-modality before/after/delta dict, sorted by modality name."""
    keys = sorted(set(before) | set(after))
    out: dict[str, dict[str, int]] = {}
    for k in keys:
        b = int(before.get(k, 0))
        a = int(after.get(k, 0))
        out[k] = {"before": b, "after": a, "delta": a - b}
    return out


def diff_summary(
    summary_a: dict[str, Any], summary_b: dict[str, Any]
) -> dict[str, Any]:
    """Compare the two ``subset_summary.json`` payloads."""
    total_before = int(summary_a.get("total_nsclc_trials", 0) or 0)
    total_after = int(summary_b.get("total_nsclc_trials", 0) or 0)

    mesh_before = int(summary_a.get("matched_by_mesh", 0) or 0)
    mesh_after = int(summary_b.get("matched_by_mesh", 0) or 0)
    alias_before = int(summary_a.get("matched_by_alias", 0) or 0)
    alias_after = int(summary_b.get("matched_by_alias", 0) or 0)

    mod_before = _pairs_to_dict(summary_a.get("modality_distribution") or [])
    mod_after = _pairs_to_dict(summary_b.get("modality_distribution") or [])

    return {
        "total_trials": {
            "before": total_before,
            "after": total_after,
            "delta": total_after - total_before,
        },
        "matched_by_mesh": {
            "before": mesh_before,
            "after": mesh_after,
            "delta": mesh_after - mesh_before,
        },
        "matched_by_alias": {
            "before": alias_before,
            "after": alias_after,
            "delta": alias_after - alias_before,
        },
        "modality_distribution": _modality_delta(mod_before, mod_after),
    }


# ---------------------------------------------------------------------------
# Workflow-level diff
# ---------------------------------------------------------------------------

def _workflow_nct_ids(workflow: dict[str, Any]) -> set[str]:
    """Flatten all nct_ids referenced by any group in a workflow result."""
    ids: set[str] = set()
    groups = workflow.get("groups") or {}
    if not isinstance(groups, dict):
        return ids
    for entries in groups.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                nct_id = entry.get("nct_id")
                if nct_id:
                    ids.add(nct_id)
    return ids


def _workflow_group_ids(
    workflow: dict[str, Any],
) -> dict[str, set[str]]:
    """Return ``{group_name: {nct_ids}}`` for a workflow payload."""
    out: dict[str, set[str]] = {}
    groups = workflow.get("groups") or {}
    if not isinstance(groups, dict):
        return out
    for name, entries in groups.items():
        ids: set[str] = set()
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict):
                    nct_id = entry.get("nct_id")
                    if nct_id:
                        ids.add(nct_id)
        out[name] = ids
    return out


def diff_workflows(
    snapshot_a: Path,
    snapshot_b: Path,
    *,
    warn: Any = None,
) -> dict[str, dict[str, Any]]:
    """Per-workflow added/removed nct_ids, and per-group breakdown.

    Workflows that exist in only one snapshot are logged (via ``warn``)
    and skipped — we cannot meaningfully diff them.
    """
    warn = warn or (lambda msg: None)
    files_a = list_workflow_files(snapshot_a)
    files_b = list_workflow_files(snapshot_b)

    only_a = sorted(set(files_a) - set(files_b))
    only_b = sorted(set(files_b) - set(files_a))
    for name in only_a:
        warn(f"workflow '{name}' only in snapshot A; skipping")
    for name in only_b:
        warn(f"workflow '{name}' only in snapshot B; skipping")

    common = sorted(set(files_a) & set(files_b))
    out: dict[str, dict[str, Any]] = {}
    for name in common:
        wa = load_workflow(files_a[name])
        wb = load_workflow(files_b[name])

        ids_a = _workflow_nct_ids(wa)
        ids_b = _workflow_nct_ids(wb)

        groups_a = _workflow_group_ids(wa)
        groups_b = _workflow_group_ids(wb)
        all_groups = sorted(set(groups_a) | set(groups_b))

        per_group: dict[str, dict[str, list[str]]] = {}
        for gname in all_groups:
            ga = groups_a.get(gname, set())
            gb = groups_b.get(gname, set())
            per_group[gname] = {
                "added": sorted(gb - ga),
                "removed": sorted(ga - gb),
            }

        out[name] = {
            "total_matched_before": int(
                wa.get("total_matched")
                if wa.get("total_matched") is not None
                else sum((wa.get("counts") or {}).values())
            ),
            "total_matched_after": int(
                wb.get("total_matched")
                if wb.get("total_matched") is not None
                else sum((wb.get("counts") or {}).values())
            ),
            "trials_added": sorted(ids_b - ids_a),
            "trials_removed": sorted(ids_a - ids_b),
            "per_group": per_group,
        }
    return out


# ---------------------------------------------------------------------------
# Top-level diff
# ---------------------------------------------------------------------------

def compute_diff(
    snapshot_a: Path,
    snapshot_b: Path,
    *,
    warn: Any = None,
) -> dict[str, Any]:
    """Return the full structured diff between two snapshot dirs."""
    warn = warn or (lambda msg: None)

    subset_a = load_subset(snapshot_a)
    subset_b = load_subset(snapshot_b)
    summary_a = load_summary(snapshot_a)
    summary_b = load_summary(snapshot_b)

    metadata = {
        "snapshot_a": str(snapshot_a),
        "snapshot_b": str(snapshot_b),
        "snapshot_a_name": snapshot_a.name,
        "snapshot_b_name": snapshot_b.name,
        "snapshot_a_date": summary_a.get("today")
        or _date_from_name(snapshot_a.name),
        "snapshot_b_date": summary_b.get("today")
        or _date_from_name(snapshot_b.name),
        "snapshot_a_total_trials": len(subset_a),
        "snapshot_b_total_trials": len(subset_b),
    }

    return {
        "metadata": metadata,
        "trials": diff_trials(subset_a, subset_b),
        "summary_delta": diff_summary(summary_a, summary_b),
        "workflow_deltas": diff_workflows(snapshot_a, snapshot_b, warn=warn),
    }


def _date_from_name(name: str) -> str | None:
    """Best-effort pull of YYYY-MM-DD prefix from a snapshot dir name."""
    if len(name) >= 10 and name[4] == "-" and name[7] == "-":
        return name[:10]
    return None


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _fmt_change_line(change: dict[str, Any]) -> str:
    field = change.get("field", "?")
    if field in LIST_FIELDS:
        added = change.get("added") or []
        removed = change.get("removed") or []
        parts = []
        if added:
            parts.append(f"added {added}")
        if removed:
            parts.append(f"removed {removed}")
        rhs = "; ".join(parts) if parts else "(no-op)"
        return f"- **{field}**: {rhs}"
    before = change.get("before")
    after = change.get("after")
    return f"- **{field}**: `{before!r}` -> `{after!r}`"


def render_markdown(diff: dict[str, Any]) -> str:
    """Render the diff dict as a human-readable markdown document."""
    meta = diff.get("metadata", {})
    trials = diff.get("trials", {})
    summary = diff.get("summary_delta", {})
    workflows = diff.get("workflow_deltas", {})

    lines: list[str] = []
    lines.append("# NSCLC Evidence Radar — Snapshot Diff")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Snapshot A**: `{meta.get('snapshot_a_name')}`  "
                 f"(date: {meta.get('snapshot_a_date')}, "
                 f"trials: {meta.get('snapshot_a_total_trials')})")
    lines.append(f"- **Snapshot B**: `{meta.get('snapshot_b_name')}`  "
                 f"(date: {meta.get('snapshot_b_date')}, "
                 f"trials: {meta.get('snapshot_b_total_trials')})")
    lines.append(f"- **Trials added**: {len(trials.get('added', []))}")
    lines.append(f"- **Trials removed**: {len(trials.get('removed', []))}")
    lines.append(f"- **Trials changed**: {len(trials.get('changed', []))}")
    lines.append("")

    # Trials Added
    lines.append("## Trials Added")
    lines.append("")
    added = trials.get("added") or []
    if not added:
        lines.append("_None._")
    else:
        for entry in added:
            lines.append(f"- `{entry['nct_id']}` — {entry.get('title') or ''}")
    lines.append("")

    # Trials Removed
    lines.append("## Trials Removed")
    lines.append("")
    removed = trials.get("removed") or []
    if not removed:
        lines.append("_None._")
    else:
        for entry in removed:
            lines.append(f"- `{entry['nct_id']}` — {entry.get('title') or ''}")
    lines.append("")

    # Trials Changed
    lines.append("## Trials Changed")
    lines.append("")
    changed = trials.get("changed") or []
    if not changed:
        lines.append("_None._")
    else:
        for entry in changed:
            lines.append(
                f"### `{entry['nct_id']}` — {entry.get('title') or ''}"
            )
            for ch in entry.get("changes") or []:
                lines.append(_fmt_change_line(ch))
            lines.append("")
    lines.append("")

    # Summary Delta
    lines.append("## Summary Delta")
    lines.append("")
    for key in ("total_trials", "matched_by_mesh", "matched_by_alias"):
        s = summary.get(key) or {}
        lines.append(
            f"- **{key}**: {s.get('before', 0)} -> {s.get('after', 0)} "
            f"(delta {s.get('delta', 0):+d})"
        )
    lines.append("")
    lines.append("### Modality distribution")
    lines.append("")
    mod = summary.get("modality_distribution") or {}
    if not mod:
        lines.append("_No modality data._")
    else:
        lines.append("| modality | before | after | delta |")
        lines.append("|---|---:|---:|---:|")
        for name, row in mod.items():
            lines.append(
                f"| {name} | {row.get('before', 0)} | "
                f"{row.get('after', 0)} | {row.get('delta', 0):+d} |"
            )
    lines.append("")

    # Workflow Deltas
    lines.append("## Workflow Deltas")
    lines.append("")
    if not workflows:
        lines.append("_No workflows in common._")
    else:
        for name in sorted(workflows):
            wf = workflows[name]
            before = wf.get("total_matched_before", 0)
            after = wf.get("total_matched_after", 0)
            delta = after - before
            lines.append(
                f"### {name}  "
                f"(matched {before} -> {after}, delta {delta:+d})"
            )
            lines.append("")
            ta = wf.get("trials_added") or []
            tr = wf.get("trials_removed") or []
            lines.append(
                f"- trials added: {len(ta)}"
                + (f" — {', '.join(ta)}" if ta else "")
            )
            lines.append(
                f"- trials removed: {len(tr)}"
                + (f" — {', '.join(tr)}" if tr else "")
            )
            per_group = wf.get("per_group") or {}
            movers = [
                (g, info)
                for g, info in per_group.items()
                if info.get("added") or info.get("removed")
            ]
            if movers:
                lines.append("- per-group movement:")
                for gname, info in movers:
                    gadded = info.get("added") or []
                    gremoved = info.get("removed") or []
                    lines.append(
                        f"    - **{gname}**: "
                        f"+{len(gadded)} / -{len(gremoved)}"
                        + (
                            f" (added: {', '.join(gadded)})"
                            if gadded
                            else ""
                        )
                        + (
                            f" (removed: {', '.join(gremoved)})"
                            if gremoved
                            else ""
                        )
                    )
            lines.append("")
    # Trailing newline for POSIX-friendly files.
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------

def write_diff(
    diff: dict[str, Any], output_dir: Path
) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "diff.json"
    md_path = output_dir / "diff.md"

    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(diff, fh, indent=2, ensure_ascii=False, default=str,
                  sort_keys=True)
        fh.write("\n")

    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(render_markdown(diff))

    return json_path, md_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _default_output_dir(a: Path, b: Path) -> Path:
    """Deterministic diff output dir next to the snapshots."""
    parent = a.parent if a.parent == b.parent else Path("data/nsclc_runs")
    return parent / f"diff_{a.name}_vs_{b.name}"


def _latest_two_snapshots(
    base_dir: Path = Path("data/nsclc_runs"),
) -> tuple[Path, Path]:
    """Return the two most recent snapshot dirs by mtime (older, newer)."""
    if not base_dir.exists():
        raise click.ClickException(
            f"--shortcut latest: base dir {base_dir} does not exist"
        )
    candidates = [
        p for p in base_dir.iterdir()
        if p.is_dir() and (p / "subset.jsonl").exists()
    ]
    if len(candidates) < 2:
        raise click.ClickException(
            f"--shortcut latest needs >=2 snapshots under {base_dir}, "
            f"found {len(candidates)}"
        )
    candidates.sort(key=lambda p: p.stat().st_mtime)
    # second-newest is "a" (older), newest is "b" (newer)
    return candidates[-2], candidates[-1]


@click.command("diff")
@click.argument("snapshot_a", required=False,
                type=click.Path(exists=False, file_okay=False))
@click.argument("snapshot_b", required=False,
                type=click.Path(exists=False, file_okay=False))
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory to write diff.json + diff.md into.  "
         "Defaults to data/nsclc_runs/diff_<a>_vs_<b>/.",
)
@click.option(
    "--shortcut",
    type=click.Choice(["latest"]),
    default=None,
    help="Convenience: 'latest' diffs the two most recent snapshots "
         "under data/nsclc_runs/ by mtime.",
)
@click.option(
    "--selftest",
    is_flag=True,
    default=False,
    help="Run the built-in synthetic-fixture test and exit.",
)
def main(
    snapshot_a: str | None,
    snapshot_b: str | None,
    output_dir: str | None,
    shortcut: str | None,
    selftest: bool,
) -> None:
    """Diff two NSCLC snapshot directories."""
    if selftest:
        _selftest()
        return

    if shortcut == "latest":
        a_path, b_path = _latest_two_snapshots()
    else:
        if not snapshot_a or not snapshot_b:
            raise click.ClickException(
                "snapshot_a and snapshot_b are required "
                "(unless --shortcut latest is given)"
            )
        a_path = Path(snapshot_a)
        b_path = Path(snapshot_b)

    if not a_path.exists():
        raise click.ClickException(f"snapshot A does not exist: {a_path}")
    if not b_path.exists():
        raise click.ClickException(f"snapshot B does not exist: {b_path}")
    if not (a_path / "subset.jsonl").exists():
        raise click.ClickException(
            f"snapshot A missing subset.jsonl: {a_path}"
        )
    if not (b_path / "subset.jsonl").exists():
        raise click.ClickException(
            f"snapshot B missing subset.jsonl: {b_path}"
        )

    out_dir = Path(output_dir) if output_dir else _default_output_dir(
        a_path, b_path
    )

    def _warn(msg: str) -> None:
        print(f"warning: {msg}", file=sys.stderr)

    diff = compute_diff(a_path, b_path, warn=_warn)
    json_path, md_path = write_diff(diff, out_dir)

    trials = diff["trials"]
    print("", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print("NSCLC snapshot diff", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"  A: {a_path}", file=sys.stderr)
    print(f"  B: {b_path}", file=sys.stderr)
    print(f"  trials added:   {len(trials['added'])}", file=sys.stderr)
    print(f"  trials removed: {len(trials['removed'])}", file=sys.stderr)
    print(f"  trials changed: {len(trials['changed'])}", file=sys.stderr)
    print(f"  wrote: {json_path}", file=sys.stderr)
    print(f"  wrote: {md_path}", file=sys.stderr)
    print("", file=sys.stderr)
    sys.stdout.write(str(out_dir) + "\n")


# ---------------------------------------------------------------------------
# Self-test: synthetic snapshots, no real data touched
# ---------------------------------------------------------------------------

def _write_synth_snapshot(
    root: Path,
    name: str,
    trials: Iterable[dict[str, Any]],
    summary: dict[str, Any],
    workflows: dict[str, dict[str, Any]],
) -> Path:
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "subset.jsonl", "w", encoding="utf-8") as fh:
        for rec in sorted(trials, key=lambda r: r["nct_id"]):
            fh.write(json.dumps(rec, sort_keys=True))
            fh.write("\n")
    with open(d / "subset_summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
        fh.write("\n")
    for wf_name, payload in workflows.items():
        with open(d / f"workflow_{wf_name}.json", "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")
    return d


def _selftest() -> None:
    """Exercise the differ against two synthetic snapshots."""
    import tempfile

    # Fixture trials.
    def t(nct_id: str, **kw: Any) -> dict[str, Any]:
        base = {
            "nct_id": nct_id,
            "title": f"Trial {nct_id}",
            "status": "RECRUITING",
            "phase": "PHASE2",
            "enrollment": 50,
            "completion_date": "2027-01",
            "modalities": ["chemotherapy"],
            "sponsor": {"name": "Sponsor X", "type": "OTHER"},
        }
        base.update(kw)
        return base

    a_trials = [
        t("NCT00000001"),  # unchanged
        t("NCT00000002", status="RECRUITING"),  # will change status
        t("NCT00000003"),  # will be removed in B
        t(
            "NCT00000004",
            modalities=["chemotherapy"],
            sponsor={"name": "Old Sponsor", "type": "INDUSTRY"},
        ),  # modalities + sponsor + enrollment change
    ]
    b_trials = [
        t("NCT00000001"),
        t("NCT00000002", status="ACTIVE_NOT_RECRUITING"),
        # NCT00000003 removed
        t(
            "NCT00000004",
            modalities=["chemotherapy", "immunotherapy"],
            sponsor={"name": "New Sponsor", "type": "INDUSTRY"},
            enrollment=75,
        ),
        t("NCT00000005"),  # added
    ]

    summary_a = {
        "today": "2026-04-10",
        "total_nsclc_trials": 4,
        "matched_by_mesh": 1,
        "matched_by_alias": 3,
        "modality_distribution": [["chemotherapy", 4]],
    }
    summary_b = {
        "today": "2026-04-12",
        "total_nsclc_trials": 4,
        "matched_by_mesh": 2,
        "matched_by_alias": 2,
        "modality_distribution": [["chemotherapy", 4], ["immunotherapy", 1]],
    }

    def wf(entries: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "workflow": "synthetic",
            "total_input_trials": 4,
            "total_matched": len(entries),
            "counts": {"main": len(entries)},
            "groups": {"main": entries},
        }

    workflows_a = {
        "fake_workflow": wf(
            [
                {"nct_id": "NCT00000001", "title": "Trial NCT00000001"},
                {"nct_id": "NCT00000003", "title": "Trial NCT00000003"},
            ]
        ),
        "only_in_a": wf([]),
    }
    workflows_b = {
        "fake_workflow": wf(
            [
                {"nct_id": "NCT00000001", "title": "Trial NCT00000001"},
                {"nct_id": "NCT00000005", "title": "Trial NCT00000005"},
            ]
        ),
        "only_in_b": wf([]),
    }

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        a = _write_synth_snapshot(
            root, "2026-04-10", a_trials, summary_a, workflows_a
        )
        b = _write_synth_snapshot(
            root, "2026-04-12", b_trials, summary_b, workflows_b
        )

        warnings: list[str] = []
        diff = compute_diff(a, b, warn=warnings.append)

        t_section = diff["trials"]
        added_ids = [x["nct_id"] for x in t_section["added"]]
        removed_ids = [x["nct_id"] for x in t_section["removed"]]
        changed_ids = [x["nct_id"] for x in t_section["changed"]]

        assert added_ids == ["NCT00000005"], added_ids
        assert removed_ids == ["NCT00000003"], removed_ids
        assert set(changed_ids) == {
            "NCT00000002",
            "NCT00000004",
        }, changed_ids

        by_id = {x["nct_id"]: x for x in t_section["changed"]}
        status_change = [
            c for c in by_id["NCT00000002"]["changes"]
            if c["field"] == "status"
        ]
        assert status_change and status_change[0]["before"] == "RECRUITING"
        assert status_change[0]["after"] == "ACTIVE_NOT_RECRUITING"

        four = {c["field"]: c for c in by_id["NCT00000004"]["changes"]}
        assert "modalities" in four
        assert four["modalities"]["added"] == ["immunotherapy"]
        assert four["modalities"]["removed"] == []
        assert four["sponsor.name"]["before"] == "Old Sponsor"
        assert four["sponsor.name"]["after"] == "New Sponsor"
        assert four["enrollment"]["before"] == 50
        assert four["enrollment"]["after"] == 75

        sd = diff["summary_delta"]
        assert sd["total_trials"]["delta"] == 0
        assert sd["matched_by_mesh"]["delta"] == 1
        assert sd["matched_by_alias"]["delta"] == -1
        assert sd["modality_distribution"]["immunotherapy"]["delta"] == 1

        wd = diff["workflow_deltas"]
        assert "fake_workflow" in wd
        assert "only_in_a" not in wd
        assert "only_in_b" not in wd
        assert wd["fake_workflow"]["trials_added"] == ["NCT00000005"]
        assert wd["fake_workflow"]["trials_removed"] == ["NCT00000003"]

        # Write files and spot-check the md output.
        out = root / "diff_out"
        json_path, md_path = write_diff(diff, out)
        assert json_path.exists()
        assert md_path.exists()
        md = md_path.read_text(encoding="utf-8")
        assert "Trials Added" in md
        assert "NCT00000005" in md
        assert "NCT00000003" in md
        assert "modalities" in md
        assert "immunotherapy" in md

        # Warnings should call out the schema-evolution workflows.
        joined = "\n".join(warnings)
        assert "only_in_a" in joined
        assert "only_in_b" in joined

    print("selftest OK", file=sys.stderr)


if __name__ == "__main__":
    main()
