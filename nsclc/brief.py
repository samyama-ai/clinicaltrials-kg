"""Consolidated markdown "radar brief" generator.

Given a snapshot directory produced by :mod:`nsclc.run`, assemble a single
``brief.md`` that pulls together the seven sections a clinical/science
reader cares about:

1. Overview
2. New trials  (vs prior snapshot, if provided)
3. Updated trials  (diff: status / phase / enrollment / modalities)
4. Targeted therapy  (egfr_brief workflow)
5. Immunotherapy  (immunotherapy_brief workflow)
6. Radiotherapy  (radiotherapy_brief workflow)
7. Notes / caveats

Deterministic, no LLM, no network.  Re-running on the same snapshot (with
the same prior) always yields byte-identical ``brief.md``.

Public API
----------
``generate_brief(snapshot_dir, prior_snapshot_dir=None) -> str``
    Return the brief as a markdown string.

``write_brief(snapshot_dir, prior_snapshot_dir=None) -> Path``
    Write ``brief.md`` into ``snapshot_dir`` and return its path.

CLI
---
    python -m nsclc brief <snapshot_dir>
    python -m nsclc brief <snapshot_dir> --prior <prior_snapshot_dir>
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from nsclc.diff_snapshots import compute_diff


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Active-status filter (required by spec).
_ACTIVE_STATUSES: tuple[str, ...] = (
    "RECRUITING",
    "ACTIVE_NOT_RECRUITING",
    "NOT_YET_RECRUITING",
)

# Same ranking we use in workflows.
_PHASE_ORDER: dict[str, float] = {
    "PHASE4": 4,
    "PHASE3": 3,
    "PHASE2/PHASE3": 2.5,
    "PHASE2": 2,
    "PHASE1/PHASE2": 1.5,
    "PHASE1": 1,
    "EARLY_PHASE1": 0.5,
    "NA": 0,
}

# The six modality tags in the order we want to report "why relevant".
_MODALITY_ORDER: tuple[str, ...] = (
    "targeted_therapy",
    "immunotherapy",
    "chemotherapy",
    "radiotherapy",
    "antiangiogenic",
    "hormonal_therapy",
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _phase_rank(phase: Any) -> float:
    if not phase:
        return -1.0
    key = str(phase).upper().replace(" ", "")
    return _PHASE_ORDER.get(key, -1.0)


def _enrollment(row: dict[str, Any]) -> int:
    value = row.get("enrollment")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _truncate(text: Any, length: int = 80) -> str:
    if text is None:
        return "-"
    s = " ".join(str(text).split())
    if not s:
        return "-"
    if len(s) <= length:
        return s
    return s[: length - 1].rstrip() + "…"


def _md_escape(value: Any) -> str:
    if value is None:
        return "-"
    s = str(value).replace("|", "\\|")
    s = s.replace("\n", " ").replace("\r", " ")
    return s.strip() or "-"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not path.exists():
        return out
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _sponsor_name(rec: dict[str, Any]) -> str | None:
    sponsor = rec.get("sponsor")
    if isinstance(sponsor, dict):
        return sponsor.get("name")
    if isinstance(sponsor, str):
        return sponsor
    return None


def _pairs_to_dict(pairs: Any) -> dict[str, int]:
    if isinstance(pairs, dict):
        return {str(k): int(v) for k, v in pairs.items()}
    out: dict[str, int] = {}
    if isinstance(pairs, list):
        for item in pairs:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out[str(item[0])] = int(item[1])
    return out


# ---------------------------------------------------------------------------
# Table rendering (consistent across sections)
# ---------------------------------------------------------------------------

_TABLE_HEADERS: tuple[str, ...] = (
    "NCT ID", "Title", "Phase", "Status", "Sponsor",
)


def _render_trial_table(rows: list[dict[str, Any]]) -> list[str]:
    """Render the canonical (nct_id | title-80 | phase | status | sponsor) table.

    Accepts both workflow-style dicts (with ``sponsor`` as a string) and
    subset-style dicts (with ``sponsor`` as ``{"name": ...}``).
    """
    lines = [
        "| " + " | ".join(_TABLE_HEADERS) + " |",
        "| " + " | ".join(["---"] * len(_TABLE_HEADERS)) + " |",
    ]
    if not rows:
        lines.append(
            "| " + " | ".join(["_no active trials_"] * len(_TABLE_HEADERS)) + " |"
        )
        return lines
    for row in rows:
        sponsor = row.get("sponsor")
        if isinstance(sponsor, dict):
            sponsor = sponsor.get("name")
        cells = [
            _md_escape(row.get("nct_id")),
            _md_escape(_truncate(row.get("title"), 80)),
            _md_escape(row.get("phase")),
            _md_escape(row.get("status")),
            _md_escape(_truncate(sponsor, 50)),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _top_active(
    rows: list[dict[str, Any]], limit: int = 3
) -> list[dict[str, Any]]:
    """Filter to active statuses, sort by phase DESC then enrollment DESC."""
    active = [
        r for r in rows
        if str(r.get("status") or "").upper() in _ACTIVE_STATUSES
    ]
    active.sort(
        key=lambda r: (
            -_phase_rank(r.get("phase")),
            -_enrollment(r),
            str(r.get("nct_id") or ""),
        )
    )
    return active[:limit]


def _why_relevant(rec: dict[str, Any]) -> str:
    """First matching modality (from a canonical order) or ``untagged``."""
    mods = set(rec.get("modalities") or [])
    for m in _MODALITY_ORDER:
        if m in mods:
            return m
    # If the record has modalities not in our canonical order, report the
    # first alphabetically-sorted one so the label is still informative.
    if mods:
        return sorted(mods)[0]
    return "untagged"


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_overview(
    summary: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    lines: list[str] = ["## Overview", ""]

    date = metadata.get("today") or summary.get("today") or "(unknown)"
    total = summary.get("total_nsclc_trials", 0)
    matched_by_mesh = int(summary.get("matched_by_mesh") or 0)
    matched_by_alias = int(summary.get("matched_by_alias") or 0)
    conds = summary.get("search_conditions") or []
    max_trials = summary.get("max_trials", "-")

    lines.append(f"- **Run date**: {date}")
    lines.append(
        f"- **Search conditions**: {', '.join(conds) if conds else '-'}  "
        f"(max_trials={max_trials})"
    )
    lines.append(f"- **Total NSCLC trials in scope**: {total}")
    lines.append(
        f"- **Matched by MeSH**: {matched_by_mesh} / "
        f"**Matched by alias**: {matched_by_alias}"
    )

    mod_dist = _pairs_to_dict(summary.get("modality_distribution") or [])
    lines.append("")
    lines.append("**Modality distribution** (top-level tags, not mutually exclusive):")
    lines.append("")
    if not mod_dist:
        lines.append("_No modality data._")
    else:
        lines.append("| modality | trials |")
        lines.append("| --- | ---: |")
        for name, n in sorted(
            mod_dist.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            lines.append(f"| {name} | {n} |")
    lines.append("")
    return lines


def _section_new_trials(
    snapshot_dir: Path,
    diff: dict[str, Any] | None,
    weekly_update: dict[str, Any],
    subset_records: dict[str, dict[str, Any]],
) -> list[str]:
    lines: list[str] = ["## New trials", ""]

    if diff is not None:
        added = diff.get("trials", {}).get("added") or []
        if not added:
            lines.append(
                "_No new trials vs prior snapshot._"
            )
            lines.append("")
            return lines
        lines.append(
            f"Trials in this snapshot but not in the prior snapshot "
            f"({len(added)})."
        )
        lines.append("")
        headers = [
            "NCT ID", "Title", "Sponsor", "Phase", "Status", "Why relevant",
        ]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for entry in added:
            nct_id = entry.get("nct_id")
            rec = subset_records.get(nct_id or "", {})
            sponsor = _sponsor_name(rec) or "-"
            cells = [
                _md_escape(nct_id),
                _md_escape(_truncate(entry.get("title") or rec.get("title"), 80)),
                _md_escape(_truncate(sponsor, 50)),
                _md_escape(rec.get("phase")),
                _md_escape(rec.get("status")),
                _md_escape(_why_relevant(rec)),
            ]
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
        return lines

    # No prior snapshot — fall back to weekly_update recently_started.
    started = (
        weekly_update.get("groups", {}).get("recently_started") or []
    )
    lines.append(
        "_No prior snapshot provided; showing the `weekly_update` "
        "`recently_started` group as a proxy._"
    )
    lines.append("")
    if not started:
        lines.append("_No recently-started trials in the current window._")
        lines.append("")
        return lines
    headers = [
        "NCT ID", "Title", "Sponsor", "Phase", "Status", "Why relevant",
    ]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in started:
        # Re-derive "why relevant" from the full subset record if we have it.
        rec = subset_records.get(row.get("nct_id") or "", {})
        mods_source = rec if rec else row
        cells = [
            _md_escape(row.get("nct_id")),
            _md_escape(_truncate(row.get("title"), 80)),
            _md_escape(_truncate(row.get("sponsor"), 50)),
            _md_escape(row.get("phase")),
            _md_escape(row.get("status")),
            _md_escape(_why_relevant(mods_source)),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def _fmt_change(change: dict[str, Any]) -> str:
    field = change.get("field", "?")
    if "added" in change or "removed" in change:
        added = change.get("added") or []
        removed = change.get("removed") or []
        parts = []
        if added:
            parts.append(f"+{added}")
        if removed:
            parts.append(f"-{removed}")
        return f"{field}: {'; '.join(parts) if parts else '(no-op)'}"
    return f"{field}: {change.get('before')!r} -> {change.get('after')!r}"


def _section_updated_trials(
    diff: dict[str, Any] | None,
) -> list[str]:
    lines: list[str] = ["## Updated trials", ""]
    if diff is None:
        lines.append("_No prior snapshot provided._")
        lines.append("")
        return lines

    # Only surface status / phase / enrollment / modality changes (per spec).
    interesting = {"status", "phase", "enrollment", "modalities"}

    changed_entries = diff.get("trials", {}).get("changed") or []
    rows: list[tuple[str, str, list[dict[str, Any]]]] = []
    for entry in changed_entries:
        filtered = [
            c for c in entry.get("changes") or []
            if c.get("field") in interesting
        ]
        if filtered:
            rows.append(
                (entry.get("nct_id") or "", entry.get("title") or "", filtered)
            )

    if not rows:
        lines.append(
            "_No field-level changes in status / phase / enrollment / "
            "modalities between snapshots._"
        )
        lines.append("")
        return lines

    lines.append(
        f"Trials with changes in status / phase / enrollment / modalities "
        f"({len(rows)})."
    )
    lines.append("")
    for nct_id, title, changes in rows:
        lines.append(f"- `{nct_id}` — {_truncate(title, 80)}")
        for ch in changes:
            lines.append(f"    - {_fmt_change(ch)}")
    lines.append("")
    return lines


def _section_targeted_therapy(egfr: dict[str, Any]) -> list[str]:
    lines: list[str] = ["## Targeted therapy", ""]

    if not egfr:
        lines.append("_No egfr_brief workflow output._")
        lines.append("")
        return lines

    drugs_tested = egfr.get("parameters", {}).get("drug_name_any_of") or []
    per_drug = egfr.get("per_drug_counts") or {}
    total = egfr.get("total_matched", 0)

    lines.append(
        f"- **Drugs tested (vocabulary)**: "
        + (", ".join(f"`{d}`" for d in drugs_tested) if drugs_tested else "-")
    )
    lines.append(f"- **EGFR-drug trials matched**: {total}")
    lines.append("")
    lines.append("**Trials per drug:**")
    lines.append("")
    if not per_drug:
        lines.append("_No per-drug data._")
    else:
        lines.append("| drug | trials |")
        lines.append("| --- | ---: |")
        for drug, n in sorted(
            per_drug.items(), key=lambda kv: (-int(kv[1]), kv[0])
        ):
            lines.append(f"| {drug} | {n} |")
    lines.append("")

    rows = egfr.get("groups", {}).get("egfr_trials") or []
    top = _top_active(rows, limit=3)
    lines.append(f"**Top {len(top)} active trials** "
                 "(filter: RECRUITING / ACTIVE_NOT_RECRUITING / NOT_YET_RECRUITING; "
                 "sort: phase DESC, enrollment DESC):")
    lines.append("")
    lines.extend(_render_trial_table(top))
    lines.append("")
    return lines


def _section_immunotherapy(immuno: dict[str, Any]) -> list[str]:
    lines: list[str] = ["## Immunotherapy", ""]
    if not immuno:
        lines.append("_No immunotherapy_brief workflow output._")
        lines.append("")
        return lines

    total = immuno.get("total_matched", 0)
    counts = immuno.get("counts") or {}

    lines.append(f"- **Immunotherapy trials matched**: {total}")
    lines.append("")
    lines.append("**Per-drug counts:**")
    lines.append("")
    if not counts:
        lines.append("_No per-drug data._")
    else:
        lines.append("| drug | trials |")
        lines.append("| --- | ---: |")
        for drug, n in counts.items():
            lines.append(f"| {drug} | {n} |")
    lines.append("")

    # Combo-therapy breakdown across all immuno groups.
    all_rows: list[dict[str, Any]] = []
    for rows in (immuno.get("groups") or {}).values():
        if isinstance(rows, list):
            all_rows.extend(rows)
    combo = sum(1 for r in all_rows if r.get("combo_therapy"))
    mono = len(all_rows) - combo
    lines.append(
        f"- **Combo therapy breakdown**: "
        f"combo={combo}, monotherapy={mono}"
    )
    lines.append("")

    top = _top_active(all_rows, limit=3)
    lines.append(f"**Top {len(top)} active trials:**")
    lines.append("")
    lines.extend(_render_trial_table(top))
    lines.append("")
    return lines


def _section_radiotherapy(radio: dict[str, Any]) -> list[str]:
    lines: list[str] = ["## Radiotherapy", ""]
    if not radio:
        lines.append("_No radiotherapy_brief workflow output._")
        lines.append("")
        return lines

    total = radio.get("total_matched", 0)
    counts = radio.get("counts") or {}
    lines.append(f"- **Radiotherapy trials matched**: {total}")
    lines.append("")
    lines.append("**Radiation-type breakdown:**")
    lines.append("")
    if not counts:
        lines.append("_No type data._")
    else:
        lines.append("| type | trials |")
        lines.append("| --- | ---: |")
        for name in ("SBRT", "proton", "brachytherapy", "other"):
            lines.append(f"| {name} | {int(counts.get(name, 0))} |")
    lines.append("")

    all_rows: list[dict[str, Any]] = []
    for rows in (radio.get("groups") or {}).values():
        if isinstance(rows, list):
            all_rows.extend(rows)
    combo_systemic = sum(1 for r in all_rows if r.get("combo_with_systemic"))
    lines.append(
        f"- **Combo-with-systemic count**: {combo_systemic} of {len(all_rows)}"
    )
    lines.append("")

    top = _top_active(all_rows, limit=3)
    lines.append(f"**Top {len(top)} active trials:**")
    lines.append("")
    lines.extend(_render_trial_table(top))
    lines.append("")
    return lines


def _section_notes(
    summary: dict[str, Any],
    metadata: dict[str, Any],
    graph_stats: dict[str, Any],
) -> list[str]:
    lines: list[str] = ["## Notes / caveats", ""]

    max_trials = summary.get("max_trials", "-")
    conds = summary.get("search_conditions") or []
    lines.append(
        f"- **Data source**: ClinicalTrials.gov API, sample of up to "
        f"{max_trials} results for query "
        + (", ".join(f"`{c}`" for c in conds) if conds else "(unknown)")
        + "."
    )

    nodes_by_label = graph_stats.get("nodes_by_label") or {}
    mesh_count = int(nodes_by_label.get("MeSHDescriptor", 0) or 0)
    drug_count = int(nodes_by_label.get("Drug", 0) or 0)
    mesh_loaded = "yes" if mesh_count > 0 else "no"
    atc_loaded = "yes" if drug_count > 0 else "no"
    lines.append(
        f"- **MeSH enrichment loaded**: {mesh_loaded} "
        f"(MeSHDescriptor nodes: {mesh_count})"
    )
    lines.append(
        f"- **Drug / ATC codes loaded**: {atc_loaded} "
        f"(Drug nodes: {drug_count})"
    )
    mbm = int(summary.get("matched_by_mesh") or 0)
    if mbm == 0:
        lines.append(
            "- **Known limitation**: 0 trials matched by MeSH — all NSCLC "
            "identification fell back to condition-name alias matching."
        )

    commit = metadata.get("commit_sha") or "unknown"
    py = metadata.get("python_version") or "unknown"
    sam = metadata.get("samyama_version") or "unknown"
    ts = metadata.get("timestamp_utc") or "-"
    lines.append(
        f"- **Reproducibility**: commit `{commit}`, "
        f"python {py}, samyama {sam}, generated at {ts}."
    )
    lines.append(
        "- This is a deterministic extract — no LLM summarization."
    )
    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _load_all(snapshot_dir: Path) -> dict[str, Any]:
    """Read every artifact the brief needs from a snapshot dir."""
    subset = _read_jsonl(snapshot_dir / "subset.jsonl")
    subset_index = {
        rec["nct_id"]: rec for rec in subset if rec.get("nct_id")
    }
    summary = _read_json(snapshot_dir / "subset_summary.json")
    metadata = _read_json(snapshot_dir / "run_metadata.json")
    return {
        "subset_index": subset_index,
        "summary": summary,
        "metadata": metadata,
        "graph_stats": metadata.get("graph_stats") or {},
        "weekly_update": _read_json(
            snapshot_dir / "workflow_weekly_update.json"
        ),
        "egfr": _read_json(snapshot_dir / "workflow_egfr_brief.json"),
        "immuno": _read_json(
            snapshot_dir / "workflow_immunotherapy_brief.json"
        ),
        "radio": _read_json(
            snapshot_dir / "workflow_radiotherapy_brief.json"
        ),
    }


def generate_brief(
    snapshot_dir: str | Path,
    prior_snapshot_dir: str | Path | None = None,
) -> str:
    """Assemble the full brief as a single markdown string."""
    snap = Path(snapshot_dir)
    if not snap.exists():
        raise FileNotFoundError(f"snapshot dir not found: {snap}")
    if not (snap / "subset.jsonl").exists():
        raise FileNotFoundError(
            f"snapshot dir is missing subset.jsonl: {snap}"
        )

    bundle = _load_all(snap)

    diff: dict[str, Any] | None = None
    prior_path: Path | None = None
    if prior_snapshot_dir is not None:
        prior_path = Path(prior_snapshot_dir)
        if not prior_path.exists():
            raise FileNotFoundError(
                f"prior snapshot dir not found: {prior_path}"
            )
        # Compute A (prior) -> B (current).  Warnings silenced — the diff is
        # advisory in this context.
        diff = compute_diff(prior_path, snap, warn=lambda _m: None)

    # Header.
    lines: list[str] = []
    lines.append("# NSCLC Evidence Radar — Brief")
    lines.append("")
    date = (
        bundle["metadata"].get("today")
        or bundle["summary"].get("today")
        or "(unknown)"
    )
    lines.append(f"_Snapshot: `{snap.name}` (date {date})_")
    if prior_path is not None:
        lines.append(f"_Prior snapshot: `{prior_path.name}` (for diff context)_")
    lines.append("")

    # Section 1.
    lines.extend(_section_overview(bundle["summary"], bundle["metadata"]))

    # Section 2.
    lines.extend(
        _section_new_trials(
            snap, diff, bundle["weekly_update"], bundle["subset_index"]
        )
    )

    # Section 3.
    lines.extend(_section_updated_trials(diff))

    # Section 4.
    lines.extend(_section_targeted_therapy(bundle["egfr"]))

    # Section 5.
    lines.extend(_section_immunotherapy(bundle["immuno"]))

    # Section 6.
    lines.extend(_section_radiotherapy(bundle["radio"]))

    # Section 7.
    lines.extend(
        _section_notes(
            bundle["summary"], bundle["metadata"], bundle["graph_stats"]
        )
    )

    return "\n".join(lines).rstrip() + "\n"


def write_brief(
    snapshot_dir: str | Path,
    prior_snapshot_dir: str | Path | None = None,
) -> Path:
    """Generate and write ``brief.md`` into ``snapshot_dir``."""
    snap = Path(snapshot_dir)
    md = generate_brief(snap, prior_snapshot_dir=prior_snapshot_dir)
    out = snap / "brief.md"
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(md)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command("brief")
@click.argument(
    "snapshot_dir",
    type=click.Path(exists=True, file_okay=False, readable=True),
)
@click.option(
    "--prior",
    "prior_snapshot_dir",
    type=click.Path(exists=True, file_okay=False, readable=True),
    default=None,
    help="Prior snapshot directory for diff context "
         "(enables 'New trials' and 'Updated trials' sections).",
)
def main(snapshot_dir: str, prior_snapshot_dir: str | None) -> None:
    """Write a consolidated brief.md into the snapshot dir."""
    out = write_brief(snapshot_dir, prior_snapshot_dir=prior_snapshot_dir)
    click.echo(str(out))


if __name__ == "__main__":
    main()
