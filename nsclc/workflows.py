"""Pre-built deterministic workflows over the NSCLC trial subset.

Each workflow is a pure function of the subset records produced by
:mod:`nsclc.build_subset`.  Workflows are declared in ``workflows.json``
(so the list is readable and editable without touching code) and executed
by :func:`run_workflow` here.  There are no LLM calls, no randomness, no
network access -- the same input always produces the same output.

The five workflows
------------------
1. ``weekly_update``        -- recently started or recently completed trials.
2. ``trial_radar``          -- active / recruiting trials across modalities.
3. ``egfr_brief``           -- trials using any EGFR-targeted drug.
4. ``immunotherapy_brief``  -- trials using PD-1 / PD-L1 / CTLA-4 agents.
5. ``radiotherapy_brief``   -- trials involving radiation therapy.

For each workflow, :func:`run_all_workflows` writes two files:
  - ``workflow_<name>.json`` -- structured result
  - ``workflow_<name>.md``   -- human-readable report

CLI
---
    python -m nsclc.workflows \
        --input /tmp/nsclc_subset.json \
        --output-dir data/nsclc_runs/<today>/

The ``--today YYYY-MM-DD`` flag pins date calculations (for reproducible
tests); it defaults to :func:`datetime.date.today`.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import click


# ---------------------------------------------------------------------------
# Workflow registry
# ---------------------------------------------------------------------------

_JSON_PATH = Path(__file__).with_name("workflows.json")

_workflows_cache: dict[str, dict[str, Any]] | None = None


def load_workflows() -> dict[str, dict[str, Any]]:
    """Return {workflow_name: workflow_def}, cached after first load."""
    global _workflows_cache  # noqa: PLW0603
    if _workflows_cache is None:
        with open(_JSON_PATH, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        _workflows_cache = {wf["name"]: wf for wf in raw["workflows"]}
    return _workflows_cache


def list_workflow_names() -> list[str]:
    """Return workflow names in declaration order."""
    return list(load_workflows().keys())


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

_PHASE_ORDER = {
    "PHASE4": 4,
    "PHASE3": 3,
    "PHASE2/PHASE3": 2.5,
    "PHASE2": 2,
    "PHASE1/PHASE2": 1.5,
    "PHASE1": 1,
    "EARLY_PHASE1": 0.5,
    "NA": 0,
}


def _phase_rank(phase: Any) -> float:
    """Map a phase label to a sortable number. Unknown values rank lowest."""
    if not phase:
        return -1.0
    key = str(phase).upper().replace(" ", "")
    return _PHASE_ORDER.get(key, -1.0)


def _parse_date(value: Any) -> _dt.date | None:
    """Parse an ISO ``YYYY-MM-DD`` (or ``YYYY-MM``, ``YYYY``) date.

    Returns ``None`` when the value is missing or unparseable -- callers
    use ``None`` to mean "no date", and exclude such rows from date-window
    filtering.
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Accept YYYY-MM-DD, YYYY-MM, YYYY.
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return _dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _enrollment(record: dict[str, Any]) -> int:
    """Best-effort integer enrollment; missing / non-numeric => 0."""
    value = record.get("enrollment")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _intervention_names(record: dict[str, Any]) -> list[str]:
    return [
        (iv.get("name") or "")
        for iv in record.get("interventions") or []
        if iv.get("name")
    ]


def _drug_names(record: dict[str, Any]) -> list[str]:
    return [
        (d.get("name") or "")
        for d in record.get("drugs") or []
        if d.get("name")
    ]


def _all_treatment_names(record: dict[str, Any]) -> list[str]:
    """Drug + intervention names, case preserved, deduplicated."""
    seen: set[str] = set()
    out: list[str] = []
    for name in _drug_names(record) + _intervention_names(record):
        low = name.lower()
        if low and low not in seen:
            seen.add(low)
            out.append(name)
    return out


def _truncate(text: str | None, length: int = 90) -> str:
    """Truncate a title for markdown tables; return ``-`` for empty values."""
    if not text:
        return "-"
    t = re.sub(r"\s+", " ", str(text)).strip()
    if len(t) <= length:
        return t
    return t[: length - 1].rstrip() + "…"


def _md_escape(value: Any) -> str:
    """Minimal escape so a cell value doesn't break a markdown table row."""
    if value is None:
        return "-"
    s = str(value).replace("|", "\\|")
    s = s.replace("\n", " ").replace("\r", " ")
    return s.strip() or "-"


# ---------------------------------------------------------------------------
# Workflow 1: weekly_update
# ---------------------------------------------------------------------------

def _run_weekly_update(
    trials: list[dict[str, Any]],
    definition: dict[str, Any],
    today: _dt.date,
) -> dict[str, Any]:
    window = definition.get("filters", {}).get("date_window_days", {})
    started_days = int(window.get("started", 365))
    completed_days = int(window.get("completed", 180))

    started_cutoff = today - _dt.timedelta(days=started_days)
    completed_cutoff = today - _dt.timedelta(days=completed_days)

    started: list[dict[str, Any]] = []
    completed: list[dict[str, Any]] = []

    for rec in trials:
        sd = _parse_date(rec.get("start_date"))
        cd = _parse_date(rec.get("completion_date"))
        if sd and started_cutoff <= sd <= today:
            started.append(rec)
        # A trial can in principle appear in both groups; that's fine.
        if cd and completed_cutoff <= cd <= today:
            completed.append(rec)

    started.sort(
        key=lambda r: (_parse_date(r.get("start_date")) or _dt.date.min, r.get("nct_id") or ""),
        reverse=True,
    )
    completed.sort(
        key=lambda r: (_parse_date(r.get("completion_date")) or _dt.date.min, r.get("nct_id") or ""),
        reverse=True,
    )

    def _summary_row(rec: dict[str, Any]) -> dict[str, Any]:
        return {
            "nct_id": rec.get("nct_id"),
            "title": rec.get("title"),
            "phase": rec.get("phase"),
            "status": rec.get("status"),
            "start_date": rec.get("start_date"),
            "completion_date": rec.get("completion_date"),
            "enrollment": rec.get("enrollment"),
            "sponsor": (rec.get("sponsor") or {}).get("name"),
            "modalities": rec.get("modalities") or [],
        }

    return {
        "workflow": "weekly_update",
        "description": definition.get("description", ""),
        "today": today.isoformat(),
        "parameters": {
            "started_window_days": started_days,
            "completed_window_days": completed_days,
            "started_cutoff": started_cutoff.isoformat(),
            "completed_cutoff": completed_cutoff.isoformat(),
        },
        "total_input_trials": len(trials),
        "counts": {
            "recently_started": len(started),
            "recently_completed": len(completed),
        },
        "groups": {
            "recently_started": [_summary_row(r) for r in started],
            "recently_completed": [_summary_row(r) for r in completed],
        },
    }


# ---------------------------------------------------------------------------
# Workflow 2: trial_radar
# ---------------------------------------------------------------------------

_RADAR_GROUPS = [
    "targeted_therapy",
    "immunotherapy",
    "chemotherapy",
    "radiotherapy",
    "antiangiogenic",
    "untagged",
]


def _run_trial_radar(
    trials: list[dict[str, Any]],
    definition: dict[str, Any],
    today: _dt.date,
) -> dict[str, Any]:
    wanted_statuses = {
        s.upper() for s in definition.get("filters", {}).get("status_in", [])
    }

    def _status(rec: dict[str, Any]) -> str:
        return str(rec.get("status") or "").upper()

    filtered = [r for r in trials if _status(r) in wanted_statuses]

    groups: dict[str, list[dict[str, Any]]] = {g: [] for g in _RADAR_GROUPS}
    for rec in filtered:
        mods = rec.get("modalities") or []
        if not mods:
            groups["untagged"].append(rec)
            continue
        for g in _RADAR_GROUPS:
            if g in mods:
                groups[g].append(rec)

    def _sort_key(rec: dict[str, Any]) -> tuple[float, int, str]:
        return (
            -_phase_rank(rec.get("phase")),
            -_enrollment(rec),
            rec.get("nct_id") or "",
        )

    def _summary_row(rec: dict[str, Any]) -> dict[str, Any]:
        return {
            "nct_id": rec.get("nct_id"),
            "title": rec.get("title"),
            "phase": rec.get("phase"),
            "status": rec.get("status"),
            "enrollment": rec.get("enrollment"),
            "start_date": rec.get("start_date"),
            "sponsor": (rec.get("sponsor") or {}).get("name"),
            "modalities": rec.get("modalities") or [],
        }

    grouped_out: dict[str, list[dict[str, Any]]] = {}
    counts: dict[str, int] = {}
    for g in _RADAR_GROUPS:
        rows = sorted(groups[g], key=_sort_key)
        grouped_out[g] = [_summary_row(r) for r in rows]
        counts[g] = len(rows)

    return {
        "workflow": "trial_radar",
        "description": definition.get("description", ""),
        "today": today.isoformat(),
        "parameters": {
            "status_in": sorted(wanted_statuses),
        },
        "total_input_trials": len(trials),
        "total_matched": len(filtered),
        "counts": counts,
        "groups": grouped_out,
    }


# ---------------------------------------------------------------------------
# Workflow 3: egfr_brief
# ---------------------------------------------------------------------------

def _egfr_drug_hits(
    rec: dict[str, Any], drug_list_lower: list[str]
) -> list[str]:
    """Return the EGFR drugs (from the configured list) found in this trial."""
    names = [n.lower() for n in _all_treatment_names(rec)]
    hits: list[str] = []
    for drug in drug_list_lower:
        if any(drug in n for n in names):
            hits.append(drug)
    return hits


def _run_egfr_brief(
    trials: list[dict[str, Any]],
    definition: dict[str, Any],
    today: _dt.date,
) -> dict[str, Any]:
    drug_list = list(
        definition.get("filters", {}).get("drug_name_any_of", [])
    )
    drug_list_lower = [d.lower() for d in drug_list]

    rows: list[dict[str, Any]] = []
    for rec in trials:
        hits = _egfr_drug_hits(rec, drug_list_lower)
        if not hits:
            continue
        rows.append(
            {
                "nct_id": rec.get("nct_id"),
                "title": rec.get("title"),
                "phase": rec.get("phase"),
                "status": rec.get("status"),
                "sponsor": (rec.get("sponsor") or {}).get("name"),
                "enrollment": rec.get("enrollment"),
                "start_date": rec.get("start_date"),
                "egfr_drugs_matched": hits,
                "modalities": rec.get("modalities") or [],
            }
        )

    rows.sort(
        key=lambda r: (
            -_phase_rank(r.get("phase")),
            -_enrollment_row(r),
            r.get("nct_id") or "",
        )
    )

    per_drug: dict[str, int] = {d: 0 for d in drug_list}
    for row in rows:
        for drug in row["egfr_drugs_matched"]:
            per_drug[drug] = per_drug.get(drug, 0) + 1

    return {
        "workflow": "egfr_brief",
        "description": definition.get("description", ""),
        "today": today.isoformat(),
        "parameters": {"drug_name_any_of": drug_list},
        "total_input_trials": len(trials),
        "total_matched": len(rows),
        "counts": {"egfr_trials": len(rows)},
        "per_drug_counts": per_drug,
        "groups": {"egfr_trials": rows},
    }


def _enrollment_row(row: dict[str, Any]) -> int:
    """Same as :func:`_enrollment` but for already-flattened summary rows."""
    value = row.get("enrollment")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Workflow 4: immunotherapy_brief
# ---------------------------------------------------------------------------

_IMMUNO_GROUPS = [
    "pembrolizumab",
    "nivolumab",
    "atezolizumab",
    "durvalumab",
    "ipilimumab",
]


def _run_immunotherapy_brief(
    trials: list[dict[str, Any]],
    definition: dict[str, Any],
    today: _dt.date,
) -> dict[str, Any]:
    modality = definition.get("filters", {}).get("modality", "immunotherapy")

    filtered = [
        r for r in trials if modality in (r.get("modalities") or [])
    ]

    groups: dict[str, list[dict[str, Any]]] = {
        g: [] for g in _IMMUNO_GROUPS
    }
    groups["others"] = []

    def _summary_row(rec: dict[str, Any], matched_drug: str) -> dict[str, Any]:
        interventions = rec.get("interventions") or []
        return {
            "nct_id": rec.get("nct_id"),
            "title": rec.get("title"),
            "phase": rec.get("phase"),
            "status": rec.get("status"),
            "enrollment": rec.get("enrollment"),
            "sponsor": (rec.get("sponsor") or {}).get("name"),
            "matched_drug": matched_drug,
            "combo_therapy": len(interventions) > 1,
            "intervention_count": len(interventions),
        }

    for rec in filtered:
        names_lower = [n.lower() for n in _all_treatment_names(rec)]
        assigned = False
        for drug in _IMMUNO_GROUPS:
            if any(drug in n for n in names_lower):
                groups[drug].append(_summary_row(rec, drug))
                assigned = True
                break  # put the trial in the first matching bucket
        if not assigned:
            groups["others"].append(_summary_row(rec, "other"))

    counts: dict[str, int] = {}
    ordered_groups: dict[str, list[dict[str, Any]]] = {}
    for g in _IMMUNO_GROUPS + ["others"]:
        rows = sorted(
            groups[g],
            key=lambda r: (
                -_phase_rank(r.get("phase")),
                -_enrollment_row(r),
                r.get("nct_id") or "",
            ),
        )
        ordered_groups[g] = rows
        counts[g] = len(rows)

    return {
        "workflow": "immunotherapy_brief",
        "description": definition.get("description", ""),
        "today": today.isoformat(),
        "parameters": {"modality": modality},
        "total_input_trials": len(trials),
        "total_matched": len(filtered),
        "counts": counts,
        "groups": ordered_groups,
    }


# ---------------------------------------------------------------------------
# Workflow 5: radiotherapy_brief
# ---------------------------------------------------------------------------

_RADIATION_TYPES = {
    "SBRT": [
        "sbrt",
        "stereotactic body",
        "stereotactic ablative",
        "sabr",
    ],
    "proton": ["proton"],
    "brachytherapy": ["brachytherapy"],
}

_SYSTEMIC_MODALITIES = {"chemotherapy", "immunotherapy", "targeted_therapy"}


def _classify_radiation(rec: dict[str, Any]) -> str:
    """Pick the most specific radiation type label for this trial."""
    names = [n.lower() for n in _intervention_names(rec) + _drug_names(rec)]
    for label in ("SBRT", "proton", "brachytherapy"):
        patterns = _RADIATION_TYPES[label]
        if any(p in n for n in names for p in patterns):
            return label
    return "other"


def _run_radiotherapy_brief(
    trials: list[dict[str, Any]],
    definition: dict[str, Any],
    today: _dt.date,
) -> dict[str, Any]:
    modality = definition.get("filters", {}).get("modality", "radiotherapy")
    filtered = [r for r in trials if modality in (r.get("modalities") or [])]

    groups: dict[str, list[dict[str, Any]]] = {
        "SBRT": [],
        "proton": [],
        "brachytherapy": [],
        "other": [],
    }

    def _summary_row(rec: dict[str, Any], rad_type: str) -> dict[str, Any]:
        mods = set(rec.get("modalities") or [])
        return {
            "nct_id": rec.get("nct_id"),
            "title": rec.get("title"),
            "phase": rec.get("phase"),
            "status": rec.get("status"),
            "enrollment": rec.get("enrollment"),
            "sponsor": (rec.get("sponsor") or {}).get("name"),
            "radiation_type": rad_type,
            "combo_with_systemic": bool(mods & _SYSTEMIC_MODALITIES),
            "modalities": sorted(mods),
        }

    for rec in filtered:
        rad = _classify_radiation(rec)
        groups[rad].append(_summary_row(rec, rad))

    counts: dict[str, int] = {}
    ordered: dict[str, list[dict[str, Any]]] = {}
    for g in ("SBRT", "proton", "brachytherapy", "other"):
        rows = sorted(
            groups[g],
            key=lambda r: (
                -_phase_rank(r.get("phase")),
                -_enrollment_row(r),
                r.get("nct_id") or "",
            ),
        )
        ordered[g] = rows
        counts[g] = len(rows)

    return {
        "workflow": "radiotherapy_brief",
        "description": definition.get("description", ""),
        "today": today.isoformat(),
        "parameters": {"modality": modality},
        "total_input_trials": len(trials),
        "total_matched": len(filtered),
        "counts": counts,
        "groups": ordered,
    }


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_RUNNERS = {
    "weekly_update": _run_weekly_update,
    "trial_radar": _run_trial_radar,
    "egfr_brief": _run_egfr_brief,
    "immunotherapy_brief": _run_immunotherapy_brief,
    "radiotherapy_brief": _run_radiotherapy_brief,
}


def run_workflow(
    workflow_name: str,
    trials: list[dict[str, Any]],
    today: _dt.date | None = None,
) -> dict[str, Any]:
    """Run one workflow by name and return its structured result."""
    definitions = load_workflows()
    if workflow_name not in definitions:
        raise ValueError(
            f"Unknown workflow: {workflow_name!r}. "
            f"Known: {sorted(definitions)}"
        )
    runner = _RUNNERS.get(workflow_name)
    if runner is None:
        raise ValueError(
            f"No runner registered for workflow {workflow_name!r}"
        )
    today = today or _dt.date.today()
    return runner(trials, definitions[workflow_name], today)


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _render_table(
    headers: list[str], rows: Iterable[list[Any]]
) -> list[str]:
    """Render a GitHub-flavoured markdown table as a list of lines."""
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    any_row = False
    for row in rows:
        any_row = True
        cells = [_md_escape(c) for c in row]
        lines.append("| " + " | ".join(cells) + " |")
    if not any_row:
        lines.append("| " + " | ".join(["_no trials match_"] * len(headers)) + " |")
    return lines


def _md_section_generic_rows(
    title: str, rows: list[dict[str, Any]], extra_cols: list[tuple[str, str]]
) -> list[str]:
    """Render a section with the standard columns + extras.

    ``extra_cols`` is a list of ``(header, record_key)`` tuples appended
    after the default columns.
    """
    headers = ["NCT ID", "Title", "Phase", "Status", "Sponsor"]
    headers += [c[0] for c in extra_cols]

    out = [f"### {title} ({len(rows)})", ""]
    table_rows = []
    for row in rows:
        base = [
            row.get("nct_id"),
            _truncate(row.get("title"), 80),
            row.get("phase"),
            row.get("status"),
            _truncate(row.get("sponsor"), 50),
        ]
        for _, key in extra_cols:
            val = row.get(key)
            if isinstance(val, list):
                val = ", ".join(str(v) for v in val) if val else "-"
            elif isinstance(val, bool):
                val = "yes" if val else "no"
            base.append(val)
        table_rows.append(base)
    out.extend(_render_table(headers, table_rows))
    out.append("")
    return out


def render_markdown(workflow_name: str, result: dict[str, Any]) -> str:
    """Render the structured result as human-readable markdown."""
    title = result.get("workflow", workflow_name).replace("_", " ").title()
    lines: list[str] = [
        f"# {title}",
        "",
        result.get("description", ""),
        "",
        f"_Run date: {result.get('today', '')}_",
        "",
        "## Summary",
        "",
        f"- Input trials: **{result.get('total_input_trials', 0)}**",
    ]

    if "total_matched" in result:
        lines.append(f"- Matched: **{result['total_matched']}**")

    counts = result.get("counts", {})
    if counts:
        lines.append("- Counts by group:")
        for g, n in counts.items():
            lines.append(f"  - `{g}`: {n}")

    if workflow_name == "weekly_update":
        p = result.get("parameters", {})
        lines += [
            f"- Started-within window: last **{p.get('started_window_days')}** days",
            f"  (cutoff {p.get('started_cutoff')})",
            f"- Completed-within window: last **{p.get('completed_window_days')}** days",
            f"  (cutoff {p.get('completed_cutoff')})",
        ]
    elif workflow_name == "trial_radar":
        statuses = result.get("parameters", {}).get("status_in", [])
        lines.append(f"- Statuses: {', '.join(statuses) or '(none)'}")
    elif workflow_name == "egfr_brief":
        per_drug = result.get("per_drug_counts", {})
        if per_drug:
            lines.append("- Trials per drug:")
            for drug, n in per_drug.items():
                lines.append(f"  - `{drug}`: {n}")

    lines.append("")
    lines.append("## Groups")
    lines.append("")

    groups = result.get("groups", {})

    if workflow_name == "weekly_update":
        lines += _md_section_generic_rows(
            "Recently Started",
            groups.get("recently_started", []),
            extra_cols=[("Start", "start_date"), ("Enrollment", "enrollment")],
        )
        lines += _md_section_generic_rows(
            "Recently Completed",
            groups.get("recently_completed", []),
            extra_cols=[
                ("Completion", "completion_date"),
                ("Enrollment", "enrollment"),
            ],
        )
    elif workflow_name == "trial_radar":
        for g in _RADAR_GROUPS:
            lines += _md_section_generic_rows(
                g.replace("_", " ").title(),
                groups.get(g, []),
                extra_cols=[
                    ("Enrollment", "enrollment"),
                    ("Start", "start_date"),
                ],
            )
    elif workflow_name == "egfr_brief":
        lines += _md_section_generic_rows(
            "EGFR Trials",
            groups.get("egfr_trials", []),
            extra_cols=[
                ("EGFR Drug(s)", "egfr_drugs_matched"),
                ("Enrollment", "enrollment"),
            ],
        )
    elif workflow_name == "immunotherapy_brief":
        for g in _IMMUNO_GROUPS + ["others"]:
            lines += _md_section_generic_rows(
                g.title(),
                groups.get(g, []),
                extra_cols=[
                    ("Enrollment", "enrollment"),
                    ("Combo", "combo_therapy"),
                ],
            )
    elif workflow_name == "radiotherapy_brief":
        for g in ("SBRT", "proton", "brachytherapy", "other"):
            lines += _md_section_generic_rows(
                g,
                groups.get(g, []),
                extra_cols=[
                    ("Enrollment", "enrollment"),
                    ("Combo+Systemic", "combo_with_systemic"),
                ],
            )
    else:
        # Unknown workflow: fall back to dumping group sizes only.
        for g, rows in groups.items():
            lines += _md_section_generic_rows(g, rows, extra_cols=[])

    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Run-all orchestration
# ---------------------------------------------------------------------------

def run_all_workflows(
    trials: list[dict[str, Any]],
    output_dir: str | os.PathLike[str],
    today: _dt.date | None = None,
) -> dict[str, Any]:
    """Run every registered workflow, write JSON + MD to ``output_dir``.

    Returns a summary mapping workflow name -> (path_json, path_md, counts).
    """
    today = today or _dt.date.today()
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "today": today.isoformat(),
        "output_dir": str(out_path),
        "total_input_trials": len(trials),
        "workflows": {},
    }

    for name in list_workflow_names():
        result = run_workflow(name, trials, today=today)
        json_path = out_path / f"workflow_{name}.json"
        md_path = out_path / f"workflow_{name}.md"
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, ensure_ascii=False, default=str)
        with open(md_path, "w", encoding="utf-8") as fh:
            fh.write(render_markdown(name, result))

        summary["workflows"][name] = {
            "json_path": str(json_path),
            "md_path": str(md_path),
            "total_matched": result.get(
                "total_matched", sum(result.get("counts", {}).values())
            ),
            "counts": result.get("counts", {}),
        }

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _load_trials_file(path: str) -> list[dict[str, Any]]:
    """Load the subset JSON written by ``build_subset.py``."""
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise click.ClickException(
            f"Expected a JSON list of trial records in {path}, got {type(data).__name__}"
        )
    return data


@click.command("workflows")
@click.option(
    "--input",
    "-i",
    "input_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Subset JSON file produced by `nsclc build-subset`.",
)
@click.option(
    "--output-dir",
    "-o",
    "output_dir",
    required=True,
    type=click.Path(file_okay=False, writable=True),
    help="Directory to write workflow_<name>.json and .md files into.",
)
@click.option(
    "--today",
    "today_str",
    type=str,
    default=None,
    help="Override today's date (YYYY-MM-DD) for reproducible runs.",
)
def main(input_path: str, output_dir: str, today_str: str | None) -> None:
    """Run all five NSCLC workflows against a subset JSON file."""
    if today_str:
        try:
            today = _dt.datetime.strptime(today_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise click.ClickException(
                f"--today must be YYYY-MM-DD, got {today_str!r}"
            ) from exc
    else:
        today = _dt.date.today()

    trials = _load_trials_file(input_path)
    summary = run_all_workflows(trials, output_dir, today=today)

    # Human-readable summary on stderr so stdout stays machine-parseable.
    print("", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print("NSCLC workflows summary", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"  today           {summary['today']}", file=sys.stderr)
    print(f"  input trials    {summary['total_input_trials']}", file=sys.stderr)
    print(f"  output dir      {summary['output_dir']}", file=sys.stderr)
    print("", file=sys.stderr)
    for name, info in summary["workflows"].items():
        matched = info.get("total_matched", 0)
        counts = info.get("counts", {})
        print(f"  {name}: matched={matched}", file=sys.stderr)
        for g, n in counts.items():
            print(f"      {g}: {n}", file=sys.stderr)
    print("", file=sys.stderr)

    # Also emit the summary as JSON on stdout for downstream consumption.
    sys.stdout.write(json.dumps(summary, indent=2, default=str))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
