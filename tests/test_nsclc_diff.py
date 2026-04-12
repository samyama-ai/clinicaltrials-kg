"""Tests for the snapshot differ (`nsclc.diff_snapshots`).

These use pytest's ``tmp_path`` to build two synthetic snapshot directories
on the fly — no live data, no network.  Each test is independent; each
builds its own fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pytest

from nsclc.diff_snapshots import compute_diff


# ---------------------------------------------------------------------------
# Fixture helpers (module-local; no shared state)
# ---------------------------------------------------------------------------

def _make_trial(nct_id: str, **overrides: Any) -> dict[str, Any]:
    """Return a minimal trial record in the shape the differ expects."""
    base: dict[str, Any] = {
        "nct_id": nct_id,
        "title": f"Trial {nct_id}",
        "status": "RECRUITING",
        "phase": "PHASE2",
        "enrollment": 50,
        "completion_date": "2027-01",
        "modalities": ["chemotherapy"],
        "sponsor": {"name": "Sponsor X", "type": "OTHER"},
    }
    base.update(overrides)
    return base


def _write_snapshot(
    root: Path,
    name: str,
    trials: Iterable[dict[str, Any]],
    *,
    summary: dict[str, Any] | None = None,
    workflows: dict[str, dict[str, Any]] | None = None,
) -> Path:
    """Write a minimal snapshot dir (subset.jsonl + optional summary/workflows)."""
    d = root / name
    d.mkdir(parents=True, exist_ok=True)

    with open(d / "subset.jsonl", "w", encoding="utf-8") as fh:
        for rec in sorted(trials, key=lambda r: r["nct_id"]):
            fh.write(json.dumps(rec, sort_keys=True))
            fh.write("\n")

    if summary is not None:
        with open(d / "subset_summary.json", "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

    if workflows:
        for wf_name, payload in workflows.items():
            with open(d / f"workflow_{wf_name}.json", "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)

    return d


def _workflow(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a minimal workflow JSON payload with a single 'main' group."""
    return {
        "workflow": "synthetic",
        "total_input_trials": len(entries),
        "total_matched": len(entries),
        "counts": {"main": len(entries)},
        "groups": {"main": entries},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_diff_detects_added_trial(tmp_path: Path):
    """B has one more trial than A -> one entry under trials.added."""
    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=[_make_trial("NCT00000001"), _make_trial("NCT00000002")],
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=[
            _make_trial("NCT00000001"),
            _make_trial("NCT00000002"),
            _make_trial("NCT00000003"),
        ],
    )

    diff = compute_diff(a, b)
    added_ids = [x["nct_id"] for x in diff["trials"]["added"]]
    removed_ids = [x["nct_id"] for x in diff["trials"]["removed"]]

    assert added_ids == ["NCT00000003"]
    assert removed_ids == []


def test_diff_detects_removed_trial(tmp_path: Path):
    """A has a trial not in B -> one entry under trials.removed."""
    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=[
            _make_trial("NCT00000001"),
            _make_trial("NCT00000002"),
            _make_trial("NCT00000003"),
        ],
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=[_make_trial("NCT00000001"), _make_trial("NCT00000002")],
    )

    diff = compute_diff(a, b)
    added_ids = [x["nct_id"] for x in diff["trials"]["added"]]
    removed_ids = [x["nct_id"] for x in diff["trials"]["removed"]]

    assert added_ids == []
    assert removed_ids == ["NCT00000003"]


def test_diff_detects_status_change(tmp_path: Path):
    """Same nct_id, status RECRUITING -> COMPLETED, surfaces as a field change."""
    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=[_make_trial("NCT00000001", status="RECRUITING")],
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=[_make_trial("NCT00000001", status="COMPLETED")],
    )

    diff = compute_diff(a, b)
    changed = diff["trials"]["changed"]
    assert len(changed) == 1
    assert changed[0]["nct_id"] == "NCT00000001"

    status_changes = [c for c in changed[0]["changes"] if c["field"] == "status"]
    assert len(status_changes) == 1
    assert status_changes[0]["before"] == "RECRUITING"
    assert status_changes[0]["after"] == "COMPLETED"


def test_diff_detects_modality_added_removed(tmp_path: Path):
    """modalities list diff reports added/removed members as a list change."""
    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=[_make_trial("NCT00000001", modalities=["chemotherapy"])],
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=[
            _make_trial(
                "NCT00000001", modalities=["chemotherapy", "immunotherapy"]
            )
        ],
    )

    diff = compute_diff(a, b)
    changed = diff["trials"]["changed"]
    assert len(changed) == 1

    modality_changes = [
        c for c in changed[0]["changes"] if c["field"] == "modalities"
    ]
    assert len(modality_changes) == 1
    assert modality_changes[0]["added"] == ["immunotherapy"]
    assert modality_changes[0]["removed"] == []


def test_diff_handles_identical_snapshots(tmp_path: Path):
    """A == B -> added/removed/changed are all empty."""
    trials = [_make_trial("NCT00000001"), _make_trial("NCT00000002")]
    a = _write_snapshot(tmp_path, "snap_a", trials=trials)
    b = _write_snapshot(tmp_path, "snap_b", trials=trials)

    diff = compute_diff(a, b)
    assert diff["trials"]["added"] == []
    assert diff["trials"]["removed"] == []
    assert diff["trials"]["changed"] == []
    # Workflow deltas dict is empty because no workflow_*.json files were
    # written into either snapshot.
    assert diff["workflow_deltas"] == {}


def test_diff_workflow_deltas(tmp_path: Path):
    """Workflow JSONs in both snapshots -> per-workflow added/removed detection."""
    trials_a = [_make_trial("NCT00000001"), _make_trial("NCT00000002")]
    trials_b = [_make_trial("NCT00000001"), _make_trial("NCT00000003")]

    wf_a = _workflow(
        [
            {"nct_id": "NCT00000001", "title": "Trial NCT00000001"},
            {"nct_id": "NCT00000002", "title": "Trial NCT00000002"},
        ]
    )
    wf_b = _workflow(
        [
            {"nct_id": "NCT00000001", "title": "Trial NCT00000001"},
            {"nct_id": "NCT00000003", "title": "Trial NCT00000003"},
        ]
    )

    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=trials_a,
        workflows={"trial_radar": wf_a},
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=trials_b,
        workflows={"trial_radar": wf_b},
    )

    diff = compute_diff(a, b)
    wd = diff["workflow_deltas"]
    assert "trial_radar" in wd
    assert wd["trial_radar"]["trials_added"] == ["NCT00000003"]
    assert wd["trial_radar"]["trials_removed"] == ["NCT00000002"]
    # Per-group tracking should mirror the same add/remove.
    per_group = wd["trial_radar"]["per_group"]
    assert per_group["main"]["added"] == ["NCT00000003"]
    assert per_group["main"]["removed"] == ["NCT00000002"]


def test_diff_sponsor_name_change(tmp_path: Path):
    """A change to sponsor.name surfaces as a 'sponsor.name' field change."""
    a = _write_snapshot(
        tmp_path, "snap_a",
        trials=[
            _make_trial(
                "NCT00000001",
                sponsor={"name": "Old Sponsor", "type": "INDUSTRY"},
            )
        ],
    )
    b = _write_snapshot(
        tmp_path, "snap_b",
        trials=[
            _make_trial(
                "NCT00000001",
                sponsor={"name": "New Sponsor", "type": "INDUSTRY"},
            )
        ],
    )

    diff = compute_diff(a, b)
    changed = diff["trials"]["changed"]
    assert len(changed) == 1
    sponsor_changes = [
        c for c in changed[0]["changes"] if c["field"] == "sponsor.name"
    ]
    assert len(sponsor_changes) == 1
    assert sponsor_changes[0]["before"] == "Old Sponsor"
    assert sponsor_changes[0]["after"] == "New Sponsor"
