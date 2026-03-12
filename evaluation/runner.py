"""Generic benchmark runner utilities for clinical trials KG evaluation.

Provides:
- load_scenarios(scenario_dir)  -- load all JSON scenario files from a directory
- format_summary_table(results) -- rich table output for terminal display
- results_to_json(results, output_path) -- save results as JSON report
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .criteria import ScenarioResult


# ---------------------------------------------------------------------------
# Scenario loading
# ---------------------------------------------------------------------------

def load_scenarios(scenario_dir: str | Path) -> list[dict[str, Any]]:
    """Load all JSON scenario files from a directory.

    Each file should contain a JSON array of scenario objects. Files are sorted
    alphabetically so scenario ordering is deterministic.

    Args:
        scenario_dir: Path to directory containing scenario JSON files.

    Returns:
        Flat list of all scenario dicts across all files.
    """
    scenario_dir = Path(scenario_dir)
    if not scenario_dir.is_dir():
        raise FileNotFoundError(f"Scenario directory not found: {scenario_dir}")

    all_scenarios: list[dict[str, Any]] = []
    for json_file in sorted(scenario_dir.glob("*.json")):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array in {json_file}, got {type(data).__name__}")
        for scenario in data:
            # Tag with source file for traceability
            scenario["_source_file"] = json_file.name
        all_scenarios.extend(data)

    return all_scenarios


# ---------------------------------------------------------------------------
# Summary table formatting
# ---------------------------------------------------------------------------

def format_summary_table(results: list[ScenarioResult]) -> str:
    """Format evaluation results as a plain-text summary table.

    If the ``rich`` library is available, uses rich Table for coloured terminal
    output. Otherwise falls back to a simple fixed-width text table.

    Args:
        results: List of ScenarioResult objects from evaluation runs.

    Returns:
        Formatted string suitable for printing to the terminal.
    """
    try:
        return _rich_table(results)
    except ImportError:
        return _plain_table(results)


def _rich_table(results: list[ScenarioResult]) -> str:
    """Build a rich Table and render it to a string."""
    from io import StringIO

    from rich.console import Console
    from rich.table import Table

    table = Table(title="Clinical Trials KG Benchmark Results", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Category", style="magenta")
    table.add_column("Difficulty")
    table.add_column("Pass", justify="center")
    table.add_column("Score", justify="right")
    table.add_column("Latency", justify="right")

    for r in results:
        pass_str = "[green]PASS[/green]" if r.passed else "[red]FAIL[/red]"
        table.add_row(
            r.scenario_id,
            r.category,
            r.difficulty,
            pass_str,
            f"{r.overall_score:.2f}",
            f"{r.latency_ms:.0f}ms",
        )

    # Category summary rows
    by_cat = _group_by_category(results)
    table.add_section()
    for cat, cat_results in sorted(by_cat.items()):
        avg = sum(r.overall_score for r in cat_results) / len(cat_results)
        pass_count = sum(1 for r in cat_results if r.passed)
        table.add_row(
            "",
            f"[bold]{cat}[/bold]",
            "",
            f"{pass_count}/{len(cat_results)}",
            f"[bold]{avg:.2f}[/bold]",
            "",
        )

    # Overall summary
    total_pass = sum(1 for r in results if r.passed)
    overall_avg = sum(r.overall_score for r in results) / len(results) if results else 0.0
    table.add_section()
    table.add_row(
        "",
        "[bold]OVERALL[/bold]",
        "",
        f"[bold]{total_pass}/{len(results)}[/bold]",
        f"[bold]{overall_avg:.2f}[/bold]",
        "",
    )

    buf = StringIO()
    console = Console(file=buf, force_terminal=False, width=120)
    console.print(table)
    return buf.getvalue()


def _plain_table(results: list[ScenarioResult]) -> str:
    """Fallback fixed-width text table when rich is not installed."""
    lines = [
        f"{'ID':<30} {'Category':<25} {'Diff':<8} {'Pass':<6} {'Score':<7} {'Latency':<10}",
        "-" * 90,
    ]
    for r in results:
        lines.append(
            f"{r.scenario_id:<30} {r.category:<25} {r.difficulty:<8} "
            f"{'PASS' if r.passed else 'FAIL':<6} {r.overall_score:<7.2f} {r.latency_ms:<10.0f}ms"
        )

    lines.append("-" * 90)
    by_cat = _group_by_category(results)
    for cat, cat_results in sorted(by_cat.items()):
        avg = sum(r.overall_score for r in cat_results) / len(cat_results)
        pass_count = sum(1 for r in cat_results if r.passed)
        lines.append(f"{'':30} {cat:<25} {'':8} {pass_count}/{len(cat_results):<4} {avg:<7.2f}")

    total_pass = sum(1 for r in results if r.passed)
    overall_avg = sum(r.overall_score for r in results) / len(results) if results else 0.0
    lines.append("-" * 90)
    lines.append(f"{'':30} {'OVERALL':<25} {'':8} {total_pass}/{len(results):<4} {overall_avg:<7.2f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------

def results_to_json(results: list[ScenarioResult], output_path: str | Path) -> Path:
    """Save evaluation results as a JSON report.

    Args:
        results: List of ScenarioResult objects.
        output_path: File path to write the JSON report.

    Returns:
        Path to the written file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    by_cat = _group_by_category(results)
    category_summaries = {}
    for cat, cat_results in sorted(by_cat.items()):
        category_summaries[cat] = {
            "count": len(cat_results),
            "passed": sum(1 for r in cat_results if r.passed),
            "avg_score": round(
                sum(r.overall_score for r in cat_results) / len(cat_results), 4
            ),
        }

    report = {
        "summary": {
            "total_scenarios": len(results),
            "total_passed": sum(1 for r in results if r.passed),
            "overall_avg_score": round(
                sum(r.overall_score for r in results) / len(results), 4
            )
            if results
            else 0.0,
            "by_category": category_summaries,
        },
        "results": [_result_to_dict(r) for r in results],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    return output_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _group_by_category(results: list[ScenarioResult]) -> dict[str, list[ScenarioResult]]:
    """Group results by category."""
    groups: dict[str, list[ScenarioResult]] = defaultdict(list)
    for r in results:
        groups[r.category].append(r)
    return dict(groups)


def _result_to_dict(result: ScenarioResult) -> dict[str, Any]:
    """Convert a ScenarioResult to a JSON-serializable dict."""
    d = asdict(result)
    # Replace raw_response with truncated version for readability
    if len(d.get("raw_response", "")) > 500:
        d["raw_response"] = d["raw_response"][:500] + "... [truncated]"
    return d
