"""Narrated terminal demo: Clinical Trials knowledge graph on Samyama.

Record with asciinema:
    asciinema rec -c "python -m demo.demo" demo/clinicaltrials.cast

Loads a REAL bounded subset of the AACT bulk download of ClinicalTrials.gov
(the first 250 registered studies + their conditions, interventions, arm
groups, sponsors, MeSH cross-references and publications) into a Samyama graph
and walks through the questions a trial-intelligence team asks: what is being
studied, with what, by whom, and how does it map onto the MeSH ontology.

Subset: 250 studies, sites/outcomes/adverse-events skipped (the multi-GB
tables) to keep the load well under a minute. All data is real AACT data.
"""

from __future__ import annotations

import time

from rich.console import Console
from rich.panel import Panel
from samyama import SamyamaClient

from etl.aact_loader import load_aact

console = Console()
G = "default"
DATA_DIR = "data/aact"
MAX_STUDIES = 250


def pause(s: float = 1.4) -> None:
    time.sleep(s)


def step(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}")
    pause(0.6)


def run(client, q, label):
    console.print(f"  [dim]cypher>[/dim] [yellow]{q}[/yellow]")
    rows = client.query(q, G).records
    one = len(rows) == 1 and len(rows[0]) == 1
    console.print(f"  [green]→[/green] {label}: [bold]{rows[0][0] if one else rows}[/bold]")
    pause()
    return rows


def main() -> None:
    console.print(Panel.fit(
        "[bold]Samyama · Clinical Trials Knowledge Graph[/bold]\n"
        "\"What is being studied, with what, and by whom?\" — trials, conditions,\n"
        "interventions, sponsors and the MeSH ontology in one graph\n"
        "[dim]data: AACT bulk download of ClinicalTrials.gov · real subset[/dim]",
        border_style="cyan",
    ))
    pause(1.2)

    step("1 · Load a real ClinicalTrials.gov subset into Samyama")
    console.print(f"  [dim]first {MAX_STUDIES} AACT studies + conditions/interventions/"
                  "arms/sponsors/MeSH/pubs…[/dim]")
    pause()
    stats = load_aact(client := SamyamaClient.embedded(), DATA_DIR,
                      max_studies=MAX_STUDIES,
                      include_sites=False, include_outcomes=False,
                      include_adverse_events=False)
    console.print(f"  [green]loaded[/green] {stats['studies']} trials, "
                  f"{stats['conditions']} conditions, {stats['interventions']} interventions, "
                  f"{stats['sponsors']} sponsors")
    run(client, "MATCH (t:ClinicalTrial) RETURN count(t) AS trials", "registered studies")
    run(client, "MATCH (m:MeSHDescriptor) RETURN count(m) AS mesh",
        "MeSH descriptors cross-referenced")

    step("2 · Which conditions draw the broadest research interest?")
    run(
        client,
        "MATCH (c:Condition)<-[:STUDIES]-(t:ClinicalTrial)-[:TESTS]->(i:Intervention) "
        "RETURN c.name AS condition, count(DISTINCT i) AS interventions "
        "ORDER BY interventions DESC LIMIT 5",
        "conditions by distinct interventions tested",
    )

    step("3 · Who are the most active sponsors in this slice?")
    run(
        client,
        "MATCH (t:ClinicalTrial)-[:SPONSORED_BY]->(s:Sponsor) "
        "RETURN s.name AS sponsor, count(t) AS trials "
        "ORDER BY trials DESC LIMIT 5",
        "top lead sponsors",
    )

    step("4 · Roll trials up to the MeSH ontology")
    console.print("  [dim]Condition → CODED_AS_MESH → MeSHDescriptor, grouped by descriptor…[/dim]")
    pause()
    run(
        client,
        "MATCH (c:Condition)-[:CODED_AS_MESH]->(m:MeSHDescriptor) "
        "RETURN m.name AS mesh_category, count(DISTINCT c) AS conditions "
        "ORDER BY conditions DESC LIMIT 5",
        "top MeSH categories spanning the studied conditions",
    )

    console.print()
    console.print(Panel.fit(
        "[bold green]Same engine, same Cypher — scales to all 575K studies and "
        "27M edges[/bold green]\nand federates with pubmed-kg / druginteractions-kg "
        "on shared MeSH & drug identifiers.",
        border_style="green",
    ))
    pause(1.5)


if __name__ == "__main__":
    main()
