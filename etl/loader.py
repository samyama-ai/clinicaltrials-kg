"""Clinical Trials Knowledge Graph -- ETL Pipeline Orchestrator

Runs the full ingestion pipeline:
  Source: ClinicalTrials.gov API (default) or AACT flat files (full dataset)
  Enrichment: MeSH hierarchy, RxNorm/ATC drug normalization, PubMed publications
  Optional: Vector embeddings for similarity search

Usage:
    # API mode (default) — ~1000 trials for 5 conditions
    python -m etl.loader

    # AACT mode — full ClinicalTrials.gov dataset (~500K+ trials)
    python -m etl.download_aact                    # Download first
    python -m etl.loader --source aact             # Load everything
    python -m etl.loader --source aact --max-studies 10000  # Load subset

    # Skip slow enrichment for faster testing
    python -m etl.loader --source aact --skip-enrichment --skip-embeddings
"""

from samyama import SamyamaClient
from etl.clinicaltrials_loader import load_trials
from etl.mesh_loader import load_mesh
from etl.drug_loader import load_drugs
from etl.publication_loader import load_publications
from etl.embedding_gen import generate_embeddings
import click
import time


@click.command()
@click.option(
    "--source",
    type=click.Choice(["api", "aact"]),
    default="api",
    help="Data source: api (ClinicalTrials.gov API, ~1K trials) or aact (AACT flat files, ~500K+ trials)",
)
@click.option(
    "--aact-dir",
    default="data/aact",
    help="Path to extracted AACT flat files (--source aact only)",
)
@click.option(
    "--max-studies",
    default=0,
    type=int,
    help="Max studies to load, 0=all (--source aact only)",
)
@click.option(
    "--skip-sites",
    is_flag=True,
    help="Skip loading facilities/sites — largest table (--source aact only)",
)
@click.option(
    "--skip-outcomes",
    is_flag=True,
    help="Skip loading outcomes (--source aact only)",
)
@click.option(
    "--skip-adverse-events",
    is_flag=True,
    help="Skip loading adverse events (--source aact only)",
)
@click.option(
    "--conditions",
    multiple=True,
    default=[
        "Type 2 Diabetes",
        "Breast Cancer",
        "Alzheimer Disease",
        "Heart Failure",
        "COVID-19",
    ],
    help="Disease conditions to load trials for (--source api only)",
)
@click.option("--max-trials", default=200, help="Max trials per condition (--source api only)")
@click.option(
    "--include-results", is_flag=True, help="Include trial results (--source api only)"
)
@click.option(
    "--skip-enrichment",
    is_flag=True,
    help="Skip MeSH/drug/publication enrichment (steps 2-4)",
)
@click.option(
    "--skip-embeddings", is_flag=True, help="Skip vector embedding generation"
)
@click.option(
    "--pubmed-api-key", default=None, help="NCBI API key for higher rate limits"
)
def main(
    source,
    aact_dir,
    max_studies,
    skip_sites,
    skip_outcomes,
    skip_adverse_events,
    conditions,
    max_trials,
    include_results,
    skip_enrichment,
    skip_embeddings,
    pubmed_api_key,
):
    """Load clinical trial data into Samyama knowledge graph."""
    t0 = time.time()

    # Initialise embedded graph client
    client = SamyamaClient.embedded()
    print("Initialised Samyama (embedded mode)")

    # ---- Step 1: Load trial data ----
    if source == "aact":
        from etl.aact_loader import load_aact

        limit_str = f" (max {max_studies:,})" if max_studies else " (all)"
        print(f"\n=== Step 1: Loading trials from AACT flat files{limit_str} ===")
        trial_counts = load_aact(
            client,
            data_dir=aact_dir,
            max_studies=max_studies,
            include_sites=not skip_sites,
            include_outcomes=not skip_outcomes,
            include_adverse_events=not skip_adverse_events,
        )
        print(f"  Total studies: {trial_counts.get('studies', 0):,}")
    else:
        print(f"\n=== Step 1: Loading trials for {len(conditions)} conditions (API) ===")
        trial_counts = load_trials(
            client,
            conditions=list(conditions),
            max_trials=max_trials,
            include_results=include_results,
        )
        print(f"  Total trials: {trial_counts['trials']}")

    # ---- Enrichment steps (optional) ----
    if skip_enrichment:
        print("\n=== Steps 2-4: Skipping enrichment (--skip-enrichment) ===")
    else:
        # Step 2 -- Enrich with MeSH hierarchy
        print("\n=== Step 2: Enriching with MeSH hierarchy ===")
        mesh_counts = load_mesh(client)
        print(f"  MeSH descriptors: {mesh_counts.get('descriptors', 0)}")

        # Step 3 -- Normalise drugs via RxNorm + ATC + OpenFDA
        print("\n=== Step 3: Normalising drugs (RxNorm / ATC / OpenFDA) ===")
        drug_counts = load_drugs(client)
        print(f"  Drugs normalised: {drug_counts.get('drugs_normalised', 0)}")

        # Step 4 -- Link publications from PubMed
        # In AACT mode, study_references already provides PMID links;
        # PubMed enrichment adds title/journal/authors metadata.
        print("\n=== Step 4: Linking PubMed publications ===")
        pub_counts = load_publications(client, api_key=pubmed_api_key)
        print(f"  Publications linked: {pub_counts.get('publications', 0)}")

    # Step 5 -- Generate vector embeddings
    if skip_embeddings:
        print("\n=== Step 5: Skipping embedding generation ===")
    else:
        print("\n=== Step 5: Generating vector embeddings ===")
        emb_counts = generate_embeddings(client)
        print(f"  Embeddings generated: {emb_counts.get('embeddings', 0)}")

    elapsed = time.time() - t0

    # Print final graph statistics
    graph_stats = client.query_readonly(
        "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC",
        "default",
    )
    print(f"\n{'='*60}")
    print(f"ETL complete in {elapsed:.1f}s")
    print("Graph statistics:")
    for row in graph_stats.records:
        print(f"  {row[0]:.<30s} {row[1]:>10,}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
