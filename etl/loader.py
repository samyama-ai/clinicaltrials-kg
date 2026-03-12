"""Clinical Trials Knowledge Graph -- ETL Pipeline Orchestrator

Runs the full ingestion pipeline:
  1. Load trials from ClinicalTrials.gov
  2. Enrich with MeSH disease hierarchy
  3. Normalize drugs via RxNorm / ATC / OpenFDA adverse events
  4. Link publications from PubMed
  5. Generate vector embeddings for similarity search
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
    "--conditions",
    multiple=True,
    default=[
        "Type 2 Diabetes",
        "Breast Cancer",
        "Alzheimer Disease",
        "Heart Failure",
        "COVID-19",
    ],
    help="Disease conditions to load trials for",
)
@click.option("--max-trials", default=200, help="Max trials per condition")
@click.option(
    "--include-results", is_flag=True, help="Include trial results (adverse events)"
)
@click.option(
    "--skip-embeddings", is_flag=True, help="Skip vector embedding generation"
)
@click.option(
    "--pubmed-api-key", default=None, help="NCBI API key for higher rate limits"
)
def main(conditions, max_trials, include_results, skip_embeddings, pubmed_api_key):
    """Load clinical trial data into Samyama knowledge graph."""
    t0 = time.time()

    # Initialise embedded graph client
    client = SamyamaClient.embedded()
    tenant = "default"
    print(f"Initialised Samyama (embedded mode)")

    # Step 1 -- Load trials from ClinicalTrials.gov
    print(f"\n=== Step 1/5: Loading trials for {len(conditions)} conditions ===")
    trial_count = 0
    for condition in conditions:
        n = load_trials(
            client,
            tenant,
            condition=condition,
            max_trials=max_trials,
            include_results=include_results,
        )
        trial_count += n
        print(f"  {condition}: {n} trials loaded")
    print(f"  Total trials: {trial_count}")

    # Step 2 -- Enrich with MeSH hierarchy
    print("\n=== Step 2/5: Enriching with MeSH hierarchy ===")
    mesh_count = load_mesh(client, tenant)
    print(f"  MeSH terms linked: {mesh_count}")

    # Step 3 -- Normalise drugs via RxNorm + ATC + OpenFDA adverse events
    print("\n=== Step 3/5: Normalising drugs (RxNorm / ATC / OpenFDA) ===")
    drug_count = load_drugs(client, tenant, include_adverse_events=include_results)
    print(f"  Drugs normalised: {drug_count}")

    # Step 4 -- Link publications from PubMed
    print("\n=== Step 4/5: Linking PubMed publications ===")
    pub_count = load_publications(client, tenant, api_key=pubmed_api_key)
    print(f"  Publications linked: {pub_count}")

    # Step 5 -- Generate vector embeddings
    if skip_embeddings:
        print("\n=== Step 5/5: Skipping embedding generation (--skip-embeddings) ===")
    else:
        print("\n=== Step 5/5: Generating vector embeddings ===")
        emb_count = generate_embeddings(client, tenant)
        print(f"  Embeddings generated: {emb_count}")

    elapsed = time.time() - t0

    # Print final graph statistics
    stats = client.query_readonly(
        tenant,
        "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC",
    )
    print(f"\n{'='*50}")
    print(f"ETL complete in {elapsed:.1f}s")
    print(f"Graph statistics:")
    for row in stats:
        print(f"  {row['label']}: {row['cnt']}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
