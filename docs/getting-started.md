# Getting Started

Setup, reproduction steps, and troubleshooting for the Clinical Trials Knowledge Graph.

## Prerequisites

- **Python 3.10+** (3.11 or 3.12 recommended)
- **Rust toolchain** (for Samyama Python SDK native extension) -- install via [rustup](https://rustup.rs/)
- **~2 GB disk** for API response cache and sentence-transformer model weights
- **Internet access** during ETL (fetches from ClinicalTrials.gov, NLM, OpenFDA, PubMed)

Optional:
- **NCBI API key** -- increases PubMed rate limit from 3 req/sec to 10 req/sec. Register at [NCBI](https://www.ncbi.nlm.nih.gov/account/) and set `NCBI_API_KEY` environment variable.
- **OpenFDA API key** -- increases rate limit from 240/min to 120K/day. Register at [open.fda.gov](https://open.fda.gov/apis/authentication/).

## Installation

```bash
# Clone the repository
git clone https://github.com/samyama-ai/clinicaltrials-kg.git
cd clinicaltrials-kg

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate    # macOS / Linux
# .venv\Scripts\activate     # Windows

# Install with dev dependencies
pip install -e ".[dev]"
```

This installs:
- `samyama>=0.6.0` -- Samyama Python SDK (builds native Rust extension via PyO3)
- `httpx` -- HTTP client for API calls
- `sentence-transformers` -- Embedding model for vector search
- `fastmcp` -- MCP server framework
- `click` -- CLI argument parsing
- `rich` -- Console output formatting
- `pytest`, `ruff` -- Dev tools

## First Run

The default ETL loads trials for 5 conditions (Type 2 Diabetes, Breast Cancer, Alzheimer Disease, Heart Failure, COVID-19) with up to 200 trials per condition:

```bash
python -m etl.loader
```

Expected output:

```
Initialised Samyama (embedded mode)

=== Step 1/5: Loading trials for 5 conditions ===
  Type 2 Diabetes: 200 trials loaded
  Breast Cancer: 200 trials loaded
  Alzheimer Disease: 200 trials loaded
  Heart Failure: 200 trials loaded
  COVID-19: 200 trials loaded
  Total trials: 1000

=== Step 2/5: Enriching with MeSH hierarchy ===
  MeSH terms linked: 85

=== Step 3/5: Normalising drugs (RxNorm / ATC / OpenFDA) ===
  Drugs normalised: 120

=== Step 4/5: Linking PubMed publications ===
  Publications linked: 340

=== Step 5/5: Generating vector embeddings ===
  Embeddings generated: 1085
```

Estimated time: 10-20 minutes (depends on network speed and API response times).

### Loading custom conditions

```bash
python -m etl.loader \
    --conditions "Parkinson Disease" \
    --conditions "Lung Cancer" \
    --conditions "Rheumatoid Arthritis" \
    --max-trials 500 \
    --include-results
```

The `--include-results` flag also fetches adverse events from the results section of completed trials.

### Skipping embeddings

If you do not need vector similarity search, skip the embedding step to avoid downloading the sentence-transformer model:

```bash
python -m etl.loader --skip-embeddings
```

## Verifying the Graph

After the ETL completes, the loader prints graph statistics. You can also verify interactively using the MCP server or by running queries directly:

```python
from samyama import SamyamaClient

client = SamyamaClient.embedded()

# Count nodes by label
result = client.query_readonly(
    "default",
    "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC"
)
for row in result:
    print(f"  {row['label']}: {row['cnt']}")

# Verify a specific relationship
result = client.query_readonly(
    "default",
    "MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition) RETURN count(t)"
)
print(f"Trial-Condition edges: {result[0][0]}")
```

## Running Tests

```bash
pytest tests/ -v
```

## Starting the MCP Server

```bash
python -m mcp_server.server
```

The server starts on stdio (standard MCP transport) and exposes 16 tools across trial, drug, disease, and analytics categories.

## Common Issues and Fixes

### `cargo` not found during `pip install`

The Samyama Python SDK is a native Rust extension built with PyO3. You need the Rust toolchain installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
pip install -e ".[dev]"
```

### ClinicalTrials.gov rate limiting (HTTP 429)

The ETL respects the 10 requests/second limit with a 120ms delay between requests. If you still hit 429 errors, increase the delay in `etl/clinicaltrials_loader.py`:

```python
REQ_INTERVAL = 0.25  # Increase from 0.12 to 0.25
```

### PubMed rate limiting (HTTP 429)

Without an API key, PubMed allows only 3 requests/second. Set your NCBI API key to increase the limit:

```bash
export NCBI_API_KEY="your_key_here"
python -m etl.loader
```

### Sentence-transformer model download fails

The `all-MiniLM-L6-v2` model (~80 MB) is downloaded on first run. If the download fails behind a proxy, download it manually:

```bash
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
```

Or skip embeddings entirely with `--skip-embeddings`.

### Stale API cache

API responses are cached in `data/cache/`. To force a fresh fetch, delete the cache directory:

```bash
rm -rf data/cache/
python -m etl.loader
```

### MeSH or RxNorm API timeouts

NLM APIs occasionally have slow response times. The loaders use a 30-second timeout and log warnings for failed lookups without aborting the pipeline. Skipped entries can be retried by re-running the ETL (it only processes nodes that lack the relevant property, e.g., `mesh_id IS NULL`).
