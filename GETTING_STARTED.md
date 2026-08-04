# Getting Started — Clinical Trials Knowledge Graph

From `git clone` to your first answer. This is a **very large KG** (7.8M nodes / 27M edges), so the
snapshot is big — the **build-from-source subset** is the friendliest way to try it first.

---

## 1. Prerequisites

- **Python ≥ 3.10** (required by the `samyama` SDK; macOS ships 3.9 — use `python3.10`+).
- **git**
- **Docker** — to run the Samyama engine (HTTP `:8080`, RESP `:6379`).

> ⚠️ The full snapshot is **~711 MB** and expands to millions of nodes — importing it needs an engine
> with plenty of memory. On a small/laptop engine, prefer the bounded build-from-source subset below.

## 2. Install

```bash
git clone https://github.com/samyama-ai/clinicaltrials-kg.git
cd clinicaltrials-kg
python3 -m venv .venv && source .venv/bin/activate     # Python >= 3.10
pip install -r requirements.txt                         # note: pulls torch (~large) for embeddings
```

## 3. Run the engine (Docker)

```bash
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

## 4. Load the graph

### Option A — bounded subset from the ClinicalTrials.gov API (friendliest first run)
```bash
python -m etl.loader                                              # ~5 conditions × 200 trials
python -m etl.loader --conditions "Lung Cancer" --max-trials 500  # custom
```
> **Note:** the loader currently writes to the **`default`** tenant (it has no `--graph`/`--tenant`
> option yet — tracked in this repo's issues). Query the `default` graph after this path.

### Option B — full snapshot (large; ~711 MB → `clinical-trials` tenant)
```bash
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v5/clinical-trials.sgsnap
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' \
  -d '{"id":"clinical-trials","name":"Clinical Trials KG"}'
curl -X POST http://localhost:8080/api/tenants/clinical-trials/snapshot/import -F "file=@clinical-trials.sgsnap"
```

## 5. Ask your first question

Most common adverse events across trials (query the tenant you loaded — `default` for Option A,
`clinical-trials` for Option B):

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "clinical-trials",
  "query": "MATCH (t:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent) RETURN ae.term AS adverse_event, count(DISTINCT t) AS trials ORDER BY trials DESC LIMIT 5"
}'
```

## 6. The ETL pipeline

- Data sources: **ClinicalTrials.gov, MeSH (NLM), RxNorm/ATC, OpenFDA FAERS, PubMed**.
- `etl/download_aact.py` — fetches the AACT bulk export (for the full build).
- `etl/loader.py` — orchestrates the loaders (trials, conditions, interventions, MeSH, drugs, …).
  Run `python -m etl.loader --help`.

## Next
- **[docs/QUERYING.md](docs/QUERYING.md)** — HTTP API and the Samyama CLI
- **[Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html)** — 100 queries
