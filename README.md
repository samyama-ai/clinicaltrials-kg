# Clinical Trials Knowledge Graph

**7.8 million nodes. 27 million edges. Every registered study on ClinicalTrials.gov in one graph.**

![Clinical Trials KG demo](demo/clinicaltrials.gif)

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG.

<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>

---

We loaded 575,778 clinical studies with their drugs, conditions, adverse events, sponsors, and trial sites, then asked:

> *"What are the most common adverse events across all trials?"*

```cypher
MATCH (t:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent)
RETURN ae.term, count(DISTINCT t) AS trials
ORDER BY trials DESC LIMIT 5
```

| Adverse Event | Trials |
|---------------|--------|
| Headache | 28,130 |
| Nausea | 26,847 |
| Fatigue | 20,513 |
| Diarrhoea | 18,924 |
| Vomiting | 16,112 |

**One query. Five data sources. 7.8M nodes.** Powered by [Samyama Graph](https://github.com/samyama-ai/samyama-graph).

[See all 100 benchmark queries →](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html)

---

## Demo

A narrated terminal demo loads a **real bounded subset** of the AACT bulk
download (the first 250 ClinicalTrials.gov studies plus their conditions,
interventions, arm groups, lead sponsors, MeSH cross-references and
publications — the multi-GB sites/outcomes/adverse-event tables are skipped so
it loads in under a minute) and walks through four domain questions.

```bash
# Run it
source ~/projects/venv/bin/activate
PYTHONUNBUFFERED=1 python -m demo.demo

# Re-record (asciinema + agg)
asciinema rec --overwrite --cols 92 --rows 32 --idle-time-limit 2.0 \
  -c "bash -c 'source ~/projects/venv/bin/activate && PYTHONUNBUFFERED=1 python -m demo.demo'" \
  demo/clinicaltrials.cast
agg demo/clinicaltrials.cast demo/clinicaltrials.gif
```

## Documentation

New here? Start with the guides:

| Guide | What it covers |
|-------|----------------|
| **[GETTING_STARTED.md](GETTING_STARTED.md)** | prerequisites (Python ≥ 3.10) · install · run the engine (Docker) · load the graph · first query |
| **[docs/QUERYING.md](docs/QUERYING.md)** | ask questions via the **HTTP API** or the **Samyama CLI** |
| [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) | 100 example queries |

## Schema

**11 node labels** -- ClinicalTrial, Condition, Intervention, ArmGroup, Outcome, Sponsor, Site, AdverseEvent, MeSHDescriptor, Drug, Publication

**11 edge types** -- STUDIES, TESTS, HAS_ARM, USES, SPONSORED_BY, CONDUCTED_AT, MEASURES, REPORTED, CODED_AS_MESH, CODED_AS_DRUG, PUBLISHED_IN

**5 data sources** -- ClinicalTrials.gov, MeSH (NLM), RxNorm/ATC, OpenFDA FAERS, PubMed

## Quick Start

**Full walkthrough → [GETTING_STARTED.md](GETTING_STARTED.md)** (prerequisites, Docker, loading, querying).

### Build from source — bounded subset (friendliest first run)

Needs **Python ≥ 3.10** and **Docker**:

```bash
pip install -r requirements.txt
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0

python -m etl.loader                    # ~5 conditions x 200 trials (into the `default` tenant)
python -m etl.loader --conditions "Lung Cancer" --max-trials 500   # custom
```

### Load from snapshot (large — ~711 MB → `clinical-trials` tenant)

```bash
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v5/clinical-trials.sgsnap
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' -d '{"id":"clinical-trials","name":"Clinical Trials KG"}'
curl -X POST http://localhost:8080/api/tenants/clinical-trials/snapshot/import -F "file=@clinical-trials.sgsnap"
```
*(The snapshot expands to millions of nodes — import it on an engine with plenty of memory.)*

## Example Queries

```cypher
-- Drug repurposing: diabetes drugs in Alzheimer trials
MATCH (d:Drug)<-[:CODED_AS_DRUG]-(i1:Intervention)<-[:TESTS]-(t1:ClinicalTrial)-[:STUDIES]->(c1:Condition)
WHERE c1.name CONTAINS 'Diabetes'
WITH d
MATCH (d)<-[:CODED_AS_DRUG]-(i2:Intervention)<-[:TESTS]-(t2:ClinicalTrial)-[:STUDIES]->(c2:Condition)
WHERE c2.name CONTAINS 'Alzheimer'
RETURN d.name, count(t2) AS alzheimer_trials ORDER BY alzheimer_trials DESC

-- Trial sites by country for breast cancer
MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition), (t)-[:CONDUCTED_AT]->(site:Site)
WHERE c.name CONTAINS 'Breast Cancer'
RETURN site.country, count(DISTINCT t) AS trials ORDER BY trials DESC
```

## Part of the Biomedical Trifecta

This KG is one of three biomedical knowledge graphs that together form Samyama's billion-edge benchmark: **Clinical Trials** (27M edges) + [Pathways](https://github.com/samyama-ai/pathways-kg) (835K edges) + [Drug Interactions](https://github.com/samyama-ai/druginteractions-kg) (388K edges), merged on load with [PubMed](https://github.com/samyama-ai/pubmed-kg) (1.04B edges).

These four graphs are also loaded as part of a larger merged corpus — fifteen KGs plus two synthetic patient cohorts in a single store. [BiomedQA](https://github.com/samyama-ai/biomedqa#beyond-the-benchmark-a-318m-node-corpus) has the node, edge and merge counts, and states how and when they were measured.

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Benchmark (100 queries) | [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |

## License

Apache 2.0
