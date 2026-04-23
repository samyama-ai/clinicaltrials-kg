# Clinical Trials Knowledge Graph

**7.8 million nodes. 27 million edges. Every registered study on ClinicalTrials.gov in one graph.**

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

## Schema

**11 node labels** -- ClinicalTrial, Condition, Intervention, ArmGroup, Outcome, Sponsor, Site, AdverseEvent, MeSHDescriptor, Drug, Publication

**11 edge types** -- STUDIES, TESTS, HAS_ARM, USES, SPONSORED_BY, CONDUCTED_AT, MEASURES, REPORTED, CODED_AS_MESH, CODED_AS_DRUG, PUBLISHED_IN

**5 data sources** -- ClinicalTrials.gov, MeSH (NLM), RxNorm/ATC, OpenFDA FAERS, PubMed

## Quick Start

### Load from snapshot (recommended)

```bash
# Download (711 MB)
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v5/clinical-trials.sgsnap

# Start Samyama and import
./target/release/samyama
curl -X POST http://localhost:8080/api/tenants \
  -H 'Content-Type: application/json' \
  -d '{"id":"clinical-trials","name":"Clinical Trials KG"}'
curl -X POST http://localhost:8080/api/tenants/clinical-trials/snapshot/import \
  -F "file=@clinical-trials.sgsnap"
```

### Build from source

```bash
git clone https://github.com/samyama-ai/clinicaltrials-kg.git && cd clinicaltrials-kg
pip install -e ".[dev]"
python -m etl.loader                    # Default: 5 conditions x 200 trials
python -m etl.loader --conditions "Lung Cancer" --max-trials 500   # Custom
```

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

This KG is one of three biomedical knowledge graphs that together form Samyama's billion-edge benchmark: **Clinical Trials** (27M edges) + [Pathways](https://github.com/samyama-ai/pathways-kg) (835K edges) + [Drug Interactions](https://github.com/samyama-ai/druginteractions-kg) (388K edges), federated with [PubMed](https://github.com/samyama-ai/pubmed-kg) (1.04B edges).

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Benchmark (100 queries) | [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |

## License

Apache 2.0
