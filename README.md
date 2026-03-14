# Clinical Trials Knowledge Graph

![Language](https://img.shields.io/badge/language-Python-3776AB)


A knowledge graph connecting clinical trials, drugs, diseases, genes, and publications -- powered by [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph) and real-world public data.

**15 node labels** | **25 relationship types** | **5 public data sources** | **16 MCP tools** | **40 evaluation scenarios**

## Thesis

For clinical research, structured graph queries over standardized ontologies outperform LLM-based reasoning over flat trial databases. ClinicalTrials.gov contains 500,000+ registered studies, but its native search is keyword-based and limited to single-hop lookups. Questions like "which diabetes drugs are being repurposed for Alzheimer's?" or "what genes encode proteins targeted by drugs in Phase 3 heart failure trials?" require multi-hop traversals across trials, drugs, conditions, genes, and proteins that are impossible with keyword search and unreliable with LLM reasoning over unstructured data.

This knowledge graph maps five public data sources into a unified property graph with standardized coding systems (MeSH, RxNorm, ATC, LOINC, MedDRA), enabling deterministic multi-hop queries, vector-based trial similarity search, and ontology-aware condition grouping.

## Data Sources

| Source | Data | Access |
|---|---|---|
| [ClinicalTrials.gov API v2](https://clinicaltrials.gov/data-api/about-api) | Trials, conditions, interventions, sponsors, sites, outcomes, adverse events | Free, no auth |
| [MeSH (NLM)](https://id.nlm.nih.gov/mesh/) | Medical Subject Headings hierarchy -- disease/condition taxonomy | Free, no auth |
| [RxNorm (NLM)](https://rxnav.nlm.nih.gov/) | Normalized drug names, RxCUI identifiers, ATC classification codes | Free, no auth |
| [OpenFDA](https://open.fda.gov/apis/) | Drug adverse event reports (FAERS), drug labels | Free, API key optional |
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25501/) | Publications linked to trials via NCT secondary source IDs | Free, API key optional |

## Graph Schema

15 node labels, 25 relationship types (see [`schema/clinicaltrials_kg.cypher`](schema/clinicaltrials_kg.cypher)):

```
ClinicalTrial ─[STUDIES]──────> Condition ─[CODED_AS_MESH]──> MeSHDescriptor
     │                              │                               │
     ├─[TESTS]──> Intervention      ├─[ASSOCIATED_WITH]<── Gene     ├─[BROADER_THAN]
     │                │              │                               │
     ├─[HAS_ARM]──> ArmGroup        │                          MeSHDescriptor
     │                │              │
     ├─[SPONSORED_BY]──> Sponsor     │
     │                               │
     ├─[CONDUCTED_AT]──> Site        │
     │                               │
     ├─[MEASURES]──> Outcome ─[MEASURED_BY]──> LabTest
     │
     ├─[REPORTED]──> AdverseEvent ─[CODED_AS_MESH]──> MeSHDescriptor
     │
     └─[PUBLISHED_IN]──> Publication ─[DESCRIBES]──> ClinicalTrial
                               │
                               └─[TAGGED_WITH]──> MeSHDescriptor

Intervention ─[CODED_AS_DRUG]──> Drug ─[CLASSIFIED_AS]──> DrugClass ─[PARENT_CLASS]──> DrugClass
                                   │
                                   ├─[TARGETS]──> Protein <──[ENCODES]── Gene
                                   ├─[TREATS]──> Condition
                                   ├─[INTERACTS_WITH]──> Drug
                                   └─[HAS_ADVERSE_EFFECT]──> AdverseEvent

ArmGroup ─[USES]──> Intervention
```

### Node Labels (15)

| Label | Key Properties | Source |
|---|---|---|
| ClinicalTrial | nct_id, title, phase, overall_status, enrollment, start_date | ClinicalTrials.gov |
| Condition | name, mesh_id, icd10_code, snomed_id | ClinicalTrials.gov + MeSH |
| Intervention | name, type (DRUG\|DEVICE\|PROCEDURE\|...), rxnorm_cui | ClinicalTrials.gov |
| ArmGroup | label, type (EXPERIMENTAL\|ACTIVE_COMPARATOR\|...) | ClinicalTrials.gov |
| Outcome | measure, type (PRIMARY\|SECONDARY), time_frame | ClinicalTrials.gov |
| Sponsor | name, class (INDUSTRY\|NIH\|FED\|OTHER) | ClinicalTrials.gov |
| Site | facility, city, state, country, latitude, longitude | ClinicalTrials.gov |
| AdverseEvent | term, organ_system, source_vocabulary, frequency, is_serious | ClinicalTrials.gov + OpenFDA |
| MeSHDescriptor | descriptor_id, name, tree_numbers, scope_note | MeSH (NLM) |
| Drug | rxnorm_cui, name, drugbank_id | RxNorm |
| DrugClass | atc_code, name, level (1-5) | RxNorm / ATC |
| Publication | pmid, title, journal, pub_date, doi | PubMed |
| Gene | gene_id, symbol, name, uniprot_id | Linked ontologies |
| Protein | uniprot_id, name, function | Linked ontologies |
| LabTest | loinc_code, name, component, system | LOINC |

## Project Structure

```
clinicaltrials-kg/
├── schema/
│   └── clinicaltrials_kg.cypher       # Graph schema (15 labels, 25 relationships)
├── etl/
│   ├── loader.py                      # Main orchestrator -- 5-step pipeline
│   ├── clinicaltrials_loader.py       # ClinicalTrials.gov API v2 -> trials, conditions, sites
│   ├── mesh_loader.py                 # MeSH API -> descriptors, BROADER_THAN hierarchy
│   ├── drug_loader.py                 # RxNorm + ATC + OpenFDA -> drugs, classes, adverse events
│   ├── publication_loader.py          # PubMed E-utilities -> publications, PUBLISHED_IN edges
│   └── embedding_gen.py               # sentence-transformers -> vector index (384d, cosine)
├── mcp_server/
│   ├── server.py                      # FastMCP entry point (16 tools)
│   └── tools/
│       ├── trial_tools.py             # search_trials, get_trial, find_similar_trials, trial_sites
│       ├── drug_tools.py              # drug_trials, drug_adverse_events, drug_interactions, drug_class
│       ├── disease_tools.py           # disease_trials, treatment_landscape, related_conditions, disease_genes
│       └── analytics_tools.py         # enrollment_by_phase, sponsor_landscape, geographic_distribution, trial_timeline
├── scenarios/                         # 40 evaluation scenario JSONs (7 categories)
├── evaluation/                        # Scoring framework
├── benchmark/                         # Benchmark runners
├── tests/
├── data/
│   └── cache/                         # Disk cache for API responses
├── docs/
│   ├── getting-started.md             # Setup, reproduction, troubleshooting
│   └── data-sources.md               # Detailed API documentation per source
└── pyproject.toml
```

## Quick Start

See [`docs/getting-started.md`](docs/getting-started.md) for full setup instructions.

```bash
# Clone
git clone https://github.com/samyama-ai/clinicaltrials-kg.git
cd clinicaltrials-kg

# Install
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Load data (default: 5 conditions x 200 trials = ~1000 trials)
python -m etl.loader

# Load with custom conditions and more trials
python -m etl.loader \
    --conditions "Parkinson Disease" \
    --conditions "Lung Cancer" \
    --max-trials 500 \
    --include-results

# Skip embedding generation (faster, no GPU needed)
python -m etl.loader --skip-embeddings

# Run tests
pytest tests/ -v

# Start MCP server (for agent integration)
python -m mcp_server.server
```

### Default Conditions

The default ETL run loads trials for five high-impact conditions:
- Type 2 Diabetes
- Breast Cancer
- Alzheimer Disease
- Heart Failure
- COVID-19

## Example Queries

### Basic: Find recruiting Phase 3 diabetes trials

```cypher
MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition)
WHERE c.name CONTAINS 'Type 2 Diabetes'
  AND t.phase = 'PHASE3'
  AND t.overall_status = 'RECRUITING'
RETURN t.nct_id, t.title, t.enrollment
ORDER BY t.enrollment DESC
LIMIT 20
```

### Drug repurposing: diabetes drugs in Alzheimer trials

```cypher
MATCH (d:Drug)<-[:CODED_AS_DRUG]-(i1:Intervention)<-[:TESTS]-(t1:ClinicalTrial)-[:STUDIES]->(c1:Condition)
WHERE c1.name CONTAINS 'Diabetes'
WITH d
MATCH (d)<-[:CODED_AS_DRUG]-(i2:Intervention)<-[:TESTS]-(t2:ClinicalTrial)-[:STUDIES]->(c2:Condition)
WHERE c2.name CONTAINS 'Alzheimer'
RETURN d.name, count(t2) AS alzheimer_trials
ORDER BY alzheimer_trials DESC
```

### Multi-hop: gene -> protein -> drug -> trial

```cypher
MATCH (g:Gene)-[:ENCODES]->(p:Protein)<-[:TARGETS]-(d:Drug)
      <-[:CODED_AS_DRUG]-(i:Intervention)<-[:TESTS]-(t:ClinicalTrial)
WHERE g.symbol = 'TCF7L2'
RETURN g.symbol, p.name, d.name, t.nct_id, t.title
LIMIT 10
```

### Ontology traversal: all conditions under a MeSH parent

```cypher
MATCH (parent:MeSHDescriptor {name: 'Diabetes Mellitus'})-[:BROADER_THAN*1..3]->(child:MeSHDescriptor)
      <-[:CODED_AS_MESH]-(c:Condition)<-[:STUDIES]-(t:ClinicalTrial)
RETURN child.name AS subcondition, count(t) AS trial_count
ORDER BY trial_count DESC
```

### Adverse event comparison across drugs in the same class

```cypher
MATCH (dc:DrugClass {atc_code: 'A10B'})<-[:CLASSIFIED_AS]-(d:Drug)-[:HAS_ADVERSE_EFFECT]->(ae:AdverseEvent)
RETURN d.name, collect(ae.term) AS adverse_events, count(ae) AS event_count
ORDER BY event_count DESC
```

### Sponsor landscape for a condition

```cypher
MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition),
      (t)-[:SPONSORED_BY]->(s:Sponsor)
WHERE c.name CONTAINS 'Heart Failure'
RETURN s.name, s.class, count(t) AS trial_count, collect(DISTINCT t.phase) AS phases
ORDER BY trial_count DESC
LIMIT 15
```

### Geographic distribution of trial sites

```cypher
MATCH (t:ClinicalTrial)-[:STUDIES]->(c:Condition),
      (t)-[:CONDUCTED_AT]->(site:Site)
WHERE c.name CONTAINS 'Breast Cancer'
RETURN site.country, count(DISTINCT t) AS trials, count(site) AS sites
ORDER BY trials DESC
```

## 40 Scenarios (7 Categories)

| Category | Count | Example |
|---|---|---|
| Trial discovery | 8 | "Find all Phase 3 recruiting trials for Type 2 Diabetes" |
| Drug analysis | 7 | "What are the most common adverse events for Metformin?" |
| Disease landscape | 6 | "Compare the treatment landscape for Alzheimer vs Parkinson" |
| Cross-indication | 5 | "Which drugs are being tested for both cancer and autoimmune diseases?" |
| Ontology traversal | 5 | "Find all trials under the MeSH subtree 'Neoplasms'" |
| Publication linkage | 4 | "Which completed trials have no published results?" |
| Geographic/temporal | 5 | "How has COVID-19 trial activity changed year over year?" |

## MCP Server

The MCP server exposes 16 tools across 4 groups for agent integration:

```bash
python -m mcp_server.server
```

### Tool Groups

**Trial tools (4)**
- `search_trials` -- Search by condition, phase, and/or recruitment status
- `get_trial` -- Full details for a specific NCT ID
- `find_similar_trials` -- Vector similarity search over trial embeddings
- `trial_sites` -- List study sites with locations

**Drug tools (4)**
- `drug_trials` -- Find all trials testing a specific drug
- `drug_adverse_events` -- Adverse events from OpenFDA data
- `drug_interactions` -- Known drug-drug interactions
- `drug_class` -- ATC classification hierarchy (L1 through L5)

**Disease tools (4)**
- `disease_trials` -- Trials studying a condition, with phase filter
- `treatment_landscape` -- All interventions for a condition, grouped by type
- `related_conditions` -- MeSH hierarchy neighbors (siblings and children)
- `disease_genes` -- Gene associations from trial biomarker data

**Analytics tools (4)**
- `enrollment_by_phase` -- Enrollment counts grouped by trial phase
- `sponsor_landscape` -- Sponsors ranked by trial count
- `geographic_distribution` -- Trial sites aggregated by country
- `trial_timeline` -- Trial activity over time with dominant phase

## Coding Systems

| System | Use | Graph Representation |
|---|---|---|
| [MeSH](https://meshb.nlm.nih.gov/) | Disease/condition hierarchy | MeSHDescriptor nodes + BROADER_THAN edges |
| [RxNorm](https://www.nlm.nih.gov/research/umls/rxnorm/) | Drug normalization (rxcui) | Drug nodes + CODED_AS_DRUG edges |
| [ATC](https://www.who.int/tools/atc-ddd-toolkit) | Drug classification (5 levels) | DrugClass nodes + PARENT_CLASS hierarchy |
| [LOINC](https://loinc.org/) | Lab test coding | LabTest nodes + MEASURED_BY edges |
| [ICD-10](https://icd.who.int/browse10/) | Diagnosis codes | Property on Condition (icd10_code) |
| [SNOMED CT](https://www.snomed.org/) | Clinical terminology | Property on Condition (snomed_id) |
| [MedDRA](https://www.meddra.org/) | Adverse event coding | Property on AdverseEvent (source_vocabulary) |

## ETL Pipeline

The 5-step ETL pipeline runs in order, with each step enriching the graph:

1. **ClinicalTrials.gov** -- Fetches trials via API v2 with pagination and disk caching. Creates ClinicalTrial, Condition, Intervention, ArmGroup, Outcome, Sponsor, Site nodes and all trial-centric edges. Deduplicates shared entities (conditions, interventions, sponsors, sites) via in-memory registries.

2. **MeSH enrichment** -- For each Condition node without a `mesh_id`, searches the NLM MeSH API, creates MeSHDescriptor nodes, CODED_AS_MESH edges, and walks up the tree to build BROADER_THAN hierarchy edges.

3. **Drug normalization** -- For each drug-type Intervention without an `rxnorm_cui`, searches RxNorm for the rxcui, creates Drug nodes with CODED_AS_DRUG edges, builds the ATC DrugClass hierarchy (5 levels), and queries OpenFDA for adverse events linked to each drug.

4. **PubMed linking** -- For each ClinicalTrial, searches PubMed E-utilities for articles mentioning the NCT ID in secondary source IDs. Creates Publication nodes and bidirectional PUBLISHED_IN / DESCRIBES edges.

5. **Vector embeddings** -- Loads the `all-MiniLM-L6-v2` sentence-transformer model (384 dimensions), encodes ClinicalTrial.brief_summary and Condition.name into vectors, and stores them in Samyama's HNSW vector index for cosine similarity search.

### Pre-built Snapshot (Recommended)

A pre-built `.sgsnap` snapshot of the full 575K-study dataset is available for instant import:

| | |
|---|---|
| **Download** | [clinical-trials.sgsnap](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/clinical-trials.sgsnap) (711 MB) |
| **Nodes** | 7,711,965 |
| **Edges** | 27,069,085 |
| **Requires** | Samyama Graph v0.6.1+ |

```bash
# Download snapshot
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/clinical-trials.sgsnap

# Create tenant and import
curl -X POST http://localhost:8080/api/tenants \
  -H 'Content-Type: application/json' \
  -d '{"id":"clinical-trials","name":"Clinical Trials KG"}'

curl -X POST http://localhost:8080/api/tenants/clinical-trials/snapshot/import \
  -F "file=@clinical-trials.sgsnap"
```

### Full-Dataset Bulk Loading (AACT)

For building the graph from scratch, Samyama Graph includes a native Rust AACT loader (`examples/aact_loader.rs`) that ingests the AACT pipe-delimited flat files directly into GraphStore, producing ~7.7M nodes and ~27M edges in roughly 5 minutes. The resulting graph can be exported as a `.sgsnap` snapshot and imported into a running Samyama instance via the HTTP API.

A Python AACT loader (`etl/aact_loader.py`) is also available in this repository for integration with the Python ETL pipeline.

## Related

- [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph) -- High-performance graph DB with OpenCypher, vector search, and optimization
- [AssetOps-KG](https://github.com/samyama-ai/assetops-kg) -- Industrial asset operations knowledge graph (same architecture pattern)
- [VaidhyaMegha KG](https://github.com/vaidhyamegha/vaidhyamegha-knowledge-graphs) -- Reference clinical knowledge graphs
- [Samyama Python SDK](https://pypi.org/project/samyama/) -- Python SDK for Samyama Graph Database

## License

Apache 2.0
