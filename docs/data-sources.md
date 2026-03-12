# Data Sources

Detailed documentation for each of the five public data sources used in the Clinical Trials Knowledge Graph.

## 1. ClinicalTrials.gov API v2

**Base URL**: `https://clinicaltrials.gov/api/v2/studies`

The primary data source. Provides trial metadata, conditions, interventions, sponsors, sites, outcomes, and (for completed trials) adverse event results.

### Endpoints Used

| Endpoint | Purpose |
|---|---|
| `GET /studies` | Search and paginate through studies |

### Key Parameters

| Parameter | Description | Example |
|---|---|---|
| `query.cond` | Condition/disease search term | `Type 2 Diabetes` |
| `pageSize` | Results per page (max 1000) | `200` |
| `pageToken` | Pagination cursor from previous response | (opaque string) |
| `fields` | Comma-separated list of sections to include | `protocolSection,resultsSection,derivedSection,hasResults` |
| `format` | Response format | `json` |

### Rate Limits

- 10 requests per second (no authentication required)
- The ETL uses a 120ms delay between requests to stay safely under the limit

### Response Structure

The API returns studies with nested modules:

```
study
├── protocolSection
│   ├── identificationModule     → nct_id, briefTitle, officialTitle
│   ├── statusModule             → overallStatus, startDate, completionDate
│   ├── designModule             → studyType, phases, enrollmentInfo
│   ├── conditionsModule         → conditions (list of condition names)
│   ├── armsInterventionsModule  → armGroups, interventions
│   ├── sponsorCollaboratorsModule → leadSponsor
│   ├── contactsLocationsModule  → locations (facilities with geo)
│   ├── outcomesModule           → primaryOutcomes, secondaryOutcomes
│   └── descriptionModule        → briefSummary
├── resultsSection (if hasResults)
│   └── adverseEventsModule      → seriousEvents, otherEvents
├── derivedSection
└── hasResults (boolean)
```

### Example: Fetch diabetes trials

```python
import httpx

resp = httpx.get("https://clinicaltrials.gov/api/v2/studies", params={
    "query.cond": "Type 2 Diabetes",
    "pageSize": 10,
    "fields": "protocolSection,hasResults",
    "format": "json",
})
data = resp.json()
for study in data["studies"]:
    proto = study["protocolSection"]
    nct_id = proto["identificationModule"]["nctId"]
    title = proto["identificationModule"]["briefTitle"]
    print(f"{nct_id}: {title}")

# Pagination
next_token = data.get("nextPageToken")
```

### Nodes Created

- **ClinicalTrial** -- one per study (keyed on `nct_id`)
- **Condition** -- deduplicated by name
- **Intervention** -- deduplicated by name, typed (DRUG, DEVICE, PROCEDURE, etc.)
- **ArmGroup** -- one per treatment arm (not deduplicated)
- **Outcome** -- one per primary/secondary outcome (not deduplicated)
- **Sponsor** -- deduplicated by name
- **Site** -- deduplicated by facility+city
- **AdverseEvent** -- from `resultsSection` when `--include-results` is used


## 2. MeSH (Medical Subject Headings) -- NLM

**Lookup URL**: `https://id.nlm.nih.gov/mesh/lookup/descriptor`
**Detail URL**: `https://id.nlm.nih.gov/mesh/{descriptorId}.json`

MeSH is the NLM's controlled vocabulary for indexing biomedical literature. Each descriptor has a unique ID (e.g., `D003924`), a human-readable name, and one or more positions in the MeSH tree hierarchy (tree numbers).

### API Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /mesh/lookup/descriptor` | Search descriptors by label |
| `GET /mesh/{id}.json` | Fetch full descriptor detail (JSON-LD) |

### Tree Number Hierarchy

MeSH descriptors are organized in a polyhierarchy. Each descriptor can appear in multiple branches:

```
C18               Nutritional and Metabolic Diseases
├── C18.452       Metabolic Diseases
│   ├── C18.452.394     Glucose Metabolism Disorders
│   │   └── C18.452.394.750   Diabetes Mellitus
│   │       ├── C18.452.394.750.149   Diabetes Mellitus, Type 1
│   │       └── C18.452.394.750.601   Diabetes Mellitus, Type 2
```

The parent tree number is derived by removing the last dotted segment (e.g., `C18.452.394.750` -> `C18.452.394`).

### Example: Look up a condition in MeSH

```python
import httpx

# Step 1: Search for a descriptor
resp = httpx.get("https://id.nlm.nih.gov/mesh/lookup/descriptor", params={
    "label": "Type 2 Diabetes",
    "match": "contains",
    "limit": 5,
})
results = resp.json()
descriptor_uri = results[0]["resource"]   # e.g., "http://id.nlm.nih.gov/mesh/D003924"
descriptor_id = descriptor_uri.split("/")[-1]  # "D003924"

# Step 2: Fetch full detail
resp = httpx.get(f"https://id.nlm.nih.gov/mesh/{descriptor_id}.json")
detail = resp.json()
name = detail["label"]["@value"]
tree_numbers = [t["@id"].split("/")[-1] for t in detail.get("treeNumber", [])]
print(f"{descriptor_id}: {name} -- trees: {tree_numbers}")
```

### Nodes and Edges Created

- **MeSHDescriptor** -- keyed on `descriptor_id`, with `name`, `tree_numbers`, `scope_note`
- **CODED_AS_MESH** edges -- from Condition to its best-matching MeSHDescriptor
- **BROADER_THAN** edges -- parent descriptor to child descriptor (hierarchy)

### Rate Limits

No official rate limit, but the ETL uses a 350ms delay between requests to be courteous to NLM servers.


## 3. RxNorm -- NLM

**Base URL**: `https://rxnav.nlm.nih.gov/REST`

RxNorm provides normalized drug names and unique concept identifiers (RxCUI). The ETL also extracts ATC (Anatomical Therapeutic Chemical) classification codes from RxNorm properties.

### API Endpoints

| Endpoint | Purpose | Example |
|---|---|---|
| `GET /drugs.json?name={name}` | Search for a drug by name | `/drugs.json?name=Metformin` |
| `GET /rxcui/{rxcui}/allProperties.json` | All properties for an RxCUI | `/rxcui/6809/allProperties.json?prop=all` |
| `GET /rxcui/{rxcui}/allrelated.json` | Related concepts | `/rxcui/6809/allrelated.json` |

### RxCUI Lookup

The drug search returns concept groups containing concept properties. The ETL takes the first `rxcui` from the first non-empty concept group:

```python
import httpx

resp = httpx.get("https://rxnav.nlm.nih.gov/REST/drugs.json", params={"name": "Metformin"})
data = resp.json()
for group in data["drugGroup"]["conceptGroup"]:
    props = group.get("conceptProperties", [])
    if props:
        rxcui = props[0]["rxcui"]
        name = props[0]["name"]
        print(f"RxCUI: {rxcui}, Name: {name}")
        break
```

### ATC Classification

ATC codes are extracted from RxNorm properties (under `ATC1_CODE` or `ATC`). The code encodes a 5-level hierarchy based on string length:

| Level | Length | Example | Description |
|---|---|---|---|
| 1 | 1 char | `A` | Anatomical main group (Alimentary tract) |
| 2 | 3 chars | `A10` | Therapeutic subgroup (Drugs used in diabetes) |
| 3 | 4 chars | `A10B` | Pharmacological subgroup (Blood glucose lowering drugs) |
| 4 | 5 chars | `A10BA` | Chemical subgroup (Biguanides) |
| 5 | 7 chars | `A10BA02` | Chemical substance (Metformin) |

The ETL derives the parent at each level by truncating the code to the parent length and creates PARENT_CLASS edges between DrugClass nodes.

### Nodes and Edges Created

- **Drug** -- keyed on `rxnorm_cui`, with `name`, `drugbank_id`
- **DrugClass** -- keyed on `atc_code`, with `name`, `level`
- **CODED_AS_DRUG** -- Intervention to Drug
- **CLASSIFIED_AS** -- Drug to its most specific DrugClass (level 5)
- **PARENT_CLASS** -- child DrugClass to parent DrugClass

### Rate Limits

No official rate limit. The ETL uses a 250ms delay between requests.


## 4. OpenFDA -- FDA

**Adverse Events URL**: `https://api.fda.gov/drug/event.json`

OpenFDA provides access to the FDA Adverse Event Reporting System (FAERS). The ETL queries for the top adverse event terms associated with each drug.

### Query Structure

OpenFDA uses a search + count pattern to aggregate adverse event reports:

```python
import httpx

resp = httpx.get("https://api.fda.gov/drug/event.json", params={
    "search": 'patient.drug.openfda.generic_name:"Metformin"',
    "count": "patient.reaction.reactionmeddrapt.exact",
    "limit": 10,
})
data = resp.json()
for result in data["results"]:
    print(f"  {result['term']}: {result['count']} reports")
```

### Key Fields

| Field | Description |
|---|---|
| `patient.drug.openfda.generic_name` | Drug generic name (search field) |
| `patient.drug.openfda.brand_name` | Drug brand name (search field) |
| `patient.reaction.reactionmeddrapt` | Adverse reaction term (MedDRA preferred term) |

### Rate Limits

- **Without API key**: 240 requests per minute, 1000 per day
- **With API key**: 120,000 requests per day

The ETL uses a 500ms delay between requests. OpenFDA returns HTTP 404 (not an error) when no results exist for a drug, which the ETL handles gracefully.

### Nodes and Edges Created

- **AdverseEvent** -- keyed on `term`, with `source_vocabulary: 'MedDRA'`
- **HAS_ADVERSE_EFFECT** -- Drug to AdverseEvent


## 5. PubMed E-utilities -- NLM

**ESearch URL**: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi`
**ESummary URL**: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi`

The ETL links clinical trials to their published results by searching PubMed for articles that reference the trial's NCT ID as a secondary source identifier.

### Search Strategy

ClinicalTrials.gov NCT IDs are indexed in PubMed under the `[si]` (Secondary Source ID) field:

```python
import httpx

# Step 1: Search for PMIDs linked to a trial
resp = httpx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi", params={
    "db": "pubmed",
    "term": "NCT04280705[si]",
    "retmode": "json",
    "retmax": 20,
})
data = resp.json()
pmids = data["esearchresult"]["idlist"]
print(f"Found {len(pmids)} publications: {pmids}")

# Step 2: Fetch article metadata
if pmids:
    resp = httpx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi", params={
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
    })
    summaries = resp.json()["result"]
    for uid in summaries.get("uids", []):
        article = summaries[uid]
        title = article.get("title", "")
        journal = article.get("fulljournalname", "")
        pub_date = article.get("pubdate", "")
        # Extract DOI
        doi = ""
        for aid in article.get("articleids", []):
            if aid.get("idtype") == "doi":
                doi = aid["value"]
                break
        print(f"  PMID {uid}: {title[:80]}...")
        print(f"    {journal}, {pub_date}, doi:{doi}")
```

### Fields Extracted

| Field | Source | Description |
|---|---|---|
| `pmid` | uid from esummary | PubMed unique identifier |
| `title` | `title` | Article title |
| `authors` | `authors[].name` | Semicolon-separated author names |
| `journal` | `fulljournalname` | Full journal name |
| `pub_date` | `pubdate` | Publication date string |
| `doi` | `articleids` where `idtype=doi` | Digital Object Identifier |

### Rate Limits

- **Without API key**: 3 requests per second
- **With API key** (`NCBI_API_KEY`): 10 requests per second

Register at [NCBI](https://www.ncbi.nlm.nih.gov/account/) to get a free API key. Set it as an environment variable:

```bash
export NCBI_API_KEY="your_key_here"
```

Or pass it to the ETL:

```bash
python -m etl.loader --pubmed-api-key "your_key_here"
```

### Nodes and Edges Created

- **Publication** -- keyed on `pmid`, with `title`, `authors`, `journal`, `pub_date`, `doi`
- **PUBLISHED_IN** -- ClinicalTrial to Publication
- **DESCRIBES** -- Publication to ClinicalTrial (reverse edge for bidirectional traversal)
