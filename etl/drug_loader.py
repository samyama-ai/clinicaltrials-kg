"""
Drug Loader — RxNorm + ATC + OpenFDA
======================================
Normalizes drug interventions via RxNorm, classifies them via ATC codes,
and enriches with FDA adverse event data. Creates:
  - Drug nodes (rxnorm_cui, name, drugbank_id, mechanism_of_action)
  - DrugClass nodes (atc_code, name, level)
  - CODED_AS_DRUG edges (Intervention -> Drug)
  - CLASSIFIED_AS edges (Drug -> DrugClass)
  - PARENT_CLASS edges (DrugClass -> DrugClass)
  - AdverseEvent nodes (term, organ_system, source_vocabulary)
  - HAS_ADVERSE_EFFECT edges (Drug -> AdverseEvent)

API endpoints:
  - RxNorm search:     https://rxnav.nlm.nih.gov/REST/drugs.json?name=...
  - RxNorm properties: https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allProperties.json
  - RxNorm related:    https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allrelated.json
  - OpenFDA events:    https://api.fda.gov/drug/event.json
"""

from samyama import SamyamaClient
import httpx
import time
import json
from pathlib import Path

RXNORM_BASE = "https://rxnav.nlm.nih.gov/REST"
OPENFDA_EVENT_URL = "https://api.fda.gov/drug/event.json"

# Rate limits: RxNorm is generous but be polite; OpenFDA has 240/min without key
RXNORM_DELAY = 0.25
OPENFDA_DELAY = 0.5

TENANT = "default"


def _escape(value: str) -> str:
    """Escape single quotes for Cypher string literals."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


# ---------------------------------------------------------------------------
# RxNorm helpers
# ---------------------------------------------------------------------------

def _search_rxnorm(http: httpx.Client, drug_name: str) -> str | None:
    """Search RxNorm for a drug name and return the best rxcui, or None."""
    try:
        resp = http.get(
            f"{RXNORM_BASE}/drugs.json",
            params={"name": drug_name},
        )
        resp.raise_for_status()
        data = resp.json()

        # Navigate the nested RxNorm response
        concept_groups = (
            data.get("drugGroup", {}).get("conceptGroup", [])
        )
        for group in concept_groups:
            props = group.get("conceptProperties", [])
            if props:
                return props[0].get("rxcui")
        return None
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError, KeyError) as exc:
        print(f"  [WARN] RxNorm search failed for '{drug_name}': {exc}")
        return None


def _fetch_rxnorm_properties(http: httpx.Client, rxcui: str) -> dict:
    """Fetch all properties for an rxcui. Returns a dict of propName -> propValue."""
    props = {}
    try:
        resp = http.get(
            f"{RXNORM_BASE}/rxcui/{rxcui}/allProperties.json",
            params={"prop": "all"},
        )
        resp.raise_for_status()
        data = resp.json()
        prop_list = (
            data.get("propConceptGroup", {}).get("propConcept", [])
        )
        for p in prop_list:
            props[p.get("propName", "")] = p.get("propValue", "")
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
        print(f"  [WARN] RxNorm properties fetch failed for rxcui={rxcui}: {exc}")
    return props


def _fetch_rxnorm_name(http: httpx.Client, rxcui: str) -> str:
    """Fetch the display name for an rxcui."""
    try:
        resp = http.get(f"{RXNORM_BASE}/rxcui/{rxcui}/allProperties.json", params={"prop": "names"})
        resp.raise_for_status()
        data = resp.json()
        props = data.get("propConceptGroup", {}).get("propConcept", [])
        for p in props:
            if p.get("propName") == "RxNorm Name":
                return p.get("propValue", "")
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError):
        pass
    return ""


def _extract_atc_code(properties: dict) -> str | None:
    """Extract the ATC code from RxNorm properties, if present."""
    # RxNorm stores ATC codes under the 'ATC' property name
    atc = properties.get("ATC1_CODE") or properties.get("ATC")
    if atc:
        return atc.strip()
    return None


# ---------------------------------------------------------------------------
# ATC hierarchy helpers
# ---------------------------------------------------------------------------

# ATC code levels:
#   Level 1: A        (1 char)  — Anatomical main group
#   Level 2: A10      (3 chars) — Therapeutic subgroup
#   Level 3: A10B     (4 chars) — Pharmacological subgroup
#   Level 4: A10BA    (5 chars) — Chemical subgroup
#   Level 5: A10BA02  (7 chars) — Chemical substance

ATC_LEVEL_LENGTHS = {1: 1, 2: 3, 3: 4, 4: 5, 5: 7}
ATC_LEVEL_NAMES = {
    1: "Anatomical main group",
    2: "Therapeutic subgroup",
    3: "Pharmacological subgroup",
    4: "Chemical subgroup",
    5: "Chemical substance",
}


def _atc_level(code: str) -> int | None:
    """Determine the ATC level from code length."""
    length = len(code)
    for level, expected_len in ATC_LEVEL_LENGTHS.items():
        if length == expected_len:
            return level
    return None


def _atc_parent(code: str) -> str | None:
    """Derive the parent ATC code by trimming to the next higher level."""
    level = _atc_level(code)
    if level is None or level <= 1:
        return None
    parent_len = ATC_LEVEL_LENGTHS[level - 1]
    return code[:parent_len]


def _create_atc_hierarchy(
    client: SamyamaClient,
    atc_code: str,
    drug_name: str,
    seen_classes: set[str],
) -> None:
    """Create DrugClass nodes for each ATC level and PARENT_CLASS edges between them.

    Also creates the CLASSIFIED_AS edge from the Drug to its most specific DrugClass.
    """
    # Build the chain from level 5 down to level 1
    current = atc_code
    child_code = None

    while current:
        level = _atc_level(current)
        if level is None:
            break

        if current not in seen_classes:
            level_name = ATC_LEVEL_NAMES.get(level, "")
            # For the leaf level, use the drug name; otherwise use the level description
            display_name = drug_name if level == 5 else f"{current} ({level_name})"
            query = (
                f"MERGE (dc:DrugClass {{atc_code: '{_escape(current)}'}}) "
                f"SET dc.name = '{_escape(display_name)}', dc.level = {level}"
            )
            client.query(TENANT, query)
            seen_classes.add(current)

        # Create PARENT_CLASS edge from child to parent (child -> parent)
        if child_code is not None:
            query = (
                f"MATCH (child:DrugClass {{atc_code: '{_escape(child_code)}'}}), "
                f"(parent:DrugClass {{atc_code: '{_escape(current)}'}}) "
                f"MERGE (child)-[:PARENT_CLASS]->(parent)"
            )
            client.query(TENANT, query)

        child_code = current
        current = _atc_parent(current)


# ---------------------------------------------------------------------------
# OpenFDA adverse events
# ---------------------------------------------------------------------------

def _fetch_adverse_events(http: httpx.Client, drug_name: str, limit: int = 10) -> list[dict]:
    """Query OpenFDA for top adverse events associated with a drug.

    Returns a list of dicts with 'term' and 'count' keys.
    """
    try:
        resp = http.get(
            OPENFDA_EVENT_URL,
            params={
                "search": f'patient.drug.openfda.generic_name:"{drug_name}"',
                "count": "patient.reaction.reactionmeddrapt.exact",
                "limit": limit,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        return [{"term": r.get("term", ""), "count": r.get("count", 0)} for r in results]
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
        # OpenFDA returns 404 when no results found — not an error
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
            return []
        print(f"  [WARN] OpenFDA query failed for '{drug_name}': {exc}")
        return []


def _create_adverse_events(
    client: SamyamaClient,
    rxnorm_cui: str,
    events: list[dict],
    seen_events: set[str],
) -> int:
    """Create AdverseEvent nodes and HAS_ADVERSE_EFFECT edges from Drug.

    Returns the number of edges created.
    """
    count = 0
    for event in events:
        term = event["term"]
        if not term:
            continue

        # Create or merge AdverseEvent node
        if term not in seen_events:
            query = (
                f"MERGE (ae:AdverseEvent {{term: '{_escape(term)}'}}) "
                f"SET ae.source_vocabulary = 'MedDRA'"
            )
            client.query(TENANT, query)
            seen_events.add(term)

        # Create HAS_ADVERSE_EFFECT edge
        query = (
            f"MATCH (d:Drug {{rxnorm_cui: '{_escape(rxnorm_cui)}'}}), "
            f"(ae:AdverseEvent {{term: '{_escape(term)}'}}) "
            f"MERGE (d)-[:HAS_ADVERSE_EFFECT]->(ae)"
        )
        client.query(TENANT, query)
        count += 1

    return count


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_drugs(client: SamyamaClient) -> dict:
    """Normalize drug interventions via RxNorm and enrich with ATC and OpenFDA data.

    Steps:
      1. Query graph for Intervention nodes with type='DRUG' lacking rxnorm_cui
      2. Search RxNorm for each drug name to get rxcui
      3. Create Drug node and CODED_AS_DRUG edge
      4. Look up ATC code, build DrugClass hierarchy
      5. Query OpenFDA for adverse events, create AdverseEvent nodes

    Returns a summary dict with counts.
    """
    print("[Drug Loader] Starting...")

    # Step 1: Find drug interventions without RxNorm mapping
    rows = client.query_readonly(
        TENANT,
        "MATCH (i:Intervention) WHERE i.type = 'DRUG' AND i.rxnorm_cui IS NULL RETURN i.name",
    )
    drug_names = [row[0] for row in rows if row[0]]
    print(f"[Drug Loader] Found {len(drug_names)} unmapped drug interventions")

    stats = {
        "drugs_processed": 0,
        "drug_nodes_created": 0,
        "atc_classes_created": 0,
        "adverse_events_linked": 0,
        "skipped": 0,
    }
    seen_drugs: set[str] = set()         # rxcui values already created
    seen_classes: set[str] = set()       # ATC codes already created
    seen_events: set[str] = set()        # AdverseEvent terms already created

    with httpx.Client(timeout=30.0) as http:
        for drug_name in drug_names:
            print(f"  Processing: {drug_name}")

            # Step 2: Search RxNorm
            time.sleep(RXNORM_DELAY)
            rxcui = _search_rxnorm(http, drug_name)
            if rxcui is None:
                print(f"    No RxNorm match found, skipping")
                stats["skipped"] += 1
                continue

            # Step 3a: Fetch properties to get additional info
            time.sleep(RXNORM_DELAY)
            properties = _fetch_rxnorm_properties(http, rxcui)
            drugbank_id = properties.get("DRUGBANK_ID", "")

            # Step 3b: Create Drug node
            if rxcui not in seen_drugs:
                rx_name = _fetch_rxnorm_name(http, rxcui) or drug_name
                time.sleep(RXNORM_DELAY)
                query = (
                    f"MERGE (d:Drug {{rxnorm_cui: '{_escape(rxcui)}'}}) "
                    f"SET d.name = '{_escape(rx_name)}'"
                )
                if drugbank_id:
                    query += f", d.drugbank_id = '{_escape(drugbank_id)}'"
                client.query(TENANT, query)
                seen_drugs.add(rxcui)
                stats["drug_nodes_created"] += 1

            # Step 3c: Create CODED_AS_DRUG edge from Intervention to Drug
            query = (
                f"MATCH (i:Intervention {{name: '{_escape(drug_name)}'}}), "
                f"(d:Drug {{rxnorm_cui: '{_escape(rxcui)}'}}) "
                f"MERGE (i)-[:CODED_AS_DRUG]->(d)"
            )
            client.query(TENANT, query)

            # Step 3d: Update Intervention with rxnorm_cui
            query = (
                f"MATCH (i:Intervention {{name: '{_escape(drug_name)}'}}) "
                f"SET i.rxnorm_cui = '{_escape(rxcui)}'"
            )
            client.query(TENANT, query)

            # Step 4: ATC classification
            atc_code = _extract_atc_code(properties)
            if atc_code:
                initial_class_count = len(seen_classes)
                _create_atc_hierarchy(client, atc_code, drug_name, seen_classes)
                stats["atc_classes_created"] += len(seen_classes) - initial_class_count

                # Create CLASSIFIED_AS edge from Drug to its leaf DrugClass
                query = (
                    f"MATCH (d:Drug {{rxnorm_cui: '{_escape(rxcui)}'}}), "
                    f"(dc:DrugClass {{atc_code: '{_escape(atc_code)}'}}) "
                    f"MERGE (d)-[:CLASSIFIED_AS]->(dc)"
                )
                client.query(TENANT, query)
            else:
                print(f"    No ATC code found for rxcui={rxcui}")

            # Step 5: OpenFDA adverse events
            time.sleep(OPENFDA_DELAY)
            events = _fetch_adverse_events(http, drug_name)
            if events:
                ae_count = _create_adverse_events(client, rxcui, events, seen_events)
                stats["adverse_events_linked"] += ae_count
                print(f"    Linked {ae_count} adverse events")
            else:
                print(f"    No adverse events found in OpenFDA")

            stats["drugs_processed"] += 1

    print(f"[Drug Loader] Done. {stats}")
    return stats
