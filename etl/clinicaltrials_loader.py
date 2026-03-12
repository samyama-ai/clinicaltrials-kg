"""
ClinicalTrials.gov ETL Loader for Samyama Graph Database.

Fetches clinical trial data from the ClinicalTrials.gov API v2 and loads it
into a Samyama property graph with deduplication, caching, and pagination.

Usage:
    from samyama import SamyamaClient
    from etl.clinicaltrials_loader import load_trials

    client = SamyamaClient.embedded()
    stats = load_trials(client, conditions=["Type 2 Diabetes"], max_trials=500)
"""

from samyama import SamyamaClient
import requests
import json
import os
import time
from pathlib import Path

API_BASE = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE = 200
REQ_INTERVAL = 0.12  # ~8 req/sec, safely under the 10/s limit
CACHE_DIR = Path("data/cache")
GRAPH = "default"


def _escape(value: str) -> str:
    """Sanitize a string for Cypher double-quoted literals.

    Samyama's parser has no escape sequences in strings, so we strip
    double quotes and normalize whitespace.
    """
    if value is None:
        return ""
    return value.replace('"', '').replace("\n", " ").replace("\r", "")


def _prop_str(props: dict) -> str:
    """Build a Cypher property map literal, skipping None values."""
    parts = []
    for key, val in props.items():
        if val is None:
            continue
        if isinstance(val, bool):
            parts.append(f'{key}: "{"true" if val else "false"}"')
        elif isinstance(val, (int, float)):
            parts.append(f"{key}: {val}")
        else:
            parts.append(f'{key}: "{_escape(str(val))}"')
    return "{" + ", ".join(parts) + "}"


# -- API fetching with disk cache -------------------------------------------

def _cache_path(condition: str, page_token: str | None) -> Path:
    safe = condition.lower().replace(" ", "_").replace("/", "_")
    token = page_token[:20] if page_token else "first"
    return CACHE_DIR / f"{safe}_{token}.json"


def _fetch_page(session: requests.Session, condition: str, page_size: int,
                page_token: str | None, include_results: bool) -> dict:
    cache = _cache_path(condition, page_token)
    if cache.exists():
        with open(cache) as f:
            return json.load(f)

    params: dict = {"query.cond": condition, "pageSize": page_size, "format": "json"}
    if include_results:
        params["fields"] = "protocolSection,resultsSection,derivedSection,hasResults"
    else:
        params["fields"] = "protocolSection,derivedSection,hasResults"
    if page_token:
        params["pageToken"] = page_token

    resp = session.get(API_BASE, params=params, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache, "w") as f:
        json.dump(data, f)
    time.sleep(REQ_INTERVAL)
    return data


def fetch_studies(condition: str, max_trials: int, include_results: bool) -> list[dict]:
    """Fetch up to *max_trials* studies for a condition, handling pagination."""
    studies: list[dict] = []
    page_token: str | None = None
    with requests.Session() as session:
        while len(studies) < max_trials:
            data = _fetch_page(session, condition, min(PAGE_SIZE, max_trials - len(studies)),
                               page_token, include_results)
            batch = data.get("studies", [])
            if not batch:
                break
            studies.extend(batch)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
    return studies[:max_trials]


# -- Deduplication registry --------------------------------------------------

class _Registry:
    """Case-insensitive name -> graph node id map for deduplication."""
    def __init__(self):
        self._map: dict[str, int] = {}

    def get(self, key: str) -> int | None:
        return self._map.get(key.lower())

    def put(self, key: str, node_id: int) -> None:
        self._map[key.lower()] = node_id

    def __len__(self) -> int:
        return len(self._map)


# -- Node helpers ------------------------------------------------------------

def _q(val: str) -> str:
    """Quote and escape a value for inline Cypher property matching."""
    return f'"{_escape(val)}"'


def _merge_node(client: SamyamaClient, label: str, props: dict) -> None:
    """MERGE a node with the given properties (first prop is the key)."""
    client.query(f"MERGE (n:{label} {_prop_str(props)})", GRAPH)


def _merge_edge(client: SamyamaClient, src_label: str, src_match: str,
                rel: str, tgt_label: str, tgt_match: str) -> None:
    """Create edge between two nodes matched by property predicates."""
    q = (f"MATCH (a:{src_label} {{{src_match}}}), (b:{tgt_label} {{{tgt_match}}}) "
         f"CREATE (a)-[:{rel}]->(b)")
    client.query(q, GRAPH)


def _create_trial(client: SamyamaClient, study: dict) -> str | None:
    """MERGE a ClinicalTrial node. Returns nct_id or None."""
    proto = study.get("protocolSection", {})
    ident = proto.get("identificationModule", {})
    status = proto.get("statusModule", {})
    design = proto.get("designModule", {})
    nct_id = ident.get("nctId", "")
    if not nct_id:
        return None

    phases = design.get("phases") or design.get("phase") or []
    phase = ", ".join(phases) if isinstance(phases, list) else str(phases)
    enroll = design.get("enrollmentInfo", {})
    enrollment = enroll.get("count") if isinstance(enroll, dict) else (int(enroll) if enroll else None)

    props = {
        "nct_id": nct_id,
        "title": ident.get("briefTitle"),
        "official_title": ident.get("officialTitle"),
        "brief_summary": (proto.get("descriptionModule", {}).get("briefSummary") or "")[:500],
        "study_type": design.get("studyType"),
        "phase": phase or None,
        "overall_status": status.get("overallStatus"),
        "enrollment": enrollment,
        "start_date": status.get("startDateStruct", {}).get("date"),
        "completion_date": status.get("completionDateStruct", {}).get("date"),
        "primary_completion_date": status.get("primaryCompletionDateStruct", {}).get("date"),
        "last_updated": status.get("lastUpdatePostDateStruct", {}).get("date"),
        "has_results": str(study.get("hasResults", False)).lower(),
        "why_stopped": status.get("whyStopped"),
    }
    _merge_node(client, "ClinicalTrial", props)
    return nct_id


def _trial_match(nct_id: str) -> str:
    return f'nct_id: "{_escape(nct_id)}"'


def _ensure_condition(client, seen: set, name: str) -> None:
    if not name or name.lower() in seen:
        return
    _merge_node(client, "Condition", {"name": name})
    seen.add(name.lower())


def _ensure_intervention(client, seen: set, interv: dict) -> None:
    name = interv.get("name", "")
    if not name or name.lower() in seen:
        return
    _merge_node(client, "Intervention", {
        "name": name, "type": interv.get("type"),
        "description": (interv.get("description") or "")[:300],
    })
    seen.add(name.lower())


def _ensure_sponsor(client, seen: set, sponsor: dict) -> None:
    name = sponsor.get("name", "")
    if not name or name.lower() in seen:
        return
    _merge_node(client, "Sponsor", {"name": name, "class": sponsor.get("class")})
    seen.add(name.lower())


def _ensure_site(client, seen: set, loc: dict) -> None:
    facility = loc.get("facility", "")
    city = loc.get("city", "")
    key = f"{facility}|{city}".strip("|").lower()
    if not key or key in seen:
        return
    geo = loc.get("geoPoint", {}) or {}
    _merge_node(client, "Site", {
        "facility": facility or None, "city": city or None,
        "state": loc.get("state"), "country": loc.get("country"),
        "zip": loc.get("zip"),
        "latitude": geo.get("lat"), "longitude": geo.get("lon"),
    })
    seen.add(key)


# -- Per-study ingestion -----------------------------------------------------

def _ingest_study(client, study, cond_seen, interv_seen, sponsor_seen, site_seen,
                  include_results, counts):
    nct_id = _create_trial(client, study)
    if nct_id is None:
        return
    counts["trials"] += 1
    proto = study.get("protocolSection", {})
    tm = _trial_match(nct_id)

    # Conditions
    for name in proto.get("conditionsModule", {}).get("conditions", []):
        if not name:
            continue
        _ensure_condition(client, cond_seen, name)
        _merge_edge(client, "ClinicalTrial", tm, "STUDIES", "Condition", f"name: {_q(name)}")
        counts["edges"] += 1

    # Arms and interventions
    arms_mod = proto.get("armsInterventionsModule", {})
    for arm in arms_mod.get("armGroups", []):
        label = arm.get("label", "")
        if not label:
            continue
        arm_props = {"label": label, "type": arm.get("type"),
                     "description": (arm.get("description") or "")[:300],
                     "trial_nct_id": nct_id}
        _merge_node(client, "ArmGroup", arm_props)
        counts["arms"] += 1
        _merge_edge(client, "ClinicalTrial", tm, "HAS_ARM", "ArmGroup",
                    f"label: {_q(label)}, trial_nct_id: {_q(nct_id)}")
        counts["edges"] += 1

    for interv in arms_mod.get("interventions", []):
        iname = interv.get("name", "")
        if not iname:
            continue
        _ensure_intervention(client, interv_seen, interv)
        _merge_edge(client, "ClinicalTrial", tm, "TESTS", "Intervention", f"name: {_q(iname)}")
        counts["edges"] += 1
        for lbl in interv.get("armGroupLabels", []):
            _merge_edge(client, "ArmGroup",
                        f"label: {_q(lbl)}, trial_nct_id: {_q(nct_id)}",
                        "USES", "Intervention", f"name: {_q(iname)}")
            counts["edges"] += 1

    # Sponsor
    lead = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
    if lead and lead.get("name"):
        _ensure_sponsor(client, sponsor_seen, lead)
        _merge_edge(client, "ClinicalTrial", tm, "SPONSORED_BY", "Sponsor",
                    f"name: {_q(lead['name'])}")
        counts["edges"] += 1

    # Sites
    for loc in proto.get("contactsLocationsModule", {}).get("locations", []):
        facility = loc.get("facility", "")
        city = loc.get("city", "")
        if not facility and not city:
            continue
        _ensure_site(client, site_seen, loc)
        if facility:
            site_match = f"facility: {_q(facility)}"
        else:
            site_match = f"city: {_q(city)}"
        _merge_edge(client, "ClinicalTrial", tm, "CONDUCTED_AT", "Site", site_match)
        counts["edges"] += 1

    # Outcomes
    outcomes_mod = proto.get("outcomesModule", {})
    for oc in outcomes_mod.get("primaryOutcomes", []):
        measure = oc.get("measure", "")
        if not measure:
            continue
        _merge_node(client, "Outcome", {"measure": measure, "type": "PRIMARY",
                    "description": (oc.get("description") or "")[:300],
                    "time_frame": oc.get("timeFrame"), "trial_nct_id": nct_id})
        counts["outcomes"] += 1
        _merge_edge(client, "ClinicalTrial", tm, "MEASURES", "Outcome",
                    f"measure: {_q(measure)}, trial_nct_id: {_q(nct_id)}")
        counts["edges"] += 1
    for oc in outcomes_mod.get("secondaryOutcomes", []):
        measure = oc.get("measure", "")
        if not measure:
            continue
        _merge_node(client, "Outcome", {"measure": measure, "type": "SECONDARY",
                    "description": (oc.get("description") or "")[:300],
                    "time_frame": oc.get("timeFrame"), "trial_nct_id": nct_id})
        counts["outcomes"] += 1
        _merge_edge(client, "ClinicalTrial", tm, "MEASURES", "Outcome",
                    f"measure: {_q(measure)}, trial_nct_id: {_q(nct_id)}")
        counts["edges"] += 1

    # Adverse events (results section)
    if include_results and study.get("hasResults"):
        ae_mod = study.get("resultsSection", {}).get("adverseEventsModule", {})
        for eg in ae_mod.get("seriousEvents", []):
            term = eg.get("term", "")
            if not term:
                continue
            _merge_node(client, "AdverseEvent", {"term": term,
                        "organ_system": eg.get("organSystem", ""),
                        "source_vocabulary": "MedDRA"})
            counts["adverse_events"] += 1
            _merge_edge(client, "ClinicalTrial", tm, "REPORTED", "AdverseEvent",
                        f"term: {_q(term)}")
            counts["edges"] += 1
        for eg in ae_mod.get("otherEvents", []):
            term = eg.get("term", "")
            if not term:
                continue
            _merge_node(client, "AdverseEvent", {"term": term,
                        "organ_system": eg.get("organSystem", ""),
                        "source_vocabulary": "MedDRA"})
            counts["adverse_events"] += 1
            _merge_edge(client, "ClinicalTrial", tm, "REPORTED", "AdverseEvent",
                        f"term: {_q(term)}")
            counts["edges"] += 1


# -- Public entry point ------------------------------------------------------

def load_trials(
    client: SamyamaClient,
    conditions: list[str],
    max_trials: int = 1000,
    include_results: bool = False,
) -> dict:
    """
    Fetch clinical trials from ClinicalTrials.gov and load them into Samyama.

    Args:
        client: A SamyamaClient instance (embedded or remote).
        conditions: Disease/condition search terms to query for.
        max_trials: Maximum number of trials to load per condition.
        include_results: Whether to load adverse events from results sections.

    Returns:
        Dict of entity and relationship counts.
    """
    cond_seen, interv_seen, sponsor_seen, site_seen = set(), set(), set(), set()
    counts = {"trials": 0, "conditions": 0, "interventions": 0, "sponsors": 0,
              "sites": 0, "arms": 0, "outcomes": 0, "adverse_events": 0, "edges": 0}
    t0 = time.time()

    for term in conditions:
        print(f"\n{'='*60}\nFetching trials for: {term}\n{'='*60}")
        try:
            studies = fetch_studies(term, max_trials, include_results)
        except requests.RequestException as exc:
            print(f"  ERROR fetching '{term}': {exc}")
            continue

        print(f"  Retrieved {len(studies)} studies from API")
        for idx, study in enumerate(studies):
            try:
                _ingest_study(client, study, cond_seen, interv_seen,
                              sponsor_seen, site_seen, include_results, counts)
            except Exception as exc:
                nct = (study.get("protocolSection", {})
                       .get("identificationModule", {}).get("nctId", "unknown"))
                print(f"  WARNING: failed to ingest {nct}: {exc}")
                continue
            if (idx + 1) % 50 == 0:
                print(f"  ... ingested {idx + 1}/{len(studies)} studies")
        print(f"  Done — {len(studies)} studies processed for '{term}'")

    counts["conditions"] = len(cond_seen)
    counts["interventions"] = len(interv_seen)
    counts["sponsors"] = len(sponsor_seen)
    counts["sites"] = len(site_seen)
    elapsed = time.time() - t0

    print(f"\n{'='*60}\nLoad complete\n{'='*60}")
    for label in ("trials", "conditions", "interventions", "sponsors",
                  "sites", "arms", "outcomes", "adverse_events", "edges"):
        print(f"  {label:<17s} {counts[label]}")
    print(f"  {'elapsed':<17s} {elapsed:.1f}s")
    return counts


# -- CLI convenience ---------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Load ClinicalTrials.gov data into Samyama")
    ap.add_argument("--conditions", nargs="+", required=True,
                    help="Disease/condition search terms")
    ap.add_argument("--max-trials", type=int, default=1000)
    ap.add_argument("--include-results", action="store_true",
                    help="Include adverse events from results section")
    ap.add_argument("--url", type=str, default=None,
                    help="Samyama server URL (omit for embedded mode)")
    args = ap.parse_args()

    c = SamyamaClient.connect(args.url) if args.url else SamyamaClient.embedded()
    load_trials(c, conditions=args.conditions, max_trials=args.max_trials,
                include_results=args.include_results)
