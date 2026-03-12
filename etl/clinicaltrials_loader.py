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
import httpx
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
    """Escape a string for safe embedding in a Cypher literal."""
    if value is None:
        return ""
    return (value.replace("\\", "\\\\").replace("'", "\\'")
            .replace("\n", " ").replace("\r", ""))


def _prop_str(props: dict) -> str:
    """Build a Cypher property map literal, skipping None values."""
    parts = []
    for key, val in props.items():
        if val is None:
            continue
        if isinstance(val, bool):
            parts.append(f"{key}: {'true' if val else 'false'}")
        elif isinstance(val, (int, float)):
            parts.append(f"{key}: {val}")
        else:
            parts.append(f"{key}: '{_escape(str(val))}'")
    return "{" + ", ".join(parts) + "}"


def _extract_id(result) -> int | None:
    """Pull the node id from a QueryResult returned by client.query()."""
    if result and len(result) > 0:
        row = result.records[0]
        if row:
            return int(row[0])
    return None


# -- API fetching with disk cache -------------------------------------------

def _cache_path(condition: str, page_token: str | None) -> Path:
    safe = condition.lower().replace(" ", "_").replace("/", "_")
    token = page_token[:20] if page_token else "first"
    return CACHE_DIR / f"{safe}_{token}.json"


def _fetch_page(http: httpx.Client, condition: str, page_size: int,
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

    resp = http.get(API_BASE, params=params, timeout=30.0)
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
    with httpx.Client() as http:
        while len(studies) < max_trials:
            data = _fetch_page(http, condition, min(PAGE_SIZE, max_trials - len(studies)),
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

def _create_trial(client: SamyamaClient, study: dict) -> int | None:
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
        "has_results": study.get("hasResults", False),
        "why_stopped": status.get("whyStopped"),
    }
    return _extract_id(client.query(f"CREATE (t:ClinicalTrial {_prop_str(props)}) RETURN id(t)", GRAPH))


def _get_or_create(client: SamyamaClient, reg: _Registry, key: str,
                   label: str, props: dict) -> int | None:
    """Generic get-or-create: return cached id or create node and cache it."""
    if not key:
        return None
    existing = reg.get(key)
    if existing is not None:
        return existing
    nid = _extract_id(client.query(f"CREATE (n:{label} {_prop_str(props)}) RETURN id(n)", GRAPH))
    if nid is not None:
        reg.put(key, nid)
    return nid


def _get_or_create_condition(client, reg, name):
    return _get_or_create(client, reg, name, "Condition", {"name": name})


def _get_or_create_intervention(client, reg, interv: dict):
    name = interv.get("name", "")
    return _get_or_create(client, reg, name, "Intervention", {
        "name": name, "type": interv.get("type"),
        "description": (interv.get("description") or "")[:300],
    })


def _get_or_create_sponsor(client, reg, sponsor: dict):
    name = sponsor.get("name", "")
    return _get_or_create(client, reg, name, "Sponsor", {
        "name": name, "class": sponsor.get("class"),
    })


def _get_or_create_site(client, reg, loc: dict):
    facility, city = loc.get("facility", ""), loc.get("city", "")
    key = f"{facility}|{city}".strip("|")
    geo = loc.get("geoPoint", {}) or {}
    return _get_or_create(client, reg, key, "Site", {
        "facility": facility or None, "city": city or None,
        "state": loc.get("state"), "country": loc.get("country"),
        "zip": loc.get("zip"),
        "latitude": geo.get("lat"), "longitude": geo.get("lon"),
    })


def _create_arm(client, arm: dict) -> int | None:
    if not arm.get("label"):
        return None
    props = {"label": arm["label"], "type": arm.get("type"),
             "description": (arm.get("description") or "")[:300]}
    return _extract_id(client.query(f"CREATE (a:ArmGroup {_prop_str(props)}) RETURN id(a)", GRAPH))


def _create_outcome(client, outcome: dict, otype: str) -> int | None:
    if not outcome.get("measure"):
        return None
    props = {"measure": outcome["measure"], "type": otype,
             "description": (outcome.get("description") or "")[:300],
             "time_frame": outcome.get("timeFrame")}
    return _extract_id(client.query(f"CREATE (o:Outcome {_prop_str(props)}) RETURN id(o)", GRAPH))


def _create_ae(client, event: dict, organ_system: str, is_serious: bool) -> int | None:
    term = event.get("term", "")
    if not term:
        return None
    stats = event.get("stats", []) or []
    affected = stats[0].get("numAffected") if stats else None
    at_risk = stats[0].get("numAtRisk") if stats else None
    freq = round(int(affected) / int(at_risk), 4) if affected and at_risk and int(at_risk) > 0 else None
    props = {"term": term, "organ_system": organ_system, "source_vocabulary": "MedDRA",
             "num_affected": affected, "num_at_risk": at_risk,
             "frequency": freq, "is_serious": is_serious}
    return _extract_id(client.query(f"CREATE (ae:AdverseEvent {_prop_str(props)}) RETURN id(ae)", GRAPH))


def _edge(client, src: int, rel: str, tgt: int) -> None:
    client.query(f"MATCH (a),(b) WHERE id(a) = {src} AND id(b) = {tgt} CREATE (a)-[:{rel}]->(b)", GRAPH)


# -- Per-study ingestion -----------------------------------------------------

def _ingest_study(client, study, cond_reg, interv_reg, sponsor_reg, site_reg,
                  include_results, counts):
    trial_id = _create_trial(client, study)
    if trial_id is None:
        return
    counts["trials"] += 1
    proto = study.get("protocolSection", {})

    # Conditions
    for name in proto.get("conditionsModule", {}).get("conditions", []):
        cid = _get_or_create_condition(client, cond_reg, name)
        if cid is not None:
            _edge(client, trial_id, "STUDIES", cid)
            counts["edges"] += 1

    # Arms and interventions
    arms_mod = proto.get("armsInterventionsModule", {})
    arm_ids: dict[str, int] = {}
    for arm in arms_mod.get("armGroups", []):
        aid = _create_arm(client, arm)
        if aid is not None:
            counts["arms"] += 1
            _edge(client, trial_id, "HAS_ARM", aid)
            counts["edges"] += 1
            if arm.get("label"):
                arm_ids[arm["label"]] = aid

    for interv in arms_mod.get("interventions", []):
        iid = _get_or_create_intervention(client, interv_reg, interv)
        if iid is not None:
            _edge(client, trial_id, "TESTS", iid)
            counts["edges"] += 1
            for lbl in interv.get("armGroupLabels", []):
                if lbl in arm_ids:
                    _edge(client, arm_ids[lbl], "USES", iid)
                    counts["edges"] += 1

    # Sponsor
    lead = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
    if lead:
        sid = _get_or_create_sponsor(client, sponsor_reg, lead)
        if sid is not None:
            _edge(client, trial_id, "SPONSORED_BY", sid)
            counts["edges"] += 1

    # Sites
    for loc in proto.get("contactsLocationsModule", {}).get("locations", []):
        sid = _get_or_create_site(client, site_reg, loc)
        if sid is not None:
            _edge(client, trial_id, "CONDUCTED_AT", sid)
            counts["edges"] += 1

    # Outcomes
    outcomes_mod = proto.get("outcomesModule", {})
    for oc in outcomes_mod.get("primaryOutcomes", []):
        oid = _create_outcome(client, oc, "PRIMARY")
        if oid is not None:
            counts["outcomes"] += 1
            _edge(client, trial_id, "MEASURES", oid)
            counts["edges"] += 1
    for oc in outcomes_mod.get("secondaryOutcomes", []):
        oid = _create_outcome(client, oc, "SECONDARY")
        if oid is not None:
            counts["outcomes"] += 1
            _edge(client, trial_id, "MEASURES", oid)
            counts["edges"] += 1

    # Adverse events (results section)
    if include_results and study.get("hasResults"):
        ae_mod = study.get("resultsSection", {}).get("adverseEventsModule", {})
        for eg in ae_mod.get("seriousEvents", []):
            ae_id = _create_ae(client, eg, eg.get("organSystem", ""), True)
            if ae_id is not None:
                counts["adverse_events"] += 1
                _edge(client, trial_id, "REPORTED", ae_id)
                counts["edges"] += 1
        for eg in ae_mod.get("otherEvents", []):
            ae_id = _create_ae(client, eg, eg.get("organSystem", ""), False)
            if ae_id is not None:
                counts["adverse_events"] += 1
                _edge(client, trial_id, "REPORTED", ae_id)
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
    cond_reg, interv_reg, sponsor_reg, site_reg = _Registry(), _Registry(), _Registry(), _Registry()
    counts = {"trials": 0, "conditions": 0, "interventions": 0, "sponsors": 0,
              "sites": 0, "arms": 0, "outcomes": 0, "adverse_events": 0, "edges": 0}
    t0 = time.time()

    for term in conditions:
        print(f"\n{'='*60}\nFetching trials for: {term}\n{'='*60}")
        try:
            studies = fetch_studies(term, max_trials, include_results)
        except httpx.HTTPError as exc:
            print(f"  ERROR fetching '{term}': {exc}")
            continue

        print(f"  Retrieved {len(studies)} studies from API")
        for idx, study in enumerate(studies):
            try:
                _ingest_study(client, study, cond_reg, interv_reg,
                              sponsor_reg, site_reg, include_results, counts)
            except Exception as exc:
                nct = (study.get("protocolSection", {})
                       .get("identificationModule", {}).get("nctId", "unknown"))
                print(f"  WARNING: failed to ingest {nct}: {exc}")
                continue
            if (idx + 1) % 50 == 0:
                print(f"  ... ingested {idx + 1}/{len(studies)} studies")
        print(f"  Done — {len(studies)} studies processed for '{term}'")

    counts["conditions"] = len(cond_reg)
    counts["interventions"] = len(interv_reg)
    counts["sponsors"] = len(sponsor_reg)
    counts["sites"] = len(site_reg)
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
