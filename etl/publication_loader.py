"""
Publication Loader for ClinicalTrials Knowledge Graph.

Links clinical trials to PubMed publications using NCBI E-utilities API.
Creates Publication nodes and PUBLISHED_IN / DESCRIBES edges.
"""

from samyama import SamyamaClient
import httpx
import time
import json
import xml.etree.ElementTree as ET
from typing import Optional


# NCBI E-utilities base URLs
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Rate limits: 3 req/sec without API key, 10 req/sec with key
RATE_LIMIT_NO_KEY = 0.34  # ~3 req/sec
RATE_LIMIT_WITH_KEY = 0.10  # ~10 req/sec


def _rate_delay(api_key: Optional[str]) -> float:
    """Return the appropriate delay between requests based on API key presence."""
    return RATE_LIMIT_WITH_KEY if api_key else RATE_LIMIT_NO_KEY


def _build_params(base_params: dict, api_key: Optional[str]) -> dict:
    """Add API key to request params if provided."""
    if api_key:
        base_params["api_key"] = api_key
    return base_params


def search_pubmed_for_trial(
    http_client: httpx.Client, nct_id: str, api_key: Optional[str] = None
) -> list[str]:
    """
    Search PubMed for articles mentioning the given NCT ID in secondary source IDs.

    Returns a list of PMIDs (as strings).
    """
    params = _build_params(
        {
            "db": "pubmed",
            "term": f"{nct_id}[si]",
            "retmode": "json",
            "retmax": 20,
        },
        api_key,
    )

    try:
        resp = http_client.get(ESEARCH_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("esearchresult", {}).get("idlist", [])
    except (httpx.HTTPError, json.JSONDecodeError, KeyError) as e:
        print(f"  [WARN] PubMed search failed for {nct_id}: {e}")
        return []


def fetch_article_summaries(
    http_client: httpx.Client, pmids: list[str], api_key: Optional[str] = None
) -> dict[str, dict]:
    """
    Fetch article summaries from PubMed for a list of PMIDs using esummary.

    Returns a dict mapping pmid -> {title, authors, journal, pub_date, doi}.
    """
    if not pmids:
        return {}

    params = _build_params(
        {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "json",
        },
        api_key,
    )

    try:
        resp = http_client.get(ESUMMARY_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        print(f"  [WARN] PubMed summary fetch failed for {pmids}: {e}")
        return {}

    results = {}
    result_block = data.get("result", {})
    uid_list = result_block.get("uids", [])

    for uid in uid_list:
        article = result_block.get(uid, {})
        if not isinstance(article, dict):
            continue

        # Extract authors list
        authors_raw = article.get("authors", [])
        authors = [a.get("name", "") for a in authors_raw if isinstance(a, dict)]

        # Extract DOI from articleids
        doi = ""
        for aid in article.get("articleids", []):
            if isinstance(aid, dict) and aid.get("idtype") == "doi":
                doi = aid.get("value", "")
                break

        results[uid] = {
            "title": article.get("title", ""),
            "authors": "; ".join(authors),
            "journal": article.get("fulljournalname", article.get("source", "")),
            "pub_date": article.get("pubdate", ""),
            "doi": doi,
        }

    return results


def _escape_cypher_string(s: str) -> str:
    """Escape single quotes and backslashes for Cypher string literals."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def create_publication_node(client: SamyamaClient, pmid: str, meta: dict) -> None:
    """Create a Publication node in the graph if it doesn't already exist."""
    title = _escape_cypher_string(meta.get("title", ""))
    authors = _escape_cypher_string(meta.get("authors", ""))
    journal = _escape_cypher_string(meta.get("journal", ""))
    pub_date = _escape_cypher_string(meta.get("pub_date", ""))
    doi = _escape_cypher_string(meta.get("doi", ""))

    query = (
        f"MERGE (p:Publication {{pmid: '{pmid}'}}) "
        f"SET p.title = '{title}', "
        f"p.authors = '{authors}', "
        f"p.journal = '{journal}', "
        f"p.pub_date = '{pub_date}', "
        f"p.doi = '{doi}' "
        f"RETURN p.pmid"
    )
    client.query("default", query)


def create_publication_edges(
    client: SamyamaClient, nct_id: str, pmid: str
) -> None:
    """Create PUBLISHED_IN (trial -> pub) and DESCRIBES (pub -> trial) edges."""
    # PUBLISHED_IN: trial -> publication
    q1 = (
        f"MATCH (t:ClinicalTrial {{nct_id: '{nct_id}'}}), "
        f"(p:Publication {{pmid: '{pmid}'}}) "
        f"MERGE (t)-[:PUBLISHED_IN]->(p)"
    )
    client.query("default", q1)

    # DESCRIBES: publication -> trial
    q2 = (
        f"MATCH (p:Publication {{pmid: '{pmid}'}}), "
        f"(t:ClinicalTrial {{nct_id: '{nct_id}'}}) "
        f"MERGE (p)-[:DESCRIBES]->(t)"
    )
    client.query("default", q2)


def load_publications(
    client: SamyamaClient, api_key: Optional[str] = None
) -> dict:
    """
    Main entry point: links clinical trials to PubMed publications.

    1. Queries the graph for all ClinicalTrial nodes.
    2. For each trial, searches PubMed for articles mentioning its NCT ID.
    3. Creates Publication nodes with metadata from esummary.
    4. Creates PUBLISHED_IN and DESCRIBES edges.
    5. Deduplicates publications by PMID (MERGE handles this).

    Args:
        client: SamyamaClient instance (embedded or remote).
        api_key: Optional NCBI API key for higher rate limits (10 req/sec).

    Returns:
        dict with stats: trials_processed, publications_found, edges_created.
    """
    delay = _rate_delay(api_key)

    # Fetch all trial NCT IDs
    rows = client.query_readonly("default", "MATCH (t:ClinicalTrial) RETURN t.nct_id")
    nct_ids = [row[0] for row in rows if row and row[0]]
    print(f"Found {len(nct_ids)} clinical trials to process.")

    seen_pmids: set[str] = set()
    stats = {
        "trials_processed": 0,
        "publications_found": 0,
        "edges_created": 0,
    }

    with httpx.Client(timeout=30.0) as http_client:
        for i, nct_id in enumerate(nct_ids):
            print(f"[{i + 1}/{len(nct_ids)}] Searching PubMed for {nct_id}...")

            # Search for PMIDs linked to this trial
            pmids = search_pubmed_for_trial(http_client, nct_id, api_key)
            time.sleep(delay)

            if not pmids:
                print(f"  No publications found for {nct_id}.")
                stats["trials_processed"] += 1
                continue

            print(f"  Found {len(pmids)} publication(s): {', '.join(pmids)}")

            # Fetch metadata for new PMIDs only
            new_pmids = [p for p in pmids if p not in seen_pmids]
            if new_pmids:
                summaries = fetch_article_summaries(
                    http_client, new_pmids, api_key
                )
                time.sleep(delay)

                # Create Publication nodes for newly seen PMIDs
                for pmid, meta in summaries.items():
                    create_publication_node(client, pmid, meta)
                    seen_pmids.add(pmid)
                    stats["publications_found"] += 1
                    print(f"  Created Publication node: PMID {pmid} - {meta.get('title', '')[:60]}...")

            # Create edges for all PMIDs (including previously seen ones)
            for pmid in pmids:
                create_publication_edges(client, nct_id, pmid)
                stats["edges_created"] += 2  # PUBLISHED_IN + DESCRIBES

            stats["trials_processed"] += 1

    print(f"\nDone. Processed {stats['trials_processed']} trials, "
          f"found {stats['publications_found']} unique publications, "
          f"created {stats['edges_created']} edges.")
    return stats


if __name__ == "__main__":
    import os

    client = SamyamaClient.embedded()
    ncbi_api_key = os.environ.get("NCBI_API_KEY")

    if ncbi_api_key:
        print("Using NCBI API key for higher rate limits (10 req/sec).")
    else:
        print("No NCBI_API_KEY set. Using public rate limit (3 req/sec).")
        print("Set NCBI_API_KEY environment variable for faster processing.")

    load_publications(client, api_key=ncbi_api_key)
