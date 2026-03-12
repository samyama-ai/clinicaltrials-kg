"""
MeSH (Medical Subject Headings) Loader
=======================================
Loads MeSH descriptors from the NLM JSON API and creates:
  - MeSHDescriptor nodes (descriptor_id, name, tree_numbers, scope_note)
  - BROADER_THAN hierarchy edges between MeSH descriptors
  - CODED_AS_MESH edges from Condition nodes to MeSH descriptors

API endpoints:
  - Descriptor lookup: https://id.nlm.nih.gov/mesh/lookup/descriptor
  - Detail fetch:      https://id.nlm.nih.gov/mesh/{descriptorId}.json
"""

from samyama import SamyamaClient
import httpx
import time
import json
from pathlib import Path

MESH_LOOKUP_URL = "https://id.nlm.nih.gov/mesh/lookup/descriptor"
MESH_DETAIL_URL = "https://id.nlm.nih.gov/mesh"

# Rate limit: be kind to NLM servers
REQUEST_DELAY = 0.35  # seconds between API calls

TENANT = "default"


def _escape(value: str) -> str:
    """Strip double quotes and normalize whitespace for Cypher string literals."""
    return value.replace('"', '').replace('\n', ' ').replace('\r', '')


def _search_mesh(http: httpx.Client, condition_name: str) -> dict | None:
    """Search MeSH for a descriptor matching the condition name.

    Returns the best match dict with 'resource' and 'label' keys, or None.
    """
    try:
        resp = http.get(
            MESH_LOOKUP_URL,
            params={"label": condition_name, "match": "contains", "limit": 5},
        )
        resp.raise_for_status()
        results = resp.json()
        if not results:
            return None
        # Results are sorted by relevance; take the first one
        return results[0]
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
        print(f"  [WARN] MeSH lookup failed for '{condition_name}': {exc}")
        return None


def _fetch_descriptor_detail(http: httpx.Client, descriptor_id: str) -> dict | None:
    """Fetch full descriptor details from NLM MeSH JSON API.

    Returns parsed JSON or None on failure.
    """
    url = f"{MESH_DETAIL_URL}/{descriptor_id}.json"
    try:
        resp = http.get(url)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
        print(f"  [WARN] MeSH detail fetch failed for {descriptor_id}: {exc}")
        return None


def _extract_descriptor_fields(detail: dict) -> dict:
    """Extract name, tree_numbers, and scope_note from the MeSH JSON-LD response."""
    name = detail.get("label", {})
    if isinstance(name, dict):
        name = name.get("@value", "")
    elif isinstance(name, list):
        name = name[0].get("@value", "") if name else ""

    # Tree numbers are in treeNumber field
    tree_numbers = []
    raw_trees = detail.get("treeNumber", [])
    if not isinstance(raw_trees, list):
        raw_trees = [raw_trees]
    for t in raw_trees:
        if isinstance(t, dict):
            # URI like http://id.nlm.nih.gov/mesh/D03.383
            uri = t.get("@id", "")
            tree_numbers.append(uri.split("/")[-1] if "/" in uri else uri)
        elif isinstance(t, str):
            tree_numbers.append(t.split("/")[-1] if "/" in t else t)

    # Scope note
    scope_note = detail.get("scopeNote", {})
    if isinstance(scope_note, dict):
        scope_note = scope_note.get("@value", "")
    elif isinstance(scope_note, list):
        scope_note = scope_note[0].get("@value", "") if scope_note else ""
    else:
        scope_note = str(scope_note) if scope_note else ""

    return {
        "name": str(name),
        "tree_numbers": tree_numbers,
        "scope_note": str(scope_note),
    }


def _create_mesh_node(client: SamyamaClient, descriptor_id: str, fields: dict) -> None:
    """Create or merge a MeSHDescriptor node in the graph."""
    name = _escape(fields["name"])
    scope = _escape(fields["scope_note"][:500])  # Truncate long scope notes
    tree_arr = json.dumps(fields["tree_numbers"])

    query = (
        f'MERGE (m:MeSHDescriptor {{descriptor_id: "{descriptor_id}", '
        f'name: "{name}", '
        f'tree_numbers: "{_escape(tree_arr)}", '
        f'scope_note: "{scope}"}})'
    )
    client.query(query, TENANT)


def _create_coded_as_mesh_edge(client: SamyamaClient, condition_name: str, descriptor_id: str) -> None:
    """Create CODED_AS_MESH edge from Condition to MeSHDescriptor."""
    query = (
        f'MATCH (c:Condition {{name: "{_escape(condition_name)}"}}), '
        f'(m:MeSHDescriptor {{descriptor_id: "{descriptor_id}"}}) '
        f'CREATE (c)-[:CODED_AS_MESH]->(m)'
    )
    client.query(query, TENANT)


def _update_condition_mesh_id(client: SamyamaClient, condition_name: str, descriptor_id: str) -> None:
    """Set the mesh_id property on the Condition node."""
    query = (
        f'MATCH (c:Condition {{name: "{_escape(condition_name)}"}}) '
        f'SET c.mesh_id = "{descriptor_id}"'
    )
    client.query(query, TENANT)


def _get_parent_tree_number(tree_number: str) -> str | None:
    """Derive the parent tree number by removing the last dotted segment.

    E.g., 'C18.452.394.750' -> 'C18.452.394'
          'C18' -> None (top level, no parent)
    """
    if "." not in tree_number:
        return None
    return tree_number.rsplit(".", 1)[0]


def _build_broader_hierarchy(
    client: SamyamaClient,
    http: httpx.Client,
    descriptor_id: str,
    tree_numbers: list[str],
    seen_descriptors: set[str],
) -> None:
    """Walk up the MeSH tree hierarchy and create BROADER_THAN edges.

    For each tree number, derive the parent tree number, look up the parent
    descriptor, and create a BROADER_THAN edge from parent to child.
    """
    for tree_num in tree_numbers:
        parent_tree = _get_parent_tree_number(tree_num)
        if parent_tree is None:
            continue

        # Search for the parent descriptor by tree number
        # NLM doesn't have a direct tree-number-to-descriptor API, so we
        # look up the parent tree number via the descriptor search
        try:
            resp = http.get(
                MESH_LOOKUP_URL,
                params={"label": parent_tree, "match": "exact", "limit": 1},
            )
            time.sleep(REQUEST_DELAY)

            # If exact tree lookup fails, try fetching via the mesh URI pattern
            # Tree numbers map to descriptors — we fetch by convention
            parent_url = f"{MESH_DETAIL_URL}/{parent_tree}.json"
            resp2 = http.get(parent_url)
            if resp2.status_code != 200:
                continue

            parent_detail = resp2.json()
            parent_id_uri = parent_detail.get("@id", "")
            parent_id = parent_id_uri.split("/")[-1] if "/" in parent_id_uri else ""
            if not parent_id or not parent_id.startswith("D"):
                continue

            time.sleep(REQUEST_DELAY)

        except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError):
            continue

        # Create parent MeSH node if we haven't seen it
        if parent_id not in seen_descriptors:
            parent_fields = _extract_descriptor_fields(parent_detail)
            _create_mesh_node(client, parent_id, parent_fields)
            seen_descriptors.add(parent_id)

        # Create BROADER_THAN edge: parent -[:BROADER_THAN]-> child
        query = (
            f'MATCH (parent:MeSHDescriptor {{descriptor_id: "{parent_id}"}}), '
            f'(child:MeSHDescriptor {{descriptor_id: "{descriptor_id}"}}) '
            f'CREATE (parent)-[:BROADER_THAN]->(child)'
        )
        client.query(query, TENANT)


def load_mesh(client: SamyamaClient) -> dict:
    """Load MeSH descriptors for all Condition nodes that lack a mesh_id.

    Steps:
      1. Query graph for Condition nodes without mesh_id
      2. Search MeSH for each condition name
      3. Create MeSHDescriptor nodes and CODED_AS_MESH edges
      4. Build BROADER_THAN hierarchy edges
      5. Update Condition nodes with mesh_id

    Returns a summary dict with counts.
    """
    print("[MeSH Loader] Starting...")

    # Find conditions without MeSH mapping
    rows = client.query_readonly(
        "MATCH (c:Condition) WHERE c.mesh_id IS NULL RETURN c.name",
        TENANT,
    )
    condition_names = [row[0] for row in rows.records if row[0]]
    print(f"[MeSH Loader] Found {len(condition_names)} unmapped conditions")

    stats = {"conditions_processed": 0, "mesh_nodes_created": 0, "edges_created": 0, "skipped": 0}
    seen_descriptors: set[str] = set()

    with httpx.Client(timeout=30.0) as http:
        for name in condition_names:
            print(f"  Processing: {name}")
            time.sleep(REQUEST_DELAY)

            # Step 2: Search MeSH
            match = _search_mesh(http, name)
            if match is None:
                print(f"    No MeSH match found, skipping")
                stats["skipped"] += 1
                continue

            # Extract descriptor ID from the resource URI
            resource_uri = match.get("resource", "")
            descriptor_id = resource_uri.split("/")[-1] if "/" in resource_uri else resource_uri
            if not descriptor_id:
                stats["skipped"] += 1
                continue

            # Step 3a: Fetch full descriptor detail
            time.sleep(REQUEST_DELAY)
            detail = _fetch_descriptor_detail(http, descriptor_id)
            if detail is None:
                stats["skipped"] += 1
                continue

            fields = _extract_descriptor_fields(detail)

            # Step 3b: Create MeSHDescriptor node
            if descriptor_id not in seen_descriptors:
                _create_mesh_node(client, descriptor_id, fields)
                seen_descriptors.add(descriptor_id)
                stats["mesh_nodes_created"] += 1

            # Step 4: Create CODED_AS_MESH edge
            _create_coded_as_mesh_edge(client, name, descriptor_id)
            stats["edges_created"] += 1

            # Step 5: Build hierarchy (BROADER_THAN edges)
            _build_broader_hierarchy(client, http, descriptor_id, fields["tree_numbers"], seen_descriptors)

            # Step 6: Update Condition with mesh_id
            _update_condition_mesh_id(client, name, descriptor_id)
            stats["conditions_processed"] += 1

    print(f"[MeSH Loader] Done. {stats}")
    return stats
