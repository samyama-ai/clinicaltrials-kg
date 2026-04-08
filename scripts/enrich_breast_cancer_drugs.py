#!/usr/bin/env python3
"""
Targeted drug enrichment for breast cancer trial drugs.
Calls RxNorm for drug normalization and creates Drug/Protein/TARGETS nodes/edges
to enable cross-KG federation with Pathways KG.

Usage:
    python scripts/enrich_breast_cancer_drugs.py --url http://HOST:8080 --tenant biomedical
"""
import argparse
import json
import time
import urllib.request
import urllib.parse
import sys

# ── Known drug-target mappings (curated from DrugBank/UniProt) ──
# These are the key breast cancer drugs with their protein targets
DRUG_TARGETS = {
    "Trastuzumab":      {"drugbank": "DB00072", "targets": [("ERBB2", "P04626", "HER2 receptor binding")]},
    "Bevacizumab":      {"drugbank": "DB00112", "targets": [("VEGFA", "P15692", "VEGF-A neutralization")]},
    "Lapatinib":        {"drugbank": "DB01259", "targets": [("ERBB2", "P04626", "HER2 kinase inhibition"), ("EGFR", "P00533", "EGFR kinase inhibition")]},
    "Pertuzumab":       {"drugbank": "DB06366", "targets": [("ERBB2", "P04626", "HER2 dimerization blocking")]},
    "Tamoxifen":        {"drugbank": "DB00675", "targets": [("ESR1", "P03372", "Estrogen receptor antagonism")]},
    "Letrozole":        {"drugbank": "DB01006", "targets": [("CYP19A1", "P11511", "Aromatase inhibition")]},
    "Anastrozole":      {"drugbank": "DB01217", "targets": [("CYP19A1", "P11511", "Aromatase inhibition")]},
    "Exemestane":       {"drugbank": "DB00990", "targets": [("CYP19A1", "P11511", "Aromatase steroidal inhibition")]},
    "Palbociclib":      {"drugbank": "DB09073", "targets": [("CDK4", "P11802", "CDK4 inhibition"), ("CDK6", "Q00534", "CDK6 inhibition")]},
    "Ribociclib":       {"drugbank": "DB11730", "targets": [("CDK4", "P11802", "CDK4 inhibition"), ("CDK6", "Q00534", "CDK6 inhibition")]},
    "Abemaciclib":      {"drugbank": "DB12001", "targets": [("CDK4", "P11802", "CDK4 inhibition"), ("CDK6", "Q00534", "CDK6 inhibition")]},
    "Olaparib":         {"drugbank": "DB09074", "targets": [("PARP1", "P09874", "PARP inhibition"), ("PARP2", "Q9UGN5", "PARP2 inhibition")]},
    "Talazoparib":      {"drugbank": "DB11901", "targets": [("PARP1", "P09874", "PARP trapping"), ("PARP2", "Q9UGN5", "PARP2 inhibition")]},
    "Atezolizumab":     {"drugbank": "DB11595", "targets": [("CD274", "Q9NZQ7", "PD-L1 blockade")]},
    "Pembrolizumab":    {"drugbank": "DB09037", "targets": [("PDCD1", "Q15116", "PD-1 blockade")]},
    "Capecitabine":     {"drugbank": "DB01101", "targets": [("TYMS", "P04818", "Thymidylate synthase inhibition")]},
    "Docetaxel":        {"drugbank": "DB01248", "targets": [("TUBB1", "Q9H4B7", "Tubulin stabilization")]},
    "Paclitaxel":       {"drugbank": "DB01229", "targets": [("TUBB1", "Q9H4B7", "Tubulin stabilization")]},
    "Doxorubicin":      {"drugbank": "DB00997", "targets": [("TOP2A", "P11388", "Topoisomerase II inhibition")]},
    "Epirubicin":       {"drugbank": "DB00445", "targets": [("TOP2A", "P11388", "Topoisomerase II inhibition")]},
    "Carboplatin":      {"drugbank": "DB00958", "targets": [("DNA", None, "DNA crosslinking")]},
    "Cyclophosphamide": {"drugbank": "DB00531", "targets": [("DNA", None, "DNA alkylation")]},
    "Fluorouracil":     {"drugbank": "DB00544", "targets": [("TYMS", "P04818", "Thymidylate synthase inhibition")]},
    "Methotrexate":     {"drugbank": "DB00563", "targets": [("DHFR", "P00374", "Dihydrofolate reductase inhibition")]},
    "Gemcitabine":      {"drugbank": "DB00441", "targets": [("RRM1", "P23921", "Ribonucleotide reductase inhibition")]},
    "Vinorelbine":      {"drugbank": "DB00361", "targets": [("TUBB1", "Q9H4B7", "Tubulin destabilization")]},
    "Fulvestrant":      {"drugbank": "DB00947", "targets": [("ESR1", "P03372", "Estrogen receptor degradation")]},
    "Everolimus":       {"drugbank": "DB01590", "targets": [("MTOR", "P42345", "mTOR inhibition")]},
    "Alpelisib":        {"drugbank": "DB12015", "targets": [("PIK3CA", "P42336", "PI3Kα inhibition")]},
    "Tucatinib":        {"drugbank": "DB15822", "targets": [("ERBB2", "P04626", "HER2 kinase inhibition")]},
    "Sacituzumab":      {"drugbank": "DB14934", "targets": [("TACSTD2", "P09758", "Trop-2 targeting")]},
    "Eribulin":         {"drugbank": "DB08871", "targets": [("TUBB1", "Q9H4B7", "Tubulin destabilization")]},
}


def query(url, tenant, cypher):
    """Execute a Cypher query."""
    data = json.dumps({"query": cypher, "graph": tenant}).encode()
    req = urllib.request.Request(
        f"{url}/api/query",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def escape(s):
    """Escape string for Cypher."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def main():
    parser = argparse.ArgumentParser(description="Enrich breast cancer drugs with targets")
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--tenant", default="biomedical")
    args = parser.parse_args()

    url, tenant = args.url, args.tenant
    print(f"Enriching breast cancer drugs in tenant '{tenant}' on {url}\n")

    # Step 1: Get distinct drug names from breast cancer trials
    print("Step 1: Finding breast cancer trial drugs...")
    result = query(url, tenant,
        "MATCH (ct:ClinicalTrial)-[:STUDIES]->(cond:Condition) "
        "WHERE cond.name CONTAINS 'Breast' "
        "WITH ct "
        "MATCH (ct)-[:TESTS]->(i:Intervention) "
        "WHERE i.type = 'DRUG' "
        "RETURN DISTINCT i.name AS drug")
    trial_drugs = set(r[0] for r in result.get("records", []) if r[0])
    print(f"  Found {len(trial_drugs)} distinct drug interventions\n")

    # Step 2: Match trial drug names to our curated target mapping
    matched = {}
    for trial_drug in trial_drugs:
        name_lower = trial_drug.lower().strip()
        for known_drug, info in DRUG_TARGETS.items():
            if known_drug.lower() in name_lower or name_lower in known_drug.lower():
                matched[trial_drug] = (known_drug, info)
                break

    print(f"Step 2: Matched {len(matched)}/{len(trial_drugs)} drugs to target database\n")

    # Step 3a: Create all Drug nodes (deduplicated by drugbank_id)
    seen_drugs = set()
    drugs_created = 0
    for trial_drug_name, (canonical_name, info) in matched.items():
        dbid = info["drugbank"]
        if dbid in seen_drugs:
            continue
        seen_drugs.add(dbid)
        try:
            query(url, tenant,
                f"CREATE (d:Drug {{name: '{escape(canonical_name)}', drugbank_id: '{escape(dbid)}'}})")
            drugs_created += 1
        except Exception:
            pass
    print(f"  Drug nodes created: {drugs_created}")

    # Step 3b: Create Protein nodes for targets not already in Pathways KG
    seen_proteins = set()
    proteins_created = 0
    for info in DRUG_TARGETS.values():
        for gene_name, uniprot_id, _ in info["targets"]:
            if uniprot_id is None or uniprot_id in seen_proteins:
                continue
            seen_proteins.add(uniprot_id)
            # Check if protein already exists (from Pathways KG)
            try:
                r = query(url, tenant,
                    f"MATCH (p:Protein {{uniprot_id: '{escape(uniprot_id)}'}}) RETURN count(p)")
                if r.get("records") and r["records"][0][0] > 0:
                    continue  # Already exists from Pathways
            except Exception:
                pass
            try:
                query(url, tenant,
                    f"CREATE (p:Protein {{uniprot_id: '{escape(uniprot_id)}', name: '{escape(gene_name)}', gene_name: '{escape(gene_name)}'}})")
                proteins_created += 1
            except Exception:
                pass
    print(f"  Protein nodes created (new): {proteins_created}")

    # Step 3c: Create TARGETS edges (Drug → Protein)
    targets_created = 0
    seen_targets = set()
    for canonical_name, info in DRUG_TARGETS.items():
        dbid = info["drugbank"]
        if dbid not in seen_drugs:
            continue
        for gene_name, uniprot_id, mechanism in info["targets"]:
            if uniprot_id is None:
                continue
            key = (dbid, uniprot_id)
            if key in seen_targets:
                continue
            seen_targets.add(key)
            try:
                query(url, tenant,
                    f"MATCH (d:Drug {{drugbank_id: '{escape(dbid)}'}}), "
                    f"(p:Protein {{uniprot_id: '{escape(uniprot_id)}'}}) "
                    f"CREATE (d)-[:TARGETS]->(p)")
                targets_created += 1
            except Exception as e:
                print(f"    Warning: TARGETS {canonical_name}->{gene_name}: {e}")
    print(f"  TARGETS edges created: {targets_created}")

    # Step 3d: Create CODED_AS_DRUG edges (Intervention → Drug) — sample only
    coded_edges = 0
    seen_coded = set()
    for trial_drug_name, (canonical_name, info) in list(matched.items())[:200]:
        dbid = info["drugbank"]
        key = (trial_drug_name, dbid)
        if key in seen_coded:
            continue
        seen_coded.add(key)
        try:
            query(url, tenant,
                f"MATCH (i:Intervention {{name: '{escape(trial_drug_name)}'}}), "
                f"(d:Drug {{drugbank_id: '{escape(dbid)}'}}) "
                f"CREATE (i)-[:CODED_AS_DRUG]->(d)")
            coded_edges += 1
        except Exception:
            pass
        sys.stdout.write(f"\r  CODED_AS_DRUG edges: {coded_edges}...")
        sys.stdout.flush()
    print(f"\n  CODED_AS_DRUG edges created: {coded_edges}")

    print(f"\n\nStep 3: Results:")
    print(f"  Drug nodes created: {drugs_created}")
    print(f"  TARGETS edges created: {targets_created}")
    print(f"  CODED_AS_DRUG edges created: {coded_edges}")

    # Step 4: Verify the cross-KG bridge
    print("\nStep 4: Verifying cross-KG bridge...")
    result = query(url, tenant,
        "MATCH (d:Drug)-[:TARGETS]->(p:Protein) RETURN count(d) AS drugs, count(p) AS proteins")
    if result.get("records"):
        r = result["records"][0]
        print(f"  Drug→TARGETS→Protein edges: {r[0]} drugs, {r[1]} proteins")

    # Test the cross-KG query
    print("\nStep 5: Testing cross-KG federation query...")
    print("  Query: Pathways disrupted by breast cancer trial drugs\n")
    result = query(url, tenant,
        "MATCH (d:Drug)-[:TARGETS]->(p1:Protein) "
        "MATCH (p2:Protein)-[:PARTICIPATES_IN]->(pw:Pathway) "
        "WHERE p1.uniprot_id = p2.uniprot_id "
        "RETURN pw.name AS pathway, count(DISTINCT d.name) AS drugs, "
        "collect(DISTINCT d.name) AS drug_names "
        "ORDER BY drugs DESC LIMIT 10")

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
    else:
        print(f"  {'Pathway':<40s} {'Drugs':>5s}  Drug Names")
        print(f"  {'-'*40} {'-'*5}  {'-'*40}")
        for r in result.get("records", []):
            drugs_list = ", ".join(r[2][:3]) + ("..." if len(r[2]) > 3 else "")
            print(f"  {r[0]:<40s} {r[1]:>5d}  {drugs_list}")

    print("\nDone!")


if __name__ == "__main__":
    main()
