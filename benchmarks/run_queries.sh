#!/bin/bash
# Run curated real-world queries against the Clinical Trials KG (AACT)
# Usage: ./benchmarks/run_queries.sh [--data-dir PATH]
#
# Loads full AACT dataset from pipe-delimited .txt files, runs 10 PROFILE
# queries, captures results to benchmarks/query_results.txt
#
# Dataset: 575K clinical trials from ClinicalTrials.gov (AACT dump)
# ~7.7M nodes, ~27M edges, load time ~5 minutes

set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
SG_DIR="${SG_DIR:-$(dirname "$REPO_DIR")/samyama-graph}"
DATA_DIR="${1:-$REPO_DIR/data/aact}"
OUTPUT="$SCRIPT_DIR/query_results.txt"

echo "=== Clinical Trials KG (AACT) — Query Benchmark ==="
echo "Data dir:   $DATA_DIR"
echo "Output:     $OUTPUT"
echo "Samyama:    $SG_DIR"
echo ""

cd "$SG_DIR"

cargo run --release --example aact_loader -- \
  --data-dir "$DATA_DIR" \
  --query << 'QUERIES' 2>&1 | tee "$OUTPUT"
MATCH (n) RETURN labels(n) AS label, count(n) AS count ORDER BY count DESC
MATCH (ct:ClinicalTrial) WHERE ct.phase = 'PHASE3' WITH ct.overall_status AS status, count(ct) AS trial_count RETURN status, trial_count ORDER BY trial_count DESC LIMIT 15
MATCH (ct:ClinicalTrial)-[:STUDIES]->(c:Condition) WITH c, count(ct) AS trial_count WHERE trial_count > 500 RETURN c.name AS condition, trial_count ORDER BY trial_count DESC LIMIT 20
MATCH (ct:ClinicalTrial)-[:TESTS]->(i:Intervention) WHERE i.type = 'DRUG' WITH i, count(ct) AS trial_count WHERE trial_count > 100 RETURN i.name AS drug, trial_count ORDER BY trial_count DESC LIMIT 20
MATCH (ct:ClinicalTrial)-[:SPONSORED_BY]->(s:Sponsor) WHERE s.class = 'INDUSTRY' WITH s, count(ct) AS trials WHERE trials > 200 RETURN s.name AS sponsor, trials ORDER BY trials DESC LIMIT 20
MATCH (ct:ClinicalTrial)-[:CONDUCTED_AT]->(site:Site) WITH site.country AS country, count(ct) AS trials WHERE trials > 1000 RETURN country, trials ORDER BY trials DESC LIMIT 20
MATCH (ct:ClinicalTrial) WHERE ct.phase = 'PHASE3' AND ct.overall_status = 'TERMINATED' RETURN ct.nct_id AS trial, ct.title AS title, ct.why_stopped AS reason LIMIT 20
MATCH (ct:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent) WITH ae, count(ct) AS trial_count WHERE trial_count > 200 RETURN ae.term AS event, trial_count ORDER BY trial_count DESC LIMIT 20
MATCH (c:Condition)-[:CODED_AS_MESH]->(m:MeSHDescriptor) WITH m, count(c) AS cond_count WHERE cond_count > 50 RETURN m.name AS mesh_term, cond_count ORDER BY cond_count DESC LIMIT 20
MATCH (ct:ClinicalTrial)-[:PUBLISHED_IN]->(pub:Publication) WITH ct, count(pub) AS pubs WHERE pubs > 3 RETURN ct.nct_id AS trial, ct.title AS title, pubs ORDER BY pubs DESC LIMIT 15
exit
QUERIES

echo ""
echo "Results saved to $OUTPUT"
