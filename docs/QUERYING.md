# Querying the Clinical Trials KG

Ways to ask the graph questions, once it's loaded (see [GETTING_STARTED.md](../GETTING_STARTED.md)).
Use the tenant you loaded into — `default` (build-from-source subset) or `clinical-trials` (snapshot).

> **Note:** this is a very large KG; the examples below are the repo's showcase **patterns** — run them
> against your loaded tenant to see current results (they weren't re-pinned to exact numbers here).

---

## 1. HTTP API (`POST /api/query`)

Most common adverse events across trials:

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "clinical-trials",
  "query": "MATCH (t:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent) RETURN ae.term AS adverse_event, count(DISTINCT t) AS trials ORDER BY trials DESC LIMIT 5"
}'
```

Drug repurposing — a drug tested across different conditions:

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "clinical-trials",
  "query": "MATCH (i:Intervention)<-[:TESTS]-(t:ClinicalTrial)-[:STUDIES]->(c:Condition) RETURN i.name AS intervention, count(DISTINCT c) AS conditions ORDER BY conditions DESC LIMIT 5"
}'
```

## 2. Samyama CLI (Redis wire protocol, `:6379`)

```bash
redis-cli -p 6379 GRAPH.QUERY clinical-trials \
  "MATCH (t:ClinicalTrial)-[:SPONSORED_BY]->(s:Sponsor) RETURN s.name, count(t) AS trials ORDER BY trials DESC LIMIT 5"
```

## 3. MCP

`mcp_server/` ships tool definitions, but the current `server.py` starts an **empty embedded graph**
(`SamyamaClient.embedded()`) and has **no `--url`** option, so its tools don't serve a loaded engine.
Tracked in this repo's issues — use the **HTTP API** meanwhile.

---

## More queries
See the [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html)
for 100 example queries.
