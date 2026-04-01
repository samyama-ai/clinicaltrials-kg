# Clinical Trials KG (AACT) — Query Results & Profiling

> Run date: 2026-04-01 | Samyama Graph v0.6.1 | MacBook Pro (local)
> KG: 7,774,446 nodes, 26,973,997 edges | Load time: 4m 37s | Source: AACT pipe-delimited flat files (575,778 studies)

---

## KG Statistics

| Label | Count |
|-------|------:|
| Outcome | 3,564,582 |
| ArmGroup | 1,055,533 |
| Site | 1,029,229 |
| Publication | 750,114 |
| ClinicalTrial | 575,778 |
| Intervention | 472,275 |
| AdverseEvent | 145,711 |
| Condition | 125,689 |
| Sponsor | 49,519 |
| MeSHDescriptor | 6,016 |
| **Total nodes** | **7,774,446** |
| **Total edges** | **26,973,997** |

| Edge Type | Count |
|-----------|------:|
| REPORTED | 11,442,570 |
| MEASURES | 3,564,582 |
| CONDUCTED_AT | 3,409,901 |
| CODED_AS_MESH | 2,623,299 |
| USES | 1,282,300 |
| HAS_ARM | 1,055,533 |
| STUDIES | 1,025,129 |
| PUBLISHED_IN | 1,021,552 |
| TESTS | 973,353 |
| SPONSORED_BY | 575,778 |

Average out-degree: 3.47

---

## Query 1: Phase 3 trial status distribution

**Clinical question:** What is the completion landscape for Phase 3 trials? How many are still recruiting vs completed vs terminated?

```cypher
MATCH (ct:ClinicalTrial)
WHERE ct.phase = 'PHASE3'
WITH ct.overall_status AS status, count(ct) AS trial_count
RETURN status, trial_count
ORDER BY trial_count DESC LIMIT 15
```

**Profile:** `NodeScan(ClinicalTrial) -> Filter(phase=PHASE3) -> WithBarrier -> Sort -> Limit` | **4,143ms**

| Status | Trial Count |
|--------|------------:|
| COMPLETED | 25,570 |
| UNKNOWN | 4,461 |
| TERMINATED | 3,412 |
| RECRUITING | 3,390 |
| ACTIVE_NOT_RECRUITING | 1,909 |
| NOT_YET_RECRUITING | 1,188 |
| WITHDRAWN | 1,099 |
| ENROLLING_BY_INVITATION | 133 |
| SUSPENDED | 101 |

**Insight:** 62% of Phase 3 trials have completed, but 8.3% were terminated and 2.7% withdrawn -- a combined 11% failure rate representing billions in lost R&D investment. The 3,390 currently recruiting Phase 3 trials represent the active late-stage drug development pipeline globally. The 4,461 "UNKNOWN" status trials reflect legacy records with no recent updates.

---

## Query 2: Most studied conditions

**Clinical question:** Which diseases attract the most clinical trial activity? (research prioritization signal)

```cypher
MATCH (ct:ClinicalTrial)-[:STUDIES]->(c:Condition)
WITH c, count(ct) AS trial_count
WHERE trial_count > 500
RETURN c.name AS condition, trial_count
ORDER BY trial_count DESC LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(STUDIES) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate

| Condition | Trials |
|-----------|-------:|
| Healthy | 10,898 |
| Breast Cancer | 8,556 |
| Obesity | 7,353 |
| Stroke | 5,073 |
| Hypertension | 4,530 |
| Depression | 4,456 |
| Pain | 4,416 |
| Prostate Cancer | 4,331 |
| HIV Infections | 3,853 |
| Cancer | 3,853 |
| Coronary Artery Disease | 3,652 |
| Heart Failure | 3,619 |
| Asthma | 3,610 |
| Colorectal Cancer | 3,380 |
| Diabetes Mellitus, Type 2 | 3,276 |
| COVID-19 | 3,165 |
| Anxiety | 3,136 |
| Lung Cancer | 3,041 |
| Cardiovascular Diseases | 2,993 |
| Diabetes | 2,979 |

**Insight:** "Healthy" leads at 10,898 trials (healthy volunteer studies for PK/safety). Oncology dominates: Breast Cancer (8,556), Prostate Cancer (4,331), Cancer (3,853), Colorectal Cancer (3,380), Lung Cancer (3,041). COVID-19 already ranks 16th with 3,165 trials despite emerging only in 2020 -- a testament to the unprecedented research mobilization. The top 20 conditions collectively represent the diseases that consume the majority of global pharmaceutical R&D spending.

---

## Query 3: Most tested drugs

**Clinical question:** Which drugs are being tested in the most clinical trials? (pipeline activity leaders)

```cypher
MATCH (ct:ClinicalTrial)-[:TESTS]->(i:Intervention)
WHERE i.type = 'DRUG'
WITH i, count(ct) AS trial_count
WHERE trial_count > 100
RETURN i.name AS drug, trial_count
ORDER BY trial_count DESC LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(TESTS) -> Filter(type=DRUG) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate + filter

| Drug | Trials |
|------|-------:|
| cyclophosphamide | 2,934 |
| Cisplatin | 2,337 |
| carboplatin | 2,316 |
| Paclitaxel | 2,166 |
| Dexamethasone | 2,106 |
| Pembrolizumab | 1,939 |
| Bevacizumab | 1,734 |
| Rituximab | 1,636 |
| Docetaxel | 1,623 |
| Gemcitabine | 1,482 |
| Nivolumab | 1,331 |
| Normal Saline | 1,322 |
| Metformin | 1,306 |
| Capecitabine | 1,269 |
| Oxaliplatin | 1,179 |
| prednisone | 1,178 |
| methotrexate | 1,134 |
| etoposide | 1,122 |
| Dexmedetomidine | 1,061 |
| Fludarabine | 1,004 |

**Insight:** Chemotherapy agents dominate: cyclophosphamide (2,934), Cisplatin (2,337), carboplatin (2,316), Paclitaxel (2,166). These are backbone agents used in combination regimens across many cancer types. Pembrolizumab (1,939) and Nivolumab (1,331) -- both PD-1 checkpoint inhibitors -- reflect the immunotherapy revolution. Metformin (1,306) appears high for a diabetes drug, consistent with extensive repurposing research for cancer prevention and aging. Normal Saline (1,322) reflects its use as a placebo/control arm.

---

## Query 4: Top industry sponsors

**Clinical question:** Which pharmaceutical companies lead the most clinical trials?

```cypher
MATCH (ct:ClinicalTrial)-[:SPONSORED_BY]->(s:Sponsor)
WHERE s.class = 'INDUSTRY'
WITH s, count(ct) AS trials WHERE trials > 200
RETURN s.name AS sponsor, trials
ORDER BY trials DESC LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(SPONSORED_BY) -> Filter(class=INDUSTRY) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate + filter

| Sponsor | Trials |
|---------|-------:|
| GlaxoSmithKline | 3,586 |
| AstraZeneca | 3,402 |
| Pfizer | 3,240 |
| Novartis Pharmaceuticals | 2,614 |
| Boehringer Ingelheim | 2,263 |
| Merck Sharp & Dohme LLC | 2,105 |
| Hoffmann-La Roche | 2,062 |
| Eli Lilly and Company | 2,033 |
| Bayer | 1,656 |
| Bristol-Myers Squibb | 1,529 |
| Sanofi | 1,513 |
| Novo Nordisk A/S | 1,341 |
| Amgen | 1,024 |
| Takeda | 1,011 |
| AbbVie | 922 |
| Janssen Research & Development, LLC | 910 |
| Novartis | 715 |
| Gilead Sciences | 677 |
| Alcon Research | 616 |
| Jiangsu HengRui Medicine Co., Ltd. | 557 |

**Insight:** The top 3 (GSK 3,586, AstraZeneca 3,402, Pfizer 3,240) each sponsor over 3,000 trials. All top-15 are established Big Pharma. Jiangsu HengRui at #20 with 557 trials is the only Chinese company in the list -- a signal of China's growing clinical trial infrastructure. Novartis appears twice (as "Novartis Pharmaceuticals" and "Novartis") due to inconsistent sponsor naming in the AACT data, a common entity resolution challenge.

---

## Query 5: Clinical trial sites by country

**Clinical question:** Where are clinical trials being conducted globally? (geographic distribution of research)

```cypher
MATCH (ct:ClinicalTrial)-[:CONDUCTED_AT]->(site:Site)
WITH site.country AS country, count(ct) AS trials
WHERE trials > 1000
RETURN country, trials
ORDER BY trials DESC LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(CONDUCTED_AT) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate (3.4M edges)

| Country | Trial-Site Links |
|---------|----------------:|
| United States | 1,460,902 |
| France | 195,707 |
| Germany | 167,598 |
| China | 157,570 |
| Spain | 113,786 |
| Japan | 108,342 |
| Italy | 103,272 |
| Canada | 102,563 |
| United Kingdom | 97,480 |
| Poland | 57,367 |
| South Korea | 54,935 |
| Australia | 48,622 |
| Russia | 44,191 |
| Belgium | 42,356 |
| Netherlands | 39,868 |
| Turkey (Turkiye) | 38,010 |
| Brazil | 37,021 |
| India | 27,872 |
| Taiwan | 27,789 |
| Czechia | 27,160 |

**Insight:** The US dominates with 1.46M trial-site links -- 7.5x France (#2) and nearly 10x China (#4). This counts site-level participation (one multi-site trial creates many links), so it reflects both trial volume and the density of participating sites. The US has vastly more clinical trial infrastructure than any other country. China (157K) has overtaken Germany to become #4, reflecting its rapid expansion in clinical research.

---

## Query 6: Terminated Phase 3 trials (with reasons)

**Clinical question:** Why do expensive Phase 3 trials get terminated? (failure analysis)

```cypher
MATCH (ct:ClinicalTrial)
WHERE ct.phase = 'PHASE3' AND ct.overall_status = 'TERMINATED'
RETURN ct.nct_id AS trial, ct.title AS title, ct.why_stopped AS reason
LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Filter(phase=PHASE3 AND status=TERMINATED) -> Limit` | **2,710ms**

| Trial | Title | Reason |
|-------|-------|--------|
| NCT02613910 | Long-Term Extension Study of Ofatumumab in Pemphigus Vulgaris | Novartis acquired rights and terminated |
| NCT00411892 | Effect of Inhaled Insulin (AERx iDMS) on Blood Glucose Control in Type 2 Diabetes | See detailed description |
| NCT03279731 | Binge Eating Liraglutide Intervention | Not meeting recruitment goals |
| NCT00381810 | Safety of Rituximab Retreatment in Systemic Lupus | Data Monitoring Committee recommendation |
| NCT03970330 | Low-Dose Naltrexone for Endometriosis | PI left institution, lack of funding |
| NCT00576667 | Rimonabant for NASH in Non-Diabetic Patients | Company decision - national health authority demands |
| NCT00784836 | Subcutaneous Avonex in MS Patients | Business reasons unrelated to safety |
| NCT02519439 | Ganaxolone for Drug-Resistant Seizures | Missed primary endpoint in double-blind study |
| NCT04649515 | TY027 Treatment for COVID-19 | Low recruitment rate |
| NCT03364738 | rhPTH(1-84) for Hypoparathyroidism | Takeda Natpara recall |
| NCT00003824 | Ciprofloxacin vs Cephalexin for Bladder Cancer | Poor accrual |
| NCT02473848 | Ingenol Mebutate for Actinic Keratoses in Kidney Transplant | Lack of enrolment |
| NCT02119663 | Ruxolitinib in Pancreatic Cancer | Lack of efficacy in similar trial |
| NCT00064649 | Minimally Invasive Therapy for BPH | Inability to recruit sample size |
| NCT04371666 | Pamrevlumab for Non-Ambulatory DMD | Did not meet primary endpoint |
| NCT02919072 | Chloroprocaine Epidural in Unplanned C-Section | No patients enrolled |
| NCT00811174 | Octagam 10% in Primary Immunodeficiency | Limited data, no efficacy/PK analysis |
| NCT00989001 | Vernakalant for Atrial Fibrillation | (not recorded) |
| NCT02150447 | PPI Prevention of Gastric Cancer Bleeding | Low enrollment rate |

**Insight:** Phase 3 termination reasons fall into clear categories: (1) recruitment failure ("poor accrual", "low recruitment rate" -- 7 of 19), (2) efficacy failure ("missed primary endpoint" -- 3), (3) corporate decisions (M&A, product recall -- 3), (4) safety committee recommendation (1). Recruitment failure is the #1 killer of Phase 3 trials -- a major cost driver in pharmaceutical R&D.

---

## Query 7: Most reported adverse events

**Clinical question:** Which adverse events are reported across the most clinical trials? (pharmacovigilance signal strength at scale)

```cypher
MATCH (ct:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent)
WITH ae, count(ct) AS trial_count
WHERE trial_count > 200
RETURN ae.term AS event, trial_count
ORDER BY trial_count DESC LIMIT 20
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(REPORTED) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate (11.4M REPORTED edges)

| Adverse Event | Trials Reporting |
|---------------|----------------:|
| Headache | 101,005 |
| Nausea | 98,703 |
| Vomiting | 84,964 |
| Fatigue | 76,306 |
| Diarrhoea | 73,627 |
| Abdominal pain | 72,508 |
| Dizziness | 70,539 |
| Back Pain | 70,429 |
| Urinary tract infection | 69,158 |
| Constipation | 66,435 |
| Pyrexia | 65,044 |
| Cough | 61,365 |
| Pneumonia | 58,929 |
| Arthralgia | 58,755 |
| Hypertension | 56,041 |
| Upper respiratory tract infection | 55,274 |
| Anaemia | 50,836 |
| Nasopharyngitis | 49,135 |
| Dyspnoea | 47,377 |
| Pain in extremity | 47,319 |

**Insight:** Headache (101K trials) and Nausea (99K) are the most universally reported adverse events -- each appearing in ~18% of all 575K trials. These form the "baseline noise" of pharmacovigilance. Any AI system evaluating drug safety must calibrate against these high-frequency events. The list mixes constitutional symptoms (headache, fatigue, nausea) with infection-related events (UTI, pneumonia, upper respiratory infection), reflecting both drug effects and background population health.

---

## Query 8: MeSH descriptor mapping coverage

**Clinical question:** Which MeSH terms map to the most conditions? (ontology coverage of the clinical trial landscape)

```cypher
MATCH (c:Condition)-[:CODED_AS_MESH]->(m:MeSHDescriptor)
WITH m, count(c) AS cond_count
WHERE cond_count > 50
RETURN m.name AS mesh_term, cond_count
ORDER BY cond_count DESC LIMIT 20
```

**Profile:** `NodeScan(Condition) -> Expand(CODED_AS_MESH) -> WithBarrier -> Sort -> Limit` | **9,379ms**

| MeSH Descriptor | Conditions |
|-----------------|----------:|
| Pathological Conditions, Signs and Symptoms | 45,684 |
| Pathologic Processes | 31,832 |
| Neoplasms | 31,322 |
| Nervous System Diseases | 26,186 |
| Signs and Symptoms | 25,153 |
| Neoplasms by Site | 22,701 |
| Urogenital Diseases | 20,455 |
| Cardiovascular Diseases | 19,646 |
| Neoplasms by Histologic Type | 19,419 |
| Neurologic Manifestations | 18,316 |
| Female Urogenital Diseases and Pregnancy Complications | 17,652 |
| Respiratory Tract Diseases | 17,558 |
| Digestive System Diseases | 17,271 |
| Infections | 16,457 |
| Immune System Diseases | 16,086 |
| Behavior | 15,658 |
| Vascular Diseases | 15,605 |
| Skin and Connective Tissue Diseases | 15,133 |
| Congenital, Hereditary, and Neonatal Diseases | 15,084 |
| Female Urogenital Diseases | 14,959 |

**Insight:** The MeSH hierarchy reveals that Neoplasms (31,322 conditions) is the largest disease category in clinical trials -- confirming oncology's dominance in pharmaceutical R&D. The broad categories (Pathological Conditions at 45K, Pathologic Processes at 32K) reflect MeSH's hierarchical structure where many specific conditions roll up to general categories. This CODED_AS_MESH graph enables hierarchical disease navigation -- e.g., querying "all Neoplasms" captures 31K+ conditions across specific cancer types.

---

## Query 9: Most-published clinical trials

**Clinical question:** Which clinical trials have generated the most publications? (research impact leaders)

```cypher
MATCH (ct:ClinicalTrial)-[:PUBLISHED_IN]->(pub:Publication)
WITH ct, count(pub) AS pubs
WHERE pubs > 3
RETURN ct.nct_id AS trial, ct.title AS title, pubs
ORDER BY pubs DESC LIMIT 15
```

**Profile:** `NodeScan(ClinicalTrial) -> Expand(PUBLISHED_IN) -> WithBarrier -> Sort -> Limit` | 1-hop aggregate (1M PUBLISHED_IN edges)

| Trial | Title | Publications |
|-------|-------|------------:|
| NCT00005129 | Bogalusa Heart Study | 599 |
| NCT06995586 | Dietary Supplements in CABG Patients | 364 |
| NCT06322212 | Type 2 Diabetes and Blood Brain Barrier | 270 |
| NCT06857136 | Glymphatic System in NPH Diagnosis | 267 |
| NCT00005279 | Tucson Epidemiology Study of COPD | 263 |
| NCT02450851 | Undiagnosed Diseases Network | 253 |
| NCT00000611 | Women's Health Initiative (WHI) | 246 |
| NCT02699736 | EuroSIDA - European HIV Patients | 245 |
| NCT00005123 | Honolulu Heart Program | 232 |
| NCT02420561 | Motivational Interviewing for Substance Use/Depression | 222 |
| NCT00137111 | Therapy for Newly Diagnosed ALL | 206 |
| NCT00482573 | Dental Anesthesia in Pregnant Women with RHD | 203 |
| NCT00327860 | CKiD - Chronic Kidney Disease in Children | 202 |
| NCT04031716 | Post-surgical Pain After Pectus/Spine Surgery | 197 |
| NCT05450822 | Precision Medicine in Epilepsy | 193 |

**Insight:** The Bogalusa Heart Study (599 publications) is the most-published trial -- a landmark longitudinal study of cardiovascular risk factors from childhood. The Women's Health Initiative (246) is another seminal study. Long-running epidemiological studies dominate the top positions because they generate decades of follow-up publications. This data enables citation-impact analysis: linking trial results to their downstream research output.

---

## Performance Summary

| Query | Complexity | Rows | Time |
|-------|-----------|-----:|-----:|
| Q1: Phase 3 status distribution | 0-hop filter + aggregate | 9 | 4,143ms |
| Q2: Most studied conditions | 1-hop aggregate | 20 | ~15s* |
| Q3: Most tested drugs | 1-hop aggregate + filter | 20 | ~15s* |
| Q4: Top industry sponsors | 1-hop aggregate + filter | 20 | ~15s* |
| Q5: Trial sites by country | 1-hop aggregate (3.4M edges) | 20 | ~15s* |
| Q6: Terminated Phase 3 reasons | 0-hop multi-filter | 20 | 2,710ms |
| Q7: Most reported adverse events | 1-hop aggregate (11.4M edges) | 20 | ~15s* |
| Q8: MeSH descriptor coverage | 1-hop aggregate (2.6M edges) | 20 | 9,379ms |
| Q9: Most-published trials | 1-hop aggregate (1M edges) | 15 | ~15s* |

\* Estimated from query mode execution; PROFILE overhead causes timeout on expand-heavy queries at this scale. Regular query mode completes successfully.

**Key observations:**
- This is a 7.7M node / 27M edge graph -- 240x larger than the Drug Interactions KG (32K nodes)
- Property-only queries (Q1, Q6) complete in 2-4 seconds, scanning 575K ClinicalTrial nodes
- 1-hop aggregations over million-edge relationships complete in the query mode (non-PROFILE)
- The REPORTED relationship (11.4M edges) is the heaviest -- Q7 aggregates across all of them
- Load time is ~4.5 minutes from 49 pipe-delimited flat files with full deduplication
- All data is held in-memory (peak ~15GB RAM for the full 575K-study graph)
