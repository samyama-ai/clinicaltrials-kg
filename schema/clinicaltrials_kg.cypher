// =============================================================================
// Clinical Trials Knowledge Graph — Schema Definition
// =============================================================================
//
// 15 node labels, 25 relationship types
// Standards: ICH E6 (GCP), CDISC, FDA 21 CFR Part 11
// Coding systems: MeSH, ICD-10, SNOMED CT, RxNorm, ATC, LOINC, UMLS
//
// Data sources:
//   - ClinicalTrials.gov API v2 (trials, conditions, interventions, sponsors, sites)
//   - MeSH (Medical Subject Headings) — NLM
//   - RxNorm — NLM (normalized drug names)
//   - OpenFDA — FDA (adverse events, drug labels)
//   - PubMed E-utilities — NLM (publications)
//   - UMLS Metathesaurus — NLM (cross-vocabulary mappings)
// =============================================================================

// --- Core Clinical Trial Entities ---

// ClinicalTrial: A registered clinical study (NCT identifier)
//   Properties: nct_id, title, official_title, brief_summary, study_type,
//               phase, overall_status, enrollment, start_date, completion_date,
//               primary_completion_date, first_posted, last_updated,
//               has_results, why_stopped
CREATE (t:ClinicalTrial {nct_id: 'NCT00000000'});

// Condition: A disease or health condition studied in a trial
//   Properties: name, mesh_id, icd10_code, snomed_id, umls_cui
CREATE (c:Condition {name: 'Type 2 Diabetes Mellitus'});

// Intervention: A drug, device, procedure, or behavioral treatment
//   Properties: name, type (DRUG|DEVICE|PROCEDURE|BEHAVIORAL|BIOLOGICAL|
//               RADIATION|DIETARY_SUPPLEMENT|GENETIC|COMBINATION|OTHER),
//               description, rxnorm_cui, atc_code
CREATE (i:Intervention {name: 'Metformin', type: 'DRUG'});

// ArmGroup: A treatment arm within a trial
//   Properties: label, type (EXPERIMENTAL|ACTIVE_COMPARATOR|PLACEBO_COMPARATOR|
//               SHAM_COMPARATOR|NO_INTERVENTION|OTHER), description
CREATE (a:ArmGroup {label: 'Treatment Arm A'});

// Outcome: A primary or secondary outcome measure
//   Properties: measure, description, time_frame, type (PRIMARY|SECONDARY|OTHER)
CREATE (o:Outcome {measure: 'HbA1c Change from Baseline'});

// Sponsor: Organization sponsoring the trial
//   Properties: name, class (INDUSTRY|FED|NIH|OTHER_GOV|NETWORK|INDIV|OTHER)
CREATE (s:Sponsor {name: 'National Institute of Diabetes'});

// Site: A facility where the trial is conducted
//   Properties: facility, city, state, country, zip, latitude, longitude
CREATE (site:Site {facility: 'Johns Hopkins Hospital', city: 'Baltimore'});

// --- Results Entities (for completed trials with results) ---

// AdverseEvent: A reported adverse event in trial results
//   Properties: term, organ_system, source_vocabulary (MedDRA),
//               num_affected, num_at_risk, frequency, is_serious
CREATE (ae:AdverseEvent {term: 'Nausea', organ_system: 'Gastrointestinal disorders'});

// --- Biomedical Ontology Entities ---

// MeSHDescriptor: A Medical Subject Heading term (hierarchical vocabulary)
//   Properties: descriptor_id, name, tree_numbers (array), scope_note
CREATE (m:MeSHDescriptor {descriptor_id: 'D003924', name: 'Diabetes Mellitus, Type 2'});

// Drug: A normalized drug entity from RxNorm
//   Properties: rxnorm_cui, name, drugbank_id, mechanism_of_action
CREATE (d:Drug {rxnorm_cui: '6809', name: 'Metformin'});

// DrugClass: ATC classification level (hierarchical)
//   Properties: atc_code, name, level (1-5)
//   Level 1: Anatomical (A = Alimentary tract)
//   Level 2: Therapeutic (A10 = Drugs used in diabetes)
//   Level 3: Pharmacological (A10B = Blood glucose lowering drugs)
//   Level 4: Chemical (A10BA = Biguanides)
//   Level 5: Chemical substance (A10BA02 = Metformin)
CREATE (dc:DrugClass {atc_code: 'A10BA02', name: 'Metformin', level: 5});

// Publication: A PubMed article linked to a trial
//   Properties: pmid, title, abstract, journal, pub_date, doi
CREATE (p:Publication {pmid: '12345678', title: 'Results of Phase III Trial...'});

// Gene: A gene associated with a condition or drug target
//   Properties: gene_id, symbol, name, uniprot_id
CREATE (g:Gene {symbol: 'TCF7L2', name: 'Transcription Factor 7 Like 2'});

// Protein: A drug target protein
//   Properties: uniprot_id, name, function
CREATE (pr:Protein {uniprot_id: 'P43220', name: 'Glucagon-like peptide 1 receptor'});

// LabTest: A laboratory observation (LOINC-coded)
//   Properties: loinc_code, name, component, system, scale_type
CREATE (lt:LabTest {loinc_code: '4548-4', name: 'Hemoglobin A1c'});


// =============================================================================
// Relationships (25 types)
// =============================================================================

// --- Trial-centric relationships ---

// Trial studies a condition
// (ClinicalTrial)-[:STUDIES]->(Condition)
CREATE (t)-[:STUDIES]->(c);

// Trial tests an intervention
// (ClinicalTrial)-[:TESTS]->(Intervention)
CREATE (t)-[:TESTS]->(i);

// Trial has treatment arms
// (ClinicalTrial)-[:HAS_ARM]->(ArmGroup)
CREATE (t)-[:HAS_ARM]->(a);

// Arm uses an intervention
// (ArmGroup)-[:USES]->(Intervention)
CREATE (a)-[:USES]->(i);

// Trial measures an outcome
// (ClinicalTrial)-[:MEASURES]->(Outcome)
CREATE (t)-[:MEASURES]->(o);

// Trial sponsored by organization
// (ClinicalTrial)-[:SPONSORED_BY]->(Sponsor)
CREATE (t)-[:SPONSORED_BY]->(s);

// Trial conducted at a site
// (ClinicalTrial)-[:CONDUCTED_AT {status: 'RECRUITING'}]->(Site)
CREATE (t)-[:CONDUCTED_AT]->(site);

// Trial reported adverse event (from results section)
// (ClinicalTrial)-[:REPORTED]->(AdverseEvent)
CREATE (t)-[:REPORTED]->(ae);

// Trial published in article
// (ClinicalTrial)-[:PUBLISHED_IN]->(Publication)
CREATE (t)-[:PUBLISHED_IN]->(p);

// --- Ontology cross-reference relationships ---

// Condition coded in MeSH
// (Condition)-[:CODED_AS_MESH]->(MeSHDescriptor)
CREATE (c)-[:CODED_AS_MESH]->(m);

// Intervention coded in RxNorm (for drugs)
// (Intervention)-[:CODED_AS_DRUG]->(Drug)
CREATE (i)-[:CODED_AS_DRUG]->(d);

// MeSH hierarchy
// (MeSHDescriptor)-[:BROADER_THAN]->(MeSHDescriptor)
CREATE (m1:MeSHDescriptor)-[:BROADER_THAN]->(m2:MeSHDescriptor);

// Drug classified in ATC hierarchy
// (Drug)-[:CLASSIFIED_AS]->(DrugClass)
CREATE (d)-[:CLASSIFIED_AS]->(dc);

// ATC hierarchy
// (DrugClass)-[:PARENT_CLASS]->(DrugClass)
CREATE (dc1:DrugClass)-[:PARENT_CLASS]->(dc2:DrugClass);

// --- Biomedical relationships ---

// Drug targets a protein
// (Drug)-[:TARGETS]->(Protein)
CREATE (d)-[:TARGETS]->(pr);

// Drug treats a condition
// (Drug)-[:TREATS]->(Condition)
CREATE (d)-[:TREATS]->(c);

// Drug interacts with another drug
// (Drug)-[:INTERACTS_WITH {severity: 'major|moderate|minor'}]->(Drug)
CREATE (d1:Drug)-[:INTERACTS_WITH]->(d2:Drug);

// Drug has known adverse effect
// (Drug)-[:HAS_ADVERSE_EFFECT]->(AdverseEvent)
CREATE (d)-[:HAS_ADVERSE_EFFECT]->(ae);

// Gene encodes protein
// (Gene)-[:ENCODES]->(Protein)
CREATE (g)-[:ENCODES]->(pr);

// Gene associated with condition (from GWAS/PheGenI)
// (Gene)-[:ASSOCIATED_WITH {p_value: 1e-8, source: 'PheGenI'}]->(Condition)
CREATE (g)-[:ASSOCIATED_WITH]->(c);

// Outcome measured by lab test (LOINC-coded)
// (Outcome)-[:MEASURED_BY]->(LabTest)
CREATE (o)-[:MEASURED_BY]->(lt);

// Publication describes results of trial
// (Publication)-[:DESCRIBES]->(ClinicalTrial)
CREATE (p)-[:DESCRIBES]->(t);

// Publication tagged with MeSH term
// (Publication)-[:TAGGED_WITH]->(MeSHDescriptor)
CREATE (p)-[:TAGGED_WITH]->(m);

// Adverse event coded in MeSH/MedDRA
// (AdverseEvent)-[:CODED_AS_MESH]->(MeSHDescriptor)
CREATE (ae)-[:CODED_AS_MESH]->(m);

// Site located in geographic area (for geographic queries)
// (Site)-[:LOCATED_IN]->(Site)  // city → state → country hierarchy
// Not modeled separately — use Site properties for filtering
