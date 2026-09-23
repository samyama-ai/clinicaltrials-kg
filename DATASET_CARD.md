---
license: other
pretty_name: Clinical Trials Knowledge Graph
tags:
  - knowledge-graph
  - samyama
  - property-graph
  - clinical
language:
  - en
size_categories:
  - 1M<n<10M
---

# Dataset Card for `clinicaltrials-kg`

**7.8 million nodes. 27 million edges. Every registered study on ClinicalTrials.gov in one graph.**

> Part of the **Samyama** ecosystem. This card describes the dataset; the repository
> holds the loader and source-data specifics.

## Structure

**11 node labels** -- ClinicalTrial, Condition, Intervention, ArmGroup, Outcome, Sponsor, Site, AdverseEvent, MeSHDescriptor, Drug, Publication

**11 edge types** -- STUDIES, TESTS, HAS_ARM, USES, SPONSORED_BY, CONDUCTED_AT, MEASURES, REPORTED, CODED_AS_MESH, CODED_AS_DRUG, PUBLISHED_IN

**5 data sources** -- ClinicalTrials.gov, MeSH (NLM), RxNorm/ATC, OpenFDA FAERS, PubMed

## Provenance and licence

Apache 2.0 covers the loader. Four of the five sources — ClinicalTrials.gov, MeSH, PubMed
(NLM) and openFDA FAERS (CC0) — are U.S. government works, redistributable with
acknowledgement of NLM and FDA. **RxNorm/ATC is not yet verified** and is the row to settle
before publishing a snapshot. See [`DATA-LICENSES.md`](DATA-LICENSES.md).


## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.

## Personal data in the text fields

The published snapshot carries **contact email addresses and phone numbers**
inside free-text and name fields: 33 distinct addresses in `Site.facility`, 12
phone numbers in `Site.zip`, and smaller numbers across ten other fields, out
of 36.5 million property values.

They are there because they are in the registry. ClinicalTrials.gov requires
contact details for recruiting trials and publishes them so patients can reach
sites; AACT redistributes that extract in full. **This ETL never reads AACT's
`contacts` or `central_contacts` tables** — it takes `facility`, `city`,
`state`, `country` and `zip` from `contactsLocationsModule.locations`. Every
occurrence is therefore incidental text inside a field copied verbatim: a
facility *name* with a contact appended, sponsor-written `brief_summary` prose,
or a postcode column somebody typed a phone number into at registration.

They are **not scrubbed**, deliberately. Removing them would edit
sponsor-authored text and make this snapshot diverge from AACT, and matching
the source is the property a benchmark snapshot most has to keep. The decision,
with the count accepted per field, is recorded in
[`docs/pii-waivers.json`](https://github.com/samyama-ai/samyama-graph/blob/main/docs/pii-waivers.json)
in the engine repository (samyama-graph#1439), and a weekly scan fails if a
*new* contact appears in a field already on that list.

If you are re-publishing this graph, that decision is ours and not
automatically yours — it rests on the source registry's own publication, and
your jurisdiction and purpose may differ.

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Benchmark (100 queries) | [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |
