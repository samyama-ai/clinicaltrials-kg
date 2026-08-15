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

Apache 2.0

> ⚠️ **The licence above covers this repository's code, not the data.** This graph is
> derived from an upstream source (ClinicalTrials.gov, MeSH (NLM), RxNorm/ATC, OpenFDA FAERS, PubMed), whose
> own terms govern redistribution and are **not stated here**. Establish and record them
> before redistributing or quoting this dataset. The frontmatter is therefore
> `license: other` rather than `apache-2.0`.

## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Benchmark (100 queries) | [Biomedical Benchmark](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |
