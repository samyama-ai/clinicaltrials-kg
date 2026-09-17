# Data licences

`LICENSE` in this repository covers the **loader code**. It says nothing about the
upstream data this repository reads and, where a snapshot is published, redistributes.
That gap is what this file closes (samyama-cloud#97).

Each row records what the source's **own terms page** says, with the URL and the date it
was read. Where a source could not be re-verified it says so rather than guessing: an
unverified licence written down as fact is worse than the silence it replaces.

| Source | What we load | What its terms page says | Checked |
|---|---|---|---|
| [ClinicalTrials.gov (incl. the AACT extract)](https://www.nlm.nih.gov/web_policies.html) | Studies, conditions, interventions, sponsors, sites, outcomes | NLM-produced content. Works produced by the U.S. government are not subject to copyright in the United States; NLM asks for acknowledgement — *Courtesy of the National Library of Medicine* or *Source: National Library of Medicine*. | 2026-09-18 |
| [MeSH (NLM)](https://www.nlm.nih.gov/databases/download/terms_and_conditions.html) | Descriptor ids, labels and tree numbers, fetched live from `id.nlm.nih.gov/mesh` | Same NLM terms: free use and redistribution with attribution, currency disclosure, no implied endorsement. | 2026-09-18 |
| [RxNorm / ATC](https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html) | Drug normalisation during load | **Not re-verified.** RxNorm is distributed by NLM under the UMLS Metathesaurus License, and some source vocabularies inside it carry their own restrictions. Which files this loader touches, and whether any restricted vocabulary reaches the graph, has to be established before a snapshot containing normalised drug names is redistributed. | not checked |
| [openFDA FAERS](https://open.fda.gov/terms/) | Adverse-event reports | Public domain under CC0 1.0 Universal; the FDA waives its rights worldwide, commercial use included. Attribution requested but optional: *Data provided by the U.S. Food and Drug Administration*. Provided as-is, with no warranty of accuracy. | 2026-09-18 |
| [PubMed](https://www.nlm.nih.gov/databases/download/terms_and_conditions.html) | Linked publications | NLM terms as above; see `pubmed-kg/DATA-LICENSES.md` for the abstract-text question. | 2026-09-18 |

**The derived graph.** Four of the five sources are U.S. government works, freely
redistributable with acknowledgement. Carry both attributions — NLM and FDA — with any
published copy.

**Before publishing a snapshot:** settle the RxNorm row. It is the only source here whose
terms are not a government-work waiver, and it is unchecked.

## How to read the "derived graph" line

A graph built from several sources carries **all** of their terms at once. The
restrictive ones win: one non-commercial source makes the join non-commercial, one
share-alike source makes the join share-alike. That is why the derived licence below is
not simply the most permissive source in the table.

## If you redistribute

- Keep the attributions named above with the data.
- State which snapshot version you took, so a reader can check it against the source.
- Re-read the terms pages: licences change, and the dates in this table are when we last
  looked.
