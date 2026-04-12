# NSCLC Evidence Radar

**Status: working MVP**

A deterministic pipeline that queries the Clinical Trials Knowledge Graph to
surface structured evidence for non-small-cell lung cancer (NSCLC) trials.

## Overview

NSCLC Evidence Radar is a thin, deterministic extension layer on top of the
clinical-trials knowledge graph.  It loads trials into an embedded Samyama
graph, identifies the NSCLC subset via MeSH descriptor `D002289` (with a
condition-name alias fallback), tags each trial by treatment modality
(targeted therapy, immunotherapy, chemotherapy, radiotherapy, antiangiogenic)
using ATC codes and drug-name aliases, then runs five declarative workflows
(weekly update, trial radar, EGFR brief, immunotherapy brief, radiotherapy
brief) against the resulting subset.  Every run is persisted as a
timestamped *snapshot directory* containing per-trial JSONL, workflow JSON +
markdown, and a consolidated `brief.md`.  No LLM calls, no network
randomness: the same graph state and arguments always produce byte-identical
artifacts, which makes snapshots diffable against one another.

## Usage

All commands are exposed as subcommands of `python -m nsclc`:

| Command | What it does |
| --- | --- |
| `run` | End-to-end pipeline: load → subset → workflows → snapshot → brief. |
| `build-subset` | Just the load + NSCLC-identification pass; emits trial records as JSON. |
| `workflows` | Run the five workflows against an existing subset JSON. |
| `diff` | Compare two snapshot directories and emit `diff.json` + `diff.md`. |
| `brief` | Re-generate `brief.md` from an existing snapshot (optionally with a prior). |

Examples:

```bash
# Full pipeline, default conditions, 200 trials/condition
python -m nsclc run

# Pinned to a specific date; smaller sample for a quick iteration
python -m nsclc run --today 2026-04-11 --max-trials 50 --label smoketest

# Just the subset JSON (stdout) + summary (stderr)
python -m nsclc build-subset --max-trials 500 --output /tmp/nsclc.json

# Workflows over an existing subset file
python -m nsclc workflows -i /tmp/nsclc.json -o /tmp/nsclc_wf/

# Diff two snapshots (shortcut: the two most recent by mtime)
python -m nsclc diff --shortcut latest

# Regenerate the brief for a snapshot with a prior for the "New / Updated trials" sections
python -m nsclc brief data/nsclc_runs/2026-04-11 --prior data/nsclc_runs/2026-04-04
```

## Architecture

Pipeline flow (all deterministic, no LLM, no network randomness):

1. **Load graph** — `etl.clinicaltrials_loader.load_trials` pulls trial
   records from ClinicalTrials.gov and ingests them into an embedded
   Samyama graph (`SamyamaClient.embedded()`; one-shot load takes ~11 s for
   the default 200-trial run).
2. **Identify NSCLC trials** — MeSH-first, alias-fallback:
    - MeSH pass: any trial `STUDIES` a `Condition` that is `CODED_AS_MESH`
      to descriptor `D002289`.
    - Alias pass: remaining trials whose `Condition.name` contains any of
      the NSCLC alias strings from `entities.yaml`.  The alias pass is a
      text fallback for un-enriched graphs; in a MeSH-complete graph it
      contributes zero matches.
3. **Tag modalities** — for each trial, `nsclc.modality.tag_modalities`
   checks ATC-code prefixes first (e.g., `L01E*` → `targeted_therapy`), and
   falls back to drug/intervention-name aliases if no ATC evidence fired.
   The rule that fired for each match is recorded in the trial record's
   `modality_evidence` list for transparency.
4. **Run workflows** — the five workflows in `workflows.json` are pure
   functions of the subset list; each returns a structured result plus a
   rendered markdown report.
5. **Write snapshot** — `nsclc.snapshot` creates
   `data/nsclc_runs/<YYYY-MM-DD>[_<label>]/` and persists every artifact
   plus `run_metadata.json` (git SHA, timestamps, graph stats).
6. **(Optional) diff against prior** — `nsclc.diff_snapshots` compares any
   two snapshot dirs and writes `diff.json` + `diff.md`.  The `run`
   subcommand auto-detects the most recent prior snapshot for you.
7. **Write brief** — `nsclc.brief.write_brief` assembles a single
   `brief.md` inside the snapshot with seven sections (see below).

The package is layered so each stage is useful on its own: `build-subset`
stops after step 2, `workflows` starts from a subset file, `brief`
regenerates just the brief from a snapshot, etc.

## Output format

Every `nsclc run` invocation produces a snapshot directory with this layout:

```
data/nsclc_runs/<YYYY-MM-DD>[_<label>]/
├── subset.jsonl                      # one NSCLC trial record per line (sorted by nct_id)
├── subset_summary.json               # counts, mesh/alias mix, modality distribution
├── workflow_weekly_update.json       # structured per-workflow results
├── workflow_weekly_update.md         # human-readable per-workflow reports
├── workflow_trial_radar.{json,md}
├── workflow_egfr_brief.{json,md}
├── workflow_immunotherapy_brief.{json,md}
├── workflow_radiotherapy_brief.{json,md}
├── run_metadata.json                 # commit SHA, python/samyama versions, graph stats, args
└── brief.md                          # consolidated seven-section narrative
```

`brief.md` has a fixed structure:

1. **Overview** — run date, total trials in scope, MeSH vs alias split,
   modality distribution table.
2. **New trials** — trials present in this snapshot but not the prior one
   (falls back to `weekly_update.recently_started` if no prior is given).
3. **Updated trials** — field-level changes (status, phase, enrollment,
   modalities) between snapshots.
4. **Targeted therapy** — EGFR-drug vocabulary, per-drug counts, top 3
   active trials.
5. **Immunotherapy** — per-drug counts, combo-vs-monotherapy breakdown, top
   3 active trials.
6. **Radiotherapy** — radiation-type breakdown (SBRT / proton /
   brachytherapy / other), combo-with-systemic count, top 3 active trials.
7. **Notes / caveats** — data-source stats, MeSH/ATC enrichment flags, git
   SHA and versions for reproducibility.

A snapshot diff (`python -m nsclc diff A B`) additionally produces a
`diff_<a>_vs_<b>/` directory with `diff.json` (trial add/remove/change,
summary delta, per-workflow group movement) and a matching `diff.md`.
