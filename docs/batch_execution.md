# Multi-dataset plan batches

CBR-Tests can create a sequential batch when the same experiment needs to be run across several datasets.

## Interactive creation

Run the normal plan creator:

```bash
python create_plan.py
```

Select the first dataset in the file browser. After each selection the creator asks whether another dataset should be added. When two or more datasets are selected, the creator switches to batch mode.

The default output is:

```text
plans/<plan-id>_batch.json
```

and the generated per-dataset plans are written under:

```text
plans/<plan-id>_batch_plans/
```

## Scripted creation

Repeat `--dataset` for every candidate dataset:

```bash
python create_plan.py \
  --name "5G dataset comparison" \
  --dataset datasets/first.csv \
  --dataset datasets/second.csv \
  --dataset datasets/third.csv
```

Existing single-dataset use is unchanged:

```bash
python create_plan.py --name "One dataset" --dataset datasets/only.csv
```

## Metric policy

By default a batch uses the metric IDs that are runnable across **every selected dataset**. Each dataset is still preflighted independently and receives its own generated plan, field translation resolution, provenance and outcome. Restricting the batch to the common metric IDs prevents later datasets from silently skipping tests selected only because the first dataset happened to contain more fields.

To deliberately keep the full runnable set for each individual dataset instead, use:

```bash
python create_plan.py \
  --name "Dataset-specific coverage" \
  --dataset datasets/first.csv \
  --dataset datasets/second.csv \
  --per-dataset-metrics
```

This mode is useful for coverage/audit work but is less suitable when result rows are intended to be compared directly across datasets.

## Running the batch

Run the manifest with:

```bash
python run_batch.py --batch plans/5g-dataset-comparison_batch.json
```

Datasets are run one after another. Every job calls the normal hardened `run_plan.py` path, so plan validation, field translation, provenance, output safety and human-readable summaries are preserved.

Outcomes default to:

```text
outcomes/<batch-id>/
```

Each batch execution receives a timestamp, and every dataset writes its own JSON outcome and Markdown summary. A `batch_summary_<timestamp>.json` file records the per-job outcome path, process return code and outcome status.

Useful controls include:

```bash
python run_batch.py \
  --batch plans/5g-dataset-comparison_batch.json \
  --workers 1 \
  --display quiet \
  --yes-field-translation-sidecar
```

Use `--fail-fast` only when later datasets should not be started after the first run/process failure. Without it, the batch continues so one problematic dataset does not prevent the remaining experiments from being attempted.

## Reference datasets

A single `--reference-dataset` can be supplied during batch creation and is passed into each per-dataset plan preflight. Raw-PCAP reference rules remain unchanged: an independent PCAP/PCAPNG reference is required and self-comparison is rejected.

For tabular reference-comparison metrics, the existing limitation in automatic reference wiring still applies until that workflow is extended separately.


## Organised result layout

New batch runs keep checkpoint state at the batch root, but place run artefacts under a timestamped `runs/` directory. Within a run, authoritative JSON outcomes are grouped by candidate dataset and then by reference dataset. Supplementary CSV/Markdown reports live separately under `reports/`.

```text
outcomes/<batch-id>/
├── batch_state.json
└── runs/<timestamp>/
    ├── summary.json
    ├── reports/
    │   ├── overview.csv
    │   ├── long.csv
    │   ├── report.md
    │   └── matrices/
    └── results/
        ├── <candidate>/
        │   ├── vs_<reference>/
        │   │   ├── outcome.json
        │   │   └── retry02.json
        │   └── ...
        └── ...
```

The hierarchy is intentionally stable: batch/campaign summaries and analysis tooling should refer to the recorded outcome paths rather than relying on flat-directory filename discovery.

Existing flat checkpoints are not moved. A resumed historical batch continues to honour the exact output paths already recorded in its checkpoint, while newly executed jobs use the organised layout.


## Research-ready comparison reports

Reference-comparison batches also produce a concise run-level `README.md` and several derived report tables under `reports/`. The authoritative evidence remains the individual JSON outcomes under `results/`; the report files are denormalised views intended to make large experiment matrices reviewable without opening every JSON file.

The key derived files are:

- `candidate_summary.csv`: one row per candidate, with intrinsic verdicts deduplicated across repeated all-v-all reference jobs and reference verdicts counted separately;
- `job_summary.csv`: one row per candidate/reference comparison;
- `taxonomy_summary.csv`: verdict and execution counts grouped by taxonomy dimension and by intrinsic/reference scope;
- `attention.csv`: execution problems plus domain `WARN`/`FAIL` results with diagnostic reason codes where available;
- `overview.csv` and `long.csv`: wide and long-form reference-comparison values for analysis;
- `matrices/`: candidate × reference matrices for each reference-comparison metric.

The generated Markdown report explicitly separates execution status from domain verdicts and does **not** calculate an aggregate realism score. Intrinsic metrics are counted once per candidate in candidate-level summaries even when an all-v-all matrix repeats them for every reference pairing. If repeated intrinsic verdicts disagree, the candidate summary records that inconsistency rather than hiding it.
