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
