# Final experiment runs

This document describes the authoritative execution path for the final CBR-Tests experiments. Historical plans and compatibility handlers remain available for reproducing earlier work, but final measurements should be run with the strict experiment contract.

## 1. Generate fresh plans

Use the current plan builder rather than selecting an older file from `plans/` solely because it matches a dataset name.

```bash
python create_plan.py --help
```

The current builder exposes canonical metric IDs and can inspect the selected candidate/reference datasets before writing the plan. Historical saved plans may contain legacy metric IDs or compatibility-only profiles whose scientific construct has since changed.

Before committing to a large batch, inspect the generated plan/report and resolve every dataset-specific configuration item such as field mappings, reference datasets, service populations, slice vocabularies, attack windows, class definitions, flow definitions, timestamp units, and other scenario assumptions required by the selected metrics.

## 2. Use strict experiment mode

For a single dataset:

```bash
python run_plan.py \
  --case path/to/final_plan.json \
  --dataset path/to/candidate.csv \
  --output outcomes/final_candidate_01.json \
  --case-id final_candidate_01 \
  --experiment-mode \
  --display quiet \
  --no-update-field-translation
```

For a prepared multi-dataset batch:

```bash
python run_batch.py \
  --batch path/to/final_batch.json \
  --experiment-mode \
  --display quiet \
  --no-update-field-translation
```

`--experiment-mode` rejects a run before metric execution unless all of the following are true:

- metric IDs use the current canonical contract;
- no compatibility-only metric profile is present;
- `execution_policy.sample_mode` is `full`;
- `execution_policy.allow_skips` is `false`;
- at least one metric is enabled.

Compatibility-only profiles currently include historical constructs whose meaning changed enough that they must be regenerated from the candidate dataset and declared scenario policy rather than renamed mechanically.

## 3. Freeze field mappings before the run

Final runs should not discover or silently edit field mappings while measurements are underway. Prepare and review field-translation sidecars first, then use `--no-update-field-translation` for the authoritative run.

A strict preflight can be performed without running metrics:

```bash
python run_plan.py \
  --case path/to/final_plan.json \
  --dataset path/to/candidate.csv \
  --output outcomes/preflight-unused.json \
  --experiment-mode \
  --field-translation-dry-run \
  --no-update-field-translation
```

Experiment mode itself also forbids plans that permit skipped metrics, so a missing required mapping cannot silently reduce the final metric set.

## 4. Resource behaviour does not change metric semantics

The runner may lower metric concurrency when the already-loaded shared dataframe is large relative to currently available memory. This only changes how many independent metrics execute concurrently; it does not sample the candidate dataset, change metric equations, thresholds, denominators, or reference data.

The requested/effective worker count, memory estimate, and cap reason are written into outcome provenance. Raw PCAP/PCAPNG packet views and reference packet views use a compact full-population representation; regression tests verify that the packet-backed metric results are identical to the original canonical representation.

CSV/TSV datasets are loaded directly rather than collecting all chunks and concatenating a second full dataframe. Reference-comparison metrics reuse cached read-only reference data and only copy narrow working columns when conversion is required.

## 5. Dataset summaries

Dataset summary sidecars are descriptive supporting artefacts, not realism metrics. If valid hash-matched summaries already exist, the runner reuses them. On a constrained experiment host, `--no-dataset-summary` may be used to suppress this additional descriptive scan for either a single run or a batch; the choice is recorded in run provenance and does not change metric calculations.

## 6. Preserve authoritative outputs

Do not use `--force-output` for normal final runs. Each outcome should have a unique path. The JSON outcome is the authoritative machine-readable record and includes hashes/provenance for the candidate dataset, plan, relevant configuration, software state, resource policy, and strict experiment contract. The Markdown companion is a human-readable summary.

For an interrupted batch, use its checkpoint and resume rather than restarting completed jobs:

```bash
python run_batch.py \
  --batch path/to/final_batch.json \
  --experiment-mode \
  --resume \
  --display quiet \
  --no-update-field-translation
```

Use `--retry-failed` with `--resume` only when failed/attention jobs should be attempted again. Retries receive separate outcome names so earlier attempts remain auditable.

## 7. Pre-run checklist

Before the first authoritative batch, confirm that:

1. the branch/commit to be used has a fully passing CI run;
2. plans were generated or reviewed under the current canonical taxonomy;
3. candidate and reference datasets are final and immutable for the run;
4. all field translations and scenario-specific assumptions are frozen;
5. experiment mode passes for every plan before execution;
6. output paths/directories are new and have sufficient storage;
7. the experiment host has sufficient RAM/swap and no unrelated high-memory workload;
8. the same code revision and plan/data hashes are retained with the resulting experiment record.
