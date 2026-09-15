# Comparing pre-overhaul and post-overhaul outcomes

The metric-conformance overhaul deliberately changes some metric semantics, applicability handling, decision-policy metadata and implementation paths. Existing bucket outcomes are therefore useful as **exploratory baselines**, but they should not be treated as interchangeable with post-overhaul results.

For representative research reruns, prefer `scripts/rerun_and_compare.py`. It archives the authoritative baseline, invokes the normal `run_plan.py` execution path, stores the post-overhaul outcome, generates both comparison formats, and writes a manifest containing the exact CBR-Tests commit, plan/dataset digests and command used.

## One-command representative rerun

```bash
python scripts/rerun_and_compare.py \
  --baseline /path/to/pre_overhaul_bucket2.json \
  --plan /path/to/bucket-plan.json \
  --dataset /home/rpaisley/CBR_Tests/datasets/Buckets/Bucket_2.pcapng \
  --record-dir outcomes/overhaul-reruns/Bucket_2
```

The record directory contains:

```text
baseline_pre_overhaul.json
baseline_summary.md              # when a companion summary exists
outcome_post_overhaul.json
outcome_post_overhaul_summary.md # written by the normal runner
comparison.json
comparison.md
rerun_manifest.json
```

The original baseline is copied before the new run begins and is never modified. Existing generated record files are not overwritten unless `--force` is supplied.

The wrapper reuses the dataset SHA-256 already recorded by the normal CBR-Tests provenance system, avoiding a second full read of a large PCAP. If the outcome does not contain a dataset digest, it falls back to hashing the dataset itself and records that fallback in the manifest.

Useful normal-run options are exposed directly:

```bash
python scripts/rerun_and_compare.py \
  --baseline /path/to/pre_overhaul_bucket2.json \
  --plan /path/to/bucket-plan.json \
  --dataset /home/rpaisley/CBR_Tests/datasets/Buckets/Bucket_2.pcapng \
  --record-dir outcomes/overhaul-reruns/Bucket_2 \
  --workers 2 \
  --display compact \
  --no-update-field-translation
```

`--yes-field-translation-sidecar`, `--no-dataset-summary` and `--refresh-dataset-summary` are also available when the rerun requires them.

## Compare two outcomes without rerunning

Use `scripts/compare_outcomes.py` when both authoritative JSON files already exist:

```bash
python scripts/compare_outcomes.py \
  outcomes/pre_overhaul_bucket2.json \
  outcomes/post_overhaul_bucket2.json
```

By default the comparison ignores volatile execution metadata such as run IDs, timestamps, output paths and runtime durations. It compares the substantive JSON values and treats changes to statuses, applicability, ratios, scores, distances, divergences, deviations, decision rules and measurement-parameter provenance as high-impact.

## Save both human-readable and machine-readable reports

```bash
python scripts/compare_outcomes.py \
  outcomes/pre_overhaul_bucket2.json \
  outcomes/post_overhaul_bucket2.json \
  --markdown outcomes/bucket2_overhaul_comparison.md \
  --json outcomes/bucket2_overhaul_comparison.json
```

The Markdown report is suitable for experiment notes and review. The JSON report is intended for later aggregation across datasets.

## Numerical tolerance

Very small floating-point differences can be ignored with explicit comparison tolerances:

```bash
python scripts/compare_outcomes.py before.json after.json \
  --abs-tol 1e-9 \
  --rel-tol 1e-7
```

These comparison tolerances only control whether two stored outcome values are considered different by the comparison tool. They do **not** alter any realism metric or metric decision threshold.

## Compare run metadata as well

For debugging execution provenance, add:

```bash
python scripts/compare_outcomes.py before.json after.json --include-volatile
```

This includes normally ignored fields such as run IDs, timestamps, output paths and runtime durations.

## CI or scripted use

`--fail-on-high-impact-change` returns exit status `2` if any high-impact difference is present. This can be used in a controlled regression workflow, but it should not be used to imply that all expected overhaul changes are failures.

```bash
python scripts/compare_outcomes.py before.json after.json \
  --fail-on-high-impact-change
```

## Expected overhaul differences

During the first reruns, pay particular attention to:

- train/test duplicate overlap and identifier contamination, which now use test-set denominators instead of Jaccard union denominators;
- zero-denominator cases that now become explicitly non-runnable/not-applicable rather than numerical zero observations;
- intrinsic statistical and temporal diagnostics that were renamed/reinterpreted so they are not confused with candidate-versus-reference realism comparisons;
- binary task metrics that now require explicit positive-label semantics;
- newly emitted `decision_rule` metadata for ratio-based verdicts;
- newly emitted `measurement_parameters` metadata for tolerances that can affect the numerator of consistency metrics;
- any status change caused by corrected applicability or denominator semantics.

A change caused by the overhaul is not automatically evidence that the new result is worse. The comparison report records **what changed**; scientific interpretation should follow the audited metric definition, reference relationship and experiment context.

## Recommended rerun record

For each bucket or representative dataset, retain:

1. the original pre-overhaul authoritative JSON outside the generated record;
2. the archived baseline copy inside the rerun record;
3. the post-overhaul authoritative JSON and its normal human-readable summary;
4. the generated comparison Markdown;
5. the generated comparison JSON;
6. the generated rerun manifest with exact CBR-Tests commit, plan/dataset hashes and command;
7. any threshold/tolerance overrides and their provenance;
8. a short note explaining scientifically material changes.

Once representative reruns have been reviewed, the audited metric contract can be frozen for the final perturbation experiments.
