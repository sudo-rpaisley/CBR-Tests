# Comparing pre-overhaul and post-overhaul outcomes

The metric-conformance overhaul deliberately changes some metric semantics, applicability handling, decision-policy metadata and implementation paths. Existing bucket outcomes are therefore useful as **exploratory baselines**, but they should not be treated as interchangeable with post-overhaul results.

Use `scripts/compare_outcomes.py` after rerunning a dataset to make the differences explicit.

## Basic comparison

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

1. the original pre-overhaul authoritative JSON;
2. the post-overhaul authoritative JSON;
3. the generated comparison Markdown;
4. the generated comparison JSON;
5. the exact CBR-Tests commit and plan used for the rerun;
6. any threshold/tolerance overrides and their provenance;
7. a short note explaining scientifically material changes.

Once representative reruns have been reviewed, the audited metric contract can be frozen for the final perturbation experiments.
