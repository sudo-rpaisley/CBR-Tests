# Metric conformance overhaul

This branch brings the executable CBR-Tests metric suite into explicit agreement with the paper taxonomy and its equations.

## Scientific source of truth

The companion `sudo-rpaisley/paper-draft` branch `overhaul/metric-conformance` contains the canonical 61 metric leaves. For every metric, the audit preserves the traceable chain:

`literature/standard -> realism relationship -> metric definition -> equation -> implementation -> oracle test -> interpretation`

A passing unit test is not sufficient evidence of conformance if it merely reproduces the implementation. Each canonical metric has a paper-side verification record and is expected to have deterministic executable coverage derived from the written equation.

## Conformance rules

1. Do not change an equation merely to make existing code pass.
2. Do not change code merely to match an equation if the equation is not well supported by literature or standards.
3. Distinguish intrinsic dataset diagnostics from candidate-versus-reference comparison metrics.
4. Distinguish metric equations from PASS/WARN/FAIL thresholds. Thresholds require separate justification.
5. Preserve raw evidence counts alongside ratios and aggregate scores.
6. Treat scenario-dependent properties as contextual rather than universally good or bad.
7. For metrics with a reference dataset, record the reference identity and any field mapping, sampling, normalisation or preprocessing applied.
8. For classification metrics, make binary/multiclass averaging semantics explicit rather than inferring a positive class silently.
9. A zero denominator is not a numerical realism observation: non-executable cases return `None` with an explicit runnable/applicability state where applicable.

## Resolved definition and implementation conflicts

The initial audit found several material mismatches. They have now been resolved on this branch:

| Area | Resolution |
|---|---|
| IP validity | `valid_ip_address_ratio` is now a dedicated address-validity calculation rather than an alias for the broader protocol-validity profile. |
| Reserved-address misuse | Invalid/missing addresses are separated from policy-defined special-use misuse; the denominator is valid checked candidate IP values and the metric is non-runnable without a misuse policy. |
| Intrinsic KS/Wasserstein/Energy/MMD | Recast and renamed as internal distribution-drift diagnostics so they are not confused with candidate-versus-reference realism comparisons. |
| Intrinsic Pearson/Spearman/distance correlation | Recast as dependency profiles; true matrix deviations remain in the Reference Model Comparison branch. |
| Intrinsic temporal metrics | Renamed/reframed as internal IAT drift, burstiness drift, day-to-day hourly/diurnal structure and lagged periodicity diagnostics. |
| Slice coverage | Paper equations and code now use explicit operational definitions and oracle cases for sample, feature and class coverage. |
| Attack-window alignment | Definition and implementation are aligned and the audit records the exact binary agreement semantics rather than implying a narrower quantity. |
| Train/test duplicate overlap | Uses the fraction of unique test keys also present in training, not Jaccard intersection-over-union. |
| Train/test identifier contamination | Uses the fraction of unique test identifiers already present in training, with raw counts retained. |
| Task metrics | Binary semantics require an explicit positive label rather than silently inferring one. |
| Zero-denominator cases | Temporal consistency and data-quality ratios now report `None`/not-runnable instead of a misleading numerical zero. |

## Taxonomy and runtime alignment

`taxonomy/master_taxonomy.json` is now structurally aligned with the strengthened paper taxonomy:

- Protocol and Network Realism, Temporal Metrics, Statistical Structure Diagnostics, Slice Representation, Label Fidelity, and Data Quality and Provenance are nested beneath `dataset_heuristics`.
- Reference Model Comparison and Task-Based Validation remain separate top-level branches.
- post-review/supporting additions such as `derived_rate_consistency_ratio`, `timestamp_coherence_profile`, and `column_quality_profile` are explicitly marked and are not silently counted as members of the original expert-reviewed 61 leaves.

The test suite now enforces that every metric ID advertised by `master_taxonomy.json` is present in the actual handler set produced by `runner.dispatch.build_metric_handlers()`. The paper-side validator separately verifies all 61 leaf contracts and their runtime-ID bindings, giving a machine-checked chain from paper leaf to executable handler.

## Current verification state

At the current overhaul head:

- Python 3.11 CI: green;
- Python 3.12 CI: green;
- generated function/test documentation: current;
- documented quickstart: green;
- taxonomy-to-runtime handler invariant: green;
- companion paper taxonomy validator: green for all 61 canonical leaves.

## Remaining overhaul work

The scientific definition/equation alignment pass is complete enough to proceed to implementation hardening, but the branch is not yet considered frozen for final experiments. Remaining work is:

1. move production metric implementations that still live under `tests/` into `cbr_tests/metrics/`, leaving compatibility imports only where needed;
2. audit PASS/WARN/FAIL thresholds separately from metric equations and label unsupported thresholds as heuristic/configuration-dependent;
3. run representative bucket datasets again and compare changed outcomes against the pre-overhaul exploratory results;
4. document any result changes caused by corrected denominators, non-runnable states, or renamed intrinsic diagnostics;
5. freeze the audited contract only after the implementation-location and threshold audits are complete.
