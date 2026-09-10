# Taxonomy conformance notes

The executable taxonomy has been reconciled with the strengthened paper taxonomy on `overhaul/metric-conformance`.

## Current structure

`taxonomy/master_taxonomy.json` now places the dataset-intrinsic branches beneath `dataset_heuristics`:

1. Protocol and Network Realism
2. Temporal Metrics
3. Statistical Structure Diagnostics
4. Slice Representation
5. Label Fidelity
6. Data Quality and Provenance

`reference_model_comparison` and `task_based_validation` remain separate top-level branches because they require a reference population/model or downstream task rather than describing intrinsic dataset evidence alone.

## Original 61 leaves versus post-review additions

The executable taxonomy contains a small number of supporting or post-expert-review additions beyond the original 61-leaf catalogue. They are explicitly labelled rather than silently folded into the expert-reviewed set:

- `derived_rate_consistency_ratio` — post-expert-review addition pending second expert review;
- `timestamp_coherence_profile` — supporting diagnostic, not one of the original 61 leaves;
- `column_quality_profile` — supporting diagnostic, not one of the original 61 leaves.

The companion paper repository remains responsible for the canonical 61 leaf contracts and their review/evidence metadata.

## Runtime alignment guard

CBR-Tests CI now collects every metric ID advertised anywhere in `master_taxonomy.json` and compares it with the actual handler set returned by `runner.dispatch.build_metric_handlers()`. This prevents taxonomy entries from existing without an executable dispatch path.

The companion `paper-draft` validator separately checks that each canonical paper leaf declares a runtime ID that exists in this executable taxonomy. Together these checks provide a machine-checked path:

`paper leaf -> runtime metric ID -> executable taxonomy -> runtime handler`.

## Freeze condition

The structural taxonomy drift identified at the start of the overhaul is resolved. Before freezing the taxonomy for final experiments, the remaining work is to:

- finish moving production metric code out of the `tests/` package;
- complete a separate threshold/decision-rule audit;
- rerun representative datasets and record the effect of corrected metric semantics.
