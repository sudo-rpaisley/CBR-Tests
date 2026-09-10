# Taxonomy conformance notes

The executable taxonomy and the paper taxonomy are not currently structurally identical.

## Current structural drift

The paper's canonical filesystem tree places the following under `Dataset Heuristics`:

1. Protocol and Network Realism
2. Temporal Metrics
3. Statistical Fidelity / Data Quality
4. Slice Representation
5. Label Fidelity

In the current `master_taxonomy.json`, only Protocol and Network Realism is nested under `dataset_heuristics`; Temporal Metrics, Statistical Fidelity, Slice Representation and Label Fidelity are top-level siblings. This must be reconciled before the taxonomy is frozen.

## Post-review additions already present in code

The executable taxonomy also contains leaves or profiles beyond the original 61-leaf paper catalogue, including:

- `derived_rate_consistency_ratio`
- `timestamp_coherence_profile`
- `column_quality_profile`

These should not be silently folded into the original expert-reviewed 61. Each must be marked as one of:

- post-review addition pending validation;
- supporting diagnostic/profile rather than taxonomy leaf; or
- accepted new leaf after the second expert review.

## Target state

The paper taxonomy and executable taxonomy should be generated from, or validated against, one shared metric contract so that path, identifier, equation, implementation status and review status cannot drift independently.
