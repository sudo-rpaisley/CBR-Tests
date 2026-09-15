# Dependency structure metrics

This page documents every dispatcher metric in the **Dependency structure** category. Return to the [complete metric index](../metric_reference.md).

## `distance_correlation_matrix_deviation`

Builds a distance-correlation matrix to detect linear and nonlinear dependence.

- **Implementation:** `cbr_tests/metrics/statistical.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** `candidate_fields`; optional `minimum_runnable_fields` default 2; optional `calculation.parameters.max_sample_size`.
- **Primary output:** Distance-correlation matrix, pair overlap counts, mean absolute correlation, and sampling metadata.
- **Interpretation:** 0 indicates no detected dependence; larger values indicate stronger dependence.
- **Computational safeguard:** Distance correlation requires O(n²) pairwise distance matrices. The implementation therefore uses deterministic evenly spaced row sampling with a default and hard maximum of 1,000 rows. A lower configured `max_sample_size` is honoured; a requested value above 1,000 is capped and recorded as `safety_cap_applied` in the outcome metadata. The bounded matrices are calculated with NumPy arrays rather than Python nested lists.
- **Compatibility note:** Despite the legacy ID, this is a dependency profile rather than a candidate-versus-reference deviation. New plans use the canonical `distance_correlation_dependency_profile` ID.

## `pearson_correlation_profile`

Builds a Pearson linear-correlation matrix for usable numeric fields.

- **Implementation:** `cbr_tests/metrics/pearson.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** `candidate_fields`, `minimum_runnable_fields`.
- **Primary output:** Matrix, pair list, overlap counts, pair count, and mean absolute correlation.
- **Interpretation:** Values near ±1 indicate strong linear association; 0 indicates weak linear association.
- **Current caveat:** Correlation is descriptive, sensitive to outliers, and does not establish causation.

## `spearman_correlation_matrix_deviation`

Builds a Spearman rank-correlation matrix for usable numeric fields.

- **Implementation:** `cbr_tests/metrics/spearman.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** `candidate_fields`; optional `minimum_runnable_fields` default 2.
- **Primary output:** Matrix, pair list, overlap counts, pair count, and mean absolute rank correlation.
- **Interpretation:** Values near ±1 indicate strong monotonic association.
- **Current caveat:** Despite the current metric ID suffix, this implementation returns a profile; it does not calculate deviation from an external reference.
