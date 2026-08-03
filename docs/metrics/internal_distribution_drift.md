# Internal distribution drift metrics

This page documents every dispatcher metric in the **Internal distribution drift** category. Return to the [complete metric index](../metric_reference.md).

## `energy_distance`

Computes sample energy distance between the first and second halves of each numeric field.

- **Implementation:** `cbr_tests/metrics/statistical.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** Same distributional parameters as KS.
- **Primary output:** Per-field energy distance plus mean/max summary.
- **Interpretation:** 0 indicates matching samples; larger is more different.
- **Current caveat:** Pairwise loops can be expensive; the implementation returns the energy expression without a square root and is scale-dependent.

## `kolmogorov_smirnov_feature_divergence`

Computes the two-sample KS statistic between the first and second halves of each numeric field.

- **Implementation:** `cbr_tests/metrics/statistical.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** `candidate_fields`; `minimum_sample_size` default 2; `max_sample_size` default 1000 per half.
- **Primary output:** Per-field KS D statistic plus mean/max summary.
- **Interpretation:** 0 means matching empirical CDFs; larger values indicate stronger internal distribution change.
- **Current caveat:** This is order-dependent internal drift/stationarity analysis, not fidelity to an external reference; it does not return a p-value.

## `maximum_mean_discrepancy`

Computes RBF-kernel squared MMD between the first and second halves of each numeric field.

- **Implementation:** `cbr_tests/metrics/statistical.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** Same distributional parameters; optional gamma is currently selected internally by median nonzero distance.
- **Primary output:** Per-field MMD value plus mean/max summary.
- **Interpretation:** 0 indicates no detected kernel mean difference; larger means stronger difference.
- **Current caveat:** Order-dependent internal drift. The returned value is squared MMD and pairwise kernel computation can be costly.

## `wasserstein_feature_distance`

Computes one-dimensional Wasserstein distance between the first and second halves of each numeric field.

- **Implementation:** `cbr_tests/metrics/statistical.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- **Inputs:** Same distributional parameters as KS.
- **Primary output:** Per-field distance plus mean/max summary.
- **Interpretation:** 0 means identical sampled distributions; larger values mean greater transport distance in the field’s native units.
- **Current caveat:** Order-dependent and scale-dependent; distances across differently scaled fields are not directly comparable.
