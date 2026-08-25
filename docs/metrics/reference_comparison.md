# Reference comparison metrics

This page documents every dispatcher metric in the **Reference comparison** category. Return to the [complete metric index](../metric_reference.md).

## `burstiness_deviation_from_reference`

Compares candidate and reference burstiness coefficients.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Timestamp and reference path.
- **Primary output:** Both coefficients and absolute deviation.
- **Interpretation:** Lower means closer burstiness.
- **Current caveat:** A single coefficient compresses the full gap distribution.

## `distance_correlation_matrix_deviation_from_reference`

Compares candidate and reference distance-correlation matrices.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same matrix inputs.
- **Primary output:** Per-pair values and mean absolute deviation.
- **Interpretation:** Lower means more similar nonlinear dependency structure.
- **Current caveat:** Quadratic memory/time for both datasets and no row cap in the current profile call.

## `feature_set_mmd_score_from_reference`

Computes one RBF MMD after concatenating sampled values from all candidate fields.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `candidate_fields`, `reference_dataset_path`.
- **Primary output:** Candidate/reference value counts and one MMD score.
- **Interpretation:** Lower means the flattened value collections are more similar.
- **Current caveat:** Values from heterogeneous features are concatenated without scaling or preserving rows, so interpretation is weak when units differ.

## `feature_wise_energy_distance_from_reference`

Energy-compares candidate and reference values per feature.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same reference inputs.
- **Primary output:** Per-field energy distance and mean/max summary.
- **Interpretation:** Lower means closer samples.
- **Current caveat:** Scale-dependent pairwise calculation.

## `feature_wise_ks_statistic_from_reference`

KS-compares candidate and reference values per feature.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same reference inputs.
- **Primary output:** Per-field KS statistic and mean/max summary.
- **Interpretation:** Lower means closer empirical CDFs.
- **Current caveat:** No p-value; missing reference can silently produce no runnable fields.

## `feature_wise_wasserstein_distance_from_reference`

Wasserstein-compares candidate and reference values per feature.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `candidate_fields`, `reference_dataset_path`, optional `max_sample_size` default 1000.
- **Primary output:** Per-field counts/distances and mean/max summary.
- **Interpretation:** Lower means closer distributions in each field’s native units.
- **Current caveat:** Reference path resolves from process CWD; missing/unsupported references become empty results. No scaling is applied.

## `flow_statistic_deviation_from_reference`

Wasserstein-compares configured flow statistics per feature.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `candidate_fields`, reference path.
- **Primary output:** Per-field distances and mean/max summary.
- **Interpretation:** Lower means closer numeric flow-statistic distributions.
- **Current caveat:** Currently an alias of the generic feature-wise Wasserstein workflow.

## `hourly_activity_divergence_from_reference`

Total-variation compares candidate and reference hourly activity.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Timestamp and reference path.
- **Primary output:** Timestamp counts and divergence in [0,1].
- **Interpretation:** Lower means closer hourly proportions.
- **Current caveat:** Timezone/capture-window differences can dominate the score; empty inputs produce zero probability vectors.

## `inter_arrival_distribution_divergence_from_reference`

KS-compares candidate and reference inter-arrival gaps.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `timestamp_field`, `reference_dataset_path`.
- **Primary output:** Gap counts and KS divergence.
- **Interpretation:** Lower means more similar event-spacing distributions.
- **Current caveat:** Timestamps are parsed/sorted independently; missing reference yields null.

## `pearson_matrix_deviation_from_reference`

Compares candidate and reference Pearson correlation matrices.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `candidate_fields`, `reference_dataset_path`.
- **Primary output:** Per-pair current/reference correlations and mean absolute deviation.
- **Interpretation:** Lower means more similar linear dependence structure.
- **Current caveat:** Only pairs available in both matrices are compared; an empty reference yields null.

## `per_slice_class_divergence_from_reference`

Compares label distributions within matching slices.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `slice_field`, `label_field`, reference path.
- **Primary output:** Per-slice total-variation divergences and mean.
- **Interpretation:** Lower means class mixes are closer by slice.
- **Current caveat:** Only runs when slice exists in both dataframes; missing fields produce no slices.

## `per_slice_feature_distribution_deviation_from_reference`

KS-compares candidate features within matching slices.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `slice_field`, `candidate_fields`, reference path.
- **Primary output:** Per-slice/field comparisons and mean KS statistic.
- **Interpretation:** Lower means closer conditional feature distributions.
- **Current caveat:** Hard-coded 1000-value prefix sampling and no scaling.

## `port_use_divergence_from_reference`

Averages total-variation distances for configured port fields.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `port_fields`, default source/destination ports, reference path.
- **Primary output:** Port fields and mean divergence.
- **Interpretation:** Lower means closer categorical port usage.
- **Current caveat:** Ports are treated as string categories and missing fields contribute empty distributions.

## `protocol_mix_divergence_from_reference`

Total-variation compares categorical protocol distributions.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `protocol_field`, default `Protocol`, reference path.
- **Primary output:** Protocol field and one divergence.
- **Interpretation:** Lower means closer protocol mix.
- **Current caveat:** Values are string categories; aliases/case variants can split categories unless translated/normalized upstream.

## `slice_proportion_deviation_from_reference`

Total-variation compares slice proportions.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `slice_field`, reference path.
- **Primary output:** Slice field and one deviation.
- **Interpretation:** Lower means closer slice mix.
- **Current caveat:** String conversion is used; missing fields produce empty distributions.

## `spearman_matrix_deviation_from_reference`

Compares candidate and reference Spearman matrices.

- **Implementation:** `tests/reference_model_comparison_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same matrix inputs.
- **Primary output:** Per-pair values and mean absolute deviation.
- **Interpretation:** Lower means more similar monotonic structure.
- **Current caveat:** Same reference loading and pair-availability limitations.
