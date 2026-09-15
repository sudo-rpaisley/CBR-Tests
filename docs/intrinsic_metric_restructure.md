# Intrinsic metric restructure

The metric-conformance overhaul reclassifies twelve historically named intrinsic metrics. Their calculations are retained where valid, but the canonical names now describe what the implementations actually measure. Direct candidate-versus-reference fidelity remains in the `reference_model_comparison` branch.

| Historical runtime ID | Canonical runtime ID | Canonical role | External-reference counterpart |
|---|---|---|---|
| `inter_arrival_time_distribution_divergence` | `inter_arrival_internal_drift_ks` | Internal IAT distribution drift | `inter_arrival_distribution_divergence_from_reference` |
| `burstiness_coefficient_deviation` | `burstiness_internal_drift` | Internal burstiness drift | `burstiness_deviation_from_reference` |
| `hourly_activity_distribution_divergence` | `day_to_day_hourly_activity_divergence` | Day-to-day hourly-profile variation | `hourly_activity_divergence_from_reference` |
| `diurnal_pattern_similarity_score` | `day_to_day_diurnal_similarity` | Day-to-day diurnal shape similarity | Hourly reference comparison is the nearest current counterpart |
| `periodicity_preservation_score` | `lagged_periodicity_similarity` | Internal configured-lag repeatability | No direct periodicity-reference leaf yet |
| `kolmogorov_smirnov_feature_divergence` | `feature_ks_internal_drift` | Ordered-half per-feature KS drift | `feature_wise_ks_statistic_from_reference` |
| `wasserstein_feature_distance` | `feature_wasserstein_internal_drift` | Ordered-half per-feature W1 drift | `feature_wise_wasserstein_distance_from_reference` |
| `energy_distance` | `feature_energy_internal_drift` | Ordered-half per-feature energy drift | `feature_wise_energy_distance_from_reference` |
| `maximum_mean_discrepancy` | `feature_mmd2_internal_drift` | Ordered-half per-feature biased empirical RBF MMD² | `feature_set_mmd_score_from_reference` (multivariate) |
| `pearson_correlation_profile` | `pearson_dependency_profile` | Candidate linear-dependency profile | `pearson_matrix_deviation_from_reference` |
| `spearman_correlation_matrix_deviation` | `spearman_dependency_profile` | Candidate monotonic-dependency profile | `spearman_matrix_deviation_from_reference` |
| `distance_correlation_matrix_deviation` | `distance_correlation_dependency_profile` | Candidate nonlinear-dependency profile | `distance_correlation_matrix_deviation_from_reference` |

## Compatibility

Historical IDs remain accepted by the runtime dispatcher so existing plans and outcomes can be replayed. They are deliberately excluded from the canonical metric catalogue used to build new plans. Existing saved plan files are not rewritten; their configuration can be migrated in memory to the canonical replacement when a new plan is generated.

## Interpretation rule

The canonical intrinsic diagnostics have `contextual` interpretation. Internal stability is not synonymous with realism, and dependency strength has no universal preferred direction. These diagnostics can support analysis of structure, drift or consistency; directional fidelity claims require an independently eligible reference or a declared scenario expectation.
