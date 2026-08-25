# Metric reference

The dispatcher supports **64 metric IDs**. The five supplied plans currently use **36** distinct IDs.

## Critical interpretation rules

1. `kolmogorov_smirnov_feature_divergence`, `wasserstein_feature_distance`, `energy_distance`, and `maximum_mean_discrepancy` compare the first and second halves of each field. They are internal drift/stationarity checks, not external fidelity measurements.
2. Top-level outcome status reports execution. A successfully executed metric may still contain domain `pass`, `warn`, `fail`, or `not_applicable`.
3. Reference paths currently resolve from the process working directory. Missing or unsupported references may yield empty or null results instead of an execution error.
4. Distance correlation is quadratic in row count.
5. Benchmark precision, recall, and F1 are binary and should use an explicit `positive_label`.

## Catalog by category

- [Core data quality](metrics/data_quality.md) — 3 metrics.
- [Dependency structure](metrics/dependency_structure.md) — 3 metrics.
- [Internal distribution drift](metrics/internal_distribution_drift.md) — 4 metrics.
- [Temporal integrity and drift](metrics/temporal.md) — 8 metrics.
- [Slice representation](metrics/slice_representation.md) — 6 metrics.
- [Label integrity](metrics/label_integrity.md) — 8 metrics.
- [Task-based validation](metrics/task_validation.md) — 4 metrics.
- [Reference comparison](metrics/reference_comparison.md) — 16 metrics.
- [Network and flow realism](metrics/network_realism.md) — 10 metrics.
- [Raw packet capture checks](metrics/packet_capture.md) — 2 metrics.

## Supplied plans

### `dad_plan.json`

Plan ID `tp-dad-packet-realism-v1`; 2 configured metrics.

`timestamp_coherence_profile`, `protocol_validity_profile`.

### `deepsecure_plan.json`

Plan ID `tp-deepsecure-flow-v1`; 28 configured metrics.

`column_quality_profile`, `pearson_correlation_profile`, `reserved_ip_address_profile`, `valid_port_range_profile`, `service_port_consistency_profile`, `tcp_flag_consistency_profile`, `handshake_plausibility_profile`, `flow_duration_consistency_profile`, `packet_byte_consistency_profile`, `valid_slice_identifier_profile`, `slice_identifier_consistency_profile`, `missing_value_ratio`, `duplicate_row_ratio`, `spearman_correlation_matrix_deviation`, `distance_correlation_matrix_deviation`, `kolmogorov_smirnov_feature_divergence`, `wasserstein_feature_distance`, `energy_distance`, `maximum_mean_discrepancy`, `label_coverage_ratio`, `class_imbalance_score`, `timestamp_parse_success_ratio`, `non_negative_duration_ratio`, `inter_arrival_time_distribution_divergence`, `burstiness_coefficient_deviation`, `hourly_activity_distribution_divergence`, `diurnal_pattern_similarity_score`, `periodicity_preservation_score`.

### `deepslice_plan.json`

Plan ID `tp-pearson-deepslice-v1`; 9 configured metrics.

`pearson_correlation_profile`, `missing_value_ratio`, `duplicate_row_ratio`, `spearman_correlation_matrix_deviation`, `distance_correlation_matrix_deviation`, `kolmogorov_smirnov_feature_divergence`, `wasserstein_feature_distance`, `energy_distance`, `maximum_mean_discrepancy`.

### `fortisedos_plan.json`

Plan ID `tp-fortisedos-kpi-v1`; 10 configured metrics.

`column_quality_profile`, `pearson_correlation_profile`, `missing_value_ratio`, `duplicate_row_ratio`, `spearman_correlation_matrix_deviation`, `distance_correlation_matrix_deviation`, `kolmogorov_smirnov_feature_divergence`, `wasserstein_feature_distance`, `energy_distance`, `maximum_mean_discrepancy`.

### `secure5g_plan.json`

Plan ID `tp-column-quality-secure5g-v1`; 16 configured metrics.

`column_quality_profile`, `reserved_ip_address_profile`, `valid_port_range_profile`, `service_port_consistency_profile`, `valid_slice_identifier_profile`, `slice_identifier_consistency_profile`, `missing_value_ratio`, `duplicate_row_ratio`, `label_coverage_ratio`, `class_imbalance_score`, `per_slice_sample_coverage_ratio`, `per_slice_feature_coverage_ratio`, `per_slice_class_coverage_ratio`, `slice_distribution_imbalance_score`, `cross_slice_duplicate_overlap_ratio`, `cross_slice_identifier_leakage_ratio`.

## Add a metric to a plan

See [Adding metrics](adding_metrics.md) for the implementation checklist and [Plan schema](plan_schema.md) for configuration structure.
