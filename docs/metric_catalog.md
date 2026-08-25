# Metric catalog

The complete catalog has moved to [Metric reference](metric_reference.md). It documents all **63** metric IDs supported by the dispatcher, not only the subset currently referenced by plans.

The supplied plans use 36 distinct metric IDs:

- `burstiness_coefficient_deviation` — `deepsecure_plan`
- `class_imbalance_score` — `deepsecure_plan`
- `column_quality_profile` — `deepsecure_plan`, `fortisedos_plan`, `secure5g_plan`
- `cross_slice_duplicate_overlap_ratio` — `deepsecure_plan`, `secure5g_plan`
- `cross_slice_identifier_leakage_ratio` — `deepsecure_plan`
- `diurnal_pattern_similarity_score` — `deepsecure_plan`
- `distance_correlation_matrix_deviation` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `duplicate_row_ratio` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`, `secure5g_plan`
- `energy_distance` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `handshake_plausibility_profile` — `deepsecure_plan`, `secure5g_plan`
- `hourly_activity_distribution_divergence` — `deepsecure_plan`
- `inter_arrival_time_distribution_divergence` — `deepsecure_plan`, `secure5g_plan`
- `kolmogorov_smirnov_feature_divergence` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `label_coverage_ratio` — `deepsecure_plan`
- `maximum_mean_discrepancy` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `missing_value_ratio` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`, `secure5g_plan`
- `non_negative_duration_ratio` — `deepsecure_plan`, `secure5g_plan`
- `packet_byte_consistency_profile` — `deepsecure_plan`, `secure5g_plan`
- `pearson_correlation_profile` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `per_slice_class_coverage_ratio` — `deepsecure_plan`
- `per_slice_feature_coverage_ratio` — `deepsecure_plan`
- `per_slice_sample_coverage_ratio` — `deepsecure_plan`
- `periodicity_preservation_score` — `deepsecure_plan`
- `pre_post_attack_label_bleed_ratio` — `deepsecure_plan`
- `protocol_validity_profile` — `dad_plan`
- `reserved_ip_address_profile` — `deepsecure_plan`, `secure5g_plan`
- `service_port_consistency_profile` — `deepsecure_plan`, `secure5g_plan`
- `slice_distribution_imbalance_score` — `deepsecure_plan`
- `slice_identifier_consistency_profile` — `deepsecure_plan`, `secure5g_plan`
- `spearman_correlation_matrix_deviation` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
- `start_end_timestamp_consistency_ratio` — `deepsecure_plan`, `secure5g_plan`
- `tcp_flag_consistency_profile` — `deepsecure_plan`, `secure5g_plan`
- `timestamp_coherence_profile` — `dad_plan`
- `timestamp_parse_success_ratio` — `deepsecure_plan`, `secure5g_plan`
- `valid_port_range_profile` — `deepsecure_plan`, `secure5g_plan`
- `valid_slice_identifier_profile` — `deepsecure_plan`, `secure5g_plan`
- `wasserstein_feature_distance` — `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`
