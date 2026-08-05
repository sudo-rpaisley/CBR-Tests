# Metric catalog

This catalog summarizes the metrics currently referenced by plans or available to plans. Keep this file updated when metrics are added, renamed, or materially changed.

The taxonomy's reviewed top-level architecture remains **Dataset Heuristics**, **Reference Model Comparison**, and **Task Performance**. Metrics added after that review must be identified as post-review additions until a second expert review is completed.

## Common tabular quality/statistical metrics

| Metric ID | Purpose | Field notes |
| --- | --- | --- |
| `missing_value_ratio` | Measures missing/null value presence. | Usually works across tabular columns; optional candidate fields may constrain scope. |
| `duplicate_row_ratio` | Measures duplicate rows. | Generally does not require specific canonical fields. |
| `pearson_correlation_profile` | Compares Pearson correlation structure. | Uses numeric candidate fields. |
| `spearman_correlation_matrix_deviation` | Compares Spearman/rank correlation structure. | Uses numeric candidate fields. |
| `kolmogorov_smirnov_feature_divergence` | Compares feature distributions. | Uses numeric candidate fields. |
| `wasserstein_feature_distance` | Computes Wasserstein distances for features. | Uses numeric candidate fields. |
| `maximum_mean_discrepancy` | Compares distributions using MMD. | Uses numeric candidate fields. |
| `energy_distance` | Compares distributions with energy distance. | Uses numeric candidate fields. |

## Temporal and label/slice metrics

| Metric ID | Purpose | Field notes |
| --- | --- | --- |
| `timestamp_parse_success_ratio` | Checks timestamp parseability. | Requires/uses timestamp fields. |
| `inter_arrival_time_distribution_divergence` | Compares inter-arrival timing. | Uses timestamp fields. |
| `hourly_activity_distribution_divergence` | Compares hourly activity patterns. | Uses timestamp fields. |
| `periodicity_preservation_score` | Checks periodicity preservation. | Uses timestamp/time-series fields. |
| `label_coverage_ratio` | Checks label availability/coverage. | Uses label fields when present. |
| `slice_identifier_presence_ratio` | Checks slice ID presence. | Uses slice identifier fields. |
| `slice_distribution_consistency_score` | Checks slice distribution consistency. | Uses slice identifier fields. |

## Network/protocol realism metrics

| Metric ID | Purpose | Field notes |
| --- | --- | --- |
| `valid_port_range_profile` | Checks port ranges. | Uses source/destination ports. |
| `service_port_consistency_profile` | Checks service/port consistency. | Uses protocol and port fields. |
| `tcp_flag_consistency_profile` | Checks TCP flag consistency. | Uses TCP flag/count fields. |
| `handshake_plausibility_profile` | Checks TCP handshake plausibility. | Uses TCP flag/count fields. |
| `flow_duration_consistency_profile` | Checks flow duration plausibility. | Uses duration fields. |
| `packet_byte_consistency_profile` | Checks packet/byte count consistency. | Uses packet and byte count fields. |
| `derived_rate_consistency_profile` | Recomputes packet/s and byte/s from counts, byte totals, and duration, then checks reported rates within declared tolerances. | Post-expert-review addition. Requires an explicit `duration_unit`, count/byte fields, and at least one reported rate field. |
| `reserved_ip_address_profile` | Checks reserved/private/bogus address use. | Uses source/destination IP fields. |
| `non_negative_duration_ratio` | Checks duration values are non-negative. | Uses duration fields. |

## Packet-level metrics

| Metric ID | Purpose | Field notes |
| --- | --- | --- |
| `timestamp_coherence_profile` | Scans raw packet timestamps for coherence. | Raw PCAP-oriented. |
| `protocol_validity_profile` | Scans raw packets for protocol validity. | Raw PCAP-oriented. |

## Maintenance note

This is a high-level catalog. For authoritative per-plan required/optional fields, inspect each metric's `field_requirements` in `plans/*.json`.
