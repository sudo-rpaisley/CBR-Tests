# Threshold and decision-rule audit

This file is a mechanically generated first-pass inventory of production metric code that may contain decision thresholds, tolerances, minimum-population requirements or verdict rules.

## Scientific rule

A metric equation and a PASS/WARN/FAIL decision rule are different claims. A mathematical/statistical reference may justify the metric without justifying any universal cutoff. Until a cutoff has explicit provenance, it must be treated as framework policy, scenario configuration, or an empirical calibration target rather than a literature-derived realism constant.

Threshold provenance classes used by the overhaul:

- `normative`: directly fixed by a protocol/standard or mathematical domain constraint;
- `literature-derived`: the cited source explicitly recommends/defines the cutoff for the same construct;
- `empirically-calibrated`: selected from declared calibration/reference data and accompanied by the calibration procedure;
- `scenario-configured`: supplied by the experiment/service/slice context;
- `framework-default`: convenience default with no claim of universal scientific validity;
- `eligibility`: minimum data required to compute a statistic reliably; not itself a realism threshold.

## Candidate inventory (193 source lines)

| File | Line | Candidate type | Source |
|---|---:|---|---|
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 16 | eligibility-or-sample-rule | `compute_maximum_mean_discrepancy,` |
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 117 | eligibility-or-sample-rule | `compute_maximum_mean_discrepancy(df, metric),` |
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 126 | eligibility-or-sample-rule | `int(requirements.get("minimum_runnable_fields", 2)),` |
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 141 | eligibility-or-sample-rule | `"minimum_runnable_fields": minimum,` |
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 161 | eligibility-or-sample-rule | `"minimum_runnable_fields": minimum,` |
| `cbr_tests/metrics/intrinsic_diagnostics.py` | 178 | eligibility-or-sample-rule | `"minimum_runnable_fields": minimum,` |
| `cbr_tests/metrics/pcap_handshake.py` | 130 | numeric-cutoff | `return False, {"error": f"Failed to analyse packet capture: {exc}", "reason_code": "dataset_load_error"}` |
| `cbr_tests/metrics/pcap_handshake.py` | 138 | verdict-rule | `status = "not_applicable"` |
| `cbr_tests/metrics/pcap_handshake.py` | 156 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/pcap_handshake.py` | 169 | verdict-rule | `status = "pass"` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 74 | threshold-or-tolerance | `checked_count: int, threshold: float, category_counts: dict, invalid_examples: list,` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 88 | verdict-rule | `if status == "fail":` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 93 | threshold-or-tolerance | `f"({invalid_ratio:.2%}), exceeding the configured failure threshold of {threshold:.2%}."` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 98 | verdict-rule | `if status == "warn":` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 104 | threshold-or-tolerance | `"the invalid-value rate did not exceed the failure threshold."` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 120 | threshold-or-tolerance | `invalid_ratio_fail_threshold = float(params.get("invalid_ratio_fail_threshold", 0.01))` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 121 | threshold-or-tolerance | `if not 0 <= invalid_ratio_fail_threshold <= 1:` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 123 | threshold-or-tolerance | `"error": "invalid_ratio_fail_threshold must be between 0 and 1.",` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 133 | numeric-cutoff | `"error": f"Failed to load dataset: {exc}",` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 236 | threshold-or-tolerance | `if invalid_address_ratio > invalid_ratio_fail_threshold:` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 237 | verdict-rule | `status = "fail"` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 239 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 241 | verdict-rule | `status = "pass"` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 249 | threshold-or-tolerance | `threshold=invalid_ratio_fail_threshold,` |
| `cbr_tests/metrics/protocol_network/address_validity/reserved_ip_address_profile.py` | 270 | threshold-or-tolerance | `"invalid_ratio_fail_threshold": invalid_ratio_fail_threshold,` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 68 | threshold-or-tolerance | `pass_threshold = float(params.get("pass_threshold", 0.99))` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 69 | threshold-or-tolerance | `warn_threshold = float(params.get("warn_threshold", 0.95))` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 70 | verdict-rule | `suspicious_flags_affect_status = bool(` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 73 | threshold-or-tolerance | `if not 0 <= warn_threshold <= pass_threshold <= 1:` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 75 | threshold-or-tolerance | `"error": "Protocol validity thresholds must satisfy 0 <= warn_threshold <= pass_threshold <= 1.",` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 312 | numeric-cutoff | `"error": f"Failed to scan PCAP protocol validity: {exc}",` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 334 | verdict-rule | `status = "not_applicable"` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 335 | threshold-or-tolerance | `elif protocol_validity_ratio >= pass_threshold:` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 336 | verdict-rule | `status = "pass"` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 337 | threshold-or-tolerance | `elif protocol_validity_ratio >= warn_threshold:` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 338 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 340 | verdict-rule | `status = "fail"` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 370 | threshold-or-tolerance | `"pass_threshold": pass_threshold,` |
| `cbr_tests/metrics/protocol_network/address_validity/valid_ip_address_profile.py` | 371 | threshold-or-tolerance | `"warn_threshold": warn_threshold,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 21 | threshold-or-tolerance | `relative_tolerance: float,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 22 | threshold-or-tolerance | `absolute_tolerance: float,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 24 | threshold-or-tolerance | `allowed = (expected.abs() * relative_tolerance).clip(lower=absolute_tolerance)` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 78 | threshold-or-tolerance | `relative_tolerance = float(parameters.get("relative_tolerance", 0.02))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 79 | threshold-or-tolerance | `absolute_tolerance = float(parameters.get("absolute_tolerance", 1e-6))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 81 | threshold-or-tolerance | `pass_threshold = float(parameters.get("pass_threshold", 0.99))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 82 | threshold-or-tolerance | `warn_threshold = float(parameters.get("warn_threshold", 0.95))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 84 | threshold-or-tolerance | `if relative_tolerance < 0 or absolute_tolerance < 0:` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 85 | threshold-or-tolerance | `return False, {"error": "Rate tolerances must be non-negative."}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 86 | threshold-or-tolerance | `if not 0 <= warn_threshold <= pass_threshold <= 1:` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 88 | threshold-or-tolerance | `"error": "Require 0 <= warn_threshold <= pass_threshold <= 1."` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 96 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 137 | threshold-or-tolerance | `packet_rate_matches = _within_tolerance(` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 140 | threshold-or-tolerance | `relative_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 141 | threshold-or-tolerance | `absolute_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 145 | threshold-or-tolerance | `data["flow_packets_per_second"].abs() > absolute_tolerance` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 150 | threshold-or-tolerance | `byte_rate_matches = _within_tolerance(` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 153 | threshold-or-tolerance | `relative_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 154 | threshold-or-tolerance | `absolute_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 158 | threshold-or-tolerance | `data["flow_bytes_per_second"].abs() > absolute_tolerance` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 172 | verdict-rule | `status = "pass" if ratio >= pass_threshold else "warn" if ratio >= warn_threshold else "fail"` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 204 | threshold-or-tolerance | `"relative_tolerance": relative_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/derived_rate_consistency_profile.py` | 205 | threshold-or-tolerance | `"absolute_tolerance": absolute_tolerance,` |
| `cbr_tests/metrics/protocol_network/flow_semantics/flow_duration_consistency_profile.py` | 18 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/flow_duration_consistency_profile.py` | 24 | threshold-or-tolerance | `tol = float(metric.get("calculation", {}).get("parameters", {}).get("tolerance", 1e-6))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/flow_duration_consistency_profile.py` | 59 | verdict-rule | `status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"` |
| `cbr_tests/metrics/protocol_network/flow_semantics/handshake_plausibility_profile.py` | 25 | numeric-cutoff | `except Exception as exc: return False,{"error":f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/handshake_plausibility_profile.py` | 52 | verdict-rule | `status="pass" if ratio>=0.95 else "warn" if ratio>=0.80 else "fail"` |
| `cbr_tests/metrics/protocol_network/flow_semantics/packet_byte_consistency_profile.py` | 18 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/packet_byte_consistency_profile.py` | 25 | threshold-or-tolerance | `tol = float(p.get("tolerance", 1e-6))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/packet_byte_consistency_profile.py` | 26 | threshold-or-tolerance | `vtol = float(p.get("variance_tolerance", 1e-3))` |
| `cbr_tests/metrics/protocol_network/flow_semantics/packet_byte_consistency_profile.py` | 72 | verdict-rule | `status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"` |
| `cbr_tests/metrics/protocol_network/flow_semantics/packet_byte_consistency_profile.py` | 83 | numeric-cutoff | `"byte_total_exceeds_max_possible_count": int(exceed.sum()),` |
| `cbr_tests/metrics/protocol_network/flow_semantics/tcp_flag_consistency_profile.py` | 19 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/flow_semantics/tcp_flag_consistency_profile.py` | 64 | verdict-rule | `status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 27 | verdict-rule | `status = pd.Series("non_integer", index=series.index, dtype="string")` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 28 | verdict-rule | `status = status.mask(missing_mask, "missing")` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 29 | verdict-rule | `status = status.mask(integer_mask & ~in_range_mask, "out_of_range")` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 30 | verdict-rule | `status = status.mask(in_range_mask, "valid")` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 98 | threshold-or-tolerance | `match_ratio: float, pass_threshold: float, warn_threshold: float,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 110 | threshold-or-tolerance | `"pass_threshold": pass_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 111 | threshold-or-tolerance | `"warn_threshold": warn_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 118 | verdict-rule | `if status == "fail":` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 120 | threshold-or-tolerance | `"reason_code": "service_port_match_below_warn_threshold",` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 123 | threshold-or-tolerance | `f"({match_ratio:.2%}); this is below the warning threshold of {warn_threshold:.2%}."` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 128 | verdict-rule | `if status == "warn":` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 129 | threshold-or-tolerance | `if match_ratio < pass_threshold:` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 132 | threshold-or-tolerance | `f"({match_ratio:.2%}); this is below the pass threshold of {pass_threshold:.2%} but not below the warning threshold."` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 134 | threshold-or-tolerance | `reason_code = "service_port_match_below_pass_threshold"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 136 | numeric-cutoff | `summary = f"Expected service-port usage passed, but {invalid_rows} selected row(s) contained invalid port values."` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 148 | threshold-or-tolerance | `f"({match_ratio:.2%}), meeting the pass threshold of {pass_threshold:.2%}."` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 178 | threshold-or-tolerance | `pass_threshold = float(params.get("pass_threshold", 0.95))` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 179 | threshold-or-tolerance | `warn_threshold = float(params.get("warn_threshold", 0.75))` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 197 | threshold-or-tolerance | `if not 0 <= warn_threshold <= pass_threshold <= 1:` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 199 | threshold-or-tolerance | `"error": "Thresholds must satisfy 0 <= warn_threshold <= pass_threshold <= 1.",` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 208 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}", "reason_code": "dataset_load_error"}` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 305 | verdict-rule | `missing_mask = port_status == "missing"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 306 | verdict-rule | `valid_mask = port_status == "valid"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 345 | verdict-rule | `port_status = status_by_field[field].loc[idx]` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 372 | verdict-rule | `status = "not_applicable"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 388 | threshold-or-tolerance | `if service_port_match_ratio >= pass_threshold:` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 389 | verdict-rule | `status = "pass"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 390 | threshold-or-tolerance | `elif service_port_match_ratio >= warn_threshold:` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 391 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 393 | verdict-rule | `status = "fail"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 394 | verdict-rule | `if invalid_port_row_count > 0 and status == "pass":` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 395 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 403 | threshold-or-tolerance | `pass_threshold=pass_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 404 | threshold-or-tolerance | `warn_threshold=warn_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 440 | threshold-or-tolerance | `"pass_threshold": pass_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/service_port_consistency_profile.py` | 441 | threshold-or-tolerance | `"warn_threshold": warn_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 23 | numeric-cutoff | `if valid_min_port <= port <= valid_max_port:` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 39 | threshold-or-tolerance | `zero_count: int, invalid_ratio: float \| None, threshold: float,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 40 | numeric-cutoff | `valid_min_port: int, valid_max_port: int, examples: list) -> dict:` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 48 | numeric-cutoff | `"valid_range": [valid_min_port, valid_max_port],` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 53 | verdict-rule | `if status == "not_applicable":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 60 | verdict-rule | `if status == "fail":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 65 | threshold-or-tolerance | `f"exceeding the configured failure threshold of {threshold:.2%}."` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 70 | verdict-rule | `if status == "warn":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 75 | threshold-or-tolerance | `"but the invalid-value rate did not exceed the failure threshold."` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 82 | numeric-cutoff | `"summary": f"All {checked} checked port values were within the configured range {valid_min_port}-{valid_max_port}.",` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 96 | numeric-cutoff | `valid_min_port = int(params.get("valid_min_port", 0))` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 97 | numeric-cutoff | `valid_max_port = int(params.get("valid_max_port", 65535))` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 98 | threshold-or-tolerance | `invalid_ratio_fail_threshold = float(params.get("invalid_ratio_fail_threshold", 0.01))` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 99 | numeric-cutoff | `if valid_min_port < 0 or valid_max_port > 65535 or valid_min_port > valid_max_port:` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 101 | numeric-cutoff | `"error": "Configured port bounds must satisfy 0 <= valid_min_port <= valid_max_port <= 65535.",` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 104 | threshold-or-tolerance | `if not 0 <= invalid_ratio_fail_threshold <= 1:` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 106 | threshold-or-tolerance | `"error": "invalid_ratio_fail_threshold must be between 0 and 1.",` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 116 | numeric-cutoff | `"error": f"Failed to load dataset: {exc}",` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 155 | verdict-rule | `value_status, parsed_port = parse_port(value, valid_min_port, valid_max_port)` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 157 | verdict-rule | `if value_status == "missing":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 165 | verdict-rule | `if value_status == "valid":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 178 | verdict-rule | `if value_status == "non_integer":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 181 | verdict-rule | `elif value_status == "out_of_range":` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 218 | verdict-rule | `status = "not_applicable"` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 219 | threshold-or-tolerance | `elif invalid_port_ratio is not None and invalid_port_ratio > invalid_ratio_fail_threshold:` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 220 | verdict-rule | `status = "fail"` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 222 | verdict-rule | `status = "warn"` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 224 | verdict-rule | `status = "pass"` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 234 | threshold-or-tolerance | `threshold=invalid_ratio_fail_threshold,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 235 | numeric-cutoff | `valid_min_port=valid_min_port,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 236 | numeric-cutoff | `valid_max_port=valid_max_port,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 258 | numeric-cutoff | `"valid_min_port": valid_min_port,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 259 | numeric-cutoff | `"valid_max_port": valid_max_port,` |
| `cbr_tests/metrics/protocol_network/port_validity/valid_port_range_profile.py` | 260 | threshold-or-tolerance | `"invalid_ratio_fail_threshold": invalid_ratio_fail_threshold,` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/slice_identifier_consistency_profile.py` | 67 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/slice_identifier_consistency_profile.py` | 140 | verdict-rule | `status = "not_applicable" if checked == 0 else ("pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail")` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/valid_slice_identifier_profile.py` | 56 | numeric-cutoff | `return False, {"error": f"Failed to load dataset: {exc}"}` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/valid_slice_identifier_profile.py` | 70 | numeric-cutoff | `pass` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/valid_slice_identifier_profile.py` | 96 | numeric-cutoff | `pass` |
| `cbr_tests/metrics/protocol_network/slice_metadata_integrity/valid_slice_identifier_profile.py` | 111 | verdict-rule | `status = "pass" if valid_ratio >= 0.99 else "warn" if valid_ratio >= 0.95 else "fail"` |
| `cbr_tests/metrics/reference_comparison.py` | 200 | eligibility-or-sample-rule | `f"max_{output_key}": round(max(values), 6) if values else None,` |
| `cbr_tests/metrics/slice_representation.py` | 53 | eligibility-or-sample-rule | `default_minimum = int(parameters.get("minimum_sample_count", 1))` |
| `cbr_tests/metrics/slice_representation.py` | 56 | eligibility-or-sample-rule | `by_slice = parameters.get("minimum_sample_count_by_slice", {})` |
| `cbr_tests/metrics/slice_representation.py` | 74 | eligibility-or-sample-rule | `"minimum_sample_count": minimum,` |
| `cbr_tests/metrics/slice_representation.py` | 245 | eligibility-or-sample-rule | `"method": "max_minus_min_observed_slice_proportion",` |
| `cbr_tests/metrics/statistical.py` | 35 | eligibility-or-sample-rule | `max_difference = 0.0` |
| `cbr_tests/metrics/statistical.py` | 43 | eligibility-or-sample-rule | `max_difference = max(max_difference, abs(left_cdf - right_cdf))` |
| `cbr_tests/metrics/statistical.py` | 44 | eligibility-or-sample-rule | `return max_difference` |
| `cbr_tests/metrics/statistical.py` | 110 | eligibility-or-sample-rule | `minimum_sample_size = parameters.get("minimum_sample_size", 2)` |
| `cbr_tests/metrics/statistical.py` | 134 | eligibility-or-sample-rule | `if len(left) < minimum_sample_size or len(right) < minimum_sample_size:` |
| `cbr_tests/metrics/statistical.py` | 154 | eligibility-or-sample-rule | `f"max_{output_key}": round(max(values), 6) if values else None,` |
| `cbr_tests/metrics/statistical.py` | 175 | eligibility-or-sample-rule | `df, metric, _rbf_mmd, "maximum_mean_discrepancy"` |
| `cbr_tests/metrics/temporal.py` | 47 | eligibility-or-sample-rule | `max_difference = 0.0` |
| `cbr_tests/metrics/temporal.py` | 53 | eligibility-or-sample-rule | `max_difference = max(` |
| `cbr_tests/metrics/temporal.py` | 54 | eligibility-or-sample-rule | `max_difference,` |
| `cbr_tests/metrics/temporal.py` | 57 | eligibility-or-sample-rule | `return max_difference` |
| `cbr_tests/metrics/temporal.py` | 82 | numeric-cutoff | `failed_mask = checked_mask & timestamps.isna()` |
| `cbr_tests/metrics/temporal.py` | 86 | numeric-cutoff | `failed_count = int(failed_mask.sum())` |
| `cbr_tests/metrics/temporal.py` | 96 | numeric-cutoff | `"failed_parse_count": failed_count,` |
| `cbr_tests/metrics/temporal.py` | 187 | eligibility-or-sample-rule | `minimum_sample_size = metric.get("calculation", {}).get("parameters", {}).get(` |
| `cbr_tests/metrics/temporal.py` | 188 | eligibility-or-sample-rule | `"minimum_sample_size", 2` |
| `cbr_tests/metrics/temporal.py` | 190 | eligibility-or-sample-rule | `runnable = len(left) >= minimum_sample_size and len(right) >= minimum_sample_size` |
| `cbr_tests/metrics/temporal.py` | 328 | eligibility-or-sample-rule | `minimum_day_count = max(2, int(parameters.get("minimum_day_count", 2)))` |
| `cbr_tests/metrics/temporal.py` | 334 | eligibility-or-sample-rule | `runnable = day_count >= minimum_day_count and divergence is not None` |
| `cbr_tests/metrics/temporal.py` | 339 | eligibility-or-sample-rule | `"minimum_day_count": minimum_day_count,` |
| `cbr_tests/metrics/temporal.py` | 363 | eligibility-or-sample-rule | `minimum_day_count = max(2, int(parameters.get("minimum_day_count", 2)))` |
| `cbr_tests/metrics/temporal.py` | 369 | eligibility-or-sample-rule | `runnable = day_count >= minimum_day_count and score is not None` |
| `cbr_tests/metrics/temporal.py` | 374 | eligibility-or-sample-rule | `"minimum_day_count": minimum_day_count,` |
| `cbr_tests/metrics/temporal.py` | 405 | eligibility-or-sample-rule | `minimum_pairs: int,` |
| `cbr_tests/metrics/temporal.py` | 410 | eligibility-or-sample-rule | `if pair_count < minimum_pairs:` |
| `cbr_tests/metrics/temporal.py` | 448 | eligibility-or-sample-rule | `minimum_lag_pairs = max(1, int(parameters.get("minimum_lag_pairs", 2)))` |
| `cbr_tests/metrics/temporal.py` | 457 | eligibility-or-sample-rule | `minimum_lag_pairs,` |
| `cbr_tests/metrics/temporal.py` | 467 | eligibility-or-sample-rule | `"minimum_lag_pairs": minimum_lag_pairs,` |
| `cbr_tests/metrics/temporal.py` | 485 | eligibility-or-sample-rule | `"minimum_lag_pairs": minimum_lag_pairs,` |
| `cbr_tests/metrics/timestamp_coherence.py` | 11 | threshold-or-tolerance | `large_gap_threshold_seconds = float(` |
| `cbr_tests/metrics/timestamp_coherence.py` | 12 | threshold-or-tolerance | `parameters.get("large_gap_threshold_seconds", 1.0)` |
| `cbr_tests/metrics/timestamp_coherence.py` | 24 | eligibility-or-sample-rule | `max_gap_seconds = 0.0` |
| `cbr_tests/metrics/timestamp_coherence.py` | 44 | threshold-or-tolerance | `if delta > large_gap_threshold_seconds:` |
| `cbr_tests/metrics/timestamp_coherence.py` | 46 | eligibility-or-sample-rule | `max_gap_seconds = max(max_gap_seconds, delta)` |
| `cbr_tests/metrics/timestamp_coherence.py` | 49 | numeric-cutoff | `return False, {"error": f"Failed to scan PCAP timestamps: {exc}"}` |
| `cbr_tests/metrics/timestamp_coherence.py` | 70 | threshold-or-tolerance | `"large_gap_threshold_seconds": large_gap_threshold_seconds,` |
| `cbr_tests/metrics/timestamp_coherence.py` | 73 | eligibility-or-sample-rule | `"max_gap_seconds": round(max_gap_seconds, 6),` |
| `cbr_tests/metrics/timestamp_coherence.py` | 74 | verdict-rule | `"status": "warn" if backwards_jump_count else "pass",` |

## Review priorities

1. Hard-coded PASS/WARN/FAIL cutoffs in canonical or compatibility metric handlers.
2. Default threshold values read from metric parameters without an explicit provenance field.
3. Tolerances that are part of a physical/mathematical consistency check and therefore may be justified by units/rounding rather than realism literature.
4. Minimum sample/population requirements, which should be labelled as eligibility/reliability rules rather than realism verdict thresholds.

This inventory is deliberately broader than the final threshold table; false positives are expected and must be classified during the manual audit.
