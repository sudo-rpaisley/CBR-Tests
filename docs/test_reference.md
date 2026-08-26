# Test suite reference

The suite contains **153 pytest test functions**. Every test and helper in `tests/test_*.py` is listed below.

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Run one test with `python -m pytest -q path/to/test.py::test_name`.

## `tests/test_correctness_reproducibility.py`

Parallel correctness, timestamps, atomic writes, and schema regression coverage.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_parallel_fail_fast_marks_unsubmitted_metrics(tmp_path: Path)` (L24) | Verifies that parallel fail fast marks unsubmitted metrics. | `run_metrics_parallel`, `_metric`, `called.append` |
| `test_parallel_payload_contains_real_metric_timestamps(tmp_path: Path)` (L51) | Verifies that parallel payload contains real metric timestamps. | `run_metrics_parallel`, `datetime.fromisoformat`, `_metric` |
| `test_parallel_collector_preserves_completed_and_not_run_records()` (L69) | Verifies that parallel collector preserves completed and not run records. | `datetime`, `collect_parallel_metric_results`, `_metric` |
| `test_write_outcome_creates_parent_and_replaces_file(tmp_path: Path)` (L113) | Verifies that write outcome creates parent and replaces file. | `write_outcome`, `destination.parent.glob`, `destination.read_text` |
| `test_plan_schema_rejects_duplicate_metric_ids()` (L122) | Verifies that plan schema rejects duplicate metric ids. | `pytest.raises`, `validate_plan_schema`, `_metric` |
| `test_plan_schema_rejects_required_optional_overlap()` (L133) | Verifies that plan schema rejects required optional overlap. | `_metric`, `pytest.raises`, `validate_plan_schema` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_metric(metric_id: str) -> dict` (L15) | Implementation helper for metric. |
| `test_parallel_fail_fast_marks_unsubmitted_metrics._failed(_dataset, _metric_config)` (L28) | Implementation helper for failed. |
| `test_parallel_fail_fast_marks_unsubmitted_metrics._unexpected(_dataset, metric_config)` (L32) | Implementation helper for unexpected. |

## `tests/test_create_plan_overwrite.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_interactive_builder_can_confirm_overwrite(monkeypatch, tmp_path)` (L41) | Verifies that interactive builder can confirm overwrite. | `_dataset`, `output.write_text`, `iter`, `monkeypatch.setattr`, `_TTYInput`, `create_plan.main`, `output.read_text`, `_args` |
| `test_interactive_builder_can_decline_overwrite(monkeypatch, tmp_path)` (L57) | Verifies that interactive builder can decline overwrite. | `_dataset`, `output.write_text`, `iter`, `monkeypatch.setattr`, `_TTYInput`, `create_plan.main`, `output.read_text`, `_args` |
| `test_noninteractive_builder_still_requires_force_to_replace(monkeypatch, tmp_path)` (L72) | Verifies that noninteractive builder still requires force to replace. | `_dataset`, `output.write_text`, `monkeypatch.setattr`, `_NonTTYInput`, `pytest.raises`, `create_plan.main`, `output.read_text`, `_args` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_TTYInput` (L8) | Data model for TTYInput. |
| `_TTYInput.isatty(self)` (L9) | Implementation helper for isatty. |
| `_NonTTYInput` (L13) | Data model for NonTTYInput. |
| `_NonTTYInput.isatty(self)` (L14) | Implementation helper for isatty. |
| `_args(dataset, output, *, force = False)` (L18) | Implementation helper for args. |
| `_dataset(path)` (L34) | Implementation helper for dataset. |

## `tests/test_data_quality_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_compute_missing_value_ratio_uses_candidate_fields()` (L7) | Verifies that compute missing value ratio uses candidate fields. | `compute_missing_value_ratio` |
| `test_compute_duplicate_row_ratio_counts_repeated_rows_after_first()` (L27) | Verifies that compute duplicate row ratio counts repeated rows after first. | `compute_duplicate_row_ratio` |
| `test_compute_spearman_profile_reports_rank_correlation()` (L42) | Verifies that compute spearman profile reports rank correlation. | `validate_spearman_candidate_fields`, `compute_spearman_profile` |

## `tests/test_derived_rate_consistency_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_accepts_rates_consistent_with_counts_bytes_and_duration(tmp_path: Path)` (L39) | Verifies that accepts rates consistent with counts bytes and duration. | `_write_dataset`, `run_derived_rate_consistency_metric`, `_metric` |
| `test_reports_rate_mismatches_and_zero_duration_volume(tmp_path: Path)` (L74) | Verifies that reports rate mismatches and zero duration volume. | `_write_dataset`, `run_derived_rate_consistency_metric`, `_metric` |
| `test_requires_an_explicit_duration_unit(tmp_path: Path)` (L111) | Verifies that requires an explicit duration unit. | `_write_dataset`, `_metric`, `run_derived_rate_consistency_metric` |
| `test_requires_at_least_one_reported_rate_field(tmp_path: Path)` (L135) | Verifies that requires at least one reported rate field. | `_write_dataset`, `_metric`, `run_derived_rate_consistency_metric` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_metric() -> dict` (L10) | Implementation helper for metric. |
| `_write_dataset(tmp_path: Path, rows: list[dict]) -> Path` (L33) | Implementation helper for write dataset. |

## `tests/test_experiment_contract.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_plan_schema_rejects_unimplemented_sample_mode()` (L48) | Verifies that plan schema rejects unimplemented sample mode. | `pytest.raises`, `validate_plan_schema`, `_plan` |
| `test_plan_schema_validates_applicability_types()` (L53) | Verifies that plan schema validates applicability types. | `_plan`, `pytest.raises`, `validate_plan_schema` |
| `test_dataset_format_applicability_is_enforced(tmp_path: Path)` (L60) | Verifies that dataset format applicability is enforced. | `pytest.raises`, `validate_dataset_format_applicability`, `_plan` |
| `test_numeric_applicability_is_enforced()` (L65) | Verifies that numeric applicability is enforced. | `pytest.raises`, `validate_loaded_dataset_applicability`, `_plan` |
| `test_allow_skips_false_is_enforced()` (L71) | Verifies that allow skips false is enforced. | `pytest.raises`, `enforce_skip_policy`, `_plan` |
| `test_allow_skips_true_and_dry_run_are_permitted()` (L76) | Verifies that allow skips true and dry run are permitted. | `enforce_skip_policy`, `_plan` |
| `test_output_path_cannot_collide_with_input(tmp_path: Path)` (L81) | Verifies that output path cannot collide with input. | `dataset.write_text`, `pytest.raises`, `validate_output_path_safety` |
| `test_existing_output_requires_explicit_overwrite(tmp_path: Path)` (L88) | Verifies that existing output requires explicit overwrite. | `output.write_text`, `validate_output_path_safety`, `pytest.raises` |
| `test_provenance_identifies_exact_inputs(tmp_path: Path)` (L96) | Verifies that provenance identifies exact inputs. | `dataset.write_bytes`, `_plan`, `plan_file.write_text`, `build_provenance_manifest`, `hashlib.sha256(dataset.read_bytes()).hexdigest`, `sha256_json`, `hashlib.sha256`, `dataset.read_bytes` |
| `test_case_provenance_resolves_referenced_plan(tmp_path: Path)` (L123) | Verifies that case provenance resolves referenced plan. | `plan_dir.mkdir`, `plan_file.write_text`, `case_file.write_text`, `resolve_plan_source_path`, `plan_file.resolve` |
| `test_outcome_schema_v2_embeds_provenance(tmp_path: Path)` (L134) | Verifies that outcome schema v2 embeds provenance. | `_plan`, `datetime.now`, `time.perf_counter`, `build_outcome` |
| `test_outcome_writer_rejects_non_finite_json_without_replacing_previous_file(tmp_path: Path)` (L158) | Verifies that outcome writer rejects non finite JSON without replacing previous file. | `write_outcome`, `pytest.raises`, `output.read_text` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_plan(*, sample_mode: str = 'full', allow_skips: bool = False) -> dict` (L23) | Implementation helper for plan. |

## `tests/test_field_translation.py`

Translation loading, detection, sidecars, reports, suggestions, and formatting.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_load_field_translation_accepts_standard_test_to_dataset_object(tmp_path)` (L11) | Verifies that load field translation accepts standard test to dataset object. | `translation_path.write_text`, `load_field_translation` |
| `test_load_field_translation_rejects_duplicate_targets(tmp_path)` (L21) | Verifies that load field translation rejects duplicate targets. | `translation_path.write_text`, `pytest.raises`, `load_field_translation` |
| `test_load_tabular_dataset_preserves_supplied_columns(tmp_path)` (L29) | Verifies that load tabular dataset preserves supplied columns. | `dataset_path.write_text`, `load_tabular_dataset` |
| `test_case_can_reference_field_translation_file(tmp_path)` (L38) | Verifies that case can reference field translation file. | `plan_path.write_text`, `translation_path.write_text`, `case_path.write_text`, `load_case_or_plan`, `translation_path.resolve` |
| `test_detect_standard_pcap_field_translation_maps_tshark_headings()` (L64) | Verifies that detect standard PCAP field translation maps tshark headings. | `detect_standard_pcap_field_translation` |
| `test_detect_standard_pcap_field_translation_chooses_one_source_per_test_field()` (L90) | Verifies that detect standard PCAP field translation chooses one source per test field. | `detect_standard_pcap_field_translation` |
| `test_merge_field_translations_allows_explicit_mapping_to_override_auto_target()` (L98) | Verifies that merge field translations allows explicit mapping to override auto target. | `merge_field_translations` |
| `test_translate_metric_fields_resolves_standard_pcap_csv_headings()` (L109) | Verifies that translate metric fields resolves standard PCAP csv headings. | `detect_standard_pcap_field_translation`, `translate_metric_fields` |
| `test_collect_required_test_fields_from_plan()` (L132) | Verifies that collect required test fields from plan. | `collect_required_test_fields` |
| `test_ensure_field_translation_file_creates_template_with_detected_fields(tmp_path)` (L152) | Verifies that ensure field translation file creates template with detected fields. | `dataset_path.write_text`, `ensure_field_translation_file`, `translation_path.read_text` |
| `test_ensure_field_translation_file_updates_template_with_new_required_fields(tmp_path)` (L183) | Verifies that ensure field translation file updates template with new required fields. | `dataset_path.write_text`, `translation_path.write_text`, `ensure_field_translation_file`, `translation_path.read_text` |
| `test_detect_standard_pcap_field_translation_for_dataset_reads_csv_header(tmp_path)` (L210) | Verifies that detect standard PCAP field translation for dataset reads csv header. | `dataset_path.write_text`, `detect_standard_pcap_field_translation_for_dataset` |
| `test_existing_sidecar_is_not_rewritten_when_no_fields_are_missing(tmp_path)` (L223) | Verifies that existing sidecar is not rewritten when no fields are missing. | `dataset_path.write_text`, `translation_path.write_text`, `ensure_field_translation_file`, `translation_path.read_text` |
| `test_metrics_missing_required_fields_reports_metrics_to_skip()` (L244) | Verifies that metrics missing required fields reports metrics to skip. | `available_translated_fields`, `metrics_missing_required_fields` |
| `test_collect_field_requirements_uses_explicit_required_and_optional_fields()` (L262) | Verifies that collect field requirements uses explicit required and optional fields. | `collect_field_requirements` |
| `test_translate_metric_fields_resolves_without_renaming_dataframe_columns()` (L281) | Verifies that translate metric fields resolves without renaming dataframe columns. | `translate_metric_fields` |
| `test_build_field_translation_report_tracks_skipped_metrics(tmp_path)` (L306) | Verifies that build field translation report tracks skipped metrics. | `build_field_translation_report` |
| `test_example_field_translations_use_standard_shape()` (L329) | Verifies that example field translations use standard shape. | `root.glob`, `load_field_translation`, `path.read_text` |
| `test_validate_field_translation_payload_rejects_missing_standard_mapping()` (L342) | Verifies that validate field translation payload rejects missing standard mapping. | `pytest.raises`, `validate_field_translation_payload` |
| `test_validate_field_translation_payload_rejects_unknown_schema_version()` (L349) | Verifies that validate field translation payload rejects unknown schema version. | `pytest.raises`, `validate_field_translation_payload` |
| `test_validate_field_translation_payload_rejects_metadata_for_unknown_fields()` (L356) | Verifies that validate field translation payload rejects metadata for unknown fields. | `pytest.raises`, `validate_field_translation_payload` |
| `test_format_column_section_uses_terminal_width(monkeypatch)` (L367) | Verifies that format column section uses terminal width. | `monkeypatch.setattr`, `format_column_section`, `wide_lines[1].count`, `fixed_lines[1].count`, `terminal_size` |
| `test_format_column_section_wraps_long_names()` (L386) | Verifies that format column section wraps long names. | `format_column_section`, `any`, `all`, `'\n'.join`, `_display_width` |
| `test_format_column_section_uses_display_width_for_unicode_padding()` (L398) | Verifies that format column section uses display width for unicode padding. | `_display_ljust`, `_display_width` |
| `test_format_column_section_uses_terminal_width_fallback(monkeypatch)` (L408) | Verifies that format column section uses terminal width fallback. | `monkeypatch.setattr`, `format_column_section`, `fallback_values.append`, `terminal_size` |
| `test_format_column_section_wraps_identifiers_at_separators()` (L427) | Verifies that format column section wraps identifiers at separators. | `format_column_section`, `lines[2].strip` |
| `test_format_metric_section_uses_category_grid_without_repeated_status_labels()` (L442) | Verifies that format metric section uses category grid without repeated status labels. | `format_metric_section`, `all`, `lines[2].count`, `'\n'.join`, `_display_width` |
| `test_format_metric_section_omits_empty_categories()` (L461) | Verifies that format metric section omits empty categories. | `'\n'.join`, `format_metric_section` |
| `test_field_translation_report_includes_suggestions_and_markdown()` (L491) | Verifies that field translation report includes suggestions and markdown. | `build_field_translation_report`, `format_field_translation_markdown_report`, `format_field_translation_report` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `test_format_column_section_uses_terminal_width_fallback.fake_get_terminal_size(fallback)` (L415) | Implementation helper for fake get terminal size. |

## `tests/test_label_fidelity_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_label_completeness_and_distribution_metrics()` (L15) | Verifies that label completeness and distribution metrics. | `compute_label_coverage_ratio`, `compute_per_slice_label_coverage_ratio`, `compute_per_slice_label_entropy_score`, `compute_class_imbalance_score` |
| `test_temporal_label_correctness_metrics()` (L28) | Verifies that temporal label correctness metrics. | `compute_attack_window_alignment_score`, `compute_pre_post_attack_label_bleed_ratio` |
| `test_split_integrity_metrics()` (L52) | Verifies that split integrity metrics. | `compute_train_test_duplicate_overlap_ratio`, `compute_train_test_identifier_contamination_ratio` |

## `tests/test_metric_failure_diagnostics.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_reserved_ip_profile_respects_disabled_private_category()` (L30) | Verifies that reserved IP profile respects disabled private category. | `_base_metric`, `run_reserved_ip_address_metric` |
| `test_valid_port_range_uses_configured_bounds_and_explains_failure()` (L48) | Verifies that valid port range uses configured bounds and explains failure. | `_base_metric`, `run_valid_port_range_metric` |
| `test_valid_port_range_is_not_applicable_when_all_ports_are_missing()` (L72) | Verifies that valid port range is not applicable when all ports are missing. | `_base_metric`, `run_valid_port_range_metric` |
| `test_service_port_consistency_does_not_treat_every_mixed_row_as_dns()` (L101) | Verifies that service port consistency does not treat every mixed row as dns. | `_service_metric`, `run_service_port_consistency_metric` |
| `test_service_port_consistency_filters_to_service_population()` (L117) | Verifies that service port consistency filters to service population. | `_service_metric`, `run_service_port_consistency_metric` |
| `test_service_port_consistency_failure_contains_threshold_and_examples()` (L136) | Verifies that service port consistency failure contains threshold and examples. | `_service_metric`, `run_service_port_consistency_metric` |
| `test_parallel_collector_keeps_execution_and_domain_status_separate()` (L154) | Verifies that parallel collector keeps execution and domain status separate. | `collect_parallel_metric_results`, `_base_metric`, `datetime` |
| `test_compact_live_state_shows_error_reason_not_bare_failed()` (L193) | Verifies that compact live state shows error reason not bare failed. | `RunState.from_plan`, `state.mark_running`, `state.mark_completed`, `render_live_taxonomy`, `datetime.now` |
| `test_parallel_progress_callback_remains_backward_compatible(tmp_path: Path)` (L233) | Verifies that parallel progress callback remains backward compatible. | `run_metrics_parallel`, `any`, `_base_metric`, `events.append` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_base_metric(metric_id: str) -> dict` (L21) | Implementation helper for base metric. |
| `_service_metric() -> dict` (L88) | Implementation helper for service metric. |
| `test_parallel_progress_callback_remains_backward_compatible.old_callback(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds)` (L237) | Implementation helper for old callback. |

## `tests/test_metric_package_layout.py`

Production-package and compatibility-import boundaries.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_legacy_metric_modules_reexport_production_implementations()` (L28) | Verifies that legacy metric modules reexport production implementations. | Assertions and fixtures in the module |
| `test_dispatch_does_not_import_moved_metric_implementations_from_tests()` (L54) | Verifies that dispatch does not import moved metric implementations from tests. | `inspect.getsource`, `any` |

## `tests/test_pcap_adapter.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_build_pcap_packet_dataframe_copies_raw_packet_fields(tmp_path)` (L37) | Verifies that build PCAP packet dataframe copies raw packet fields. | `_write_capture`, `build_pcap_packet_dataframe` |
| `test_build_pcap_flow_dataframe_reconstructs_bidirectional_view(tmp_path)` (L52) | Verifies that build PCAP flow dataframe reconstructs bidirectional view. | `_write_capture`, `build_pcap_flow_dataframe`, `pytest.approx` |
| `test_packet_adapted_metrics_run_on_raw_packet_view(tmp_path)` (L78) | Verifies that packet adapted metrics run on raw packet view. | `_write_capture`, `build_pcap_packet_dataframe`, `runners.items`, `pcap_metric_template`, `runner` |
| `test_self_derived_flow_invariants_are_not_exposed_as_pcap_templates()` (L103) | Verifies that self derived flow invariants are not exposed as PCAP templates. | `pcap_metric_template` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_write_capture(path: Path) -> None` (L25) | Implementation helper for write capture. |

## `tests/test_pcap_all_runnable_metrics.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_pcap_supported_set_contains_every_current_automatic_packet_metric()` (L43) | Verifies that PCAP supported set contains every current automatic packet metric. | Assertions and fixtures in the module |
| `test_all_packet_view_metrics_execute_on_one_shared_capture(tmp_path)` (L50) | Verifies that all packet view metrics execute on one shared capture. | `_write_capture`, `build_pcap_packet_dataframe`, `build_metric_handlers`, `pcap_metric_template`, `handlers['timestamp_parse_success_ratio']`, `AssertionError`, `handlers[metric_id]` |
| `test_automatic_pcap_plan_contains_all_twenty_currently_runnable_metrics(tmp_path)` (L77) | Verifies that automatic PCAP plan contains all twenty currently runnable metrics. | `_write_capture`, `build_plan` |
| `test_distance_correlation_pcap_template_declares_computational_cap(tmp_path)` (L93) | Verifies that distance correlation PCAP template declares computational cap. | `_write_capture`, `build_pcap_packet_dataframe`, `build_metric_handlers`, `pcap_metric_template`, `handlers['distance_correlation_matrix_deviation']` |
| `test_context_configuration_reasons_are_not_silent_exclusions()` (L111) | Verifies that context configuration reasons are not silent exclusions. | Assertions and fixtures in the module |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_write_capture(path: Path, packet_count: int = 64) -> None` (L21) | Implementation helper for write capture. |
| `test_all_packet_view_metrics_execute_on_one_shared_capture.forbidden_loader(_path)` (L59) | Implementation helper for forbidden loader. |

## `tests/test_plan_builder.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_metric_catalog_covers_runtime_dispatcher()` (L13) | Verifies that metric catalog covers runtime dispatcher. | `available_metric_ids`, `_slug`, `build_metric_catalog` |
| `test_automatic_plan_requires_dataset()` (L22) | Verifies that automatic plan requires dataset. | `pytest.raises`, `build_plan` |
| `test_generated_plan_contains_only_ready_enabled_metrics(tmp_path)` (L27) | Verifies that generated plan contains only ready enabled metrics. | `build_plan`, `all`, `validate_plan_schema` |
| `test_dataset_preflight_includes_ready_port_range_and_excludes_service_rule(tmp_path)` (L54) | Verifies that dataset preflight includes ready port range and excludes service rule. | `build_plan` |
| `test_reference_metrics_are_reported_but_never_written_without_reference_configuration(tmp_path)` (L72) | Verifies that reference metrics are reported but never written without reference configuration. | `build_plan`, `metric_id.endswith` |
| `test_existing_field_translation_sidecar_can_make_metric_runnable(tmp_path)` (L94) | Verifies that existing field translation sidecar can make metric runnable. | `sidecar.write_text`, `build_plan`, `sidecar.resolve` |
| `test_pcap_plan_includes_direct_and_independent_packet_adapter_metrics(tmp_path)` (L123) | Verifies that PCAP plan includes direct and independent packet adapter metrics. | `dataset.write_bytes`, `build_plan`, `all` |
| `test_include_exclude_rejects_unknown_metric_ids(tmp_path)` (L139) | Verifies that include exclude rejects unknown metric ids. | `pytest.raises`, `build_plan` |
| `test_write_plan_is_valid_and_requires_force_for_overwrite(tmp_path)` (L152) | Verifies that write plan is valid and requires force for overwrite. | `build_plan`, `write_plan`, `validate_plan_schema`, `pytest.raises` |

## `tests/test_protocol_validity_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_ipv4_values_are_classified()` (L13) | Verifies that ipv4 values are classified. | `classify_ip_value` |
| `test_ipv6_values_are_classified()` (L18) | Verifies that ipv6 values are classified. | `classify_ip_value` |
| `test_invalid_values_are_separate_from_missing()` (L23) | Verifies that invalid values are separate from missing. | `classify_ip_value` |
| `test_protocol_validity_checks_ports_protocol_structure_and_addresses(tmp_path)` (L37) | Verifies that protocol validity checks ports protocol structure and addresses. | `_write_packets`, `run_protocol_validity_metric`, `IP`, `TCP`, `Raw`, `UDP` |
| `test_protocol_validity_detects_declared_tcp_without_decodable_tcp_header(tmp_path)` (L61) | Verifies that protocol validity detects declared TCP without decodable TCP header. | `_write_packets`, `run_protocol_validity_metric`, `IP`, `Raw` |
| `test_suspicious_tcp_flags_are_reported_but_not_invalid_by_default(tmp_path)` (L78) | Verifies that suspicious TCP flags are reported but not invalid by default. | `_write_packets`, `run_protocol_validity_metric`, `IP`, `TCP` |
| `test_suspicious_tcp_flags_can_affect_status_only_when_explicitly_configured(tmp_path)` (L96) | Verifies that suspicious TCP flags can affect status only when explicitly configured. | `_write_packets`, `run_protocol_validity_metric`, `IP`, `TCP` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_write_packets(path: Path, packets) -> None` (L31) | Implementation helper for write packets. |

## `tests/test_reference_model_comparison_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_reference_distribution_dependency_and_temporal_metrics(tmp_path)` (L14) | Verifies that reference distribution dependency and temporal metrics. | `reference.to_csv`, `compute_feature_wise_ks_statistic_from_reference`, `compute_pearson_matrix_deviation_from_reference`, `compute_hourly_activity_divergence_from_reference` |
| `test_reference_slice_and_protocol_metrics(tmp_path)` (L34) | Verifies that reference slice and protocol metrics. | `reference.to_csv`, `compute_slice_proportion_deviation_from_reference`, `compute_per_slice_class_divergence_from_reference`, `compute_protocol_mix_divergence_from_reference`, `compute_port_use_divergence_from_reference` |

## `tests/test_run_plan_field_translation.py`

Command-level translation dry-run and sidecar behavior.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_field_translation_dry_run_creates_sidecar_and_report(tmp_path)` (L29) | Verifies that field translation dry run creates sidecar and report. | `dataset.write_text`, `_write_plan`, `subprocess.run`, `text_report.read_text`, `markdown_report.read_text`, `sidecar.read_text`, `report.read_text`, `output.exists` |
| `test_no_update_field_translation_does_not_create_sidecar(tmp_path)` (L83) | Verifies that no update field translation does not create sidecar. | `dataset.write_text`, `_write_plan`, `subprocess.run`, `(tmp_path / 'dataset.field_translation.json').exists` |
| `test_field_translation_dry_run_fails_for_missing_dataset(tmp_path)` (L112) | Verifies that field translation dry run fails for missing dataset. | `_write_plan`, `subprocess.run` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_write_plan(path: Path)` (L10) | Implementation helper for write plan. |

## `tests/test_run_plan_helpers.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_detect_ip_fields_prefers_first_matching_candidates()` (L23) | Verifies that detect IP fields prefers first matching candidates. | `_DummyFrame`, `detect_ip_fields` |
| `test_detect_ip_fields_defaults_to_na_when_missing()` (L30) | Verifies that detect IP fields defaults to na when missing. | `_DummyFrame`, `detect_ip_fields` |
| `test_build_title_box_lines_adds_frame_and_status_separator()` (L37) | Verifies that build title box lines adds frame and status separator. | `build_title_box_lines`, `any` |
| `test_build_base_header_lines_with_dataset_size(tmp_path: Path)` (L45) | Verifies that build base header lines with dataset size. | `dataset.write_text`, `build_base_header_lines`, `any`, `line.startswith` |
| `test_build_outcome_contains_expected_fields()` (L57) | Verifies that build outcome contains expected fields. | `datetime`, `build_outcome` |
| `test_configure_signal_handlers_registers_sigint(monkeypatch)` (L79) | Verifies that configure signal handlers registers sigint. | `monkeypatch.setattr`, `configure_signal_handlers`, `any`, `calls.append` |
| `test_parse_run_plan_args_reads_required_case(monkeypatch)` (L92) | Verifies that parse run plan args reads required case. | `monkeypatch.setattr`, `parse_run_plan_args` |
| `test_parse_run_plan_args_requires_case_without_tui(monkeypatch)` (L101) | Verifies that parse run plan args requires case without tui. | `monkeypatch.setattr`, `pytest.raises`, `parse_run_plan_args` |
| `test_parse_run_plan_args_tui_can_supply_case(monkeypatch)` (L108) | Verifies that parse run plan args tui can supply case. | `monkeypatch.setattr`, `parse_run_plan_args`, `SimpleNamespace` |
| `test_update_live_header_formats_and_forwards(monkeypatch)` (L127) | Verifies that update live header formats and forwards. | `monkeypatch.setattr`, `update_live_header`, `any` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_DummyFrame` (L18) | Data model for DummyFrame. |
| `_DummyFrame.__init__(self, columns)` (L19) | Implementation helper for init. |
| `test_configure_signal_handlers_registers_sigint._fake_signal(sig, handler)` (L82) | Implementation helper for fake signal. |
| `test_parse_run_plan_args_tui_can_supply_case._fake_launch_tui(args)` (L113) | Implementation helper for fake launch tui. |
| `test_update_live_header_formats_and_forwards._fake_set_live_header(lines)` (L130) | Implementation helper for fake set live header. |

## `tests/test_runner_execution_invariants.py`

Execution and live-rendering invariants.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_render_live_taxonomy_status_labels()` (L7) | Verifies that render live taxonomy status labels. | `render_live_taxonomy` |
| `test_run_metric_with_heartbeat_executes_handler()` (L26) | Verifies that run metric with heartbeat executes handler. | `run_metric_with_heartbeat` |
| `test_render_live_taxonomy_compact_summarizes_branches()` (L49) | Verifies that render live taxonomy compact summarizes branches. | `render_live_taxonomy` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `test_run_metric_with_heartbeat_executes_handler.handler(_dataset, _metric)` (L29) | Implementation helper for handler. |

## `tests/test_runner_order.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_order_metrics_by_taxonomy_basic()` (L4) | Verifies that order metrics by taxonomy basic. | `order_metrics_by_taxonomy` |
| `test_order_metrics_by_taxonomy_strict_raises()` (L11) | Verifies that order metrics by taxonomy strict raises. | `order_metrics_by_taxonomy` |

## `tests/test_runner_progress_invariants.py`

Progress and terminal-output invariants.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_overall_progress_not_100_before_completion()` (L4) | Verifies that overall progress not 100 before completion. | `render_overall_progress_line` |
| `test_overall_progress_100_at_completion()` (L9) | Verifies that overall progress 100 at completion. | `render_overall_progress_line` |
| `test_activity_bar_has_expected_width()` (L14) | Verifies that activity bar has expected width. | `render_metric_activity_bar` |
| `test_colorize_status_no_color_in_non_tty_context()` (L20) | Verifies that colorize status no color in non tty context. | `colorize_status` |

## `tests/test_runner_schema_taxonomy.py`

Plan schema, registry, and taxonomy behavior.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_validate_plan_schema_rejects_missing_plan_id()` (L6) | Verifies that validate plan schema rejects missing plan id. | `validate_plan_schema` |
| `test_metric_registry_contains_core_metrics()` (L15) | Verifies that metric registry contains core metrics. | Assertions and fixtures in the module |
| `test_taxonomy_builders_basic_shape()` (L20) | Verifies that taxonomy builders basic shape. | `build_plan_taxonomy`, `build_result_taxonomy`, `build_test_results_taxonomy` |
| `test_validate_plan_schema_requires_taxonomy_and_calculation_method()` (L30) | Verifies that validate plan schema requires taxonomy and calculation method. | `validate_plan_schema` |

## `tests/test_service_port_consistency_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_parse_port_categories()` (L4) | Verifies that parse port categories. | `parse_port` |

## `tests/test_slice_representation_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_slice_coverage_and_balance_metrics()` (L13) | Verifies that slice coverage and balance metrics. | `compute_per_slice_sample_coverage_ratio`, `compute_per_slice_feature_coverage_ratio`, `compute_per_slice_class_coverage_ratio`, `compute_slice_distribution_imbalance_score` |
| `test_cross_slice_isolation_metrics()` (L27) | Verifies that cross slice isolation metrics. | `compute_cross_slice_duplicate_overlap_ratio`, `compute_cross_slice_identifier_leakage_ratio` |

## `tests/test_statistical_fidelity_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_distributional_metrics_report_zero_for_matching_halves()` (L19) | Verifies that distributional metrics report zero for matching halves. | `_metric`, `compute_ks_feature_divergence`, `compute_wasserstein_feature_distance`, `compute_energy_distance`, `compute_maximum_mean_discrepancy` |
| `test_distributional_metrics_detect_shifted_halves()` (L29) | Verifies that distributional metrics detect shifted halves. | `_metric`, `compute_ks_feature_divergence`, `compute_wasserstein_feature_distance`, `compute_energy_distance`, `compute_maximum_mean_discrepancy` |
| `test_distance_correlation_profile_reports_nonlinear_dependency()` (L39) | Verifies that distance correlation profile reports nonlinear dependency. | `compute_distance_correlation_profile` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_metric(*fields)` (L12) | Implementation helper for metric. |

## `tests/test_task_based_validation_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_benchmark_model_metrics_from_predictions()` (L11) | Verifies that benchmark model metrics from predictions. | `compute_benchmark_model_accuracy`, `compute_benchmark_model_precision`, `compute_benchmark_model_recall`, `compute_benchmark_model_f1_score` |
| `test_benchmark_model_metrics_ignore_missing_labels_or_predictions()` (L24) | Verifies that benchmark model metrics ignore missing labels or predictions. | `compute_benchmark_model_accuracy` |

## `tests/test_telemetry.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_run_state_tracks_metric_statuses_and_events()` (L25) | Verifies that run state tracks metric statuses and events. | `_state`, `state.mark_running`, `state.mark_completed`, `state.mark_skipped`, `any`, `state.status_counts`, `state.completed_statuses`, `state.completed_durations` |
| `test_compact_renderer_can_read_run_state()` (L41) | Verifies that compact renderer can read run state. | `_state`, `state.mark_running`, `state.mark_skipped`, `render_live_taxonomy` |
| `test_interactive_renderer_uses_tui_dashboard()` (L64) | Verifies that interactive renderer uses tui dashboard. | `_state`, `state.mark_running`, `state.mark_completed`, `state.mark_skipped`, `render_live_taxonomy` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_state()` (L8) | Implementation helper for state. |

## `tests/test_temporal_metrics_profile.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_temporal_consistency_metrics()` (L15) | Verifies that temporal consistency metrics. | `compute_timestamp_parse_success_ratio`, `compute_start_end_timestamp_consistency_ratio`, `compute_non_negative_duration_ratio` |
| `test_temporal_behaviour_metrics_compare_first_and_second_halves()` (L28) | Verifies that temporal behaviour metrics compare first and second halves. | `compute_inter_arrival_time_distribution_divergence`, `compute_burstiness_coefficient_deviation`, `compute_hourly_activity_distribution_divergence`, `compute_diurnal_pattern_similarity_score`, `compute_periodicity_preservation_score` |

## `tests/test_tui.py`

Tests and local helpers in this module.

### Pytest cases

| Test | What it verifies | Primary code exercised |
| --- | --- | --- |
| `test_build_default_tui_fields_discovers_case_and_plan_choices(tmp_path)` (L32) | Verifies that build default tui fields discovers case and plan choices. | `(tmp_path / 'cases').mkdir`, `(tmp_path / 'plans').mkdir`, `(tmp_path / 'cases' / 'case_a.json').write_text`, `(tmp_path / 'plans' / 'plan_a.json').write_text`, `build_default_tui_fields`, `next`, `_args` |
| `test_apply_tui_fields_converts_blank_optional_values_and_worker_count()` (L45) | Verifies that apply tui fields converts blank optional values and worker count. | `_args`, `build_default_tui_fields`, `apply_tui_fields` |
| `test_validate_required_run_args_rejects_missing_case()` (L63) | Verifies that validate required run args rejects missing case. | `pytest.raises`, `validate_required_run_args`, `_args` |
| `test_dataset_field_uses_file_browser()` (L68) | Verifies that dataset field uses file browser. | `build_default_tui_fields`, `next`, `_args` |
| `test_list_file_browser_entries_sorts_directories_first_and_skips_internal_dirs(tmp_path)` (L78) | Verifies that list file browser entries sorts directories first and skips internal dirs. | `(tmp_path / 'z_data.csv').write_text`, `(tmp_path / 'datasets').mkdir`, `(tmp_path / 'datasets' / 'sample.csv').write_text`, `(tmp_path / '.git').mkdir` |
| `test_describe_tui_field_explains_selected_field_actions()` (L91) | Verifies that describe tui field explains selected field actions. | `build_default_tui_fields`, `next`, `describe_tui_field`, `any`, `_args` |
| `test_worker_field_shows_detected_max_workers()` (L103) | Verifies that worker field shows detected max workers. | `build_default_tui_fields`, `next`, `describe_tui_field`, `_args`, `detected_max_workers` |
| `test_post_dry_run_result_lines_offer_run_action()` (L114) | Verifies that post dry run result lines offer run action. | `_result_lines`, `_args` |
| `test_post_dry_run_result_lines_show_attention_for_skips()` (L122) | Verifies that post dry run result lines show attention for skips. | `_result_lines`, `_args` |
| `test_default_outcome_path_uses_plan_title_and_timestamp(tmp_path)` (L128) | Verifies that default outcome path uses plan title and timestamp. | `(tmp_path / 'plans').mkdir`, `plan.write_text`, `default_outcome_path`, `datetime` |
| `test_default_outcome_path_uses_referenced_plan_title_for_case(tmp_path)` (L142) | Verifies that default outcome path uses referenced plan title for case. | `(tmp_path / 'plans').mkdir`, `(tmp_path / 'cases').mkdir`, `(tmp_path / 'plans' / 'example_plan.json').write_text`, `(tmp_path / 'cases' / 'case_example.json').write_text`, `default_outcome_path`, `datetime` |
| `test_default_output_is_auto_managed_unless_explicit(tmp_path)` (L163) | Verifies that default output is auto managed unless explicit. | `(tmp_path / 'plans').mkdir`, `(tmp_path / 'plans' / 'example_plan.json').write_text`, `build_default_tui_fields`, `next`, `automatic_output.value.startswith`, `_args` |
| `test_report_fields_explain_each_report_format()` (L181) | Verifies that report fields explain each report format. | `build_default_tui_fields`, `next`, `_args` |
| `test_result_sections_include_expandable_human_readable_metric_results(tmp_path)` (L192) | Verifies that result sections include expandable human readable metric results. | `output.write_text`, `build_result_sections`, `any` |
| `test_field_mapping_choices_remove_already_selected_columns()` (L209) | Verifies that field mapping choices remove already selected columns. | `_field_mapping_choices` |
| `test_save_field_mappings_updates_test_to_dataset_fields(tmp_path)` (L218) | Verifies that save field mappings updates test to dataset fields. | `save_field_mappings`, `__import__('json').loads`, `path.read_text`, `__import__` |

### Test helpers

| Helper | Purpose |
| --- | --- |
| `_args(**overrides)` (L9) | Implementation helper for args. |
