# Function and class reference

Exhaustive AST-generated reference for runtime code, metric implementations, compatibility modules, and scripts. Nested helpers are included. Actual pytest cases are documented in [Test suite reference](test_reference.md).

**Public** only means the leaf name lacks a leading underscore; it is not an API-stability promise. Nested functions are always internal.

## `cbr_tests/metrics/column_quality.py`

Column completeness, numeric usability, and variation metrics.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `compute_column_quality_profile(df` (L6) | function | Public | Build per-field and aggregate quality details for requested fields. |
| `compute_column_quality_profile._mean(key` (L76) | nested function | Internal | Implementation helper for mean. |

## `cbr_tests/metrics/data_quality.py`

Missing-value and duplicate-row metrics.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_select_fields(df` (L6) | function | Internal | Implementation helper for select fields. |
| `compute_missing_value_ratio(df` (L13) | function | Public | Computes missing value ratio and returns a structured result. |
| `compute_duplicate_row_ratio(df` (L49) | function | Public | Computes duplicate row ratio and returns a structured result. |

## `cbr_tests/metrics/pearson.py`

Numeric validation and Pearson correlation profiles.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `validate_candidate_fields(df` (L8) | function | Public | Validate and coerce fields that may be used for correlation metrics. |
| `compute_pearson_profile(df` (L53) | function | Public | Compute the Pearson correlation profile for runnable fields. |

## `cbr_tests/metrics/spearman.py`

Spearman rank-correlation profiles.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `compute_spearman_profile(df` (L10) | function | Public | Computes spearman profile and returns a structured result. |
| `validate_spearman_candidate_fields(df` (L40) | function | Public | Validates spearman candidate fields. |

## `cbr_tests/metrics/statistical.py`

Internal distribution drift and distance-correlation calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_clean_numeric_values(df` (L12) | function | Internal | Implementation helper for clean numeric values. |
| `_split_values(values` (L17) | function | Internal | Implementation helper for split values. |
| `_mean_pairwise_abs_distance(left` (L22) | function | Internal | Implementation helper for mean pairwise abs distance. |
| `_ks_statistic(left` (L29) | function | Internal | Computes the largest empirical-CDF difference between two samples. |
| `_wasserstein_distance(left` (L47) | function | Internal | Computes one-dimensional Wasserstein distance from empirical CDFs. |
| `_energy_distance(left` (L70) | function | Internal | Computes the sample energy-distance expression from cross- and within-sample distances. |
| `_rbf_mmd(left` (L77) | function | Internal | Computes squared RBF-kernel MMD with a median-distance bandwidth when gamma is omitted. |
| `_rbf_mmd.kernel_mean(a_values` (L88) | nested function | Internal | Implementation helper for kernel mean. |
| `_build_distributional_metric(df` (L102) | function | Internal | Implementation helper for build distributional metric. |
| `compute_ks_feature_divergence(df` (L159) | function | Public | Computes KS feature divergence and returns a structured result. |
| `compute_wasserstein_feature_distance(df` (L163) | function | Public | Computes wasserstein feature distance and returns a structured result. |
| `compute_energy_distance(df` (L169) | function | Public | Computes energy distance and returns a structured result. |
| `compute_maximum_mean_discrepancy(df` (L173) | function | Public | Computes maximum mean discrepancy and returns a structured result. |
| `_distance_matrix(values` (L179) | function | Internal | Implementation helper for distance matrix. |
| `_double_center(matrix` (L183) | function | Internal | Implementation helper for double center. |
| `_mean_product(left` (L203) | function | Internal | Implementation helper for mean product. |
| `_distance_correlation(left` (L213) | function | Internal | Computes distance correlation from double-centered pairwise distance matrices. |
| `compute_distance_correlation_profile(df` (L227) | function | Public | Computes distance correlation profile and returns a structured result. |

## `cbr_tests/metrics/task_validation.py`

Accuracy and binary precision, recall, and F1 calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L6) | function | Internal | Implementation helper for normalise. |
| `_fields(metric` (L13) | function | Internal | Implementation helper for fields. |
| `_positive_label(metric` (L21) | function | Internal | Uses an explicit positive class or the lexicographically last class when exactly two labels are observed. |
| `_confusion_counts(df` (L29) | function | Internal | Implementation helper for confusion counts. |
| `compute_benchmark_model_accuracy(df` (L80) | function | Public | Computes benchmark model accuracy and returns a structured result. |
| `compute_benchmark_model_precision(df` (L91) | function | Public | Computes benchmark model precision and returns a structured result. |
| `compute_benchmark_model_recall(df` (L99) | function | Public | Computes benchmark model recall and returns a structured result. |
| `compute_benchmark_model_f1_score(df` (L107) | function | Public | Computes benchmark model f1 score and returns a structured result. |

## `cbr_tests/metrics/temporal.py`

Timestamp, duration, timing-drift, hourly, and periodicity calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_timestamp_field(metric` (L8) | function | Internal | Implementation helper for timestamp field. |
| `_parse_timestamp_series(df` (L12) | function | Internal | Implementation helper for parse timestamp series. |
| `_ks_statistic(left` (L18) | function | Internal | Computes the largest empirical-CDF difference between two samples. |
| `_split_list(values` (L37) | function | Internal | Implementation helper for split list. |
| `_inter_arrival_seconds(timestamps` (L42) | function | Internal | Implementation helper for inter arrival seconds. |
| `compute_timestamp_parse_success_ratio(df` (L50) | function | Public | Computes timestamp parse success ratio and returns a structured result. |
| `compute_start_end_timestamp_consistency_ratio(df` (L68) | function | Public | Computes start end timestamp consistency ratio and returns a structured result. |
| `compute_non_negative_duration_ratio(df` (L98) | function | Public | Computes non negative duration ratio and returns a structured result. |
| `compute_inter_arrival_time_distribution_divergence(df` (L132) | function | Public | Computes inter arrival time distribution divergence and returns a structured result. |
| `_burstiness(values` (L155) | function | Internal | Implementation helper for burstiness. |
| `compute_burstiness_coefficient_deviation(df` (L165) | function | Public | Computes burstiness coefficient deviation and returns a structured result. |
| `_hourly_counts(timestamps` (L192) | function | Internal | Implementation helper for hourly counts. |
| `_probabilities(counts` (L199) | function | Internal | Implementation helper for probabilities. |
| `compute_hourly_activity_distribution_divergence(df` (L204) | function | Public | Computes hourly activity distribution divergence and returns a structured result. |
| `compute_diurnal_pattern_similarity_score(df` (L230) | function | Public | Computes diurnal pattern similarity score and returns a structured result. |
| `_autocorrelation(values` (L253) | function | Internal | Implementation helper for autocorrelation. |
| `compute_periodicity_preservation_score(df` (L265) | function | Public | Computes periodicity preservation score and returns a structured result. |

## `cbr_tests/metrics/timestamp_coherence.py`

Raw packet timestamp coherence scanning.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_timestamp_coherence_metric(dataset_path` (L8) | function | Public | Scan a PCAP and assess whether packet timestamps are coherent. |

## `export_outcomes_for_graphs.py`

Flattens selected outcome fields into CSV tables for graphing.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `load_json(path` (L11) | function | Public | Loads JSON. |
| `main()` (L16) | function | Public | Implementation helper for main. |

## `run_plan.py`

Top-level command workflow from parsed arguments to the atomic outcome JSON.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_confirm_sidecar_update(action` (L56) | function | Internal | Implementation helper for confirm sidecar update. |
| `main()` (L69) | function | Public | Implementation helper for main. |
| `main._load_dataset_for_metric(path` (L218) | nested function | Internal | Implementation helper for load dataset for metric. |

## `runner/dataset_loading.py`

Shared dataframe loading with progress presentation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `is_tabular_dataset(dataset_path` (L15) | function | Public | Return true when the dataset extension is handled by the tabular loader. |
| `load_shared_tabular_dataset(*, dataset_path` (L20) | function | Public | Load a tabular dataset once while updating the live loading display. |
| `load_shared_tabular_dataset._chunk_progress(chunk_idx` (L37) | nested function | Internal | Implementation helper for chunk progress. |
| `_update_loaded_dataset_header(plan` (L75) | function | Internal | Implementation helper for update loaded dataset header. |

## `runner/dispatch.py`

Metric registry, wrappers, field translation, and handler construction.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `register_metric(metric_id` (L112) | function | Public | Implementation helper for register metric. |
| `register_metric._decorator(function)` (L113) | nested function | Internal | Implementation helper for decorator. |
| `run_pearson_metric(dataset_path` (L120) | function | Public | Runs pearson metric. |
| `run_spearman_metric(dataset_path` (L142) | function | Public | Runs spearman metric. |
| `run_missing_value_metric(dataset_path` (L170) | function | Public | Runs missing value metric. |
| `run_duplicate_row_metric(dataset_path` (L182) | function | Public | Runs duplicate row metric. |
| `run_tabular_metric(dataset_path` (L194) | function | Public | Runs tabular metric. |
| `run_distance_correlation_metric(dataset_path` (L206) | function | Public | Runs distance correlation metric. |
| `run_column_quality_metric(dataset_path` (L232) | function | Public | Runs column quality metric. |
| `_timestamp_metric@register_metric('timestamp_coherence_profile')
def _timestamp_metric(dataset_path` (L245) | function | Internal | Implementation helper for timestamp metric. |
| `_protocol_metric@register_metric('protocol_validity_profile')
def _protocol_metric(dataset_path` (L250) | function | Internal | Implementation helper for protocol metric. |
| `_reserved_ip_metric@register_metric('reserved_ip_address_profile')
def _reserved_ip_metric(dataset_path` (L255) | function | Internal | Implementation helper for reserved IP metric. |
| `_valid_port_metric@register_metric('valid_port_range_profile')
def _valid_port_metric(dataset_path` (L260) | function | Internal | Implementation helper for valid port metric. |
| `_service_port_metric@register_metric('service_port_consistency_profile')
def _service_port_metric(dataset_path` (L265) | function | Internal | Implementation helper for service port metric. |
| `_tcp_flag_metric@register_metric('tcp_flag_consistency_profile')
def _tcp_flag_metric(dataset_path` (L270) | function | Internal | Implementation helper for TCP flag metric. |
| `_handshake_metric@register_metric('handshake_plausibility_profile')
def _handshake_metric(dataset_path` (L275) | function | Internal | Implementation helper for handshake metric. |
| `_flow_duration_metric@register_metric('flow_duration_consistency_profile')
def _flow_duration_metric(dataset_path` (L280) | function | Internal | Implementation helper for flow duration metric. |
| `_packet_byte_metric@register_metric('packet_byte_consistency_profile')
def _packet_byte_metric(dataset_path` (L285) | function | Internal | Implementation helper for packet byte metric. |
| `_slice_valid_metric@register_metric('valid_slice_identifier_profile')
def _slice_valid_metric(dataset_path` (L290) | function | Internal | Implementation helper for slice valid metric. |
| `_slice_consistency_metric@register_metric('slice_identifier_consistency_profile')
def _slice_consistency_metric(dataset_path` (L295) | function | Internal | Implementation helper for slice consistency metric. |
| `_wrap_registered_handler(handler, shared_df` (L349) | function | Internal | Implementation helper for wrap registered handler. |
| `_wrap_registered_handler._wrapped(dataset_path` (L354) | nested function | Internal | Implementation helper for wrapped. |
| `_make_tabular_compute_handler(metric_id` (L369) | function | Internal | Implementation helper for make tabular compute handler. |
| `build_metric_handlers(shared_df` (L390) | function | Public | Builds the metric-ID-to-callable mapping for a run. |
| `build_metric_handlers._translate(metric` (L400) | nested function | Internal | Implementation helper for translate. |

## `runner/execution.py`

Live status rendering and bounded parallel metric execution.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_metric_status_line(metric_id` (L18) | function | Internal | Implementation helper for metric status line. |
| `_metric_display_status(metric_id` (L36) | function | Internal | Implementation helper for metric display status. |
| `render_compact_run_state(run_state` (L44) | function | Public | Render a compact dashboard from centralized run telemetry. |
| `render_compact_taxonomy(metrics` (L105) | function | Public | Render a screen-friendly taxonomy summary with attention items expanded. |
| `render_live_taxonomy(metrics` (L198) | function | Public | Renders live taxonomy for terminal output. |
| `run_metric_with_heartbeat(dataset_path` (L283) | function | Public | Runs metric with heartbeat. |
| `auto_worker_count(num_metrics` (L348) | function | Public | Implementation helper for auto worker count. |
| `_not_run_payload(status` (L355) | function | Internal | Implementation helper for not run payload. |
| `run_metrics_parallel(dataset_path` (L366) | function | Public | Run metrics with bounded submission and deterministic result records. At most ``workers`` metrics are submitted at once. When fail-fast is enabled, a failed metric stops new submissions. Already-running work is allowed to finish because Python threads cannot be safely terminated; metrics that were never started are explicitly marked ``not_run_fail_fast``. Cancellation returns promptly, attempts to cancel queued futures, and marks all unfinished or unsubmitted metrics ``not_run_cancelled``. |
| `run_metrics_parallel._timed_call(metric_id` (L394) | nested function | Internal | Implementation helper for timed call. |
| `run_metrics_parallel._submit_available() -> None` (L412) | nested function | Internal | Implementation helper for submit available. |

## `runner/field_translation.py`

Public field-translation facade and compatibility exports.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `detect_standard_pcap_field_translation(columns) -> dict[str, str]` (L26) | function | Public | Detect Wireshark/tshark-style packet columns and map them to test fields. Returns a dataset-column -> canonical-test-field mapping. When multiple PCAP columns could satisfy the same test field, the first present candidate wins to avoid creating duplicate canonical columns. |
| `merge_field_translations(automatic` (L42) | function | Public | Merge automatic and explicit translations, with explicit mappings winning. |
| `FieldTranslationError` (L61) | class | Public | Raised when a dataset field translation file is invalid or unsafe. |
| `load_field_translation(path` (L65) | function | Public | Load a dataset-to-test field translation mapping from JSON. Preferred file shape:: {"fields": {"Dataset Column": "canonical_test_column"}} The mapping direction is always dataset column name -> field name used by test plans/metrics. For readability, files may also use ``dataset_to_test_fields`` with the same direction, or ``test_to_dataset_fields`` in the opposite direction. |
| `_validate_unique_targets(mapping` (L89) | function | Internal | Implementation helper for validate unique targets. |
| `_validate_no_column_collisions(columns, rename_map` (L101) | function | Internal | Implementation helper for validate no column collisions. |
| `default_field_translation_path(dataset_path` (L115) | function | Public | Return the sidecar translation path for a dataset. |
| `collect_required_test_fields(plan` (L120) | function | Public | Collect canonical field names referenced by enabled metric input requirements. |
| `ensure_field_translation_file(*, dataset_path` (L132) | function | Public | Create or update the dataset sidecar translation template. The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``. It uses ``test_to_dataset_fields`` so every canonical field required by the plan is visible to users, even when the dataset column is not known yet. |
| `detect_standard_pcap_field_translation_for_dataset(dataset_path` (L188) | function | Public | Read tabular headers, when possible, and detect standard PCAP heading mappings. |
| `_collect_required_fields_from_requirements(requirements` (L211) | function | Internal | Implementation helper for collect required fields from requirements. |
| `_load_existing_translation_payload(path` (L231) | function | Internal | Implementation helper for load existing translation payload. |
| `_payload_to_test_to_dataset_fields(payload` (L242) | function | Internal | Implementation helper for payload to test to dataset fields. |
| `validate_field_translation_payload(payload` (L253) | function | Public | Validate the standard field translation sidecar payload shape. |
| `load_field_translation_from_payload(payload` (L282) | function | Public | Load the standard test-to-dataset mapping from an already parsed payload. |
| `_invert_translation(dataset_to_test` (L302) | function | Internal | Implementation helper for invert translation. |
| `collect_required_test_fields_for_metric(metric` (L306) | function | Public | Collect canonical field names referenced by one metric's input requirements. |
| `read_tabular_dataset_columns(dataset_path` (L315) | function | Public | Read only dataset headers for supported tabular formats. |
| `available_translated_fields(columns, field_translation` (L331) | function | Public | Return canonical fields available before metrics run. |
| `metrics_missing_required_fields(metrics` (L341) | function | Public | Return required fields unavailable for each metric. |
| `detect_known_field_translation(columns, required_fields` (L365) | function | Public | Detect common dataset aliases and map them to canonical test fields. |
| `collect_field_requirements_for_metric(metric` (L378) | function | Public | Return required and optional canonical fields for one metric. |
| `collect_field_requirements(plan` (L398) | function | Public | Collect required/optional field usage across enabled plan metrics. |
| `field_resolver(field_translation` (L413) | function | Public | Build canonical field -> dataset field resolver mapping. |
| `translate_metric_fields(metric` (L423) | function | Public | Return a metric copy with canonical input fields resolved to dataset columns. |
| `metrics_missing_optional_fields(metrics` (L433) | function | Public | Return optional fields unavailable for each metric. |
| `_normalise_field_name(value` (L444) | function | Internal | Implementation helper for normalise field name. |
| `suggest_field_mappings(fields` (L448) | function | Public | Suggest dataset columns for unmapped canonical fields using loose name matching. |
| `field_mapping_details(*, detected_translation` (L464) | function | Public | Return canonical-field mapping details for reports. |
| `build_field_translation_report(*, dataset_path` (L481) | function | Public | Build a machine-readable field translation validation report. |
| `write_field_translation_report(path` (L538) | function | Public | Writes field translation report. |
| `format_field_translation_report(report` (L545) | function | Public | Format a field translation report for humans. |
| `format_field_translation_markdown_report(report` (L578) | function | Public | Format a field translation report as Markdown. |
| `write_text_report(path` (L605) | function | Public | Writes text report. |
| `_translate_requirement_value(value, resolver` (L610) | function | Internal | Implementation helper for translate requirement value. |

## `runner/field_translation_reports.py`

Translation report construction and formatting.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise_field_name(value` (L11) | function | Internal | Implementation helper for normalise field name. |
| `suggest_field_mappings(fields` (L15) | function | Public | Suggest dataset columns for unmapped canonical fields using loose name matching. |
| `field_mapping_details(*, detected_translation` (L31) | function | Public | Return canonical-field mapping details for reports. |
| `build_field_translation_report(*, dataset_path` (L48) | function | Public | Build a machine-readable field translation validation report. |
| `write_field_translation_report(path` (L107) | function | Public | Writes field translation report. |
| `_display_width(value` (L118) | function | Internal | Return terminal display width for a string, ignoring ANSI escapes. |
| `_display_ljust(value` (L128) | function | Internal | Left-pad based on display width rather than Python character count. |
| `_character_display_width(character` (L133) | function | Internal | Implementation helper for character display width. |
| `_split_display_width(value` (L141) | function | Internal | Split a string into display-width-limited chunks. Prefer splitting long identifiers at separators so metric names such as ``inter_arrival_time_distribution_divergence`` do not break in the middle of words. |
| `_wrap_display_width(value` (L182) | function | Internal | Wrap a value on spaces when possible, falling back to display-width chunks. |
| `format_column_grid(items` (L209) | function | Public | Format values as a display-width-aware grid without a section title. |
| `format_column_section(title` (L243) | function | Public | Format a long list as a readable fixed-width column section. By default, use the current terminal width so the report displays as many columns as will fit on the user's display. Long names are wrapped inside cells, and width calculations use terminal display width for better Unicode alignment. A ``max_width`` can still be provided by tests or callers that need deterministic wrapping. |
| `_metric_detail_entry(metric_id` (L257) | function | Internal | Format a metric name with any available status details. |
| `_status_title(status` (L274) | function | Internal | Implementation helper for status title. |
| `format_metric_section(report` (L278) | function | Public | Format metric statuses as non-empty category sections. |
| `format_field_translation_report(report` (L315) | function | Public | Format a field translation report for humans. |
| `format_field_translation_markdown_report(report` (L348) | function | Public | Format a field translation report as Markdown. |
| `write_text_report(path` (L375) | function | Public | Writes text report. |

## `runner/field_translation_schema.py`

Translation payload validation and mapping normalization.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `FieldTranslationError` (L8) | class | Public | Raised when a dataset field translation file is invalid or unsafe. |
| `validate_unique_targets(mapping` (L12) | function | Public | Reject mappings where multiple dataset fields map to one canonical field. |
| `validate_field_translation_payload(payload` (L25) | function | Public | Validate the standard field translation sidecar payload shape. |
| `load_field_translation_from_payload(payload` (L54) | function | Public | Load a dataset-to-test mapping from an already parsed standard sidecar payload. |

## `runner/field_translation_sidecar.py`

Sidecar detection, creation, and extension.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `default_field_translation_path(dataset_path` (L11) | function | Public | Return the standard sidecar translation path for a dataset. |
| `ensure_field_translation_file(*, dataset_path` (L16) | function | Public | Create or update the dataset sidecar translation template. The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``. Existing sidecars are only updated when enabled plan metrics introduce new canonical fields that are not already present in ``test_to_dataset_fields``. |
| `_load_existing_translation_payload(path` (L74) | function | Internal | Implementation helper for load existing translation payload. |
| `_payload_to_test_to_dataset_fields(payload` (L85) | function | Internal | Implementation helper for payload to test to dataset fields. |
| `_invert_translation(dataset_to_test` (L96) | function | Internal | Implementation helper for invert translation. |

## `runner/field_translation_workflow.py`

Translation preflight and requested-report workflow.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `FieldTranslationContext` (L29) | class | Public | Prepared field translation state for a run. |
| `prepare_field_translation_context(*, args, dataset_path` (L45) | function | Public | Load, create/update, validate, and report field translations for a run. |
| `skipped_metric_records(skipped_metrics` (L142) | function | Public | Return outcome-ready skipped metric records for missing field mappings. |
| `print_field_translation_dry_run_summary(context` (L155) | function | Public | Print the dry-run field translation summary and completion status. |
| `_confirm_sidecar_update(action` (L169) | function | Internal | Implementation helper for confirm sidecar update. |
| `_print_skipped_metric_warning(skipped_metrics` (L182) | function | Internal | Implementation helper for print skipped metric warning. |
| `_write_requested_reports(args, field_translation_report` (L190) | function | Internal | Implementation helper for write requested reports. |
| `_should_use_color() -> bool` (L202) | function | Internal | Implementation helper for should use color. |

## `runner/io.py`

Case/plan loading and path resolution.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `resolve_path(base_dir` (L6) | function | Public | Resolves path. |
| `load_case_or_plan(case_file` (L13) | function | Public | Loads case or plan. |

## `runner/live_rendering.py`

ANSI interactive dashboard rendering.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_metric_status_line(metric_id` (L9) | function | Internal | Implementation helper for metric status line. |
| `_metric_display_status(metric_id` (L24) | function | Internal | Implementation helper for metric display status. |
| `_terminal_width(value` (L32) | function | Internal | Implementation helper for terminal width. |
| `_clip(value` (L36) | function | Internal | Implementation helper for clip. |
| `_tui_border(width` (L46) | function | Internal | Implementation helper for tui border. |
| `_tui_row(content` (L56) | function | Internal | Implementation helper for tui row. |
| `_progress_bar(completed` (L61) | function | Internal | Implementation helper for progress bar. |
| `render_interactive_run_state(run_state` (L68) | function | Public | Render an ANSI-friendly dashboard for ``--display interactive``. This avoids optional third-party TUI dependencies while still providing a full-screen dashboard with boxed sections, progress, active metrics, branch summaries, and recent events. |
| `render_compact_run_state(run_state` (L158) | function | Public | Render a compact dashboard from centralized run telemetry. |
| `render_compact_taxonomy(metrics` (L216) | function | Public | Render a screen-friendly taxonomy summary with attention items expanded. |
| `render_live_taxonomy(metrics` (L285) | function | Public | Renders live taxonomy for terminal output. |

## `runner/order.py`

Taxonomy-order loading and deterministic metric ordering.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_walk_taxonomy(node` (L6) | function | Internal | Implementation helper for walk taxonomy. |
| `load_taxonomy_order(taxonomy_file` (L18) | function | Public | Loads taxonomy order. |
| `order_metrics_by_taxonomy(metrics` (L26) | function | Public | Implementation helper for order metrics by taxonomy. |

## `runner/parallel_progress.py`

Parallel progress callbacks and telemetry updates.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `build_parallel_progress_callback(*, plan` (L12) | function | Public | Build the live progress callback used by parallel metric execution. |
| `build_parallel_progress_callback._parallel_progress(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds)` (L32) | nested function | Internal | Implementation helper for parallel progress. |
| `_dataset_summary_line(shared_tabular_df, total` (L99) | function | Internal | Implementation helper for dataset summary line. |

## `runner/parallel_results.py`

Parallel record normalization and result aggregation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise_status(success` (L9) | function | Internal | Implementation helper for normalise status. |
| `collect_parallel_metric_results(*, parallel_out, metrics` (L16) | function | Public | Collect parallel outputs without discarding already-completed work. ``fail_fast`` is retained for call-site compatibility. The executor is responsible for stopping submission and marking metrics that were not run. |

## `runner/progress.py`

Terminal colour, progress bars, and live output.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `supports_color() -> bool` (L18) | function | Public | Implementation helper for supports color. |
| `colorize_status(status` (L22) | function | Public | Implementation helper for colorize status. |
| `render_metric_activity_bar(elapsed` (L31) | function | Public | Renders metric activity bar for terminal output. |
| `render_overall_progress_line(current` (L41) | function | Public | Renders overall progress line for terminal output. |
| `print_live_status(task_line` (L59) | function | Public | Implementation helper for print live status. |
| `set_live_header(lines` (L84) | function | Public | Implementation helper for set live header. |
| `set_live_output_enabled(enabled` (L89) | function | Public | Implementation helper for set live output enabled. |

## `runner/run_context.py`

Input resolution, validation, ordering, signals, display, and telemetry setup.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `PreparedRunContext` (L16) | class | Public | Resolved run inputs and display/control state needed by the runner. |
| `prepare_run_context(args, default_metric_predictions` (L35) | function | Public | Resolve CLI inputs, validate the plan, and prepare run state. |

## `runner/run_display.py`

Display-mode configuration and phase/title presentation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `configure_display(args) -> tuple[bool, str, int | None]` (L12) | function | Public | Configure live output and return render-enabled, mode, and max-line settings. |
| `compact_overall_progress_line(overall_header` (L25) | function | Public | Return the short version of the overall progress line used in live headers. |
| `print_title_box(lines` (L31) | function | Public | Print a boxed title/header block. |
| `print_phase_status(phase` (L37) | function | Public | Print a timestamped phase status line. |

## `runner/run_plan_helpers.py`

CLI parsing, headers, signal handlers, outcome construction, and atomic writes.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `build_outcome(status` (L17) | function | Public | Builds outcome. |
| `write_outcome(output_path` (L56) | function | Public | Write an outcome atomically, creating its destination directory first. |
| `detect_ip_fields(tabular_df) -> tuple[str, str]` (L80) | function | Public | Detects IP fields. |
| `build_title_box_lines(lines` (L86) | function | Public | Builds title box lines. |
| `build_base_header_lines(plan` (L98) | function | Public | Builds base header lines. |
| `configure_signal_handlers(control_state` (L121) | function | Public | Configures signal handlers. |
| `configure_signal_handlers._handle_sigint(_signum, _frame)` (L122) | nested function | Internal | Implementation helper for handle sigint. |
| `configure_signal_handlers._handle_sigusr1(_signum, _frame)` (L128) | nested function | Internal | Implementation helper for handle sigusr1. |
| `configure_signal_handlers._handle_sigusr2(_signum, _frame)` (L132) | nested function | Internal | Implementation helper for handle sigusr2. |
| `parse_run_plan_args() -> argparse.Namespace` (L143) | function | Public | Parses run plan args. |
| `update_live_header(lines` (L201) | function | Public | Updates live header. |

## `runner/run_plan_serial.py`

Serial metric execution and interruption handling.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_serial_metrics(*, dataset_path` (L12) | function | Public | Runs metrics one at a time, updates display and telemetry, and builds an outcome. |

## `runner/schema.py`

Plan JSON structural validation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_require_non_empty_string(value, path` (L6) | function | Internal | Implementation helper for require non empty string. |
| `_validate_string_list(value, path` (L12) | function | Internal | Implementation helper for validate string list. |
| `validate_plan_schema(plan` (L21) | function | Public | Validates plan metadata, execution policy, metric IDs, taxonomy paths, requirements, calculation blocks, and retention blocks. |

## `runner/tabular.py`

CSV, TSV, XLSX, and XLS loading.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `load_tabular_dataset(dataset_path` (L6) | function | Public | Loads tabular dataset. |

## `runner/taxonomy.py`

Plan and result taxonomy tree construction.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `ensure_taxonomy_path(root` (L3) | function | Public | Implementation helper for ensure taxonomy path. |
| `build_plan_taxonomy(metrics` (L10) | function | Public | Builds plan taxonomy. |
| `build_result_taxonomy(metrics` (L22) | function | Public | Builds result taxonomy. |
| `build_test_results_taxonomy(metrics` (L37) | function | Public | Builds test results taxonomy. |
| `print_taxonomy_summary(result_taxonomy` (L49) | function | Public | Implementation helper for print taxonomy summary. |

## `runner/telemetry.py`

Run, metric, and event state models.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `RunEvent` (L13) | class | Public | Data model for RunEvent. |
| `RunEvent.to_dict(self) -> dict[str, Any]` (L20) | method | Public | Implementation helper for to dict. |
| `MetricState` (L34) | class | Public | Data model for MetricState. |
| `MetricState.branch@property
def branch(self) -> str` (L45) | method | Public | Implementation helper for branch. |
| `RunState` (L50) | class | Public | Data model for RunState. |
| `RunState.from_plan@classmethod
def from_plan(cls, *, case_id` (L62) | method | Public | Implementation helper for from plan. |
| `RunState.record_event(self, event_type` (L91) | method | Public | Implementation helper for record event. |
| `RunState.mark_running(self, metric_id` (L94) | method | Public | Implementation helper for mark running. |
| `RunState.mark_completed(self, metric_id` (L103) | method | Public | Implementation helper for mark completed. |
| `RunState.mark_skipped(self, metric_id` (L127) | method | Public | Implementation helper for mark skipped. |
| `RunState.status_counts(self) -> dict[str, int]` (L141) | method | Public | Implementation helper for status counts. |
| `RunState.branch_summaries(self) -> dict[str, dict[str, int]]` (L147) | method | Public | Implementation helper for branch summaries. |
| `RunState.completed_statuses(self) -> dict[str, str]` (L158) | method | Public | Implementation helper for completed statuses. |
| `RunState.completed_durations(self) -> dict[str, float]` (L165) | method | Public | Implementation helper for completed durations. |
| `RunState.recent_completed(self, limit` (L172) | method | Public | Implementation helper for recent completed. |
| `RunState.attention_metrics(self) -> list[MetricState]` (L177) | method | Public | Implementation helper for attention metrics. |

## `scripts/build_documentation_inventory.py`

Repository inventory generator used for documentation audits.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `Symbol` (L15) | class | Public | Data model for Symbol. |
| `_annotation(node` (L26) | function | Internal | Implementation helper for annotation. |
| `_signature(node` (L30) | function | Internal | Implementation helper for signature. |
| `_iter_python_files(root` (L61) | function | Internal | Implementation helper for iter python files. |
| `_symbol(file` (L69) | function | Internal | Implementation helper for symbol. |
| `collect_symbols(root` (L84) | function | Public | Collects symbols. |
| `_constant_string(node` (L102) | function | Internal | Implementation helper for constant string. |
| `collect_cli_options(root` (L108) | function | Public | Collects CLI options. |
| `collect_metric_ids(root` (L136) | function | Public | Collects metric ids. |
| `collect_json_files(root` (L160) | function | Public | Collects JSON files. |
| `build_inventory(root` (L182) | function | Public | Builds inventory. |
| `render_markdown(inventory` (L201) | function | Public | Renders markdown for terminal output. |
| `parse_args() -> argparse.Namespace` (L249) | function | Public | Parses args. |
| `main() -> int` (L257) | function | Public | Implementation helper for main. |

## `scripts/build_reference_documentation.py`

Generator for the function and test references.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `Symbol` (L80) | class | Public | Data model for Symbol. |
| `_function_signature(node` (L90) | function | Internal | Implementation helper for function signature. |
| `_called_names(node` (L97) | function | Internal | Implementation helper for called names. |
| `_walk_symbols(file` (L111) | function | Internal | Implementation helper for walk symbols. |
| `collect_symbols(root` (L150) | function | Public | Collects symbols. |
| `_words(name` (L163) | function | Internal | Implementation helper for words. |
| `describe_symbol(symbol` (L179) | function | Public | Implementation helper for describe symbol. |
| `_module_summary(path` (L219) | function | Internal | Implementation helper for module summary. |
| `_escape_table_cell(value` (L229) | function | Internal | Implementation helper for escape table cell. |
| `render_function_reference(symbols` (L233) | function | Public | Renders function reference for terminal output. |
| `_relevant_calls(symbol` (L269) | function | Internal | Implementation helper for relevant calls. |
| `render_test_reference(symbols` (L298) | function | Public | Renders test reference for terminal output. |
| `_write_or_check(path` (L363) | function | Internal | Implementation helper for write or check. |
| `parse_args() -> argparse.Namespace` (L371) | function | Public | Parses args. |
| `main() -> int` (L378) | function | Public | Implementation helper for main. |

## `tests/label_fidelity_profile.py`

Label-integrity metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L6) | function | Internal | Implementation helper for normalise. |
| `_label_field(metric` (L13) | function | Internal | Implementation helper for label field. |
| `_slice_field(metric` (L17) | function | Internal | Implementation helper for slice field. |
| `_timestamp_field(metric` (L21) | function | Internal | Implementation helper for timestamp field. |
| `_parse_timestamps(df` (L25) | function | Internal | Implementation helper for parse timestamps. |
| `_label_values(metric` (L31) | function | Internal | Implementation helper for label values. |
| `_observed_labels(series` (L35) | function | Internal | Implementation helper for observed labels. |
| `compute_label_coverage_ratio(df` (L39) | function | Public | Computes label coverage ratio and returns a structured result. |
| `compute_per_slice_label_coverage_ratio(df` (L57) | function | Public | Computes per slice label coverage ratio and returns a structured result. |
| `_entropy_score(labels` (L87) | function | Internal | Implementation helper for entropy score. |
| `compute_per_slice_label_entropy_score(df` (L108) | function | Public | Computes per slice label entropy score and returns a structured result. |
| `compute_class_imbalance_score(df` (L124) | function | Public | Computes class imbalance score and returns a structured result. |
| `_attack_windows(metric` (L143) | function | Internal | Implementation helper for attack windows. |
| `_in_any_window(timestamp` (L153) | function | Internal | Implementation helper for in any window. |
| `compute_attack_window_alignment_score(df` (L157) | function | Public | Computes attack window alignment score and returns a structured result. |
| `compute_pre_post_attack_label_bleed_ratio(df` (L176) | function | Public | Computes pre post attack label bleed ratio and returns a structured result. |
| `_split_masks(df` (L197) | function | Internal | Implementation helper for split masks. |
| `compute_train_test_duplicate_overlap_ratio(df` (L208) | function | Public | Computes train test duplicate overlap ratio and returns a structured result. |
| `compute_train_test_identifier_contamination_ratio(df` (L221) | function | Public | Computes train test identifier contamination ratio and returns a structured result. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/address_validity/reserved_ip_address_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `get_reserved_categories(addr) -> list[str]` (L21) | function | Public | Implementation helper for get reserved categories. |
| `run_reserved_ip_address_metric(dataset_path` (L55) | function | Public | Runs reserved IP address metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/address_validity/valid_ip_address_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `classify_ip_value(ip_value) -> str` (L5) | function | Public | Classify an IP field value as one of: missing, ipv4, ipv6, invalid. |
| `run_protocol_validity_metric(dataset_path` (L26) | function | Public | Runs protocol validity metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/flow_duration_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_flow_duration_consistency_metric(dataset_path` (L7) | function | Public | Runs flow duration consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/handshake_plausibility_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_to_float(v)` (L7) | function | Internal | Implementation helper for to float. |
| `run_handshake_plausibility_metric(dataset_path` (L20) | function | Public | Runs handshake plausibility metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/packet_byte_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_packet_byte_consistency_metric(dataset_path` (L7) | function | Public | Runs packet byte consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/tcp_flag_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_tcp_flag_consistency_metric(dataset_path` (L7) | function | Public | Runs TCP flag consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/port_validity/service_port_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `normalize_port_series(series)` (L5) | function | Public | Normalizes port series. |
| `parse_port(value)` (L23) | function | Public | Parses port. |
| `run_service_port_consistency_metric(dataset_path` (L45) | function | Public | Heuristic check of expected service ports in tabular flow data. This metric is a heuristic, not an absolute validity test. A service-port mismatch can be legitimate because services may run on non-standard ports, and attack traffic may intentionally target unusual ports. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/port_validity/valid_port_range_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `parse_port(value)` (L5) | function | Public | Parses port. |
| `classify_port_range(port` (L29) | function | Public | Classifies port range. |
| `run_valid_port_range_metric(dataset_path` (L39) | function | Public | Runs valid port range metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/slice_metadata_integrity/slice_identifier_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_norm(v, case_sensitive` (L6) | function | Internal | Implementation helper for norm. |
| `_rule_match(field_value, operator, target, case_sensitive)` (L15) | function | Internal | Implementation helper for rule match. |
| `run_slice_identifier_consistency_metric(dataset_path` (L37) | function | Public | Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/slice_metadata_integrity/valid_slice_identifier_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `normalise_slice_id(value, case_sensitive` (L6) | function | Public | Normalizes slice id. |
| `run_valid_slice_identifier_metric(dataset_path` (L20) | function | Public | Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable. |

## `tests/reference_model_comparison_profile.py`

Reference-comparison metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_reference_path(metric` (L16) | function | Internal | Implementation helper for reference path. |
| `_load_reference_df(metric` (L22) | function | Internal | Implementation helper for load reference dataframe. |
| `_candidate_fields(metric` (L35) | function | Internal | Implementation helper for candidate fields. |
| `_numeric_values(df` (L39) | function | Internal | Implementation helper for numeric values. |
| `_feature_metric(df` (L46) | function | Internal | Implementation helper for feature metric. |
| `compute_feature_wise_wasserstein_distance_from_reference(df` (L69) | function | Public | Computes feature wise wasserstein distance from reference and returns a structured result. |
| `compute_feature_wise_ks_statistic_from_reference(df` (L73) | function | Public | Computes feature wise KS statistic from reference and returns a structured result. |
| `compute_feature_wise_energy_distance_from_reference(df` (L77) | function | Public | Computes feature wise energy distance from reference and returns a structured result. |
| `compute_feature_set_mmd_score_from_reference(df` (L81) | function | Public | Computes feature set MMD score from reference and returns a structured result. |
| `_matrix_deviation(current_matrix` (L93) | function | Internal | Implementation helper for matrix deviation. |
| `_correlation_profile(df` (L105) | function | Internal | Implementation helper for correlation profile. |
| `compute_pearson_matrix_deviation_from_reference(df` (L117) | function | Public | Computes pearson matrix deviation from reference and returns a structured result. |
| `compute_spearman_matrix_deviation_from_reference(df` (L124) | function | Public | Computes spearman matrix deviation from reference and returns a structured result. |
| `compute_distance_correlation_matrix_deviation_from_reference(df` (L131) | function | Public | Computes distance correlation matrix deviation from reference and returns a structured result. |
| `_timestamp_field(metric` (L140) | function | Internal | Implementation helper for timestamp field. |
| `compute_inter_arrival_distribution_divergence_from_reference(df` (L144) | function | Public | Computes inter arrival distribution divergence from reference and returns a structured result. |
| `compute_burstiness_deviation_from_reference(df` (L152) | function | Public | Computes burstiness deviation from reference and returns a structured result. |
| `compute_hourly_activity_divergence_from_reference(df` (L160) | function | Public | Computes hourly activity divergence from reference and returns a structured result. |
| `_slice_field(metric` (L170) | function | Internal | Implementation helper for slice field. |
| `_label_field(metric` (L174) | function | Internal | Implementation helper for label field. |
| `_categorical_distribution(df` (L178) | function | Internal | Implementation helper for categorical distribution. |
| `_tv_distance(left` (L186) | function | Internal | Implementation helper for tv distance. |
| `compute_slice_proportion_deviation_from_reference(df` (L191) | function | Public | Computes slice proportion deviation from reference and returns a structured result. |
| `compute_per_slice_class_divergence_from_reference(df` (L197) | function | Public | Computes per slice class divergence from reference and returns a structured result. |
| `compute_per_slice_feature_distribution_deviation_from_reference(df` (L213) | function | Public | Computes per slice feature distribution deviation from reference and returns a structured result. |
| `compute_protocol_mix_divergence_from_reference(df` (L234) | function | Public | Computes protocol mix divergence from reference and returns a structured result. |
| `compute_port_use_divergence_from_reference(df` (L241) | function | Public | Computes port use divergence from reference and returns a structured result. |
| `compute_flow_statistic_deviation_from_reference(df` (L250) | function | Public | Computes flow statistic deviation from reference and returns a structured result. |

## `tests/slice_representation_profile.py`

Slice-representation metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L4) | function | Internal | Implementation helper for normalise. |
| `_slice_field(metric` (L11) | function | Internal | Implementation helper for slice field. |
| `_observed_slices(df` (L15) | function | Internal | Implementation helper for observed slices. |
| `compute_per_slice_sample_coverage_ratio(df` (L21) | function | Public | Computes per slice sample coverage ratio and returns a structured result. |
| `compute_per_slice_feature_coverage_ratio(df` (L42) | function | Public | Computes per slice feature coverage ratio and returns a structured result. |
| `compute_per_slice_class_coverage_ratio(df` (L72) | function | Public | Computes per slice class coverage ratio and returns a structured result. |
| `compute_slice_distribution_imbalance_score(df` (L108) | function | Public | Computes slice distribution imbalance score and returns a structured result. |
| `compute_cross_slice_duplicate_overlap_ratio(df` (L131) | function | Public | Computes cross slice duplicate overlap ratio and returns a structured result. |
| `compute_cross_slice_identifier_leakage_ratio(df` (L154) | function | Public | Computes cross slice identifier leakage ratio and returns a structured result. |
