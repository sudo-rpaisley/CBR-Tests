# Function and class reference

Exhaustive AST-generated reference for runtime code, metric implementations, compatibility modules, and scripts. Nested helpers are included. Actual pytest cases are documented in [Test suite reference](test_reference.md).

**Public** only means the leaf name lacks a leading underscore; it is not an API-stability promise. Nested functions are always internal.

## `cbr_tests/metrics/column_quality.py`

Column completeness, numeric usability, and variation metrics.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `compute_column_quality_profile(df: pd.DataFrame, candidate_fields: list[str]) -> dict` (L6) | function | Public | Build per-field and aggregate quality details for requested fields. |
| `compute_column_quality_profile._mean(key: str) -> float | None` (L76) | nested function | Internal | Implementation helper for mean. |

## `cbr_tests/metrics/data_quality.py`

Missing-value and duplicate-row metrics.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_select_fields(df: pd.DataFrame, metric: dict, key: str) -> list[str]` (L6) | function | Internal | Implementation helper for select fields. |
| `compute_missing_value_ratio(df: pd.DataFrame, metric: dict) -> dict` (L13) | function | Public | Computes missing value ratio and returns a structured result. |
| `compute_duplicate_row_ratio(df: pd.DataFrame, metric: dict) -> dict` (L49) | function | Public | Computes duplicate row ratio and returns a structured result. |

## `cbr_tests/metrics/pearson.py`

Numeric validation and Pearson correlation profiles.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `validate_candidate_fields(df: pd.DataFrame, candidate_fields: list[str]) -> tuple[list[dict], list[str], pd.DataFrame]` (L8) | function | Public | Validate and coerce fields that may be used for correlation metrics. |
| `compute_pearson_profile(df: pd.DataFrame, runnable_fields: list[str]) -> dict` (L53) | function | Public | Compute the Pearson correlation profile for runnable fields. |

## `cbr_tests/metrics/spearman.py`

Spearman rank-correlation profiles.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `compute_spearman_profile(df: pd.DataFrame, runnable_fields: list[str]) -> dict` (L10) | function | Public | Computes spearman profile and returns a structured result. |
| `validate_spearman_candidate_fields(df: pd.DataFrame, candidate_fields: list[str]) -> tuple[list[dict], list[str], pd.DataFrame]` (L40) | function | Public | Validates spearman candidate fields. |

## `cbr_tests/metrics/statistical.py`

Internal distribution drift and distance-correlation calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_clean_numeric_values(df: pd.DataFrame, field: str) -> list[float]` (L12) | function | Internal | Implementation helper for clean numeric values. |
| `_split_values(values: list[float]) -> tuple[list[float], list[float]]` (L17) | function | Internal | Implementation helper for split values. |
| `_mean_pairwise_abs_distance(left: list[float], right: list[float]) -> float` (L22) | function | Internal | Implementation helper for mean pairwise abs distance. |
| `_ks_statistic(left: list[float], right: list[float]) -> float` (L29) | function | Internal | Computes the largest empirical-CDF difference between two samples. |
| `_wasserstein_distance(left: list[float], right: list[float]) -> float` (L47) | function | Internal | Computes one-dimensional Wasserstein distance from empirical CDFs. |
| `_energy_distance(left: list[float], right: list[float]) -> float` (L70) | function | Internal | Computes the sample energy-distance expression from cross- and within-sample distances. |
| `_rbf_mmd(left: list[float], right: list[float], gamma: float | None = None) -> float` (L77) | function | Internal | Computes squared RBF-kernel MMD with a median-distance bandwidth when gamma is omitted. |
| `_rbf_mmd.kernel_mean(a_values: list[float], b_values: list[float]) -> float` (L88) | nested function | Internal | Implementation helper for kernel mean. |
| `_build_distributional_metric(df: pd.DataFrame, metric: dict, calculator, output_key: str) -> dict` (L102) | function | Internal | Implementation helper for build distributional metric. |
| `compute_ks_feature_divergence(df: pd.DataFrame, metric: dict) -> dict` (L159) | function | Public | Computes KS feature divergence and returns a structured result. |
| `compute_wasserstein_feature_distance(df: pd.DataFrame, metric: dict) -> dict` (L163) | function | Public | Computes wasserstein feature distance and returns a structured result. |
| `compute_energy_distance(df: pd.DataFrame, metric: dict) -> dict` (L169) | function | Public | Computes energy distance and returns a structured result. |
| `compute_maximum_mean_discrepancy(df: pd.DataFrame, metric: dict) -> dict` (L173) | function | Public | Computes maximum mean discrepancy and returns a structured result. |
| `_distance_matrix(values: list[float]) -> list[list[float]]` (L179) | function | Internal | Implementation helper for distance matrix. |
| `_double_center(matrix: list[list[float]]) -> list[list[float]]` (L183) | function | Internal | Implementation helper for double center. |
| `_mean_product(left: list[list[float]], right: list[list[float]]) -> float` (L203) | function | Internal | Implementation helper for mean product. |
| `_distance_correlation(left: list[float], right: list[float]) -> float` (L213) | function | Internal | Computes distance correlation from double-centered pairwise distance matrices. |
| `compute_distance_correlation_profile(df: pd.DataFrame, candidate_fields: list[str]) -> dict` (L227) | function | Public | Computes distance correlation profile and returns a structured result. |

## `cbr_tests/metrics/task_validation.py`

Accuracy and binary precision, recall, and F1 calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L6) | function | Internal | Implementation helper for normalise. |
| `_fields(metric: dict) -> tuple[str, str]` (L13) | function | Internal | Implementation helper for fields. |
| `_positive_label(metric: dict, labels: list[str]) -> str | None` (L21) | function | Internal | Uses an explicit positive class or the lexicographically last class when exactly two labels are observed. |
| `_confusion_counts(df: pd.DataFrame, metric: dict) -> dict` (L29) | function | Internal | Implementation helper for confusion counts. |
| `compute_benchmark_model_accuracy(df: pd.DataFrame, metric: dict) -> dict` (L80) | function | Public | Computes benchmark model accuracy and returns a structured result. |
| `compute_benchmark_model_precision(df: pd.DataFrame, metric: dict) -> dict` (L91) | function | Public | Computes benchmark model precision and returns a structured result. |
| `compute_benchmark_model_recall(df: pd.DataFrame, metric: dict) -> dict` (L99) | function | Public | Computes benchmark model recall and returns a structured result. |
| `compute_benchmark_model_f1_score(df: pd.DataFrame, metric: dict) -> dict` (L107) | function | Public | Computes benchmark model f1 score and returns a structured result. |

## `cbr_tests/metrics/temporal.py`

Timestamp, duration, timing-drift, hourly, and periodicity calculations.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_timestamp_field(metric: dict, default: str = 'timestamp') -> str` (L8) | function | Internal | Implementation helper for timestamp field. |
| `_parse_timestamp_series(df: pd.DataFrame, field: str) -> pd.Series` (L12) | function | Internal | Implementation helper for parse timestamp series. |
| `_ks_statistic(left: list[float], right: list[float]) -> float` (L18) | function | Internal | Computes the largest empirical-CDF difference between two samples. |
| `_split_list(values: list) -> tuple[list, list]` (L37) | function | Internal | Implementation helper for split list. |
| `_inter_arrival_seconds(timestamps: pd.Series) -> list[float]` (L42) | function | Internal | Implementation helper for inter arrival seconds. |
| `compute_timestamp_parse_success_ratio(df: pd.DataFrame, metric: dict) -> dict` (L50) | function | Public | Computes timestamp parse success ratio and returns a structured result. |
| `compute_start_end_timestamp_consistency_ratio(df: pd.DataFrame, metric: dict) -> dict` (L68) | function | Public | Computes start end timestamp consistency ratio and returns a structured result. |
| `compute_non_negative_duration_ratio(df: pd.DataFrame, metric: dict) -> dict` (L98) | function | Public | Computes non negative duration ratio and returns a structured result. |
| `compute_inter_arrival_time_distribution_divergence(df: pd.DataFrame, metric: dict) -> dict` (L132) | function | Public | Computes inter arrival time distribution divergence and returns a structured result. |
| `_burstiness(values: list[float]) -> float | None` (L155) | function | Internal | Implementation helper for burstiness. |
| `compute_burstiness_coefficient_deviation(df: pd.DataFrame, metric: dict) -> dict` (L165) | function | Public | Computes burstiness coefficient deviation and returns a structured result. |
| `_hourly_counts(timestamps: list[pd.Timestamp]) -> list[int]` (L192) | function | Internal | Implementation helper for hourly counts. |
| `_probabilities(counts: list[int]) -> list[float]` (L199) | function | Internal | Implementation helper for probabilities. |
| `compute_hourly_activity_distribution_divergence(df: pd.DataFrame, metric: dict) -> dict` (L204) | function | Public | Computes hourly activity distribution divergence and returns a structured result. |
| `compute_diurnal_pattern_similarity_score(df: pd.DataFrame, metric: dict) -> dict` (L230) | function | Public | Computes diurnal pattern similarity score and returns a structured result. |
| `_autocorrelation(values: list[int], lag: int) -> float | None` (L253) | function | Internal | Implementation helper for autocorrelation. |
| `compute_periodicity_preservation_score(df: pd.DataFrame, metric: dict) -> dict` (L265) | function | Public | Computes periodicity preservation score and returns a structured result. |

## `cbr_tests/metrics/timestamp_coherence.py`

Raw packet timestamp coherence scanning.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_timestamp_coherence_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L8) | function | Public | Scan a PCAP and assess whether packet timestamps are coherent. |

## `export_outcomes_for_graphs.py`

Flattens selected outcome fields into CSV tables for graphing.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `load_json(path: Path) -> dict` (L11) | function | Public | Loads JSON. |
| `main()` (L16) | function | Public | Implementation helper for main. |

## `run_plan.py`

Top-level command workflow from parsed arguments to the atomic outcome JSON.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_confirm_sidecar_update(action: str, path: Path, args) -> bool` (L57) | function | Internal | Implementation helper for confirm sidecar update. |
| `main()` (L70) | function | Public | Implementation helper for main. |
| `main._load_dataset_for_metric(path: Path)` (L234) | nested function | Internal | Implementation helper for load dataset for metric. |

## `runner/contract.py`

Python symbols defined by `runner/contract.py`.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `dataset_format(dataset_path: Path) -> str` (L9) | function | Public | Return the normalized dataset format derived from its filename suffix. |
| `validate_dataset_format_applicability(plan: dict, dataset_path: Path) -> None` (L14) | function | Public | Reject a dataset whose format is incompatible with the plan declaration. |
| `validate_loaded_dataset_applicability(plan: dict, dataframe) -> None` (L30) | function | Public | Validate applicability rules that require access to loaded tabular data. |
| `enforce_skip_policy(plan: dict, skipped_metrics: dict[str, list[str]], *, dry_run: bool = False) -> None` (L45) | function | Public | Enforce execution_policy.allow_skips once field preflight is complete. |
| `collect_reference_paths(plan: dict, *, base_dir: Path | None = None) -> list[Path]` (L62) | function | Public | Collect configured reference dataset paths for safety/provenance checks. |
| `validate_output_path_safety(output_path: Path, *, protected_paths: list[Path], allow_overwrite: bool = False) -> None` (L82) | function | Public | Prevent results from overwriting experiment inputs or existing output accidentally. |

## `runner/dataset_loading.py`

Shared dataframe loading with progress presentation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `is_tabular_dataset(dataset_path: Path) -> bool` (L15) | function | Public | Return true when the dataset extension is handled by the tabular loader. |
| `load_shared_tabular_dataset(*, dataset_path: Path, plan: dict, case_id: str, output_path: Path, metrics: list[dict], field_translation: dict[str, str], default_metric_predictions: dict[str, float], display_mode: str, display_max_lines: int | None, run_state)` (L20) | function | Public | Load a tabular dataset once while updating the live loading display. |
| `load_shared_tabular_dataset._chunk_progress(chunk_idx: int, total_rows: int) -> None` (L37) | nested function | Internal | Implementation helper for chunk progress. |
| `_update_loaded_dataset_header(plan: dict, case_id: str, dataset_path: Path, output_path: Path, shared_tabular_df, metric_count: int) -> None` (L75) | function | Internal | Implementation helper for update loaded dataset header. |

## `runner/dispatch.py`

Metric registry, wrappers, field translation, and handler construction.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `register_metric(metric_id: str)` (L115) | function | Public | Implementation helper for register metric. |
| `register_metric._decorator(function)` (L116) | nested function | Internal | Implementation helper for decorator. |
| `run_pearson_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L123) | function | Public | Runs pearson metric. |
| `run_spearman_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L145) | function | Public | Runs spearman metric. |
| `run_missing_value_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L173) | function | Public | Runs missing value metric. |
| `run_duplicate_row_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L185) | function | Public | Runs duplicate row metric. |
| `run_tabular_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None, metric_id: str, compute_fn)` (L197) | function | Public | Runs tabular metric. |
| `run_distance_correlation_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L209) | function | Public | Runs distance correlation metric. |
| `run_column_quality_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None)` (L235) | function | Public | Runs column quality metric. |
| `_timestamp_metric(dataset_path: Path, metric: dict)` (L248) | function | Internal | Implementation helper for timestamp metric. |
| `_protocol_metric(dataset_path: Path, metric: dict)` (L253) | function | Internal | Implementation helper for protocol metric. |
| `_reserved_ip_metric(dataset_path: Path, metric: dict)` (L258) | function | Internal | Implementation helper for reserved IP metric. |
| `_valid_port_metric(dataset_path: Path, metric: dict)` (L263) | function | Internal | Implementation helper for valid port metric. |
| `_service_port_metric(dataset_path: Path, metric: dict)` (L268) | function | Internal | Implementation helper for service port metric. |
| `_tcp_flag_metric(dataset_path: Path, metric: dict)` (L273) | function | Internal | Implementation helper for TCP flag metric. |
| `_handshake_metric(dataset_path: Path, metric: dict)` (L278) | function | Internal | Implementation helper for handshake metric. |
| `_flow_duration_metric(dataset_path: Path, metric: dict)` (L283) | function | Internal | Implementation helper for flow duration metric. |
| `_packet_byte_metric(dataset_path: Path, metric: dict)` (L288) | function | Internal | Implementation helper for packet byte metric. |
| `_derived_rate_metric(dataset_path: Path, metric: dict)` (L293) | function | Internal | Implementation helper for derived rate metric. |
| `_slice_valid_metric(dataset_path: Path, metric: dict)` (L298) | function | Internal | Implementation helper for slice valid metric. |
| `_slice_consistency_metric(dataset_path: Path, metric: dict)` (L303) | function | Internal | Implementation helper for slice consistency metric. |
| `_wrap_registered_handler(handler, shared_df: pd.DataFrame | None, field_translation: dict[str, str] | None = None)` (L357) | function | Internal | Implementation helper for wrap registered handler. |
| `_wrap_registered_handler._wrapped(dataset_path: Path, metric: dict)` (L362) | nested function | Internal | Implementation helper for wrapped. |
| `_make_tabular_compute_handler(metric_id: str, compute_fn, shared_df: pd.DataFrame | None, load_tabular_dataset, field_translation: dict[str, str] | None = None)` (L377) | function | Internal | Implementation helper for make tabular compute handler. |
| `build_metric_handlers(shared_df: pd.DataFrame | None, load_tabular_dataset, field_translation: dict[str, str] | None = None)` (L398) | function | Public | Builds the metric-ID-to-callable mapping for a run. |
| `build_metric_handlers._translate(metric: dict)` (L408) | nested function | Internal | Implementation helper for translate. |

## `runner/execution.py`

Live status rendering and bounded parallel metric execution.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_metric_with_heartbeat(dataset_path: Path, metric: dict, metrics: list[dict], completed_statuses: dict[str, str], completed_durations: dict[str, float], current: int, total: int, shutdown_requested: dict, run_start_perf: float | None, metric_handlers: dict, default_predictions: dict[str, float], display_mode: str = 'full', max_lines: int | None = None, run_state = None)` (L13) | function | Public | Runs metric with heartbeat. |
| `auto_worker_count(num_metrics: int) -> int` (L62) | function | Public | Implementation helper for auto worker count. |
| `_not_run_payload(status: str, reason: str) -> dict` (L69) | function | Internal | Implementation helper for not run payload. |
| `_notify_progress(progress_callback, *args, payload = None) -> None` (L81) | function | Internal | Call progress callbacks without breaking the historical eight-argument API. |
| `run_metrics_parallel(dataset_path: Path, metrics: list[dict], metric_handlers: dict, workers: int, progress_callback = None, control_state: dict | None = None, fail_fast: bool = False) -> list[tuple[int, bool, dict]]` (L103) | function | Public | Run metrics with bounded submission and deterministic result records. At most ``workers`` metrics are submitted at once. When fail-fast is enabled, a failed metric stops new submissions. Already-running work is allowed to finish because Python threads cannot be safely terminated; metrics that were never started are explicitly marked ``not_run_fail_fast``. Cancellation returns promptly, attempts to cancel queued futures, and marks all unfinished or unsubmitted metrics ``not_run_cancelled``. |
| `run_metrics_parallel._timed_call(metric_id: str, metric: dict)` (L131) | nested function | Internal | Implementation helper for timed call. |
| `run_metrics_parallel._submit_available() -> None` (L152) | nested function | Internal | Implementation helper for submit available. |

## `runner/field_translation.py`

Public field-translation facade and compatibility exports.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `detect_standard_pcap_field_translation(columns) -> dict[str, str]` (L26) | function | Public | Detect Wireshark/tshark-style packet columns and map them to test fields. Returns a dataset-column -> canonical-test-field mapping. When multiple PCAP columns could satisfy the same test field, the first present candidate wins to avoid creating duplicate canonical columns. |
| `merge_field_translations(automatic: dict[str, str], explicit: dict[str, str]) -> dict[str, str]` (L42) | function | Public | Merge automatic and explicit translations, with explicit mappings winning. |
| `FieldTranslationError` (L61) | class | Public | Raised when a dataset field translation file is invalid or unsafe. |
| `load_field_translation(path: Path | None) -> dict[str, str]` (L65) | function | Public | Load a dataset-to-test field translation mapping from JSON. Preferred file shape:: {"fields": {"Dataset Column": "canonical_test_column"}} The mapping direction is always dataset column name -> field name used by test plans/metrics. For readability, files may also use ``dataset_to_test_fields`` with the same direction, or ``test_to_dataset_fields`` in the opposite direction. |
| `_validate_unique_targets(mapping: dict[str, str]) -> None` (L89) | function | Internal | Implementation helper for validate unique targets. |
| `_validate_no_column_collisions(columns, rename_map: dict[str, str]) -> None` (L101) | function | Internal | Implementation helper for validate no column collisions. |
| `default_field_translation_path(dataset_path: Path) -> Path` (L115) | function | Public | Return the sidecar translation path for a dataset. |
| `collect_required_test_fields(plan: dict) -> list[str]` (L120) | function | Public | Collect canonical field names referenced by enabled metric input requirements. |
| `ensure_field_translation_file(*, dataset_path: Path, plan: dict, detected_dataset_to_test: dict[str, str] | None = None) -> Path | None` (L132) | function | Public | Create or update the dataset sidecar translation template. The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``. It uses ``test_to_dataset_fields`` so every canonical field required by the plan is visible to users, even when the dataset column is not known yet. |
| `detect_standard_pcap_field_translation_for_dataset(dataset_path: Path) -> dict[str, str]` (L188) | function | Public | Read tabular headers, when possible, and detect standard PCAP heading mappings. |
| `_collect_required_fields_from_requirements(requirements: dict, fields: set[str]) -> None` (L211) | function | Internal | Implementation helper for collect required fields from requirements. |
| `_load_existing_translation_payload(path: Path) -> dict[str, Any]` (L231) | function | Internal | Implementation helper for load existing translation payload. |
| `_payload_to_test_to_dataset_fields(payload: dict[str, Any]) -> dict[str, str]` (L242) | function | Internal | Implementation helper for payload to test to dataset fields. |
| `validate_field_translation_payload(payload: dict[str, Any]) -> None` (L253) | function | Public | Validate the standard field translation sidecar payload shape. |
| `load_field_translation_from_payload(payload: dict[str, Any]) -> dict[str, str]` (L282) | function | Public | Load the standard test-to-dataset mapping from an already parsed payload. |
| `_invert_translation(dataset_to_test: dict[str, str]) -> dict[str, str]` (L302) | function | Internal | Implementation helper for invert translation. |
| `collect_required_test_fields_for_metric(metric: dict) -> list[str]` (L306) | function | Public | Collect canonical field names referenced by one metric's input requirements. |
| `read_tabular_dataset_columns(dataset_path: Path) -> list[str]` (L315) | function | Public | Read only dataset headers for supported tabular formats. |
| `available_translated_fields(columns, field_translation: dict[str, str]) -> set[str]` (L331) | function | Public | Return canonical fields available before metrics run. |
| `metrics_missing_required_fields(metrics: list[dict], available_fields: set[str]) -> dict[str, list[str]]` (L341) | function | Public | Return required fields unavailable for each metric. |
| `detect_known_field_translation(columns, required_fields: list[str] | None = None) -> dict[str, str]` (L365) | function | Public | Detect common dataset aliases and map them to canonical test fields. |
| `collect_field_requirements_for_metric(metric: dict) -> dict[str, list[str]]` (L378) | function | Public | Return required and optional canonical fields for one metric. |
| `collect_field_requirements(plan: dict) -> dict[str, dict[str, list[str]]]` (L398) | function | Public | Collect required/optional field usage across enabled plan metrics. |
| `field_resolver(field_translation: dict[str, str], dataset_columns = None) -> dict[str, str]` (L413) | function | Public | Build canonical field -> dataset field resolver mapping. |
| `translate_metric_fields(metric: dict, field_translation: dict[str, str], dataset_columns = None) -> dict` (L423) | function | Public | Return a metric copy with canonical input fields resolved to dataset columns. |
| `metrics_missing_optional_fields(metrics: list[dict], available_fields: set[str]) -> dict[str, list[str]]` (L433) | function | Public | Return optional fields unavailable for each metric. |
| `_normalise_field_name(value: str) -> str` (L444) | function | Internal | Implementation helper for normalise field name. |
| `suggest_field_mappings(fields: list[str], columns: list[str]) -> dict[str, list[str]]` (L448) | function | Public | Suggest dataset columns for unmapped canonical fields using loose name matching. |
| `field_mapping_details(*, detected_translation: dict[str, str] | None, explicit_translation: dict[str, str] | None, field_translation: dict[str, str] | None = None) -> dict[str, dict[str, str]]` (L464) | function | Public | Return canonical-field mapping details for reports. |
| `build_field_translation_report(*, dataset_path: Path, translation_path: Path | None, plan: dict, metrics: list[dict], available_fields: set[str], skipped_metrics: dict[str, list[str]], dataset_columns: list[str] | None = None, detected_translation: dict[str, str] | None = None, explicit_translation: dict[str, str] | None = None, field_translation: dict[str, str] | None = None, sidecar_status: str | None = None, missing_optional_fields: dict[str, list[str]] | None = None) -> dict` (L481) | function | Public | Build a machine-readable field translation validation report. |
| `write_field_translation_report(path: Path, report: dict) -> None` (L538) | function | Public | Writes field translation report. |
| `format_field_translation_report(report: dict, use_color: bool = False) -> str` (L545) | function | Public | Format a field translation report for humans. |
| `format_field_translation_markdown_report(report: dict) -> str` (L578) | function | Public | Format a field translation report as Markdown. |
| `write_text_report(path: Path, text: str) -> None` (L605) | function | Public | Writes text report. |
| `_translate_requirement_value(value, resolver: dict[str, str])` (L610) | function | Internal | Implementation helper for translate requirement value. |

## `runner/field_translation_reports.py`

Translation report construction and formatting.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise_field_name(value: str) -> str` (L11) | function | Internal | Implementation helper for normalise field name. |
| `suggest_field_mappings(fields: list[str], columns: list[str]) -> dict[str, list[str]]` (L15) | function | Public | Suggest dataset columns for unmapped canonical fields using loose name matching. |
| `field_mapping_details(*, detected_translation: dict[str, str] | None, explicit_translation: dict[str, str] | None, field_translation: dict[str, str] | None = None) -> dict[str, dict[str, str]]` (L31) | function | Public | Return canonical-field mapping details for reports. |
| `build_field_translation_report(*, dataset_path: Path, translation_path: Path | None, plan: dict, metrics: list[dict], available_fields: set[str], skipped_metrics: dict[str, list[str]], dataset_columns: list[str] | None = None, detected_translation: dict[str, str] | None = None, explicit_translation: dict[str, str] | None = None, field_translation: dict[str, str] | None = None, sidecar_status: str | None = None, missing_optional_fields: dict[str, list[str]] | None = None) -> dict` (L48) | function | Public | Build a machine-readable field translation validation report. |
| `write_field_translation_report(path: Path, report: dict) -> None` (L107) | function | Public | Writes field translation report. |
| `_display_width(value: str) -> int` (L118) | function | Internal | Return terminal display width for a string, ignoring ANSI escapes. |
| `_display_ljust(value: str, width: int) -> str` (L128) | function | Internal | Left-pad based on display width rather than Python character count. |
| `_character_display_width(character: str) -> int` (L133) | function | Internal | Implementation helper for character display width. |
| `_split_display_width(value: str, width: int) -> list[str]` (L141) | function | Internal | Split a string into display-width-limited chunks. Prefer splitting long identifiers at separators so metric names such as ``inter_arrival_time_distribution_divergence`` do not break in the middle of words. |
| `_wrap_display_width(value: str, width: int) -> list[str]` (L182) | function | Internal | Wrap a value on spaces when possible, falling back to display-width chunks. |
| `format_column_grid(items: list[str], *, indent: int = 2, max_width: int | None = None) -> list[str]` (L209) | function | Public | Format values as a display-width-aware grid without a section title. |
| `format_column_section(title: str, items: list[str], *, indent: int = 2, max_width: int | None = None) -> list[str]` (L243) | function | Public | Format a long list as a readable fixed-width column section. By default, use the current terminal width so the report displays as many columns as will fit on the user's display. Long names are wrapped inside cells, and width calculations use terminal display width for better Unicode alignment. A ``max_width`` can still be provided by tests or callers that need deterministic wrapping. |
| `_metric_detail_entry(metric_id: str, details: dict) -> str` (L257) | function | Internal | Format a metric name with any available status details. |
| `_status_title(status: str) -> str` (L274) | function | Internal | Implementation helper for status title. |
| `format_metric_section(report: dict, use_color: bool = False, *, max_width: int | None = None) -> list[str]` (L278) | function | Public | Format metric statuses as non-empty category sections. |
| `format_field_translation_report(report: dict, use_color: bool = False) -> str` (L315) | function | Public | Format a field translation report for humans. |
| `format_field_translation_markdown_report(report: dict) -> str` (L348) | function | Public | Format a field translation report as Markdown. |
| `write_text_report(path: Path, text: str) -> None` (L375) | function | Public | Writes text report. |

## `runner/field_translation_schema.py`

Translation payload validation and mapping normalization.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `FieldTranslationError` (L8) | class | Public | Raised when a dataset field translation file is invalid or unsafe. |
| `validate_unique_targets(mapping: dict[str, str]) -> None` (L12) | function | Public | Reject mappings where multiple dataset fields map to one canonical field. |
| `validate_field_translation_payload(payload: dict[str, Any]) -> None` (L25) | function | Public | Validate the standard field translation sidecar payload shape. |
| `load_field_translation_from_payload(payload: dict[str, Any]) -> dict[str, str]` (L54) | function | Public | Load a dataset-to-test mapping from an already parsed standard sidecar payload. |

## `runner/field_translation_sidecar.py`

Sidecar detection, creation, and extension.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `default_field_translation_path(dataset_path: Path) -> Path` (L11) | function | Public | Return the standard sidecar translation path for a dataset. |
| `ensure_field_translation_file(*, dataset_path: Path, plan: dict, detected_dataset_to_test: dict[str, str] | None = None) -> Path | None` (L16) | function | Public | Create or update the dataset sidecar translation template. The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``. Existing sidecars are only updated when enabled plan metrics introduce new canonical fields that are not already present in ``test_to_dataset_fields``. |
| `_load_existing_translation_payload(path: Path) -> dict[str, Any]` (L74) | function | Internal | Implementation helper for load existing translation payload. |
| `_payload_to_test_to_dataset_fields(payload: dict[str, Any]) -> dict[str, str]` (L85) | function | Internal | Implementation helper for payload to test to dataset fields. |
| `_invert_translation(dataset_to_test: dict[str, str]) -> dict[str, str]` (L96) | function | Internal | Implementation helper for invert translation. |

## `runner/field_translation_workflow.py`

Translation preflight and requested-report workflow.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `FieldTranslationContext` (L29) | class | Public | Prepared field translation state for a run. |
| `prepare_field_translation_context(*, args, dataset_path: Path, plan: dict, metrics: list[dict], all_enabled_metrics: list[dict], translation_path: Path | None, run_state) -> FieldTranslationContext` (L45) | function | Public | Load, create/update, validate, and report field translations for a run. |
| `skipped_metric_records(skipped_metrics: dict[str, list[str]]) -> list[dict[str, Any]]` (L142) | function | Public | Return outcome-ready skipped metric records for missing field mappings. |
| `print_field_translation_dry_run_summary(context: FieldTranslationContext) -> None` (L155) | function | Public | Print the dry-run field translation summary and completion status. |
| `_confirm_sidecar_update(action: str, path: Path, args) -> bool` (L169) | function | Internal | Implementation helper for confirm sidecar update. |
| `_print_skipped_metric_warning(skipped_metrics: dict[str, list[str]]) -> None` (L182) | function | Internal | Implementation helper for print skipped metric warning. |
| `_write_requested_reports(args, field_translation_report: dict[str, Any], human_report: str) -> None` (L190) | function | Internal | Implementation helper for write requested reports. |
| `_should_use_color() -> bool` (L202) | function | Internal | Implementation helper for should use color. |

## `runner/io.py`

Case/plan loading and path resolution.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `resolve_path(base_dir: Path, path_str: str) -> Path` (L6) | function | Public | Resolves path. |
| `load_case_or_plan(case_file: Path, dataset_arg: str | None, output_arg: str | None, case_id_arg: str, field_translation_arg: str | None = None)` (L13) | function | Public | Loads case or plan. |

## `runner/live_rendering.py`

ANSI interactive dashboard rendering.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_metric_status_line(metric_id: str, status: str, duration: float | None = None, elapsed: float | None = None, expected: float | None = None) -> str` (L12) | function | Internal | Implementation helper for metric status line. |
| `_metric_display_status(metric_id: str, current_metric_id: str, completed_statuses: dict[str, str]) -> str` (L30) | function | Internal | Implementation helper for metric display status. |
| `_diagnostic_suffix(metric) -> str` (L38) | function | Internal | Implementation helper for diagnostic suffix. |
| `_has_branch_attention(counts: dict[str, int]) -> bool` (L50) | function | Internal | Implementation helper for has branch attention. |
| `_terminal_width(value: str) -> int` (L54) | function | Internal | Implementation helper for terminal width. |
| `_clip(value: str, width: int) -> str` (L58) | function | Internal | Implementation helper for clip. |
| `_tui_border(width: int, left: str, fill: str, right: str, title: str = '') -> str` (L68) | function | Internal | Implementation helper for tui border. |
| `_tui_row(content: str, width: int) -> str` (L78) | function | Internal | Implementation helper for tui row. |
| `_progress_bar(completed: int, total: int, width: int) -> str` (L83) | function | Internal | Implementation helper for progress bar. |
| `render_interactive_run_state(run_state: RunState, default_predictions: dict[str, float], running_elapsed: dict[str, float] | None = None, max_lines: int | None = None) -> str` (L90) | function | Public | Renders interactive run state for terminal output. |
| `render_compact_run_state(run_state: RunState, default_predictions: dict[str, float], running_elapsed: dict[str, float] | None = None, max_lines: int | None = 24) -> str` (L176) | function | Public | Renders compact run state for terminal output. |
| `render_compact_taxonomy(metrics: list[dict], current_metric_id: str, completed_statuses: dict[str, str], completed_durations: dict[str, float], default_predictions: dict[str, float], predicted_metric_total: float, elapsed: float | None = None, running_elapsed: dict[str, float] | None = None, max_lines: int | None = 24) -> str` (L236) | function | Public | Renders compact taxonomy for terminal output. |
| `render_live_taxonomy(metrics: list[dict], current_metric_id: str, completed_statuses: dict[str, str], completed_durations: dict[str, float], default_predictions: dict[str, float], predicted_metric_total: float, elapsed: float | None = None, completed: bool = False, running_elapsed: dict[str, float] | None = None, display_mode: str = 'full', max_lines: int | None = None, run_state: RunState | None = None) -> str` (L308) | function | Public | Renders live taxonomy for terminal output. |

## `runner/metric_diagnostics.py`

Python symbols defined by `runner/metric_diagnostics.py`.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `extract_metric_result(metric_id: str, payload: dict | None) -> dict | None` (L8) | function | Public | Implementation helper for extract metric result. |
| `extract_result_status(metric_id: str, payload: dict | None) -> str | None` (L18) | function | Public | Implementation helper for extract result status. |
| `_infer_execution_reason_code(payload: dict, error: str) -> str` (L29) | function | Internal | Implementation helper for infer execution reason code. |
| `extract_diagnostic(metric_id: str, success: bool, payload: dict | None) -> dict[str, Any] | None` (L43) | function | Public | Implementation helper for extract diagnostic. |
| `display_status(metric_id: str, success: bool, payload: dict | None) -> str` (L78) | function | Public | Implementation helper for display status. |

## `runner/order.py`

Taxonomy-order loading and deterministic metric ordering.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_walk_taxonomy(node: dict, ranks: dict[str, int], counter: list[int]) -> None` (L6) | function | Internal | Implementation helper for walk taxonomy. |
| `load_taxonomy_order(taxonomy_file: Path) -> dict[str, int]` (L18) | function | Public | Loads taxonomy order. |
| `order_metrics_by_taxonomy(metrics: list[dict], ranks: dict[str, int], strict: bool = False) -> list[dict]` (L26) | function | Public | Implementation helper for order metrics by taxonomy. |

## `runner/parallel_progress.py`

Parallel progress callbacks and telemetry updates.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `build_parallel_progress_callback(*, plan: dict, case_id: str, dataset_path: Path, output_path: Path, metrics: list[dict], shared_tabular_df, mode: str, completed_statuses: dict[str, str], completed_durations: dict[str, float], default_metric_predictions: dict[str, float], run_start_perf: float, display_mode: str, display_max_lines: int | None, run_state)` (L13) | function | Public | Build the live progress callback used by parallel metric execution. |
| `build_parallel_progress_callback._parallel_progress(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds, payload = None)` (L33) | nested function | Internal | Implementation helper for parallel progress. |
| `_dataset_summary_line(shared_tabular_df, total: int) -> str` (L121) | function | Internal | Implementation helper for dataset summary line. |

## `runner/parallel_results.py`

Parallel record normalization and result aggregation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise_status(success: bool, payload: dict) -> str` (L11) | function | Internal | Implementation helper for normalise status. |
| `collect_parallel_metric_results(*, parallel_out, metrics: list[dict], run_started_at: datetime, fail_fast: bool, completed_statuses: dict[str, str], completed_durations: dict[str, float]) -> tuple[str, dict, list[dict], dict]` (L18) | function | Public | Collect parallel outputs without discarding already-completed work. Execution status remains separate from a metric's domain result. A handler can therefore execute successfully while producing ``pass``, ``warn``, ``fail`` or ``not_applicable`` in ``result_status``. ``completed_statuses`` keeps historical execution labels for execution failures/not-run records. Successfully executed metrics may expose their domain result there for post-run rendering; live parallel rendering already receives the richer status directly from the progress callback. |

## `runner/progress.py`

Terminal colour, progress bars, and live output.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `supports_color() -> bool` (L23) | function | Public | Implementation helper for supports color. |
| `colorize_status(status: str) -> str` (L27) | function | Public | Implementation helper for colorize status. |
| `render_metric_activity_bar(elapsed: float, expected_seconds: float = 60.0, width: int = 12) -> str` (L36) | function | Public | Renders metric activity bar for terminal output. |
| `render_overall_progress_line(current: int, total: int, run_elapsed: float | None = None, in_metric_elapsed: float | None = None) -> str` (L46) | function | Public | Renders overall progress line for terminal output. |
| `print_live_status(task_line: str, overall_line: str, warning_line: str | None = None) -> None` (L64) | function | Public | Implementation helper for print live status. |
| `set_live_header(lines: list[str]) -> None` (L89) | function | Public | Implementation helper for set live header. |
| `set_live_output_enabled(enabled: bool) -> None` (L94) | function | Public | Implementation helper for set live output enabled. |

## `runner/provenance.py`

Python symbols defined by `runner/provenance.py`.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_canonical_json_bytes(value: Any) -> bytes` (L20) | function | Internal | Implementation helper for canonical JSON bytes. |
| `sha256_json(value: Any) -> str` (L30) | function | Public | Implementation helper for sha256 JSON. |
| `sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str` (L34) | function | Public | Implementation helper for sha256 file. |
| `file_manifest(path: Path) -> dict[str, Any]` (L45) | function | Public | Implementation helper for file manifest. |
| `_git_metadata(repo_root: Path) -> dict[str, Any]` (L63) | function | Internal | Implementation helper for git metadata. |
| `_dependency_versions() -> dict[str, str | None]` (L91) | function | Internal | Implementation helper for dependency versions. |
| `software_manifest() -> dict[str, Any]` (L101) | function | Public | Implementation helper for software manifest. |
| `resolve_plan_source_path(case_file: Path) -> Path` (L119) | function | Public | Resolve the plan file used by a case, or return the direct plan file itself. |
| `build_provenance_manifest(*, plan: dict, dataset_path: Path, case_file: Path, plan_source_path: Path, field_translation: dict[str, str], translation_path: Path | None, taxonomy_path: Path | None, cli_arguments: dict[str, Any]) -> dict[str, Any]` (L136) | function | Public | Build the immutable experiment-identification metadata stored with an outcome. |

## `runner/run_context.py`

Input resolution, validation, ordering, signals, display, and telemetry setup.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `PreparedRunContext` (L24) | class | Public | Resolved run inputs and display/control state needed by the runner. |
| `prepare_run_context(args, default_metric_predictions: dict[str, float]) -> PreparedRunContext` (L48) | function | Public | Resolve CLI inputs, validate the plan, and prepare run state. |

## `runner/run_display.py`

Display-mode configuration and phase/title presentation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `configure_display(args) -> tuple[bool, str, int | None]` (L12) | function | Public | Configure live output and return render-enabled, mode, and max-line settings. |
| `compact_overall_progress_line(overall_header: str) -> str` (L25) | function | Public | Return the short version of the overall progress line used in live headers. |
| `print_title_box(lines: list[str]) -> None` (L31) | function | Public | Print a boxed title/header block. |
| `print_phase_status(phase: str, detail: str = '') -> None` (L37) | function | Public | Print a timestamped phase status line. |

## `runner/run_plan_helpers.py`

CLI parsing, headers, signal handlers, outcome construction, and atomic writes.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `build_outcome(status: str, case_id: str, plan_id: str, metrics: list[dict], dataset_path: Path, metric_results: list[dict], test_results: dict, run_started_at: datetime, run_start_perf: float, column_validations: dict, skipped_metrics: list[dict] | None = None, all_metrics: list[dict] | None = None, provenance: dict | None = None) -> dict` (L17) | function | Public | Builds outcome. |
| `write_outcome(output_path: Path, outcome: dict) -> None` (L60) | function | Public | Write an outcome atomically, creating its destination directory first. |
| `detect_ip_fields(tabular_df) -> tuple[str, str]` (L84) | function | Public | Detects IP fields. |
| `build_title_box_lines(lines: list[str], status_lines: list[str] | None = None, width: int = 108) -> list[str]` (L90) | function | Public | Builds title box lines. |
| `build_base_header_lines(plan: dict, case_id: str, dataset_path: Path, output_path: Path, include_dataset_size: bool = False) -> list[str]` (L102) | function | Public | Builds base header lines. |
| `configure_signal_handlers(control_state: dict, shutdown_requested: dict) -> None` (L125) | function | Public | Configures signal handlers. |
| `configure_signal_handlers._handle_sigint(_signum, _frame)` (L126) | nested function | Internal | Implementation helper for handle sigint. |
| `configure_signal_handlers._handle_sigusr1(_signum, _frame)` (L132) | nested function | Internal | Implementation helper for handle sigusr1. |
| `configure_signal_handlers._handle_sigusr2(_signum, _frame)` (L136) | nested function | Internal | Implementation helper for handle sigusr2. |
| `parse_run_plan_args() -> argparse.Namespace` (L147) | function | Public | Parses run plan args. |
| `update_live_header(lines: list[str], status_lines: list[str] | None = None, width: int = 108) -> None` (L206) | function | Public | Updates live header. |

## `runner/run_plan_serial.py`

Serial metric execution and interruption handling.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_serial_metrics(*, dataset_path: Path, output_path: Path, plan: dict, case_id: str, metrics: list[dict], metric_handlers: dict, shutdown_requested: dict, control_state: dict, default_metric_predictions: dict, live_render_enabled: bool, fail_fast: bool, run_started_at: datetime, run_start_perf: float, completed_statuses: dict, completed_durations: dict, skipped_metrics: list[dict] | None = None, all_metrics: list[dict] | None = None, display_mode: str = 'full', display_max_lines: int | None = None, run_state: RunState | None = None, provenance: dict | None = None)` (L15) | function | Public | Runs metrics one at a time, updates display and telemetry, and builds an outcome. |

## `runner/schema.py`

Plan JSON structural validation.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_require_non_empty_string(value, path: str) -> str` (L7) | function | Internal | Implementation helper for require non empty string. |
| `_validate_string_list(value, path: str, *, allow_empty: bool = True) -> None` (L13) | function | Internal | Implementation helper for validate string list. |
| `_validate_applicability(applicability: object) -> None` (L22) | function | Internal | Implementation helper for validate applicability. |
| `validate_plan_schema(plan: dict) -> None` (L41) | function | Public | Validates plan metadata, execution policy, metric IDs, taxonomy paths, requirements, calculation blocks, and retention blocks. |

## `runner/tabular.py`

CSV, TSV, XLSX, and XLS loading.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `load_tabular_dataset(dataset_path: Path, progress_callback = None, field_translation: dict[str, str] | None = None)` (L6) | function | Public | Loads tabular dataset. |

## `runner/taxonomy.py`

Plan and result taxonomy tree construction.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `ensure_taxonomy_path(root: dict, taxonomy_path: list[str]) -> dict` (L4) | function | Public | Implementation helper for ensure taxonomy path. |
| `build_plan_taxonomy(metrics: list[dict]) -> dict` (L11) | function | Public | Builds plan taxonomy. |
| `_display_status(record: dict) -> str` (L23) | function | Internal | Implementation helper for display status. |
| `build_result_taxonomy(metrics: list[dict], metric_results: list[dict], test_results: dict) -> dict` (L32) | function | Public | Builds result taxonomy. |
| `build_test_results_taxonomy(metrics: list[dict], test_results: dict) -> dict` (L51) | function | Public | Builds test results taxonomy. |
| `print_taxonomy_summary(result_taxonomy: dict, indent: int = 0) -> None` (L63) | function | Public | Implementation helper for print taxonomy summary. |

## `runner/telemetry.py`

Run, metric, and event state models.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `RunEvent` (L14) | class | Public | Data model for RunEvent. |
| `RunEvent.to_dict(self) -> dict[str, Any]` (L21) | method | Public | Implementation helper for to dict. |
| `MetricState` (L35) | class | Public | Data model for MetricState. |
| `MetricState.branch(self) -> str` (L50) | method | Public | Implementation helper for branch. |
| `MetricState.display_status(self) -> str` (L54) | method | Public | Implementation helper for display status. |
| `RunState` (L63) | class | Public | Data model for RunState. |
| `RunState.from_plan(cls, *, case_id: str, plan: dict, metrics: list[dict], dataset_path: Path, output_path: Path, started_at: datetime) -> 'RunState'` (L75) | method | Public | Implementation helper for from plan. |
| `RunState.record_event(self, event_type: str, message: str, metric_id: str | None = None, **payload: Any) -> None` (L104) | method | Public | Implementation helper for record event. |
| `RunState.mark_running(self, metric_id: str, *, started_at: datetime | None = None) -> None` (L107) | method | Public | Implementation helper for mark running. |
| `RunState.mark_completed(self, metric_id: str, status: str, *, elapsed_seconds: float | None = None, error: str | None = None, result_status: str | None = None, diagnostic: dict[str, Any] | None = None, finished_at: datetime | None = None) -> None` (L116) | method | Public | Implementation helper for mark completed. |
| `RunState.mark_skipped(self, metric_id: str, missing_fields: list[str]) -> None` (L154) | method | Public | Implementation helper for mark skipped. |
| `RunState.status_counts(self) -> dict[str, int]` (L171) | method | Public | Implementation helper for status counts. |
| `RunState.branch_summaries(self) -> dict[str, dict[str, int]]` (L190) | method | Public | Implementation helper for branch summaries. |
| `RunState.completed_statuses(self) -> dict[str, str]` (L199) | method | Public | Implementation helper for completed statuses. |
| `RunState.completed_durations(self) -> dict[str, float]` (L206) | method | Public | Implementation helper for completed durations. |
| `RunState.recent_completed(self, limit: int = 5) -> list[MetricState]` (L213) | method | Public | Implementation helper for recent completed. |
| `RunState.attention_metrics(self) -> list[MetricState]` (L224) | method | Public | Implementation helper for attention metrics. |

## `scripts/build_documentation_inventory.py`

Repository inventory generator used for documentation audits.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `Symbol` (L15) | class | Public | Data model for Symbol. |
| `_annotation(node: ast.expr | None) -> str | None` (L26) | function | Internal | Implementation helper for annotation. |
| `_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str` (L30) | function | Internal | Implementation helper for signature. |
| `_iter_python_files(root: Path) -> list[Path]` (L61) | function | Internal | Implementation helper for iter python files. |
| `_symbol(file: Path, root: Path, node: ast.AST, qualified_name: str, kind: str) -> Symbol` (L69) | function | Internal | Implementation helper for symbol. |
| `collect_symbols(root: Path) -> list[Symbol]` (L84) | function | Public | Collects symbols. |
| `_constant_string(node: ast.AST) -> str | None` (L102) | function | Internal | Implementation helper for constant string. |
| `collect_cli_options(root: Path) -> list[dict[str, Any]]` (L108) | function | Public | Collects CLI options. |
| `collect_metric_ids(root: Path) -> list[dict[str, Any]]` (L136) | function | Public | Collects metric ids. |
| `collect_json_files(root: Path) -> list[dict[str, Any]]` (L160) | function | Public | Collects JSON files. |
| `build_inventory(root: Path) -> dict[str, Any]` (L182) | function | Public | Builds inventory. |
| `render_markdown(inventory: dict[str, Any]) -> str` (L201) | function | Public | Renders markdown for terminal output. |
| `parse_args() -> argparse.Namespace` (L249) | function | Public | Parses args. |
| `main() -> int` (L257) | function | Public | Implementation helper for main. |

## `scripts/reference_documentation.py`

AST engine that generates the exhaustive function and test references.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `Symbol` (L81) | class | Public | Data model for Symbol. |
| `_annotation(node: ast.expr | None) -> str | None` (L91) | function | Internal | Implementation helper for annotation. |
| `_argument(argument: ast.arg, default: ast.expr | None = None) -> str` (L95) | function | Internal | Implementation helper for argument. |
| `function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str` (L105) | function | Public | Implementation helper for function signature. |
| `called_names(node: ast.AST) -> tuple[str, ...]` (L136) | function | Public | Implementation helper for called names. |
| `walk_symbols(file: Path, root: Path, body: list[ast.stmt], prefix: str = '', parent_kind: str | None = None) -> list[Symbol]` (L150) | function | Public | Implementation helper for walk symbols. |
| `collect_symbols(root: Path) -> list[Symbol]` (L187) | function | Public | Collects symbols. |
| `words(name: str) -> str` (L200) | function | Public | Implementation helper for words. |
| `describe_symbol(symbol: Symbol) -> str` (L216) | function | Public | Implementation helper for describe symbol. |
| `module_summary(path: str) -> str` (L254) | function | Public | Implementation helper for module summary. |
| `escape_table_cell(value: str) -> str` (L264) | function | Public | Implementation helper for escape table cell. |
| `render_function_reference(symbols: list[Symbol]) -> str` (L268) | function | Public | Renders function reference for terminal output. |
| `relevant_calls(symbol: Symbol) -> str` (L304) | function | Public | Implementation helper for relevant calls. |
| `render_test_reference(symbols: list[Symbol]) -> str` (L333) | function | Public | Renders test reference for terminal output. |
| `write_or_check(path: Path, content: str, check: bool) -> bool` (L398) | function | Public | Writes or check. |
| `parse_args() -> argparse.Namespace` (L406) | function | Public | Parses args. |
| `main() -> int` (L413) | function | Public | Implementation helper for main. |

## `tests/label_fidelity_profile.py`

Label-integrity metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L6) | function | Internal | Implementation helper for normalise. |
| `_label_field(metric: dict) -> str` (L13) | function | Internal | Implementation helper for label field. |
| `_slice_field(metric: dict) -> str` (L17) | function | Internal | Implementation helper for slice field. |
| `_timestamp_field(metric: dict) -> str` (L21) | function | Internal | Implementation helper for timestamp field. |
| `_parse_timestamps(df: pd.DataFrame, field: str) -> pd.Series` (L25) | function | Internal | Implementation helper for parse timestamps. |
| `_label_values(metric: dict, key: str) -> set[str]` (L31) | function | Internal | Implementation helper for label values. |
| `_observed_labels(series: pd.Series) -> list[str]` (L35) | function | Internal | Implementation helper for observed labels. |
| `compute_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L39) | function | Public | Computes label coverage ratio and returns a structured result. |
| `compute_per_slice_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L57) | function | Public | Computes per slice label coverage ratio and returns a structured result. |
| `_entropy_score(labels: list[str], expected_classes: list[str]) -> float` (L87) | function | Internal | Implementation helper for entropy score. |
| `compute_per_slice_label_entropy_score(df: pd.DataFrame, metric: dict) -> dict` (L108) | function | Public | Computes per slice label entropy score and returns a structured result. |
| `compute_class_imbalance_score(df: pd.DataFrame, metric: dict) -> dict` (L124) | function | Public | Computes class imbalance score and returns a structured result. |
| `_attack_windows(metric: dict) -> list[tuple[pd.Timestamp, pd.Timestamp]]` (L143) | function | Internal | Implementation helper for attack windows. |
| `_in_any_window(timestamp: pd.Timestamp, windows: list[tuple[pd.Timestamp, pd.Timestamp]]) -> bool` (L153) | function | Internal | Implementation helper for in any window. |
| `compute_attack_window_alignment_score(df: pd.DataFrame, metric: dict) -> dict` (L157) | function | Public | Computes attack window alignment score and returns a structured result. |
| `compute_pre_post_attack_label_bleed_ratio(df: pd.DataFrame, metric: dict) -> dict` (L176) | function | Public | Computes pre post attack label bleed ratio and returns a structured result. |
| `_split_masks(df: pd.DataFrame, metric: dict) -> tuple[pd.Series, pd.Series, str]` (L197) | function | Internal | Implementation helper for split masks. |
| `compute_train_test_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict` (L208) | function | Public | Computes train test duplicate overlap ratio and returns a structured result. |
| `compute_train_test_identifier_contamination_ratio(df: pd.DataFrame, metric: dict) -> dict` (L221) | function | Public | Computes train test identifier contamination ratio and returns a structured result. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/address_validity/reserved_ip_address_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `get_reserved_categories(addr) -> list[str]` (L35) | function | Public | Implementation helper for get reserved categories. |
| `_enabled_categories(categories: list[str], params: dict) -> list[str]` (L65) | function | Internal | Implementation helper for enabled categories. |
| `_diagnostic(status: str, *, invalid_count: int, invalid_ratio: float, reserved_count: int, checked_count: int, threshold: float, category_counts: dict, invalid_examples: list, reserved_examples: list) -> dict` (L73) | function | Internal | Implementation helper for diagnostic. |
| `run_reserved_ip_address_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L116) | function | Public | Runs reserved IP address metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/address_validity/valid_ip_address_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `classify_ip_value(ip_value) -> str` (L5) | function | Public | Classify an IP field value as one of: missing, ipv4, ipv6, invalid. |
| `run_protocol_validity_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L26) | function | Public | Runs protocol validity metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/derived_rate_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_within_tolerance(reported: pd.Series, expected: pd.Series, relative_tolerance: float, absolute_tolerance: float) -> pd.Series` (L18) | function | Internal | Implementation helper for within tolerance. |
| `run_derived_rate_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L28) | function | Public | Check whether reported packet/byte rates agree with counts, bytes, and duration. The duration unit must be declared in ``calculation.parameters.duration_unit``. At least one of ``flow_packets_per_second`` and ``flow_bytes_per_second`` must be mapped. This prevents the metric from silently assuming a dataset-specific duration unit or rate convention. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/flow_duration_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_flow_duration_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L7) | function | Public | Runs flow duration consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/handshake_plausibility_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_to_float(v)` (L7) | function | Internal | Implementation helper for to float. |
| `run_handshake_plausibility_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L20) | function | Public | Runs handshake plausibility metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/packet_byte_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_packet_byte_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L7) | function | Public | Runs packet byte consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/flow_semantics/tcp_flag_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `run_tcp_flag_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L7) | function | Public | Runs TCP flag consistency metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/port_validity/service_port_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `normalize_port_series(series)` (L18) | function | Public | Normalizes port series. |
| `parse_port(value)` (L36) | function | Public | Parses port. |
| `_not_applicable_result(*, service_name: str, expected_ports: list[int], match_mode: str, existing_fields: list[str], missing_fields: list[str], row_count: int, population_basis: str, reason_code: str, summary: str, suggestion: str, service_field: str | None = None, service_values: list[str] | None = None) -> tuple[bool, dict]` (L54) | function | Internal | Implementation helper for not applicable result. |
| `_diagnostic(status: str, *, service_name: str, checked: int, matching: int, mismatching: int, match_ratio: float, pass_threshold: float, warn_threshold: float, invalid_rows: int, population_rows: int, population_basis: str, expected_ports: list[int], mismatch_examples: list, invalid_examples: list) -> dict` (L97) | function | Internal | Implementation helper for diagnostic. |
| `run_service_port_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L154) | function | Public | Heuristically check expected ports only within an independently selected service population. A mixed flow dataset must not be treated as though every row belongs to the configured service. In ``auto`` mode the metric therefore requires a usable service/application field, unless ``assume_dataset_service`` is explicitly enabled. A plan can also use ``population_mode=all_rows`` when the dataset is known to contain only the named service. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/port_validity/valid_port_range_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `parse_port(value, valid_min_port: int = 0, valid_max_port: int = 65535)` (L6) | function | Public | Parses port. |
| `classify_port_range(port: int) -> str` (L28) | function | Public | Classifies port range. |
| `_diagnostic(status: str, *, checked: int, invalid: int, non_integer: int, out_of_range: int, zero_count: int, invalid_ratio: float | None, threshold: float, valid_min_port: int, valid_max_port: int, examples: list) -> dict` (L38) | function | Internal | Implementation helper for diagnostic. |
| `run_valid_port_range_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L87) | function | Public | Runs valid port range metric. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/slice_metadata_integrity/slice_identifier_consistency_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_norm(v, case_sensitive: bool)` (L6) | function | Internal | Implementation helper for norm. |
| `_rule_match(field_value, operator, target, case_sensitive)` (L15) | function | Internal | Implementation helper for rule match. |
| `run_slice_identifier_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L37) | function | Public | Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable. |

## `tests/metrics/dataset_heuristics/protocol_and_network_realism/slice_metadata_integrity/valid_slice_identifier_profile.py`

Network/protocol realism implementation module awaiting migration from the test package.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `normalise_slice_id(value, case_sensitive: bool, aliases: dict)` (L6) | function | Public | Normalizes slice id. |
| `run_valid_slice_identifier_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]` (L20) | function | Public | Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable. |

## `tests/reference_model_comparison_profile.py`

Reference-comparison metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_reference_path(metric: dict) -> str | None` (L16) | function | Internal | Implementation helper for reference path. |
| `_load_reference_df(metric: dict) -> pd.DataFrame` (L22) | function | Internal | Implementation helper for load reference dataframe. |
| `_candidate_fields(metric: dict) -> list[str]` (L35) | function | Internal | Implementation helper for candidate fields. |
| `_numeric_values(df: pd.DataFrame, field: str, max_sample_size: int) -> list[float]` (L39) | function | Internal | Implementation helper for numeric values. |
| `_feature_metric(df: pd.DataFrame, metric: dict, output_key: str, calculator) -> dict` (L46) | function | Internal | Implementation helper for feature metric. |
| `compute_feature_wise_wasserstein_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L69) | function | Public | Computes feature wise wasserstein distance from reference and returns a structured result. |
| `compute_feature_wise_ks_statistic_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L73) | function | Public | Computes feature wise KS statistic from reference and returns a structured result. |
| `compute_feature_wise_energy_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L77) | function | Public | Computes feature wise energy distance from reference and returns a structured result. |
| `compute_feature_set_mmd_score_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L81) | function | Public | Computes feature set MMD score from reference and returns a structured result. |
| `_matrix_deviation(current_matrix: dict, reference_matrix: dict) -> dict` (L93) | function | Internal | Implementation helper for matrix deviation. |
| `_correlation_profile(df: pd.DataFrame, fields: list[str], method: str) -> dict` (L105) | function | Internal | Implementation helper for correlation profile. |
| `compute_pearson_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L117) | function | Public | Computes pearson matrix deviation from reference and returns a structured result. |
| `compute_spearman_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L124) | function | Public | Computes spearman matrix deviation from reference and returns a structured result. |
| `compute_distance_correlation_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L131) | function | Public | Computes distance correlation matrix deviation from reference and returns a structured result. |
| `_timestamp_field(metric: dict) -> str` (L140) | function | Internal | Implementation helper for timestamp field. |
| `compute_inter_arrival_distribution_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L144) | function | Public | Computes inter arrival distribution divergence from reference and returns a structured result. |
| `compute_burstiness_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L152) | function | Public | Computes burstiness deviation from reference and returns a structured result. |
| `compute_hourly_activity_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L160) | function | Public | Computes hourly activity divergence from reference and returns a structured result. |
| `_slice_field(metric: dict) -> str` (L170) | function | Internal | Implementation helper for slice field. |
| `_label_field(metric: dict) -> str` (L174) | function | Internal | Implementation helper for label field. |
| `_categorical_distribution(df: pd.DataFrame, field: str) -> dict[str, float]` (L178) | function | Internal | Implementation helper for categorical distribution. |
| `_tv_distance(left: dict[str, float], right: dict[str, float]) -> float` (L186) | function | Internal | Implementation helper for tv distance. |
| `compute_slice_proportion_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L191) | function | Public | Computes slice proportion deviation from reference and returns a structured result. |
| `compute_per_slice_class_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L197) | function | Public | Computes per slice class divergence from reference and returns a structured result. |
| `compute_per_slice_feature_distribution_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L213) | function | Public | Computes per slice feature distribution deviation from reference and returns a structured result. |
| `compute_protocol_mix_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L234) | function | Public | Computes protocol mix divergence from reference and returns a structured result. |
| `compute_port_use_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L241) | function | Public | Computes port use divergence from reference and returns a structured result. |
| `compute_flow_statistic_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict` (L250) | function | Public | Computes flow statistic deviation from reference and returns a structured result. |

## `tests/slice_representation_profile.py`

Slice-representation metric implementations awaiting package migration.

| Symbol | Kind | Visibility | Purpose |
| --- | --- | --- | --- |
| `_normalise(value) -> str | None` (L4) | function | Internal | Implementation helper for normalise. |
| `_slice_field(metric: dict) -> str` (L11) | function | Internal | Implementation helper for slice field. |
| `_observed_slices(df: pd.DataFrame, slice_field: str) -> list[str]` (L15) | function | Internal | Implementation helper for observed slices. |
| `compute_per_slice_sample_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L21) | function | Public | Computes per slice sample coverage ratio and returns a structured result. |
| `compute_per_slice_feature_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L42) | function | Public | Computes per slice feature coverage ratio and returns a structured result. |
| `compute_per_slice_class_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L72) | function | Public | Computes per slice class coverage ratio and returns a structured result. |
| `compute_slice_distribution_imbalance_score(df: pd.DataFrame, metric: dict) -> dict` (L108) | function | Public | Computes slice distribution imbalance score and returns a structured result. |
| `compute_cross_slice_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict` (L131) | function | Public | Computes cross slice duplicate overlap ratio and returns a structured result. |
| `compute_cross_slice_identifier_leakage_ratio(df: pd.DataFrame, metric: dict) -> dict` (L154) | function | Public | Computes cross slice identifier leakage ratio and returns a structured result. |
