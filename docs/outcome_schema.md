# Outcome JSON schema reference

Each run writes one outcome JSON file. The outcome is the canonical machine-readable run result.

## Top-level fields

| Field | Description |
| --- | --- |
| `status` | Overall run status: `success`, `failed`, or `cancelled`. |
| `case_id` | Case identifier. |
| `plan_id` | Plan identifier. |
| `metric_ids` | Enabled metric IDs included in the plan/taxonomy view. |
| `dataset_path` | Dataset path used for the run. |
| `plan_taxonomy` | Taxonomy tree derived from enabled plan metrics. |
| `metric_results` | List of metric execution records, including skipped metrics when present. |
| `test_results` | Metric-specific result payloads keyed by metric/test IDs. |
| `test_results_taxonomy` | Taxonomy view populated with test result payloads. |
| `result_taxonomy` | Taxonomy view populated with metric status/result summaries. |
| `run_started_at` | ISO timestamp when the run started. |
| `run_finished_at` | ISO timestamp when the outcome was built. |
| `run_elapsed_seconds` | Total elapsed run time. |
| `column_validations` | Optional metric-specific column validation diagnostics. |
| `skipped_metrics` | Optional list of metrics skipped due to missing required mappings. |

## Metric result record

```json
{
  "metric_id": "missing_value_ratio",
  "status": "success",
  "started_at": "2026-07-10T00:00:00+00:00",
  "finished_at": "2026-07-10T00:00:01+00:00",
  "elapsed_seconds": 1.0
}
```

Failed metrics include an `error` field. Skipped metrics include a skip `reason` and `missing_fields`.

## Skipped metric record

```json
{
  "metric_id": "reserved_ip_address_profile",
  "status": "skipped",
  "reason": "missing_field_mappings",
  "missing_fields": ["Source IP", "Destination IP"]
}
```

Skipped metrics are included in both `skipped_metrics` and `metric_results` for visibility.
