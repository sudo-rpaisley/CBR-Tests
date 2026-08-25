# Outcome JSON schema reference

Each run writes one outcome JSON file. It is the canonical machine-readable execution record.

## Top-level fields

| Field | Description |
| --- | --- |
| `schema_version` | Outcome schema version. Current value: `1`. |
| `status` | Overall **execution** status: `success`, `failed`, or `cancelled`. |
| `case_id` | Case identifier. |
| `plan_id` | Plan identifier. |
| `metric_ids` | Enabled metric IDs included in the plan/taxonomy view, including preflight-skipped metrics. |
| `dataset_path` | Dataset path used for the run. |
| `plan_taxonomy` | Taxonomy tree derived from enabled plan metrics. |
| `metric_results` | Metric execution records, including skipped and not-run records. |
| `test_results` | Metric-specific result payloads keyed by metric ID. |
| `test_results_taxonomy` | Taxonomy populated with test payloads. |
| `result_taxonomy` | Taxonomy populated with execution and result summaries. |
| `run_started_at` | UTC ISO timestamp when execution preparation began. |
| `run_finished_at` | UTC ISO timestamp when the outcome was assembled. |
| `run_elapsed_seconds` | Wall-clock elapsed time measured with a monotonic timer. |
| `column_validations` | Optional per-metric column validation diagnostics. |
| `skipped_metrics` | Optional preflight skips caused by missing required field mappings. |

## Metric execution records

Successful execution:

```json
{
  "metric_id": "missing_value_ratio",
  "status": "success",
  "started_at": "2026-08-03T10:00:00+00:00",
  "finished_at": "2026-08-03T10:00:00.050000+00:00",
  "elapsed_seconds": 0.05
}
```

Execution failure adds `error`. A handler may also supply `reason`.

Preflight skip:

```json
{
  "metric_id": "reserved_ip_address_profile",
  "status": "skipped",
  "reason": "missing_field_mappings",
  "missing_fields": ["Source IP", "Destination IP"]
}
```

Parallel fail-fast and cancellation may produce:

```json
{
  "metric_id": "later_metric",
  "status": "not_run_fail_fast",
  "reason": "previous_metric_failed",
  "elapsed_seconds": 0.0
}
```

or `not_run_cancelled` with reason `run_cancelled`.

## Execution status versus metric assessment

Top-level and `metric_results[].status` values describe whether code ran successfully. Many metric payloads under `test_results` also include a domain `status` such as `pass`, `warn`, `fail`, or `not_applicable`.

These layers are independent. A domain-level `fail` returned by a successful handler does not currently make the outcome top-level status `failed`. Automated consumers should apply their own policy to domain scores/statuses in addition to checking execution status.

## Test result shapes

Shapes are metric-specific. Common conventions include:

- `summary`: aggregate counts and primary score;
- `fields`: per-field results;
- `slices`: per-slice results;
- `pairs`: correlation/deviation pairs;
- `examples`: bounded diagnostic examples;
- `status`: domain assessment where the metric defines thresholds.

See [Metric reference](metric_reference.md) for the primary output and interpretation of every metric.

## Write guarantees

The destination parent directory is created automatically. JSON is written to a temporary file in the same directory, flushed and fsynced, then moved into place with `os.replace`. This prevents a partially written destination from replacing the previous complete outcome.
