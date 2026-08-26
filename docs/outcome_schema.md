# Outcome JSON schema reference

Each completed run writes two companion files:

- the outcome JSON, which is the canonical machine-readable execution record;
- a human-readable Markdown summary with the same base filename and suffix `.summary.md`.

For example, `outcomes/experiment.json` is accompanied by `outcomes/experiment.summary.md`.

The Markdown file is derived from the JSON outcome. It is intended for rapid review and interpretation; the JSON remains authoritative for reproducibility, downstream analysis, and research claims.

## Top-level fields

| Field | Description |
| --- | --- |
| `schema_version` | Outcome schema version. Current value: `2`. |
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
| `run_id` | Unique run identifier when provenance capture is enabled. |
| `provenance` | Reproducibility manifest containing dataset/plan hashes, code revision, dependency information, field translations and reference datasets. |
| `column_validations` | Optional per-metric column validation diagnostics. |
| `skipped_metrics` | Optional preflight skips caused by missing required field mappings. |

## Human-readable companion summary

The `.summary.md` file is generated automatically whenever the JSON outcome is written, including serial, parallel and fail-fast completion paths. It contains:

- an at-a-glance execution and domain-result count table;
- a plain-English interpretation that keeps execution failure separate from metric-domain `fail` results;
- a focused section for execution errors, skipped metrics, domain failures and warnings;
- structured reason codes, bounded evidence and suggested actions where metrics provide them;
- a compact table covering every metric result;
- dataset, plan and code reproducibility identifiers when provenance is available.

The summary deliberately **does not calculate an aggregate realism score or invent a combined scientific pass/fail verdict**. Metrics that do not expose a domain `pass`, `warn`, `fail` or `not_applicable` status are shown as informational rather than silently classified.

This distinction is important because a runner may complete successfully while one or more metrics legitimately identify unrealistic or suspicious dataset properties.

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

When a metric defines a domain assessment, the execution record may additionally contain `result_status`:

```json
{
  "metric_id": "valid_port_range_profile",
  "status": "success",
  "result_status": "fail",
  "diagnostic": {
    "reason_code": "invalid_port_values",
    "summary": "Some transport ports are outside the valid range."
  }
}
```

Execution failure adds `error`. A handler may also supply `reason`, `reason_code`, and a structured `diagnostic`.

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

Top-level and `metric_results[].status` values describe whether code ran successfully. Many metric payloads under `test_results` also include a domain `status` such as `pass`, `warn`, `fail`, or `not_applicable`; normalized domain status is also copied to `metric_results[].result_status` where available.

These layers are independent. A domain-level `fail` returned by a successful handler does not make the outcome top-level status `failed`. Automated consumers should apply their explicit research policy to domain scores/statuses in addition to checking execution status.

The human-readable companion reinforces this separation rather than collapsing both layers into a single verdict.

## Test result shapes

Shapes are metric-specific. Common conventions include:

- `summary`: aggregate counts and primary score;
- `fields`: per-field results;
- `slices`: per-slice results;
- `pairs`: correlation/deviation pairs;
- `examples`: bounded diagnostic examples;
- `status`: domain assessment where the metric defines thresholds;
- `diagnostic`: structured reason code, interpretation, evidence and suggested action where available.

See [Metric reference](metric_reference.md) for the primary output and interpretation of every metric.

## Write guarantees

The destination parent directory is created automatically. The JSON and Markdown summary are both written to temporary files in the destination directory, flushed and fsynced before publication. The summary is moved into place first and the authoritative JSON second, so publication of a new JSON outcome implies that its companion summary has already been published.

The files are named deterministically from the requested output path. If the JSON output is `outcomes/example.json`, the summary is `outcomes/example.summary.md`.
