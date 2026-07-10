# Plan schema reference

Plans describe which metrics to run and how those metrics should be grouped, configured, and validated.

## Top-level shape

```json
{
  "plan_meta": {
    "plan_id": "example-plan-v1",
    "name": "Example Plan",
    "version": "1.0.0",
    "description": "Human readable plan description."
  },
  "applicability": {
    "dataset_formats": ["csv"],
    "dataset_family": ["network_flow"],
    "requires_numeric_fields": true,
    "minimum_numeric_fields": 2
  },
  "execution_policy": {
    "fail_fast": true,
    "allow_skips": false,
    "sample_mode": "full"
  },
  "metrics": []
}
```

## Required fields

| Field | Description |
| --- | --- |
| `plan_meta` | Object containing plan metadata. |
| `plan_meta.plan_id` | Stable identifier written into outcome JSON. |
| `metrics` | Non-empty list of metric objects. |
| `metrics[].metric_id` | Metric identifier matched by the dispatcher. |
| `metrics[].taxonomy_path` | Non-empty list defining the metric's taxonomy position. |

## Common optional top-level fields

| Field | Description |
| --- | --- |
| `plan_meta.name` | Human-readable plan name shown in live output. |
| `plan_meta.version` | Plan version. |
| `plan_meta.description` | Longer plan description. |
| `applicability` | Dataset compatibility hints for humans/tools. |
| `execution_policy.fail_fast` | Stops at first failed metric when true. |
| `execution_policy.allow_skips` | Documents whether skipped metrics are expected/allowed. |
| `execution_policy.sample_mode` | Documents intended sampling behavior. |

## Metric object

```json
{
  "metric_id": "missing_value_ratio",
  "label": "Missing Value Ratio",
  "taxonomy_path": ["dataset_metrics", "quality", "missing_values"],
  "enabled": true,
  "input_requirements": {
    "candidate_fields": ["Duration", "Packet Length"],
    "minimum_runnable_fields": 1
  },
  "calculation": {
    "method": "missing_value_ratio",
    "parameters": {}
  },
  "field_requirements": {
    "required": [],
    "optional": ["Duration", "Packet Length"]
  },
  "retention": {
    "store_raw_output": true,
    "store_summary": true,
    "comparison_key": "missing_value_ratio_v1"
  }
}
```

## Metric fields

| Field | Required | Description |
| --- | --- | --- |
| `metric_id` | Yes | Identifier used to find a registered or tabular metric handler. |
| `label` | No | Human-readable metric label. |
| `taxonomy_path` | Yes | Non-empty taxonomy path used in live output and result taxonomy. |
| `enabled` | No | Defaults to enabled when omitted. Set false to skip before execution. |
| `input_requirements` | No | Metric-specific input configuration. These values are translated before dispatch. |
| `calculation.method` | Required when `calculation` exists | Calculation method identifier. |
| `calculation.parameters` | No | Method-specific parameters. |
| `field_requirements.required` | No | Canonical fields required before the metric can run. Missing fields skip only that metric. |
| `field_requirements.optional` | No | Canonical fields that enrich a metric but do not skip it when absent. |
| `retention` | No | Output retention/comparison hints. |

## Field translation interaction

- `input_requirements` are translated before metric handlers run.
- `field_requirements.required` drives sidecar templates and skipped-metric checks.
- `field_requirements.optional` appears in reports but does not cause skipping.
- Keep `input_requirements` and `field_requirements` aligned so generated sidecars list every field users may need.
