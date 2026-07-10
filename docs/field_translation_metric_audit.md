# Field translation metric audit

Field translation is resolver-based: metric `input_requirements` are translated before metric handlers run, and loaded dataset columns are preserved. Metrics should therefore read field names from `input_requirements` or explicit `field_requirements` instead of hard-coding dataset column names.

Audit checklist for new or changed metrics:

1. Declare `field_requirements.required` for fields that must exist before the metric can run.
2. Declare `field_requirements.optional` for fields that enrich results but should not skip the metric.
3. Read columns through `input_requirements` values that can be resolved by `translate_metric_fields`.
4. Avoid direct dataframe references to canonical names such as `df["Source IP"]` unless that canonical field is also guaranteed to be present in the supplied dataset.
5. If a metric has default field names, mirror those defaults in `field_requirements` so sidecar templates stay complete.

Known follow-up: perform a metric-by-metric review of registered protocol/network metrics that still contain built-in default candidate lists.
