# Adding metrics

## Checklist

1. Implement or register the metric handler.
2. Add the metric to the appropriate plan JSON.
3. Provide a stable `metric_id`.
4. Add a non-empty `taxonomy_path`.
5. Define `input_requirements` used by the metric handler.
6. Define `field_requirements.required` for fields that must exist.
7. Define `field_requirements.optional` for enrichment fields that should not skip the metric.
8. Add or update tests.
9. Update `docs/metric_catalog.md`.

## Field translation rules

- Metric handlers should read field names from translated `input_requirements`.
- Avoid hard-coded dataframe column names when the field may need translation.
- Required fields skip only the affected metric when missing.
- Optional fields should be handled gracefully by the metric.

## Tests to add

- Metric succeeds with expected fields present.
- Metric handles optional fields being absent.
- Metric is skipped when required fields are missing, when applicable.
- Plan schema validation still passes.
