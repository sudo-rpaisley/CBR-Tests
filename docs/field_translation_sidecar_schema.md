# Field translation sidecar schema

Field translation sidecars use a single standard JSON shape. The mapping is written from canonical test/plan field name to supplied dataset column name.

## Required fields

```json
{
  "schema_version": 1,
  "test_to_dataset_fields": {
    "Source IP": "Src IP",
    "Destination IP": "Dst IP"
  }
}
```

- `schema_version`: currently `1`.
- `test_to_dataset_fields`: object whose keys are canonical test/plan fields and whose values are dataset column names.
- Empty string values are allowed in generated templates and mean the user still needs to fill in that mapping.

## Optional metadata

Generated sidecars may include metadata:

```json
{
  "schema_version": 1,
  "description": "Dataset field translation template. Fill empty values with dataset column names.",
  "dataset": "datasets/example.csv",
  "plan_id": "example-plan",
  "generated_at": "2026-07-09T00:00:00+00:00",
  "test_to_dataset_fields": {
    "Source IP": "Src IP"
  },
  "field_metadata": {
    "Source IP": {
      "required_by": ["valid_ip_address_profile"],
      "optional_for": [],
      "mapping_source": "auto_detected"
    }
  }
}
```

- `field_metadata` keys must also exist in `test_to_dataset_fields`.
- `required_by` lists metrics that require the canonical field.
- `optional_for` lists metrics that can use the field without requiring it.
- `mapping_source` describes whether the sidecar value was auto-detected or left as a template.

## Validation rules

- The payload must be a JSON object.
- `schema_version`, when present, must be `1`.
- `test_to_dataset_fields` is required and must be an object.
- Every `test_to_dataset_fields` key and value must be a string.
- Canonical field names cannot be blank.
- A dataset column cannot be mapped to multiple canonical fields.
- `field_metadata` must be an object when provided.
- `field_metadata` cannot contain fields missing from `test_to_dataset_fields`.
