# Case schema reference

Case files pin together a plan, dataset, output path, and optional field translation configuration.

## Basic shape

```json
{
  "case_id": "case_example_001",
  "dataset": {
    "path": "../datasets/example.csv",
    "format": "csv",
    "name": "Example Dataset"
  },
  "test_plan": {
    "path": "../plans/example_plan.json"
  },
  "output": {
    "path": "../outcomes/example_outcome.json"
  }
}
```

Paths in case files are resolved relative to the case file location.

## Top-level fields

| Field | Required | Description |
| --- | --- | --- |
| `case_id` | Recommended | Stable case identifier written to the outcome. |
| `dataset` | Yes | Dataset reference and metadata. |
| `test_plan` | Yes | Plan reference. |
| `output` | Yes | Outcome location. |

## Dataset fields

| Field | Required | Description |
| --- | --- | --- |
| `dataset.path` | Yes | Dataset path. Relative paths resolve from the case file. |
| `dataset.format` | No | Human/tool hint such as `csv`, `pcap`, `xlsx`, or `tsv`. |
| `dataset.name` | No | Human-readable dataset name. |
| `dataset.field_translation.path` | No | Explicit translation file path. Relative paths resolve from the case file. |

## Plan and output fields

| Field | Required | Description |
| --- | --- | --- |
| `test_plan.path` | Yes | Plan JSON path. |
| `output.path` | Yes | Outcome JSON path. |

## Explicit translation example

```json
{
  "case_id": "case_example_001",
  "dataset": {
    "path": "../datasets/example.csv",
    "field_translation": {
      "path": "../field_translations/example.field_translation.json"
    }
  },
  "test_plan": {
    "path": "../plans/example_plan.json"
  },
  "output": {
    "path": "../outcomes/example_outcome.json"
  }
}
```

If `dataset.field_translation.path` is omitted, the runner looks for the default sidecar next to the dataset.
