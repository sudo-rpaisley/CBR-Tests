# Field translation report schema reference

The field translation JSON report is requested with `--field-translation-report <path>`.

## Purpose

The report explains what fields are available, how mappings were resolved, which metrics can run, and which metrics were skipped because required mappings were missing.

## Common sections

| Field | Description |
| --- | --- |
| `dataset_path` | Dataset path inspected by the runner. |
| `translation_path` | Explicit or sidecar translation path, when one was used or created. |
| `sidecar_status` | Sidecar state such as `created`, `existing`, `updated`, `unchanged`, `suppressed`, `explicit`, or `none`. |
| `dataset_columns` | Raw dataset columns read from the tabular header. |
| `available_fields` | Canonical fields available after identity, explicit, and detected mappings. |
| `detected_translation` | Auto-detected dataset-to-canonical mappings. |
| `explicit_translation` | Explicit mappings loaded from sidecar or `--field-translation`. |
| `field_translation` | Final merged dataset-to-canonical mapping used by the resolver. |
| `field_usage` | Metrics requiring or optionally using each canonical field. |
| `missing_optional_fields` | Optional fields that were not available. |
| `skipped_metrics` | Metrics skipped because required mappings were missing. |
| `unused_dataset_columns` | Dataset columns not used by a mapping. |
| `suggestions` | Best-effort likely mappings for unmapped fields. |

Exact report keys may evolve as report details improve; keep this document updated when report payloads change.

## Human-readable reports

- `--field-translation-text-report` writes a text summary.
- `--field-translation-markdown-report` writes a Markdown summary.
- Human reports include labels such as `[RUNNABLE]` and `[SKIPPED]` for quick scanning.
