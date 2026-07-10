# Run telemetry design

The runner keeps a shared telemetry model for live display and future reports.

## Why telemetry exists

Long runs need several views of the same facts:

- compact live terminal view,
- future interactive/Textual view,
- outcome JSON,
- field translation reports,
- future event logs or Markdown run reports.

Telemetry keeps metric state and events in one place so these views do not diverge.

## Core objects

`runner.telemetry` provides:

- `RunState`: run-level state, including case/plan IDs, paths, metric states, events, and warnings.
- `MetricState`: per-metric status, taxonomy path, timing, errors, and missing fields.
- `RunEvent`: structured timestamped event with event type, message, optional metric ID, and payload.

## Metric statuses

Current metric statuses are:

| Status | Meaning |
| --- | --- |
| `pending` | Metric has not started. |
| `running` | Metric is currently executing. |
| `success` | Metric completed successfully. |
| `failed` | Metric completed with an error/failure. |
| `skipped` | Metric was not run because required field mappings were missing. |
| `cancelled` | Metric/run was cancelled. |
| `stopping` | Cancellation is in progress for pending work. |

## Event examples

Events are stored in memory today and are designed to be serializable later:

```json
{"event_type": "run_initialized", "message": "Initialized run for deepsecure_plan"}
{"event_type": "metric_started", "metric_id": "missing_value_ratio"}
{"event_type": "metric_completed", "metric_id": "missing_value_ratio", "payload": {"elapsed_seconds": 0.91}}
{"event_type": "metric_skipped", "metric_id": "reserved_ip_address_profile", "payload": {"missing_fields": ["Source IP"]}}
```

## Derived summaries

`RunState` derives:

- status counts,
- taxonomy branch summaries,
- completed statuses,
- completed durations,
- recent completed metrics,
- active/attention metrics.

The compact live view consumes these summaries when telemetry is available.

## Future extensions

The telemetry model is intended to support:

- `--event-log` JSONL output,
- whole-run Markdown reports,
- report bundles,
- full Textual expand/collapse dashboard,
- better warnings and operator controls.
