# run_plan controls and feature reference

This document lists the runner controls, side effects, reports, and display modes in one place.

## Required inputs

| Option | Required | Description |
| --- | --- | --- |
| `--case <path>` | Yes | Case JSON or plan JSON to execute. |
| `--dataset <path>` | When `--case` is a plan | Dataset path for ad-hoc plan runs. Case files can provide this instead. |
| `--output <path>` | When `--case` is a plan | Outcome JSON path for ad-hoc plan runs. Case files can provide this instead. |
| `--case-id <id>` | No | Case identifier for ad-hoc plan runs. Defaults to `ad_hoc_case`. |

## Execution controls

| Option / control | Behavior |
| --- | --- |
| `--workers <n>` | Overrides worker count. `1` forces serial execution; values above `1` run metrics in parallel. Tabular datasets are capped to a lower worker count to reduce memory pressure. |
| `--taxonomy-file <path>` | Loads an external taxonomy ordering file and orders enabled metrics to match it. |
| `--taxonomy-strict` | Fails if enabled metrics are missing from the taxonomy order. |
| `Ctrl-C` | Requests cancellation. The current implementation cancels pending work and writes what it can before exiting. |
| `SIGUSR1` | Pauses execution. |
| `SIGUSR2` | Resumes execution after `SIGUSR1`. |

## Live display modes

Use `--display <mode>` to control terminal output.

| Mode | Use when | Behavior |
| --- | --- | --- |
| `compact` | Default and recommended for tmux panes | Shows branch summaries, active/attention metrics, recent completions, and recent events instead of every metric leaf. |
| `full` | Debugging or large scrollback logs | Shows the full taxonomy/metric list. |
| `quiet` | CI, scripts, or logs where live redraws are noise | Suppresses live taxonomy/status redraws. Outcome JSON is still written. |
| `interactive` | Future Textual workflow | Currently uses the compact fallback while the full expand/collapse Textual UI is developed. |

The compact view is backed by shared run telemetry. That means skipped metrics, missing fields, recent events, and branch counts are tracked centrally and can be reused by future reports and the interactive UI.

## Field translation controls

| Option | Behavior |
| --- | --- |
| `--field-translation <path>` | Uses an explicit translation JSON and overrides sidecar lookup. |
| `--field-translation-dry-run` | Validates/creates/updates translation sidecars and reports missing mappings, then exits before running metrics. |
| `--no-update-field-translation` | Prevents sidecar creation or update. Existing mappings may still be loaded. |
| `--yes-field-translation-sidecar` | Allows sidecar creation/update without an interactive prompt. Useful for dry-runs and non-interactive shells. |
| `--field-translation-report <path>` | Writes a machine-readable JSON field translation report. |
| `--field-translation-text-report <path>` | Writes a human-readable text field translation report. |
| `--field-translation-markdown-report <path>` | Writes a Markdown field translation report. |

## Sidecar behavior

- Default sidecar path: `<dataset-stem>.field_translation.json` next to the dataset.
- Existing sidecars are loaded automatically when `--field-translation` is not supplied.
- Sidecars are only created if missing and allowed by prompt/flags.
- Existing sidecars are only updated when enabled metrics require fields that are not already listed.
- `--no-update-field-translation` disables sidecar create/update behavior.
- Generated templates list every canonical field needed by enabled metric `field_requirements` so users know what to fill in.
- Auto-detected aliases and PCAP/tshark headings are pre-populated when possible.

## Skipped metric behavior

- Metrics with missing required field mappings are marked `[SKIPPED]` and not executed.
- Other runnable metrics continue to run.
- Skipped metrics are included in the outcome JSON and field translation reports.
- Missing optional fields do not skip a metric; they are reported separately.

## Reports and outputs

| Output | How to request | Purpose |
| --- | --- | --- |
| Outcome JSON | `--output <path>` or case file output | Canonical run result containing metric results, test results, taxonomies, timestamps, skipped metrics, and validations. |
| Field translation JSON report | `--field-translation-report <path>` | Machine-readable mapping/availability/skipped-metric report. |
| Field translation text report | `--field-translation-text-report <path>` | Human-readable mapping/availability/skipped-metric report. |
| Field translation Markdown report | `--field-translation-markdown-report <path>` | Shareable Markdown mapping/availability/skipped-metric report. |

## Recommended commands

### Dry-run field translations and reports

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --field-translation-dry-run \
  --yes-field-translation-sidecar \
  --field-translation-report outcomes/deepsecure_fields.json \
  --field-translation-text-report outcomes/deepsecure_fields.txt \
  --field-translation-markdown-report outcomes/deepsecure_fields.md
```

### Run metrics with tmux-friendly output

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --display compact
```

### Run metrics with full taxonomy output

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --display full
```

### Run metrics quietly in CI

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/deepsecure_trial.json \
  --display quiet \
  --no-update-field-translation
```
