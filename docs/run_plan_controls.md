# `run_plan.py` controls and feature reference

## Invocation forms

### Case file

```bash
python run_plan.py --case path/to/case.json
```

The case supplies the plan, dataset, output, case ID, and optional field-translation file. Relative paths are resolved from the case directory.

### Plan file

```bash
python run_plan.py \
  --case path/to/plan.json \
  --dataset path/to/data.csv \
  --output path/to/outcome.json \
  --case-id optional_identifier
```

`--dataset` and `--output` are mandatory when `--case` is a plan.

## Command-line options

| Option | Required | Description |
| --- | --- | --- |
| `--case <path>` | Always | Case JSON or plan JSON. |
| `--dataset <path>` | Direct plan only | Dataset for a direct plan run. |
| `--output <path>` | Direct plan only | Outcome JSON for a direct plan run. |
| `--case-id <id>` | No | Direct-plan case ID; defaults to `ad_hoc_case`. |
| `--workers <n>` | No | Worker override. `1` is serial; larger values request parallel execution. |
| `--display <mode>` | No | `compact`, `full`, `quiet`, or `interactive`; default `compact`. |
| `--taxonomy-file <path>` | No | External taxonomy/order JSON. |
| `--taxonomy-strict` | No | Fail if an enabled metric is absent from the external order. |
| `--field-translation <path>` | No | Explicit translation JSON; overrides case/sidecar lookup. |
| `--no-update-field-translation` | No | Prevent sidecar creation/update. Existing mappings may still load. |
| `--field-translation-dry-run` | No | Perform translation preflight/reporting and exit before metrics. |
| `--yes-field-translation-sidecar` | No | Permit sidecar writes without interactive confirmation. |
| `--field-translation-report <path>` | No | Machine-readable JSON mapping report. |
| `--field-translation-text-report <path>` | No | Human-readable text mapping report. |
| `--field-translation-markdown-report <path>` | No | Shareable Markdown mapping report. |

Use `python run_plan.py --help` as the executable source of truth.

## Worker selection

Without `--workers`, the runner chooses at most the number of metrics and normally one fewer than available CPUs. A tabular run that uses a shared dataframe is capped to four workers. The final value is always at least one.

Parallel execution is bounded: only up to the worker count is submitted at once. Result records are restored to plan order.

## Display modes

| Mode | Behavior |
| --- | --- |
| `compact` | Branch counts, active/attention metrics, recent completions, and events. Recommended for normal terminals and tmux. |
| `full` | Full taxonomy and every metric leaf. Useful for detailed debugging. |
| `quiet` | Suppresses live taxonomy/status redraws. Outcome and requested reports are still written. |
| `interactive` | Uses the ANSI interactive dashboard path when terminal capabilities permit; the implementation may fall back to compact behavior. |

Non-TTY output uses plain lines rather than in-place redraws or color-dependent meaning.

## Execution policy from the plan

`execution_policy.fail_fast` controls stopping after an **execution-level** metric failure.

### Serial

The first failed handler stops later metrics and the partial outcome is written.

### Parallel

The first completed failure:

- stops new submissions;
- attempts to cancel queued futures;
- records never-started/cancelled metrics as `not_run_fail_fast`;
- preserves results from metrics already completed;
- allows already-running threads to finish because they cannot be killed safely.

`execution_policy.allow_skips` is currently descriptive; required-field preflight skips are still recorded and runnable metrics continue.

`execution_policy.sample_mode` is also descriptive unless a metric explicitly reads it or its own calculation parameters.

## Signals and operator controls

| Control | Effect |
| --- | --- |
| `Ctrl-C` / `SIGINT` | Requests cancellation and marks shutdown state. |
| `SIGUSR1` | Requests pause on platforms that expose the signal. |
| `SIGUSR2` | Resumes after pause on platforms that expose the signal. |

Parallel cancellation stops new submissions and attempts to cancel queued work. Running threads may finish after the command has moved toward shutdown.

## Field translation order

The effective field resolver considers:

1. identity mappings for actual dataset columns;
2. known common aliases;
3. Wireshark/tshark-style headings;
4. sidecar or case translation;
5. explicit `--field-translation`, which takes precedence.

The dataframe is not renamed. Metric `input_requirements` are copied and translated before handler execution.

## Sidecar behavior

Default path:

```text
<dataset-stem>.field_translation.json
```

A generated sidecar lists canonical fields required by enabled metrics. Auto-detected columns may be filled in; unresolved values are empty strings. Existing user values are preserved, and the file is extended only for newly required fields.

Interactive runs prompt before creation/update. Noninteractive runs need `--yes-field-translation-sidecar`. `--no-update-field-translation` always prevents writing.

## Skipped metrics

A metric whose `field_requirements.required` fields remain unavailable is not dispatched. It receives:

- status `skipped`;
- reason `missing_field_mappings`;
- a `missing_fields` list.

Optional missing fields are reported without skipping. Skips appear in translation reports, `skipped_metrics`, and `metric_results`.

## Recommended commands

### Self-contained example

```bash
python run_plan.py \
  --case examples/quickstart/case.json \
  --workers 1 \
  --display quiet \
  --no-update-field-translation
```

### Translation dry run

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset /data/example.csv \
  --output outcomes/dry_run_placeholder.json \
  --field-translation-dry-run \
  --yes-field-translation-sidecar \
  --field-translation-report outcomes/fields.json \
  --field-translation-text-report outcomes/fields.txt \
  --field-translation-markdown-report outcomes/fields.md
```

### Quiet serial troubleshooting run

```bash
python run_plan.py \
  --case path/to/case.json \
  --workers 1 \
  --display quiet \
  --no-update-field-translation
```
