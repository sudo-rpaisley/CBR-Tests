# `run_plan.py` controls and feature reference

## Invocation forms

### Interactive terminal UI

```bash
python run_plan.py --tui
```

`--tui` opens the curses run-configuration interface. It can discover existing case and plan JSON files, browse for a dataset, configure execution and field-translation settings, perform a field-mapping dry run, and return to the setup menu after a run. An interactive stdin/stdout terminal is required.

A case does not need to be supplied on the command line in this mode: the TUI must select one before execution begins. Unless `--display` is explicitly supplied, TUI runs default to the `interactive` live dashboard.

The TUI is only a front end to the normal runner. Plan/schema validation, dataset applicability, output-path safety, skip policy, provenance capture and outcome writing use the same execution path as command-line runs.

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
| `--tui` | No | Open the curses setup/navigation UI. |
| `--case <path>` | CLI runs | Case JSON or plan JSON. In TUI mode it may be selected interactively instead. |
| `--dataset <path>` | Direct plan only | Dataset for a direct plan run. |
| `--output <path>` | Direct plan only | Outcome JSON for a direct plan run. |
| `--case-id <id>` | No | Direct-plan case ID; defaults to `ad_hoc_case`. |
| `--force-output` | No | Permit replacement of an existing outcome path. Protected experiment inputs still cannot be overwritten. |
| `--workers <n>` | No | Worker override. `1` is serial; larger values request parallel execution. |
| `--display <mode>` | No | `compact`, `full`, `quiet`, or `interactive`; default `compact` outside TUI. |
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

## TUI navigation

The setup screen is keyboard driven. Use the arrow keys to move, Enter to act on the selected field, Space to toggle Boolean fields, `e` to type a path/value, `r` to start the configured run, and `q`/Esc to leave the UI. The dataset field includes a file browser.

After a dry run, the results screen can return to setup, run immediately with the same settings, or open the field-mapping helper when unresolved required fields were reported. After a normal run, the results screen exposes expandable result groups and can return to the setup menu for another run.

`--tui` and `--display interactive` are deliberately different concepts:

- `--tui` is the setup, file-selection, mapping and post-run navigation interface;
- `--display interactive` is the live ANSI status dashboard during metric execution.

## Worker selection

Without `--workers`, the runner chooses at most the number of metrics and normally one fewer than available CPUs. A tabular run that uses a shared dataframe is capped to four workers. The final value is always at least one.

Parallel execution is bounded: only up to the worker count is submitted at once. Result records are restored to plan order.

## Display modes

| Mode | Behaviour |
| --- | --- |
| `compact` | Branch counts, active/attention metrics, recent completions, and events. Recommended for normal terminals and tmux. |
| `full` | Full taxonomy and every metric leaf. Useful for detailed debugging. |
| `quiet` | Suppresses live taxonomy/status redraws. Outcome and requested reports are still written. |
| `interactive` | Uses the ANSI interactive dashboard path when terminal capabilities permit. |

Non-TTY output uses plain lines rather than in-place redraws or colour-dependent meaning.

## Execution policy from the plan

`execution_policy.fail_fast` controls stopping after an **execution-level** metric failure.

### Serial

The first failed handler stops later metrics when fail-fast is enabled and the partial outcome is written.

### Parallel

With fail-fast enabled, the first completed execution failure:

- stops new submissions;
- attempts to cancel queued futures;
- records never-started/cancelled metrics explicitly;
- preserves results from metrics already completed;
- allows already-running threads to finish because they cannot be killed safely.

`execution_policy.allow_skips` is enforced after field preflight. If required mappings are missing and `allow_skips` is `false`, execution stops instead of silently dropping those metrics. Field-translation dry runs may still report the would-be skips without executing metrics.

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

## Sidecar behaviour

Default path:

```text
<dataset-stem>.field_translation.json
```

A generated sidecar lists canonical fields required by enabled metrics. Auto-detected columns may be filled in; unresolved values are empty strings. Existing user values are preserved, and the file is extended only for newly required fields.

Interactive runs prompt before creation/update. Non-interactive runs need `--yes-field-translation-sidecar`. `--no-update-field-translation` always prevents writing.

## Skipped metrics

A metric whose `field_requirements.required` fields remain unavailable is not dispatched. It receives a recorded skip only when the plan permits skips (or during a dry-run report). With `execution_policy.allow_skips: false`, unresolved required mappings fail preflight before metric execution.

Optional missing fields are reported without blocking execution.

## Recommended commands

### Interactive TUI

```bash
python run_plan.py --tui
```

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
