# CBR Tests Runner

This repository provides a plan-driven metric runner for dataset quality and network-flow realism checks.

## What this does

- Loads either a **case JSON** or a direct **plan JSON**.
- Runs enabled metrics from that plan (serial or parallel).
- Streams a live taxonomy/progress view in TTY terminals.
- Writes a single JSON outcome file containing metric/test/taxonomy results.

Primary entrypoint:

- `run_plan.py`

For guided terminal use, launch the selector TUI:

```bash
python run_plan.py --tui
```

The selector groups run options by purpose, explains the highlighted field in a footer, and lets you choose common run flags, browse for the dataset, set paths, display mode, worker count, taxonomy controls, and field-translation report options before starting the run.

## Requirements

- Python 3.11+
- Runtime dependencies installed with `python -m pip install -r requirements.txt`.
- Test/development dependencies installed with `python -m pip install -r requirements-dev.txt`.

## Quick start

### 1) Run from a plan file

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset datasets/DeepSecure/CICDDoS2019/01-12/DrDoS_DNS.csv \
  --output outcomes/outcome_example.json
```

### 2) Run from a case file

If your case JSON already references the plan, dataset, and output, just pass `--case`.

```bash
python run_plan.py --case cases/example_case.json
```


## Documentation map

- `docs/run_plan_controls.md`: complete CLI/control/display/report reference.
- `docs/plan_schema.md`: plan JSON schema and metric object reference.
- `docs/case_schema.md`: case JSON schema and path resolution rules.
- `docs/outcome_schema.md`: outcome JSON structure.
- `docs/field_translation_sidecar_schema.md`: sidecar schema.
- `docs/field_translation_report_schema.md`: field translation report structure.
- `docs/field_translation_workflows.md`: common translation and display commands.
- `docs/dataset_inputs.md`: supported dataset/input formats and auto-detection notes.
- `docs/display_modes.md`: compact/full/quiet/interactive display behavior.
- `docs/run_telemetry.md`: shared telemetry model for live display and future reports.
- `docs/adding_metrics.md`: metric author checklist.
- `docs/metric_catalog.md`: high-level metric catalog.
- `docs/troubleshooting.md`: common problems and fixes.

## CLI options

`run_plan.py` supports:

- `--tui`: open a terminal selector so flags can be chosen instead of remembered.
- `--case` (required unless selected in `--tui`): path to case JSON **or** plan JSON.
- `--dataset`: required when `--case` points directly to a plan JSON.
- `--output`: required when `--case` points directly to a plan JSON.
- `--case-id`: optional override when running a plan directly. Default: `ad_hoc_case`.
- `--taxonomy-file`: optional taxonomy ordering file.
- `--taxonomy-strict`: fail when enabled metrics are missing from taxonomy order.
- `--workers`: optional worker override.
  - `1` forces serial mode.
  - `>1` uses parallel mode.
- `--display`: live terminal display mode. Use `compact` for tmux-friendly summaries, `full` for every taxonomy leaf, `quiet` to suppress live taxonomy redraws, or `interactive` for the full ANSI dashboard.
- `--field-translation`: optional translation JSON path. Overrides sidecar lookup.
- `--no-update-field-translation`: run without creating or updating sidecar translation templates.
- `--field-translation-dry-run`: validate translations/templates and report skipped metrics without running metrics.
- `--field-translation-report`: optional JSON report path for field availability and skipped metric details.
- `--field-translation-text-report`: optional text report path for a human-readable field translation summary.
- `--field-translation-markdown-report`: optional Markdown report path for field translation validation details.
- `--yes-field-translation-sidecar`: create/update sidecars without an interactive yes/no prompt.


## Dataset field translations

Datasets can use different column names for the same concepts. Add a small JSON
translation file per dataset so metrics can keep using the plan's canonical field
names. The runner resolves canonical test fields to supplied dataset columns at
metric dispatch time; it does not rename or rewrite the loaded dataset columns.
Translation files use one standard shape: `test_to_dataset_fields`. The key is
the canonical test/plan field, and the value is the supplied dataset column:

```json
{
  "schema_version": 1,
  "test_to_dataset_fields": {
    "Source IP": "Src IP",
    "Destination IP": "Dst IP",
    "Source Port": "Src Port",
    "Destination Port": "Dst Port"
  }
}
```

By default, the runner looks for a sidecar file next to the dataset named
`<dataset-stem>.field_translation.json`. If a sidecar needs to be created or
updated, interactive runs prompt for yes/no confirmation. Non-interactive runs
skip the update unless `--yes-field-translation-sidecar` is provided. The
generated sidecar uses `test_to_dataset_fields` so every canonical field
required by the enabled plan metrics is listed for users to fill in:

```json
{
  "test_to_dataset_fields": {
    "Source IP": "",
    "Destination IP": "",
    "timestamp": ""
  }
}
```

If a sidecar already exists, it is only updated when enabled metrics require a
field that is not already listed in the template. To run without creating or
updating sidecars, pass `--no-update-field-translation`.

Reference a non-sidecar translation file from a case with
`dataset.field_translation.path`:

```json
{
  "dataset": {
    "path": "../datasets/example.csv",
    "field_translation": {"path": "../field_translations/example.json"}
  }
}
```

When running a plan directly, pass `--field-translation <path>`. Use `--field-translation-dry-run` to create/update templates, report missing mappings, and exit before metrics run. Add `--field-translation-report <path>` to write the machine-readable validation report, `--field-translation-text-report <path>` to write a human-readable summary, and `--field-translation-markdown-report <path>` to write a Markdown report.

The runner also auto-detects common aliases and Wireshark/tshark packet headings in tabular exports, such as `Src IP`, `source_ip`, `frame.time_epoch`, `ip.src`, `ip.dst`, `tcp.srcport`, and `tcp.dstport`, and maps them to the canonical fields used by the tests. Generated sidecar templates are pre-populated with those detected headings when possible and include `schema_version`, `field_metadata`, and per-field `mapping_source` values so users can see what was auto-filled versus left as a template. Explicit translation files still take precedence over auto-detected headings. Raw `.pcap`/`.pcapng` inputs continue to be handled directly by packet-aware metrics.

Metrics can declare explicit field needs with `field_requirements`, using `required` and `optional` lists. Missing required fields cause that metric to be skipped with a warning while other runnable metrics continue. Skipped metrics are included in the outcome JSON and, when `--field-translation-report` is provided, in the report file. Example translation files live under `examples/field_translations/`; cases are not modified to point at examples by default. See `docs/run_plan_controls.md` for all runner controls, `docs/field_translation_workflows.md` for common translation commands, `docs/field_translation_metric_audit.md` for metric authoring guidance, and `docs/field_translation_sidecar_schema.md` for the sidecar schema reference.


### Live display modes

See `docs/run_plan_controls.md` for the complete control reference and `docs/run_telemetry.md` for the shared telemetry model.

Large plans can produce more live taxonomy rows than fit in a tmux pane. The default `--display compact` mode shows branch counts, active/attention metrics, and recent completions instead of every metric. Use `--display full` to restore the original full taxonomy list, `--display quiet` for minimal output, or `--display interactive` for the full ANSI dashboard. Use `python run_plan.py --tui` when you want to select run flags from a terminal menu before execution. The compact view is backed by shared run telemetry so branch counts, skipped metrics, attention items, and recent events can also feed future reports and interactive displays.

## Execution behavior

### Serial vs parallel

- Worker count is auto-selected unless overridden with `--workers`.
- For tabular datasets (`.csv/.tsv/.xlsx/.xls`), worker count is capped to reduce memory pressure.

### Fail-fast

- Plan execution policy controls fail-fast behavior.
- In fail-fast mode, execution stops at first failed metric and writes outcome immediately.

### Pause / resume / cancel

- `Ctrl-C` requests cancellation.
- `SIGUSR1` pauses execution.
- `SIGUSR2` resumes execution.

## Live output

In TTY terminals with color support, the runner displays:

- Header block (run title, paths, status)
- Taxonomy or compact branch summary with metric status
- Active/attention metrics in compact mode
- Recent completed metrics and recent telemetry events in compact mode
- Overall progress line

If output is non-TTY, it falls back to plain printed lines. Use `--display quiet` to suppress live taxonomy/status redraws.

## Outcome JSON

Each run writes one JSON outcome with:

- run status (`success` / `failed` / `cancelled`)
- case/plan identifiers
- metric execution results
- test results
- taxonomy views
- timestamps and elapsed time
- optional column validation details

## Common workflows

### Force serial run

```bash
python run_plan.py --case plans/deepsecure_plan.json --dataset <data> --output <out> --workers 1
```

### Keep plan metrics in taxonomy order

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset <data> \
  --output <out> \
  --taxonomy-file plans/taxonomy_order.json
```

### Enforce strict taxonomy coverage

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset <data> \
  --output <out> \
  --taxonomy-file plans/taxonomy_order.json \
  --taxonomy-strict
```

## Developer notes

Key modules:

- `run_plan.py`: main orchestration entrypoint.
- `runner/run_plan_helpers.py`: shared CLI/header/signal/outcome helpers.
- `runner/run_plan_serial.py`: serial execution flow.
- `runner/execution.py`: parallel execution + heartbeat rendering helpers.
- `runner/progress.py`: live rendering and progress bar formatting.

Tests:

```bash
pytest -q tests/test_run_plan_helpers.py
```

## Troubleshooting

- If output seems noisy in non-interactive environments, ensure TTY behavior matches your shell/session.
- If a run appears paused, send `SIGUSR2` to resume.
- If a metric fails early, check `execution_policy.fail_fast` in the plan.

## License

Internal/project-specific. Add your team’s license text here if needed.
