# CBR Tests Runner

This repository provides a plan-driven metric runner for dataset quality and network-flow realism checks.

## What this does

- Loads either a **case JSON** or a direct **plan JSON**.
- Runs enabled metrics from that plan (serial or parallel).
- Streams a live taxonomy/progress view in TTY terminals.
- Writes a single JSON outcome file containing metric/test/taxonomy results.

Primary entrypoint:

- `run_plan.py`

## Requirements

- Python 3.11+
- Dependencies installed in your environment (for example via your project’s requirements/venv process).

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

## CLI options

`run_plan.py` supports:

- `--case` (required): path to case JSON **or** plan JSON.
- `--dataset`: required when `--case` points directly to a plan JSON.
- `--output`: required when `--case` points directly to a plan JSON.
- `--case-id`: optional override when running a plan directly. Default: `ad_hoc_case`.
- `--taxonomy-file`: optional taxonomy ordering file.
- `--taxonomy-strict`: fail when enabled metrics are missing from taxonomy order.
- `--workers`: optional worker override.
  - `1` forces serial mode.
  - `>1` uses parallel mode.

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
- Taxonomy tree with metric status
- Overall progress line

If output is non-TTY, it falls back to plain printed lines.

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
