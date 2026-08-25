# Getting started

## What the runner does

A run combines three things:

1. a **plan**, which lists metric IDs and their configuration;
2. a **dataset**, such as CSV, TSV, Excel, PCAP, or PCAPNG;
3. an **outcome path**, where the runner writes one versioned JSON result.

A **case** is a convenience JSON file that references all three. You can either execute a case or execute a plan directly while supplying the dataset and output on the command line.

## Install

Python 3.11 and 3.12 are tested in CI.

```bash
git clone <repository-url>
cd CBR-Tests
python -m venv .venv
```

Activate the environment:

```bash
# Linux or macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install runtime dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For development and tests, install:

```bash
python -m pip install -r requirements-dev.txt
```

The root `start.sh` contains a machine-specific historical virtual-environment path and is not the portable installation method.

## Run the included self-contained example

The repository includes a small CSV, plan, and case under `examples/quickstart/`.

```bash
python run_plan.py \
  --case examples/quickstart/case.json \
  --workers 1 \
  --display quiet \
  --no-update-field-translation
```

The outcome is written to `outcomes/quickstart_example.json`. The example deliberately contains one duplicate row and one missing value so the output is easy to inspect.

## Run a case

```bash
python run_plan.py --case cases/case_deepsecure_drdos_dns_001.json
```

Paths inside a case are resolved relative to the case file. The datasets referenced by the supplied research cases are not stored in this repository; provide them at the configured paths or edit/copy the case for your environment.

A minimal case looks like:

```json
{
  "case_id": "example_case",
  "test_plan": {"path": "../plans/example_plan.json"},
  "dataset": {"path": "../datasets/example.csv"},
  "output": {"path": "../outcomes/example.json"}
}
```

## Run a plan directly

When `--case` points to a plan rather than a case, `--dataset` and `--output` are required.

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset /data/CICDDoS2019/DrDoS_DNS.csv \
  --output outcomes/deepsecure_dns.json \
  --case-id deepsecure_dns_local
```

Direct plan dataset and output paths are resolved from the current process directory.

## Choose serial or parallel execution

```bash
# Serial and easiest to debug
python run_plan.py --case examples/quickstart/case.json --workers 1

# Explicit parallel worker count
python run_plan.py --case examples/quickstart/case.json --workers 4
```

When `--workers` is omitted, the runner derives a value from CPU availability and metric count. Runs using a shared tabular dataframe are capped at four workers to reduce concurrent memory pressure.

## Choose a display mode

```bash
--display compact      # default, branch summaries and attention items
--display full         # every taxonomy path and metric
--display quiet        # no live redraws; suitable for CI and logs
--display interactive  # current ANSI dashboard path; falls back where needed
```

Outcome JSON is written regardless of display mode.

## Translate dataset fields

Plans use canonical names such as `Source IP`, `Destination Port`, or `timestamp`, while datasets may use `Src IP`, `tcp.dstport`, or another convention. The runner resolves field names at dispatch time; it does **not** rename or rewrite the dataframe.

The default sidecar name is:

```text
<dataset-stem>.field_translation.json
```

Example:

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

Validate mappings without running metrics:

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset /data/example.csv \
  --output outcomes/unused_dry_run.json \
  --field-translation-dry-run \
  --yes-field-translation-sidecar \
  --field-translation-markdown-report outcomes/field_report.md
```

Explicit `--field-translation <path>` mappings take precedence over auto-detected aliases and sidecar mappings. Missing required fields skip only the affected metric. Missing optional fields are reported but do not skip it.

## Pause or cancel a run

- `Ctrl-C` requests cancellation.
- On platforms exposing POSIX user signals, `SIGUSR1` pauses and `SIGUSR2` resumes.

For example on Linux:

```bash
kill -USR1 <pid>
kill -USR2 <pid>
```

Threads already executing a metric cannot be terminated safely. Parallel cancellation and fail-fast stop new submissions and mark unstarted work explicitly, while already-running metric calls may finish.

## Read the outcome

The top-level `status` describes **execution**, not whether every domain score passed its threshold. Important fields are:

- `schema_version`
- `status`
- `case_id` and `plan_id`
- `metric_results`
- `test_results`
- `plan_taxonomy`, `test_results_taxonomy`, and `result_taxonomy`
- start, finish, and elapsed timestamps
- optional `column_validations` and `skipped_metrics`

A metric handler can execute successfully and return a domain-level `status` such as `pass`, `warn`, `fail`, or `not_applicable` inside `test_results`. At present, a domain-level `fail` does not automatically make the top-level run `failed`; the handler must return an execution failure for that. Consumers should evaluate both layers.

## Recommended first workflow for a new dataset

1. Copy an existing plan close to your dataset family.
2. Set the dataset and outcome paths in a case.
3. Run a field-translation dry run and inspect the Markdown report.
4. Fill any required mappings.
5. Execute serially with `--workers 1 --display full` while developing.
6. Inspect `metric_results`, `test_results`, and domain statuses.
7. Use parallel execution only after the plan is stable and expensive metrics have suitable sample limits.

See [Runner controls](run_plan_controls.md), [Metric reference](metric_reference.md), and [Outcome schema](outcome_schema.md) for the complete reference.
