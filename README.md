# CBR Tests Runner

CBR Tests is a plan-driven Python runner for evaluating dataset quality and realism. It supports tabular and packet-capture inputs, serial or bounded parallel execution, canonical field translation, taxonomy-based reporting, and versioned JSON outcomes.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python run_plan.py \
  --case examples/quickstart/case.json \
  --workers 1 \
  --display quiet \
  --no-update-field-translation
```

This writes `outcomes/quickstart_example.json` using the repository’s self-contained sample dataset.

### Interactive terminal UI

For an interactive run setup, use:

```bash
python run_plan.py --tui
```

The curses TUI lets you select an existing case or plan, browse for a dataset, configure execution and field-translation options, run mapping dry-runs, and review results without having to remember the command-line flags. `--case` is not required when `--tui` is used. The outcome filename is auto-filled from the selected plan/case title with a date/time suffix and refreshes at run time unless you type a custom output path. The TUI feeds the selected values into the same hardened runner as the normal CLI, so plan validation, output safety, skip policy and provenance checks still apply.

`--tui` is the setup/navigation interface. `--display interactive` is the live ANSI dashboard shown while a configured run is executing.

## What it evaluates

The dispatcher currently supports 64 metric IDs across:

- completeness, duplicates, and column usability;
- Pearson, Spearman, distance correlation, and distribution drift;
- timestamp, duration, inter-arrival, diurnal, and periodicity behavior;
- label coverage, class balance, attack-window alignment, and split contamination;
- slice coverage, balance, duplicate overlap, and identifier leakage;
- reference-dataset comparisons;
- IP, port, protocol, TCP flag, handshake, flow, packet/byte, derived-rate, and slice metadata realism;
- benchmark accuracy, precision, recall, and F1.

Not every supported metric appears in the five supplied research plans. See the full [metric reference](docs/metric_reference.md).

## Run a case

```bash
python run_plan.py --case cases/case_deepsecure_drdos_dns_001.json
```

The datasets referenced by research cases are not committed to this repository. Paths inside cases are relative to the case file.

## Run a plan directly

```bash
python run_plan.py \
  --case plans/deepsecure_plan.json \
  --dataset /data/example.csv \
  --output outcomes/example.json \
  --case-id example_local
```

### Raw PCAP support

Automatic plans for `.pcap`/`.pcapng` include **21 existing metrics that are currently runnable from raw packet evidence without inventing research configuration**. This includes a capture-boundary-safe raw TCP handshake profile in addition to the existing protocol/timestamp, address/port, data-quality, dependency, distribution-drift and temporal checks.

The handshake profile evaluates only attempts whose opening SYN is actually observed. Mid-stream connections, SYN-only attempts, resets and missing handshake packets are evidence categories rather than automatic realism failures. An independent reference PCAP supplied with `--reference-dataset` can add 12 packet-level reference-comparison metrics, and an explicitly single-service capture can add service-port consistency with `--single-service` plus `--expected-service-ports`. These configured additions are deliberately kept separate from the 21 configuration-free automatic metrics so the reported count never implies that research assumptions were inferred automatically.

Metrics that still need labels, slices, attack windows, train/test information, benchmark configuration, or flow-segmentation/exporter semantics remain visible in preflight but are not inserted into the plan. Flow self-consistency checks are also excluded where both sides of the comparison would be calculated by CBR-Tests itself, because that would test the adapter rather than provide independent realism evidence.

## Documentation

Start with [the documentation index](docs/index.md).

- [Getting started](docs/getting_started.md)
- [Metric reference](docs/metric_reference.md)
- [Runner controls](docs/run_plan_controls.md)
- [Plan schema](docs/plan_schema.md)
- [Case schema](docs/case_schema.md)
- [Outcome schema](docs/outcome_schema.md)
- [Field translation workflows](docs/field_translation_workflows.md)
- [Architecture](docs/architecture.md)
- [Development guide](docs/development.md)
- [Function and class reference](docs/function_reference.md)
- [Test suite reference](docs/test_reference.md)
- [Troubleshooting](docs/troubleshooting.md)

The function and test references are generated from the source AST and checked in CI.

## Development validation

```bash
python -m pip install -r requirements-dev.txt
python -m compileall -q -x '(^|/)(\.git|\.venv|venv)/' .
python scripts/build_reference_documentation.py --check
python -m pytest -q
```

CI runs the complete suite on Python 3.11 and 3.12.

## Current architecture note

Foundational metrics have moved to `cbr_tests.metrics`. Legacy import modules remain as compatibility shims. Label, slice, reference-comparison, and nested network-realism implementations are still being migrated out of `tests/`; see [Architecture](docs/architecture.md) for the exact boundary.

## License

No project license has yet been declared. Add an appropriate license before public redistribution or external reuse.

## Automatic dataset-aware plan creation

Interactive plan creation asks for a plan name, derives the plan ID automatically, lets you browse for the dataset, and defaults the output to `plans/<plan-id>_plan.json`:

```bash
python create_plan.py
```

For scripted use, provide the plan name and dataset. For example:

```bash
python create_plan.py --name "My Plan" --dataset path/to/dataset.csv
```

This writes `plans/my-plan_plan.json`. The creator discovers metrics from the live dispatcher, applies field translations, excludes tests that need unresolved fields/configuration or an incompatible input type, and writes only runnable metrics. See [Creating plans](docs/plan_creation.md) for the full workflow.
