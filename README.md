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

## What it evaluates

The dispatcher currently supports 63 metric IDs across:

- completeness, duplicates, and column usability;
- Pearson, Spearman, distance correlation, and distribution drift;
- timestamp, duration, inter-arrival, diurnal, and periodicity behavior;
- label coverage, class balance, attack-window alignment, and split contamination;
- slice coverage, balance, duplicate overlap, and identifier leakage;
- reference-dataset comparisons;
- IP, port, protocol, TCP flag, handshake, flow, packet/byte, and slice metadata realism;
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

Generate a plan containing only tests that preflight as structurally runnable for a supplied dataset:

```bash
python create_plan.py --plan-id my-plan --dataset path/to/dataset.csv
```

The creator discovers metrics from the live dispatcher, applies field translations, excludes tests that need unresolved fields/configuration or an incompatible input type, and writes only runnable metrics. See [Creating plans](docs/plan_creation.md) for the full workflow.

