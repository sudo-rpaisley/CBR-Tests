# Development guide

## Environment

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

## Standard validation

```bash
python -m compileall -q -x '(^|/)(\.git|\.venv|venv)/' .
python scripts/build_reference_documentation.py --check
python -m pytest -q
```

Run the working example as a command-level smoke test:

```bash
python run_plan.py \
  --case examples/quickstart/case.json \
  --workers 1 \
  --display quiet \
  --no-update-field-translation
```

Remove the generated `outcomes/quickstart_example.json` if you do not want to keep local run output.

## CI

GitHub Actions tests Python 3.11 and 3.12. It compiles all Python sources, checks generated documentation, runs the quickstart smoke test on Python 3.11, and executes the full pytest suite.

## Repository layout

```text
cbr_tests/metrics/        production metric implementations
runner/                   orchestration, execution, translation, taxonomy, display
plans/                    reusable metric plans
cases/                    dataset/plan/output bindings
examples/                 portable examples and translation files
tests/test_*.py           pytest suite
tests/*_profile.py        temporary compatibility or not-yet-migrated metric code
docs/                     curated and generated documentation
scripts/                  documentation and maintenance utilities
```

## Adding a metric

1. Implement the calculation under `cbr_tests.metrics`.
2. Choose a stable `metric_id`.
3. Register it through the current dispatcher mechanism.
4. Define translated `input_requirements` rather than hard-coding dataset-specific names.
5. Add `field_requirements.required` and `.optional` to plan entries.
6. Return a consistent structured result.
7. Add focused unit tests and, where appropriate, dispatch or command-level tests.
8. Add the metric to [Metric reference](metric_reference.md).
9. Regenerate the function and test references.
10. Run the standard validation commands.

See [Adding metrics](adding_metrics.md) for the checklist and field-translation rules.

## Result design

Prefer a handler return of:

```python
return True, {
    "test_results": {
        metric_id: result
    }
}
```

Use `False` only for execution-level inability to perform the metric, such as a missing mandatory input, unreadable dataset, or calculation exception. Domain assessment belongs in the result payload with an explicit status or score.

Document whether a higher score is better or worse, any thresholds, how missing values are treated, and whether the result compares against a reference or only against another portion of the same dataset.

## Field translation

Metric code receives translated `input_requirements`. Do not rename the shared dataframe. When a metric uses a nested `field_map`, all values are translated recursively.

Use required fields only when the metric cannot make a meaningful calculation without them. Use optional fields for enrichment or alternate paths. Test both missing-required and missing-optional behavior.

## Adding a plan or case

Plans must pass `validate_plan_schema`:

- non-empty `plan_meta.plan_id`;
- non-empty metrics list;
- unique non-empty metric IDs;
- non-empty taxonomy paths;
- correctly typed execution policy;
- no overlap between required and optional fields.

Case paths are relative to the case file. Do not assume a contributor has the same absolute dataset paths.

## Generated documentation

Regenerate exhaustive references after adding, deleting, renaming, or changing a function/test signature:

```bash
python scripts/build_reference_documentation.py
```

Verify them without rewriting:

```bash
python scripts/build_reference_documentation.py --check
```

The generator includes nested functions. “Public” in the generated reference means only that the leaf name lacks a leading underscore; it is not a semantic-versioning promise. Nested functions are always marked internal.

The broader inventory utility can be run when auditing documentation coverage:

```bash
python scripts/build_documentation_inventory.py \
  --json documentation-inventory.json \
  --markdown documentation-inventory.md
```

Those inventory outputs are audit artifacts and are not intended to be committed routinely.

## Refactoring rules

During the package migration:

- preserve metric IDs and outcome shapes;
- move implementation before removing compatibility imports;
- make old modules thin re-exports rather than duplicate copies;
- add boundary tests proving old and new imports resolve to the same function;
- do not combine structural moves with scientific-semantic changes in the same commit or PR;
- use a squash merge when connector-based file writes produce many mechanical commits.

## Coding concerns worth addressing next

- move label, slice, reference, and network-realism implementations out of `tests/`;
- modularize the central metric registry;
- consolidate duplicate field-translation reporting functions;
- separate execution from terminal rendering;
- introduce typed metric-result objects;
- add deterministic/resource-bounded sampling;
- add linting, formatting, type checking, coverage, and dependency locking.
