# Adding metrics

## Checklist

1. Implement the metric in `cbr_tests.metrics`; do not add new production implementations under `tests/`.
2. Choose a stable and descriptive `metric_id`.
3. Register the metric or add it to the tabular compute map in `runner/dispatch.py` until modular definitions replace the central registry.
4. Add the metric to an appropriate plan or create a focused example plan.
5. Add a non-empty `taxonomy_path`.
6. Define `input_requirements` consumed by the implementation.
7. Define `field_requirements.required` only for fields without which the metric cannot run.
8. Define `field_requirements.optional` for useful but nonessential fields.
9. Return a structured result and distinguish execution failure from a domain-level fail/warn result.
10. Add unit tests, missing-input tests, and dispatch/integration tests where appropriate.
11. Add the metric to `docs/metric_reference.md` with inputs, output direction, thresholds, cost, and caveats.
12. Run `python scripts/build_reference_documentation.py` and commit the generated references.
13. Run compilation, documentation check, quickstart smoke test, and the full test suite.

## Field translation rules

- Metric handlers receive translated values in `input_requirements`.
- Avoid hard-coded dataset-specific dataframe column names.
- Nested structures such as `field_map` are translated recursively.
- Do not rename or mutate shared dataframe columns globally.
- Missing required fields skip only the affected metric during preflight.
- Missing optional fields must be handled gracefully and are reported separately.

## Handler contract

Typical compute functions accept:

```python
compute_metric(df: pandas.DataFrame, metric: dict) -> dict
```

The dispatcher wraps them into the runner contract. Registered handlers generally use:

```python
run_metric(dataset_path: pathlib.Path, metric: dict) -> tuple[bool, dict]
```

Return `False` for an execution-level inability to calculate, not merely because the measured data is poor. Put pass/warn/fail assessment in the successful payload.

## Statistical documentation requirements

For statistical metrics, state explicitly:

- what the two populations are;
- whether data order affects the result;
- sampling method and seed;
- sample limit and algorithmic complexity;
- scaling or normalization;
- missing-value handling;
- score range and direction;
- threshold source, if any;
- whether the output is a descriptive statistic, hypothesis-test statistic, distance, similarity, or normalized quality score.

This prevents an internal drift check from being presented as external fidelity and makes results reproducible.

## Tests to add

At minimum:

- expected output on a small deterministic input;
- missing mandatory columns;
- nonnumeric or unparseable values where relevant;
- empty or insufficient sample behavior;
- optional fields absent;
- threshold boundary behavior if the metric emits domain statuses;
- translation of canonical fields to differently named dataset columns;
- outcome/registry presence where the dispatch path is new.

## Compatibility during module moves

When relocating an existing implementation, leave a thin import shim at the old path until downstream callers migrate. Add a boundary test asserting that the old and new imports are the same function object. Do not keep two editable implementation copies.
