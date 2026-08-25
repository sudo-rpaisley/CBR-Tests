# CBR Tests documentation

CBR Tests is a plan-driven runner for assessing dataset quality, statistical structure, temporal behaviour, label and slice integrity, and network-flow realism. This documentation describes both the user workflow and the implementation as it exists in the repository.

## Start here

- [Getting started](getting_started.md) — install the project, run the included example, use plans and cases, translate fields, and read outcomes.
- [Metric reference](metric_reference.md) — all metric IDs currently supported by the dispatcher, their inputs, outputs, interpretation, and limitations.
- [Runner controls](run_plan_controls.md) — every command-line option, signal, display mode, and report output.
- [Troubleshooting](troubleshooting.md) — common failures and how to diagnose them.

## Configuration and output formats

- [Plan schema](plan_schema.md)
- [Case schema](case_schema.md)
- [Dataset inputs](dataset_inputs.md)
- [Outcome schema](outcome_schema.md)
- [Field translation workflows](field_translation_workflows.md)
- [Field translation sidecar schema](field_translation_sidecar_schema.md)
- [Field translation report schema](field_translation_report_schema.md)

## Internals and development

- [Architecture](architecture.md) — execution flow, components, concurrency, statuses, and package boundaries.
- [Development guide](development.md) — local setup, testing, CI, adding metrics, and keeping generated documentation current.
- [Adding metrics](adding_metrics.md)
- [Function and class reference](function_reference.md) — exhaustive generated reference for runtime code, metric implementations, compatibility modules, and scripts.
- [Test suite reference](test_reference.md) — every pytest case and test helper.
- [Run telemetry](run_telemetry.md)
- [Display modes](display_modes.md)

## Documentation guarantees

The function and test references are generated from the Python abstract syntax tree by `scripts/build_reference_documentation.py`. CI fails when those committed references no longer match the source. The curated guides explain semantics and design decisions that cannot be recovered reliably from names alone.
