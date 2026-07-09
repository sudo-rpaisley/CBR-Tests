# Field translation backlog

This document tracks follow-up fixes, hardening work, and feature ideas for dataset field translation. Items are grouped by priority and type so they can be reviewed and promoted into issues or implementation tasks.

## Near-term fixes and hardening

### Metric compatibility audit

- Audit every metric implementation for hard-coded canonical field names.
- Replace direct dataframe column references with values from `input_requirements` where possible.
- Ensure metrics that use default field names also declare those fields in `field_requirements`.
- Review registered protocol/network metrics that still contain built-in default candidate lists.
- Confirm metrics that load datasets directly still receive translated metric requirements.

### Field requirement review

- Review generated `field_requirements` in all plan files for correctness.
- Split each metric's requirements into truly required versus optional fields.
- Add optional fields for enrichment-only inputs so metrics are not skipped unnecessarily.
- Add tests for metrics where optional fields are absent.

### Skipped metric behavior

- Verify skipped metrics are visible in outcome JSON, result taxonomy, text reports, and JSON reports.
- Add tests that assert skipped metrics remain visible when some metrics still run.
- Ensure skipped metrics are clearly represented in serial and parallel execution paths.
- Consider whether skipped metrics should affect overall run status or only appear as warnings.

### Sidecar safety

- Add tests for interactive sidecar prompt accepted and declined paths.
- Add tests for `--yes-field-translation-sidecar` in non-interactive runs.
- Verify existing sidecars are not rewritten unless new fields are needed.
- Validate behavior when dataset directories are read-only.
- Validate behavior when the sidecar file contains invalid JSON.

### Example validation

- Validate example mappings against real dataset headers when datasets are available.
- Rename examples to `*.example.json` if they remain illustrative rather than verified.
- Add comments or README notes explaining that examples may require local adjustment.

## Feature improvements

### CLI and workflow UX

- Add a dedicated CLI mode or subcommand for field translation workflows, such as `fields init`, `fields validate`, and `fields report`.
- Add a concise console summary for normal runs, not only dry runs.
- Add an option to fail the run when any metric is skipped due to missing mappings.
- Add an option to initialize/update sidecars without requiring a full run plan execution path.

### Reports

- Include sidecar status in reports: created, updated, unchanged, skipped, or suppressed.
- Include missing optional fields separately from missing required fields.
- Include mapping source per field in reports: explicit, auto-detected alias, auto-detected PCAP heading, or blank template.
- Include dataset column names that were not used by any mapping.
- Include suggestions for likely mappings when no exact alias is found.
- Add CSV or Markdown report output in addition to JSON and text.

### Sidecar schema

- Keep sidecar schema documentation current as the schema evolves.
- Add schema validation for sidecar files.
- Add sidecar migrations if the schema changes.
- Add optional plan metadata such as `plan_id`, plan version, plan hash, generated timestamp, and generated metric IDs.
- Add field-level notes to explain why each field is needed.

### Alias and detection improvements

- Expand common aliases for network-flow datasets, CICFlowMeter exports, Kubernetes datasets, 5G slice datasets, and tshark/Wireshark exports.
- Add case-insensitive and punctuation-insensitive alias matching.
- Add confidence levels for auto-detected mappings.
- Detect ambiguous mappings and require user confirmation or leave them blank.
- Support user-provided alias packs.

### Resolver improvements

- Add a resolver object instead of passing raw translation dictionaries around.
- Make resolver behavior explicit for missing fields, optional fields, and identity mappings.
- Add resolver-level diagnostics used by reports and skipped-metric decisions.
- Consider moving all field access through a shared resolver API in metrics over time.

## Testing backlog

### Run-level integration tests

- Complete sidecar allows metric execution.
- Incomplete sidecar skips only affected metrics while runnable metrics execute.
- Skipped metrics are written to outcome JSON and metric results.
- `--field-translation-report` contains detected, explicit, missing, and skipped details.
- `--field-translation-text-report` contains human-readable `[SKIPPED]` and `[RUNNABLE]` labels.
- `--no-update-field-translation` prevents sidecar creation and update.
- `--yes-field-translation-sidecar` creates/updates in non-interactive mode.
- Interactive prompt accepts sidecar creation.
- Interactive prompt declines sidecar creation.

### Unit tests

- Sidecar schema validation rejects invalid shapes.
- Duplicate and ambiguous mappings are reported clearly.
- Optional fields do not cause metric skipping.
- Known aliases resolve common dataset headers.
- PCAP/tshark headings resolve without renaming dataframes.
- Reports include all required metadata.

### Regression tests

- Existing cases remain unchanged unless explicitly edited by maintainers.
- Existing plans preserve taxonomy ordering after skipped metrics are added to outcomes.
- Full test suite remains green after baseline updates.

## Documentation backlog

- Add a quick-start guide for translation sidecars.
- Add a guide for metric authors explaining `field_requirements`.
- Add a troubleshooting section for skipped metrics.
- Add examples for common dataset families.
- Add an explanation of raw PCAP versus PCAP CSV export behavior.
- Add a sidecar schema reference.
- Add a report schema reference.

## Product decisions to revisit

- Should normal runs prompt by default, or should sidecar creation always require an explicit init/dry-run mode?
- Should skipped metrics make a run `success`, `warning`, or `failed`?
- Should examples remain in the repo, move to a separate examples package, or be generated from validated datasets?
- Should translation reports be written by default in CI workflows?
- Should field translation become its own command rather than flags on `run_plan.py`?
