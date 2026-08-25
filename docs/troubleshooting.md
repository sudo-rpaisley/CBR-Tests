# Troubleshooting

## `unrecognized arguments`

Run `python run_plan.py --help`. You may be using an older branch or checkout. The current CLI includes case/plan, dataset/output, workers/display, taxonomy, and field-translation controls.

## Dataset path does not exist

The runner validates paths before loading. Case-relative paths are resolved from the case file directory; direct `--dataset` paths resolve from the current process. The research datasets referenced by repository cases are not included, so place them at the configured paths or edit a copied case.

## Taxonomy file does not exist or strict ordering fails

Check `--taxonomy-file`. With `--taxonomy-strict`, every enabled metric must appear in the external order file. Without strict mode, unlisted metrics are appended after ordered metrics.

## Sidecar was not created

Creation/update requires interactive confirmation or `--yes-field-translation-sidecar`. `--field-translation-dry-run` performs the workflow but noninteractive execution still needs the yes flag. `--no-update-field-translation` disables writes completely.

## Existing sidecar was not updated

Sidecars are extended only when enabled metrics require canonical fields not already listed. Existing user values are preserved. Use a dry run plus Markdown report to see unresolved fields.

## Metrics were skipped

A preflight skip means one or more `field_requirements.required` fields are unavailable after identity names, aliases, tshark detection, and explicit mappings are considered. Inspect the field report and `skipped_metrics` in the outcome.

## Dry-run passes but execution fails

Dry-run validates field availability, not data semantics. A field may exist but contain insufficient numeric values, invalid timestamps, malformed configuration, or unsupported content. Inspect `metric_results[].error`, `column_validations`, and the metric-specific result.

## The outcome says success but a metric says fail

This is expected under the current two-layer status model. Top-level success means every handler executed successfully. A metric’s `test_results.<metric>.status: "fail"` is a domain assessment and does not currently change the top-level execution status. Apply a reporting/policy rule to both layers.

## Parallel fail-fast still allowed some metrics to finish

The executor stops submitting new work after a failure and cancels queued work where possible. Python threads already running cannot be killed safely, so they are allowed to finish and their results are preserved. Metrics never started are marked `not_run_fail_fast`.

## Cancellation returned while CPU work continued briefly

Cancellation requests non-blocking shutdown and marks unfinished work. Already-running Python threads may complete in the background. Use serial execution for the simplest interruption behavior.

## Run is killed by the operating system

This commonly indicates memory pressure. Try:

```bash
--workers 1
```

Be particularly cautious with distance correlation and other pairwise metrics. Distance correlation constructs quadratic matrices; use smaller datasets until resource limits/sampling are added.

## Reference metric returns empty or `null`

Current reference-comparison code resolves `reference_dataset_path` from the process working directory. Missing, unreadable, or unsupported reference data can become an empty dataframe and yield empty/`null` results rather than a hard error. Verify the path and CSV/TSV/XLS/XLSX format manually. This behavior is scheduled for correction.

## Precision, recall, or F1 is `null`

These implementations are binary. Set `input_requirements.positive_label`. Without it, the code uses the lexicographically last class only when exactly two labels are observed. A zero denominator also yields `null`.

## Live output is too tall or corrupt in logs

Use `--display compact` for a small terminal or `--display quiet` for CI and redirected logs. `full` is designed for debugging with sufficient scrollback.

## Pause/resume signals do nothing on Windows

`SIGUSR1` and `SIGUSR2` are registered only when the Python platform exposes them. `Ctrl-C` cancellation remains available.

## Outcome file is missing after abrupt process termination

The writer is atomic once outcome assembly begins, but a hard kill can occur before any write. Check the configured destination directory and process logs. Temporary files use a hidden name beginning with the output filename and `.tmp`; the normal exception path removes them.

## Local test collection fails with missing dependencies

Install `requirements-dev.txt`, not only `pytest`. The runtime imports Scapy for packet metrics and pandas/openpyxl for tabular and Excel inputs.
