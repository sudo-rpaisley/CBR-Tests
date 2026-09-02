# Batch-aware Runner TUI

`python run_plan.py --tui` now opens a mode chooser instead of assuming every interactive run contains exactly one dataset.

## Single dataset run

Choose **Single dataset run** to use the existing case/plan runner. Its behaviour is unchanged.

## Batch / comparison run

Choose **Batch / comparison run** to build and execute a dataset batch from the same TUI entry point.

The batch screen provides:

- a batch name;
- multi-select candidate datasets;
- multi-select independent reference datasets;
- common-across-all-jobs or per-job metric policy;
- worker count and live display mode;
- dataset-summary controls;
- fail-fast control; and
- an explicit overwrite option for an existing generated batch.

The screen continuously shows the number of selected candidates, selected references and resulting jobs. When references are selected, jobs are the candidate × reference combinations after self-comparisons are removed.

## Multi-select file browser

Candidate and reference selectors use a file browser rather than requiring typed paths.

```text
Space       select/unselect a file
Enter       open a directory or select/unselect a file
d           finish selection
c           clear the current selection
Backspace   go to the parent directory
q / Esc     cancel without changing the field
```

Selected files are marked with `[x]`, and the footer shows the selection count.

## Execution

Press `r` on the batch setup screen to run the selected experiment. The TUI reuses the normal plan-builder and batch-runner pipeline rather than implementing separate metric execution.

That means batch TUI runs retain the existing behaviour for:

- per-dataset preflight;
- candidate × reference plan generation;
- exclusion of self-comparisons;
- common metric intersection by default;
- independent field-translation sidecars;
- sequential `run_batch.py` execution;
- per-job authoritative JSON outcomes;
- human-readable Markdown outcomes; and
- comparison overview, long-form and per-metric matrix CSV reports.

Batch mode automatically builds generated per-job plans from all structurally runnable tests. Use **Per-job runnable metrics** only when coverage is more important than strict cross-job comparability.
