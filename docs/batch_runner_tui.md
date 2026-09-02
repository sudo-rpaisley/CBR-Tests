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
e           type/paste a path or directory
R           jump to the repository root
H           jump to the current user home directory
M           jump to /media when available
q / Esc     cancel without changing the field
```

Selected files are marked with `[x]`, and the footer shows the selection count. The browser only exposes supported dataset files (`.csv`, `.tsv`, `.xlsx`, `.xls`, `.pcap`, `.pcapng`) while still showing directories for navigation.

Each TUI batch also receives an automatically generated run ID such as `20260902-122045`. The human-readable batch name remains reusable, while the generated plan/batch ID combines the name and run ID so repeated experiments do not collide by default.

## Execution

Press `r` on the batch setup screen to run the selected experiment. The TUI reuses the normal plan-builder and batch-runner pipeline rather than implementing separate metric execution.

During execution, batch position remains visible above the current job's normal metric progress. For example:

```text
Batch: Reference experiment (reference-experiment)
Batch progress: [#######-----------------------]  25% (6/24 complete) | running job 7/24
Batch results so far: 5 successful | 1 needing attention | 17 waiting after current
Candidate: synthetic-b.csv | Reference: real-a.csv
```

The batch progress bar counts completed jobs, while `running job N/total` shows the current position. This avoids implying that a long-running job is complete before its outcome has actually been written. The same completed/total progress is printed between jobs and in the final batch summary, including compact/quiet runs where the live dashboard is not being redrawn.

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

## Checkpoints, resume and retry

`run_batch.py` writes an atomic `batch_state.json` checkpoint in the batch output directory before and after job execution. The checkpoint records the exact batch-manifest SHA-256 fingerprint, run timestamp, current job and completed results. A resume is rejected if the manifest has changed, preventing a partially completed experiment from silently continuing with different plans or datasets.

If a run is interrupted, resume it with:

```bash
python run_batch.py --batch plans/<batch>_batch.json --resume
```

Previously completed/attempted jobs are skipped. A job that was actively running when interruption occurred is restarted automatically as a new attempt with a new outcome filename such as `_retry02`, so a possibly partial first output is never overwritten.

To deliberately rerun jobs whose previous result needs attention, use:

```bash
python run_batch.py --batch plans/<batch>_batch.json --resume --retry-failed
```

Retry attempts retain attempt history in the batch checkpoint and write new outcome files rather than replacing the previous attempt.
