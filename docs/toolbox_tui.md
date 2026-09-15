# CBR-Tests Toolbox TUI

Launch the interactive toolbox with:

```bash
python run_plan.py --tui
```

The TUI is the main interactive front door to CBR-Tests. The underlying command-line tools remain available for scripts, automation and reproducible experiment records.

## Main toolbox

The landing screen groups tools by workflow rather than exposing every low-level option at once.

### Run experiments

- **Run one dataset** — configure and execute one prepared case or plan.
- **Run batch / comparison** — select several candidate datasets and optional references, build one comparison matrix and execute it.
- **Run comparison campaign** — execute an ordered queue of several independent batch/comparison matrices, with campaign-level checkpointing and resume/retry.

### Prepare experiments

- **Build plan / batch definition** — create a dataset-aware plan or one batch/comparison matrix using the current metric catalogue.
- **Build comparison campaign** — select previously prepared batch manifests, arrange them in execution order, and save the queue as one campaign manifest.
- **Validate / map dataset fields** — open the single-run workflow directly in field-translation preflight mode so missing canonical fields can be reviewed and mapped before metrics run.
- **Validate plan** — validate a saved plan against the live schema and metric registry.
- **Migrate legacy plan IDs** — migrate safe one-to-one legacy intrinsic metric IDs to canonical IDs.

### Review results

- **Compare outcomes** — compare two saved outcome JSON files.
- **Rerun and compare** — preserve a baseline outcome, rerun the same experiment and create a reproducibility comparison record.
- **Export graph / analysis tables** — flatten saved outcomes into CSV tables under `graph_data/`.

### Reference

- **Browse metric catalogue** — show every metric currently discoverable by plan creation.

### Maintenance

- **Check generated documentation** — verify generated function/test references.
- **Build documentation inventory** — regenerate the repository documentation inventory.

## Integrated plan builder

The plan builder no longer requires hand-editing JSON or dropping into a separate prompt-driven workflow.

The default screen shows only:

- plan name;
- candidate datasets;
- optional reference datasets;
- metric policy for multi-job batches.

Press `a` to expose advanced options:

- plan description;
- explicit field-translation JSON;
- include-only metric selection;
- metric exclusions;
- explicit single-service PCAP assertion and expected ports;
- output path;
- overwrite control.

Metric include/exclude selection uses a multi-select list. Leaving the include list empty means **all structurally runnable metrics** are considered.

The builder uses the same `build_plan` and batch-construction implementation as `create_plan.py`; the TUI does not maintain a separate scientific definition of plan eligibility.

## Comparison campaigns

A batch is one candidate/reference comparison matrix. A campaign is an ordered queue of those matrices.

The campaign builder shows the job count for every selected matrix and lets you add, remove and reorder matrices before saving. The same matrix may deliberately appear more than once; each queue position receives a separate identity and output directory.

When a campaign runs, each matrix is delegated to the existing `run_batch.py` implementation. This gives two levels of checkpointing:

- the campaign checkpoint records which matrix is active/completed;
- the batch checkpoint records which dataset/reference jobs inside that matrix are active/completed.

If a campaign is interrupted, **Run comparison campaign** detects the existing checkpoint and resumes it. If results need attention, the TUI offers either to continue pending matrices or retry failed/attention matrices.

Strict final-experiment mode is the default TUI choice. In strict mode the entire campaign queue is preflighted before Matrix 1 begins, so an invalid later plan cannot be discovered only after earlier matrices have already run.

See [Queued comparison campaigns](comparison_campaigns.md) for the full CLI, checkpoint and directory layout.

## Field mapping preflight

**Validate / map dataset fields** is a shortcut into the existing dry-run workflow. It starts with field validation enabled, reports missing required mappings without executing metrics, and then lets the normal results workflow open the field-mapping screen. This means field translation remains part of the same runner contract rather than becoming a second implementation.

## Review before action

Run and plan-builder screens show a readiness state before expensive work starts. Pressing the run/build shortcut first opens a review screen rather than immediately executing the operation.

## CLI equivalents remain supported

The TUI is optional. Existing commands such as the following remain valid:

```bash
python create_plan.py ...
python create_campaign.py ...
python run_plan.py ...
python run_batch.py ...
python run_campaign.py ...
python scripts/compare_outcomes.py ...
python scripts/migrate_plan_to_canonical_ids.py ...
python scripts/rerun_and_compare.py ...
python export_outcomes_for_graphs.py
```

This is intentional: the TUI improves interactive use while the CLI remains the stable automation and reproducibility interface.
