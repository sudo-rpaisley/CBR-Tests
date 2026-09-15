# Queued comparison campaigns

A normal CBR-Tests batch represents one comparison matrix: one or more candidate datasets crossed with zero or more reference datasets. A **comparison campaign** is the level above that batch. It queues several independent matrix manifests and runs them in a defined order.

This is useful when an experiment contains separate dataset families or stages that should not be flattened into one giant candidate/reference matrix.

## Example structure

A campaign can represent:

```text
Campaign: Final realism experiments

1. Baseline datasets
   4 candidates × 3 references = 12 jobs

2. Perturbation family A
   8 candidates × 2 references = 16 jobs

3. Perturbation family B
   6 candidates × 2 references = 12 jobs

4. Synthetic generator comparison
   10 candidates × 3 references = 30 jobs
```

The runner completes matrix 1 before starting matrix 2, and so on. Each matrix is still executed by `run_batch.py`, so its existing plan validation, strict experiment mode, per-job provenance, outcome files, comparison reports and batch checkpoint are unchanged.

## Toolbox workflow

Launch:

```bash
python run_plan.py --tui
```

Use **Build plan / batch definition** to create each individual matrix. Then use **Build comparison campaign** to add those saved batch manifests to an ordered queue.

The campaign builder supports:

- adding batch/comparison manifests;
- removing queue entries;
- moving matrices up or down;
- showing the job count for each matrix;
- reviewing the total matrix and job count before saving;
- deliberately queueing the same batch more than once when the experiment requires it.

Use **Run comparison campaign** to execute the saved queue. Strict final-experiment mode is the default interactive choice.

## Command-line creation

Campaign manifests can also be created reproducibly from the CLI. Repeating `--batch` defines the execution order:

```bash
python create_campaign.py \
  --name "Final realism experiments" \
  --batch plans/baseline_batch.json \
  --batch plans/perturbation-a_batch.json \
  --batch plans/perturbation-b_batch.json
```

The default output is:

```text
campaigns/final-realism-experiments_campaign.json
```

## Running a campaign

```bash
python run_campaign.py \
  --campaign campaigns/final-realism-experiments_campaign.json \
  --experiment-mode \
  --no-update-field-translation
```

The default campaign output directory is `outcomes/<campaign-id>/`. Each queued matrix receives its own numbered child directory, for example:

```text
outcomes/final-realism-experiments/
├── campaign_state.json
├── campaign_summary.json
├── 001_baseline/
│   ├── batch_state.json
│   └── ... batch outcomes/reports ...
├── 002_perturbation-a/
│   ├── batch_state.json
│   └── ...
└── 003_perturbation-b/
    ├── batch_state.json
    └── ...
```

## Checkpoint and resume behaviour

Checkpointing exists at two levels:

1. `campaign_state.json` records which comparison matrices have been attempted and which matrix was active if execution stopped.
2. Each matrix retains its normal `batch_state.json`, which records completed dataset/reference jobs inside that matrix.

If execution is interrupted, resume with:

```bash
python run_campaign.py \
  --campaign campaigns/final-realism-experiments_campaign.json \
  --experiment-mode \
  --resume \
  --no-update-field-translation
```

Completed matrices are not rerun. If interruption happened halfway through a matrix, that matrix's own batch checkpoint is resumed, so its completed comparison jobs are also preserved.

If one or more matrices/jobs need attention, retry them with:

```bash
python run_campaign.py \
  --campaign campaigns/final-realism-experiments_campaign.json \
  --experiment-mode \
  --resume \
  --retry-failed \
  --no-update-field-translation
```

The underlying batch runner retains its existing attempt history and writes retry outcomes separately.

## Failure policy

By default, a campaign continues to later matrices even if an earlier matrix needs attention. This is useful for long experiment queues where independent matrices can still produce valid results.

Use `--fail-fast` to stop the campaign before starting the next matrix after a matrix failure. Use `--batch-fail-fast` when you also want each individual matrix to stop after its first failed dataset job.

## Scientific boundary

A campaign is orchestration only. It does not combine metric populations, change thresholds, alter reference mappings, create aggregate realism scores or make matrices scientifically dependent on each other. Each saved batch remains an independent experiment definition and each `run_plan.py` invocation remains the authoritative metric execution unit.
