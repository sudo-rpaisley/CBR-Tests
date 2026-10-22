# Recursive leave-one-out GAN corpus experiments

Select **one dataset root folder** in the CBR-Tests Toolbox:
**GAN recursive leave-one-out corpus**. The runner discovers supported PCAPs
in that directory and every descendant directory, irrespective of nesting
depth. It treats each *directory* containing PCAPs as an independent group.
It does **not** pool different directories into one GAN training dataset.

For each directory with **2 or more** regular `.pcap` or `.pcapng` files:

1. Sort filenames deterministically (case-insensitive, then by original spelling).
2. Select the first capture as held-out capture 1. Train CTGAN using **all
   remaining PCAPs in that directory**.
3. Compare the held-out capture with itself at 0% synthetic replacement,
   then with 10%, 20%, ..., 100% of its trailing packet positions replaced.
4. Hold out the second capture, retrain on all other files, and repeat.
5. Continue through all captures in this folder, and then every eligible
   directory anywhere below the selected root.

**Example**: `A.pcap`, `B.pcap`, `C.pcap`, `D.pcap` in one folder:

| Training PCAPs | Held-out | Evaluation |
| --- | --- | --- |
| B, C, D | A | A vs A (0%), A vs A-10%, ..., A vs A-100% |
| A, C, D | B | B vs B (0%), B vs B-10%, ..., B vs B-100% |
| A, B, D | C | C vs C (0%), C vs C-10%, ..., C vs C-100% |
| A, B, C | D | D vs D (0%), D vs D-10%, ..., D vs D-100% |

A folder containing **exactly one** PCAP is recorded as skipped, not run.
A folder with no PCAPs is simply traversed to find descendants. Files directly
inside one folder are a group; a single PCAP in a parent and a single PCAP
in a child are **not** combined. A directory with identical-content PCAPs
at different paths will fail the source-hash independence check for affected
held-outs, and is recorded as needing attention.

## Launch via the Toolbox

From the feature branch:

```bash
python -m pip install -r gan_generator/requirements.txt
python run_plan.py --tui
```

Choose **GAN recursive leave-one-out corpus**. The directory browser starts
at `datasets/` if it exists. Navigate with Enter; **d** selects the current
directory, not just the highlighted child. **R**, **H**, **M**, **e**, Backspace
and arrow keys work as in other CBR-Tests file selectors.

Once a dataset directory is selected, the Toolbox displays how many groups,
hold-outs, single-file folders and planned comparisons it found. Confirm by
typing `RUN`; everything else cancels. Training defaults to 100 epochs on
CPU, a batch size of 100, seed 42 and 0–100% by increments of 10. The output
is automatically stored in a **new, UTC timestamped directory** under
`outcomes/gan-corpus/`. You only need to select the source folder.

## CLI and pilot settings

```bash
# Only discover folders, skip singletons and report planned work
python run_perturbation_corpus.py --dataset-root datasets/IEEEDataset --preview

# Small end-to-end pilot, new timestamped output directory
python run_perturbation_corpus.py --dataset-root datasets/IEEEDataset \
  --percentages 0,10,20 --epochs 50 --device cpu

# Complete experiment
python run_perturbation_corpus.py --dataset-root datasets/IEEEDataset \
  --percentages 0,10,20,30,40,50,60,70,80,90,100 \
  --seeds 42,43,44 --epochs 100

# Resume the exact same inputs/settings and output directory
python run_perturbation_corpus.py --dataset-root datasets/IEEEDataset \
  --seeds 42,43,44 --epochs 100 --resume \
  --output-dir outcomes/gan-corpus/YOUR-UTC-RUN-ID
```

On resume, all options (including percentage list and device) **must be the
same** as in the original run, or the manifest-fingerprint check refuses to
resume. Data and generator hashes are checked again. Successfully completed
cases with valid checkpoints are skipped. Failed or interrupted cases are
retired as earlier attempts; retry is created in a **new attempt directory**.
No original outcome JSON or model checkpoint is overwritten. An interrupted
single held-out case currently restarts that case, not an individual
seed/percentage.

## What is archived — no metric data thrown away

```text
outcomes/gan-corpus/<UTC-run-id>/
  campaign.json              # complete inventory, SHA hashes, cases and statuses
  cases/
    <group-id>/<holdout-id>/
      attempt_001/
        experiment.json      # selected inputs, model and generator identities
        auto_comparison_plan.json
        auto_plan_report.json
        training/
          training_manifest.json
          ctgan.pkl
          training_flows.csv
          individual_features/
        seed_42/
          captures/          # GAN replacements, labels and per-level provenance
          fixed_plan_batch.json
          evaluation/
            batch_state.json
            runs/...         # FULL original CBR-Tests outcome JSONs
  exports/
    jobs.csv                 # one row per evaluation/seed/percentage
    metric_results.csv       # one row per metric; preserves native scalar
    measurements_long.csv    # EVERY nested metric-result leaf
    metric_payloads.jsonl    # full native execution record and raw metric payload
    export_manifest.json     # exported row counts
```

The **original CBR-Tests outcome JSONs remain the authoritative record**,
including `metric_results`, `test_results`, `test_results_taxonomy`,
other execution metadata and metric-specific outputs. The JSONL archive
repeats full metric execution records and raw per-metric results, without
choosing only the top-level mean. The `measurements_long.csv` represents
every nested scalar and array element using an escaped JSON Pointer, with
value type, JSON representation and a numeric column for quantitative
analysis. Blank, null and empty-array structures are distinguishable. The
`metric_results.csv` is a **convenience** overview rather than a replacement
for the full measurements.

All exports are indexed with UTC campaign start time, source group,
held-out path/hash, training paths, seed, GAN fraction and source outcome
hash. This makes comparisons across research runs and dates auditable.
Reports are rebuilt from the saved complete outcomes, rather than blindly
appended, to avoid duplicate rows on restart. Record permanent storage or
backups for these outputs — they may be large.

## Reference and scientific caveats

**Self-comparison** is deliberate: the 0% test is a content-identical PCAP
used as both candidate and reference. The ordinary two-dataset workflow still
forbids input path self-comparisons; this corpus workflow enables them only
explicitly. An independent training PCAP supplies the *temporary* reference
template needed to build the common metric plan, but the evaluation plan is
**rebound to the actual held-out PCAP** before any metric execution. The GAN
is never trained on the held-out PCAP. All perturbation levels reuse one
frozen metric plan per held-out case.

This remains CTGAN trained on tabular flow features followed by
packet-template rewriting; it does not generate end-to-end protocol coherent
sessions. Output timestamps may be preserved. Synthetic data is not automatically
realistic. Some networks have too few eligible flows (the training pipeline
requires at least 50 extracted flows in total); those cases are recorded
as failed rather than silently discarded.

**Disk usage**: the full campaign can create approximately
`number_of_seeds × number_of_percentages × sum_of_all_eligible_PCAP_sizes`
of synthetic captures, in addition to flow tables, CTGAN checkpoints,
outcome JSONs, intermediate labels and CSV/JSONL exports. Budget generously;
the estimate is not a guarantee. Start with a small pilot.

**Data privacy**: captures, generated packet content, CTGAN model checkpoints,
and detailed outcomes stay on the runner host. Never commit them to public GitHub.
