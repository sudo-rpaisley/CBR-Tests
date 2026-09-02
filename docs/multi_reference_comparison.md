# Multiple reference dataset comparisons

`create_plan.py` can build a sequential comparison matrix from one or more candidate datasets and one or more independent reference datasets.

## Interactive workflow

Run:

```bash
python create_plan.py
```

When a file is needed, the interactive builder opens the repository file browser. Candidate datasets and reference datasets are selected by browsing; the interactive workflow does not require typing their paths. Existing field-translation sidecars are discovered automatically, so the normal interactive workflow does not ask users to type a translation-file path either.

After selecting a candidate dataset, choose whether to add another. The builder then asks whether reference datasets should be added. Reference selection uses the same browser and can be repeated.

Examples:

- 1 candidate and 0 references -> one normal plan.
- 1 candidate and 1 reference -> one comparison plan.
- 1 candidate and 3 references -> a three-job comparison batch.
- 3 candidates and 2 references -> a six-job candidate/reference matrix.

Candidate/reference pairs that point to the same file are skipped because a realism reference must be independent of the candidate.

## Command-line equivalent

Paths can still be supplied explicitly for automation and CI:

```bash
python create_plan.py \
  --name "Synthetic against real references" \
  --dataset datasets/synthetic-a.csv \
  --dataset datasets/synthetic-b.csv \
  --reference-dataset datasets/real-a.csv \
  --reference-dataset datasets/real-b.csv
```

The example produces four jobs:

```text
synthetic-a vs real-a
synthetic-a vs real-b
synthetic-b vs real-a
synthetic-b vs real-b
```

Run the generated matrix with:

```bash
python run_batch.py --batch plans/synthetic-against-real-references_batch.json
```

Each job receives its own plan and outcome file. Outcome filenames include the candidate and reference names so repeated candidate runs remain distinguishable.

## Human-readable comparison outputs

Reference batches automatically produce comparison reports after the individual JSON outcomes have been written. The JSON outcome files remain authoritative; the CSV and Markdown files are denormalised views for comparison and analysis.

A typical output directory contains:

```text
outcomes/synthetic-against-real-references/
├── outcome_01_synthetic_a_vs_real_a_....json
├── outcome_02_synthetic_a_vs_real_b_....json
├── outcome_03_synthetic_b_vs_real_a_....json
├── outcome_04_synthetic_b_vs_real_b_....json
├── batch_summary_....json
├── comparison_overview_....csv
├── comparison_long_....csv
├── comparison_report_....md
└── comparison_matrices_..../
    ├── overall_status.csv
    ├── feature_wise_ks_statistic_from_reference.csv
    ├── feature_wise_wasserstein_distance_from_reference.csv
    ├── pearson_matrix_deviation_from_reference.csv
    └── ...
```

### Wide overview CSV

`comparison_overview_<timestamp>.csv` has one row per candidate/reference pair. Each reference metric receives a numeric value column and a companion result-status column.

Example:

```text
candidate,reference,feature_wise_ks_statistic_from_reference,feature_wise_ks_statistic_from_reference__result_status,protocol_mix_divergence_from_reference
synthetic-a.csv,real-a.csv,0.12,pass,0.08
synthetic-a.csv,real-b.csv,0.31,warn,0.16
synthetic-b.csv,real-a.csv,0.18,pass,0.11
synthetic-b.csv,real-b.csv,0.45,fail,0.23
```

This format is convenient for Excel, LibreOffice, pandas, R and statistical analysis because comparison values remain numeric rather than being mixed with presentation text.

### Long-form CSV

`comparison_long_<timestamp>.csv` has one row per candidate/reference/metric combination. It includes:

- candidate and reference labels and full paths;
- job ID;
- metric ID;
- metric execution status;
- metric result status;
- primary scalar comparison value;
- optional maximum value for feature-wise metrics;
- the metric summary object serialized as JSON;
- authoritative outcome path.

The long form is generally the most useful representation for plotting, grouping, statistical analysis and importing into experiment notebooks.

### Per-metric matrices

Every reference metric receives its own literal candidate × reference CSV matrix under `comparison_matrices_<timestamp>/`.

For example, `feature_wise_ks_statistic_from_reference.csv` might contain:

```text
candidate,real-a.csv,real-b.csv
synthetic-a.csv,0.12,0.31
synthetic-b.csv,0.18,0.45
```

Rows are candidate datasets and columns are reference datasets. Cells remain numeric so conditional formatting, heat maps and calculations can be applied directly in spreadsheet software.

`overall_status.csv` uses the same shape but records each job's overall execution status.

### Markdown comparison report

`comparison_report_<timestamp>.md` renders the overall status matrix followed by a matrix for every reference metric. Where a metric exposes a domain result status, the Markdown cell shows it alongside the value, for example `0.12 (pass)`.

For the current reference-distance/divergence metrics, lower values generally indicate a closer match. The metric-specific documentation remains the source of truth for exact interpretation.

The batch summary JSON records the paths of all generated comparison reports so experiment tooling can discover them programmatically.

## Common-metric policy

The default batch policy keeps only metric IDs that are runnable across every generated candidate/reference job. This is the preferred mode for comparative experiments because every pair is measured with the same set of tests.

Use `--per-dataset-metrics` when coverage is more important than strict cross-job comparability.

## Tabular reference datasets

CSV, TSV, XLSX and XLS candidates can be compared with tabular references. The plan builder inspects a small prefix sample to identify numeric-compatible shared fields and automatically configures reference-comparison metrics whose requirements are satisfied by both datasets.

Candidate and reference files do not need identical raw column names when their field-translation sidecars map them to the same canonical CBR-Tests fields. The generated metric stores a reference-field mapping so the runtime aligns the reference columns with the candidate fields before computing the comparison.

For example:

```text
Candidate raw field        Canonical field        Reference raw field
Flow Duration              Flow Duration          flow_duration
Src Port                   Source Port            source_port
Dst Port                   Destination Port       destination_port
```

This keeps comparison semantics tied to canonical fields rather than exporter-specific spelling.

## PCAP references

Raw PCAP/PCAPNG candidates must use raw PCAP/PCAPNG references. Tabular candidates cannot be paired with raw PCAP references because their representation semantics differ.

The existing packet-level reference metric restrictions remain in force; unsupported flow- or slice-level comparisons are still excluded during preflight.
