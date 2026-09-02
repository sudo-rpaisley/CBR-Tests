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
