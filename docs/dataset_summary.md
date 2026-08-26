# Dataset summary sidecars

CBR-Tests can maintain a human-readable descriptive summary beside each dataset it runs.

By default a run creates the sidecar on first use and reuses it while the dataset has not changed. For a dataset named:

```text
capture.pcap
```

the companion is:

```text
capture.pcap.summary.md
```

The original extension is retained in the sidecar name so datasets such as `capture.csv` and `capture.pcap` cannot collide.

## Cache and refresh behaviour

Every normal run already computes a SHA-256 digest for provenance. Dataset-summary caching uses that same digest.

The sidecar contains machine-readable metadata with:

- dataset SHA-256;
- dataset-summary schema version.

Before a run, CBR-Tests compares the current dataset hash with the sidecar metadata. The dataset is rescanned only when:

- no summary exists;
- the dataset SHA-256 changed;
- the dataset-summary schema version changed; or
- `--refresh-dataset-summary` was requested.

An unchanged dataset with a current summary is therefore not rescanned merely to recreate the report.

## Contents

The report is descriptive and does **not** itself make a realism judgement. Depending on the available dataset fields it contains:

- path, filename, format and file size;
- SHA-256 and file modification timestamp;
- row count or decoded IPv4/IPv6 packet count;
- field/column count and field names;
- numeric-field count;
- missing-cell and exact-duplicate-row counts for tabular datasets;
- first/last timestamp, covered duration and observed UTC dates where time can be interpreted safely;
- unique IP endpoint and port counts where applicable;
- protocol-value counts;
- packet-length and inter-arrival descriptive statistics when those fields are present.

For raw PCAP/PCAPNG, the record count refers to decoded IPv4/IPv6 packets in the canonical packet view. Non-IP frames are not represented in that count.

For tabular datasets, CBR-Tests will not guess the unit of a numeric timestamp merely to populate the report. If the timestamp cannot be interpreted safely, time coverage is reported as not determined.

## Controls

Dataset summaries are enabled by default:

```bash
python run_plan.py --case plans/example_plan.json --dataset data/example.csv --output outcomes/example.json
```

Disable them when a dataset directory is intentionally read-only or you do not want a sidecar:

```bash
python run_plan.py ... --no-dataset-summary
```

Force regeneration even when the stored hash still matches:

```bash
python run_plan.py ... --refresh-dataset-summary
```

The TUI exposes the same **Dataset summary sidecar** and **Force summary refresh** controls.

If a summary cannot be written, the runner records the summary error in provenance and warns, but the metric run itself is not discarded solely because an optional descriptive sidecar could not be published.
