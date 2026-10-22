# GAN research results catalogue and explorer

The CBR-Tests measurement runner already writes complete outcome JSON files.
This feature adds an **optional analytical layer** with a persistent
[DuckDB](https://duckdb.org/) database, compressed Parquet extracts and a
read-only, paged results explorer. **No original experiment files are
modified or deleted.** Indexing is deterministic and repeatable.

## Install on the Spark

On the `feature/gan-results-catalog-20261022` branch:

```bash
python -m pip install -r requirements-results.txt
```

This installs the DuckDB Python package. The ordinary metrics runner does
not import DuckDB and can still run without this extra dependency.

## Import existing and future experiments

```bash
# Import every existing GAN run under outcomes, including earlier single runs
python results_catalogue.py index --search-root outcomes

# Optional: import a second, older results archive too
python results_catalogue.py index \
    --search-root outcomes --search-root /data/older-research-outcomes

# View row counts
python results_catalogue.py stats

# Interactive terminal browser
python results_catalogue.py browse
```

Alternatively start `python run_plan.py --tui` and choose
**GAN results explorer** in **Review results**. The Toolbox automatically
refreshes the index from `outcomes/` whenever the explorer opens and when
you press **r** on its first screen. For archives elsewhere, import them
explicitly with `--search-root` once; those previously imported records
remain in the same DuckDB catalogue.

The default paths are:

```text
outcomes/results-catalogue.duckdb
outcomes/results-parquet/
    campaigns.parquet
    cases.parquet
    jobs.parquet
    metrics.parquet
    measurements.parquet
    catalogue_manifest.json
```

The command line can select a different DuckDB file using
`--db /path/to/my-catalogue.duckdb`, or another Parquet destination via
`--parquet-dir /path/to/exports`.

## Browsing large amounts of data

The explorer opens **campaigns → held-out capture → metric → measurement
at a particular percentage and seed → full nested values**.

| Control | Action |
| --- | --- |
| ↑ / ↓ | Move through results |
| PgUp / PgDn | Query earlier/later pages; no full-table loading |
| Enter | Open the selected campaign, capture, metric or measurement |
| b / Backspace | Go back one level |
| / | Filter campaign, capture or metric names |
| t | View the selected metric **across all indexed campaigns and dates** |
| o | Show the original CBR outcome JSON path |
| r | Re-index archives (from the first screen) |
| q / Esc | Quit |

The database contains five main tables:

- **campaigns:** UTC timestamps, status, source file and experiment settings
  fingerprint.
- **cases:** group, held-out capture, reference capture, training list and
  status.
- **jobs:** perturbation percentage, seed, attempt, original outcome path and
  SHA-256 hash, model and plan identities.
- **metrics:** *every* metric, status, native scalar when one exists, plus
  losslessly serialized complete metric payload and execution record.
- **measurements:** *every* nested result leaf, tagged with JSON Pointer
  paths, data type, original JSON value and a numeric column where possible.

A virtual view, `metric_observations`, makes cross-run queries convenient.

## Query or export one metric across dates

```bash
python results_catalogue.py metric \
  --name ks_statistic_from_reference \
  --held-out tft5g2.pcap \
  --date-from 2026-10-01 \
  --date-to 2026-11-01 \
  --csv /data/analysis/ks_october.csv
```

Dates are compared as UTC ISO-formatted strings from source manifests.
The `--date-to` boundary is exclusive; you may omit either bound.
Use the actual metric IDs emitted by your CBR plan, not a guessed name.
You can also query DuckDB directly:

```sql
SELECT
    campaign_id, started_at, held_out_path,
    perturbation_pct, seed, primary_value, domain_status
FROM metric_observations
WHERE metric_id = 'ks_statistic_from_reference'
ORDER BY started_at, held_out_path, perturbation_pct, seed;
```

For a large study, load `outcomes/results-parquet/*.parquet` into a
notebook or other analytical environment. The files are compressed using
Zstandard and can be regenerated at any point from DuckDB.

## Repeatability and safety

Re-importing unchanged archives **does not duplicate records**. The indexer
tracks source manifest and individual outcome file metadata, and only
replaces the affected campaign's catalogue rows when changes are detected.
Replacement occurs in a database transaction so failed imports leave
earlier indexed data intact. The `--force` option reimports all selected
archives.

Failed and interrupted campaigns with available outcomes are included.
Jobs without an outcome JSON remain visible in the jobs table with a
missing outcome reference; no imaginary metric values are introduced.
Earlier single-experiment `experiment.json` files are supported; when they
belong to a recursive campaign they are not imported a second time.

**Keep the original JSON files.** DuckDB and Parquet are derived analytical
indexes, not a substitute for full primary records, PCAP provenance or
training model archives. Move/copy archives carefully: if you move the
underlying JSON outcome directories after importing, their stored paths
become stale until you reimport at the new location. The catalogue uses
paths to distinguish distinct sources even if they share a campaign ID.

For data held across different dates, comparability depends on fixed
metric definitions, reference data, GAN parameters and software versions.
Never interpret two values as directly comparable merely because the
metric IDs match; use the stored hashes and plans to inspect changes.

**Scaling limitation (first release):** the catalogue refreshes each
changed campaign atomically rather than indexing individual metric records
asynchronously during a running evaluation. The Toolbox refreshes on entry
or when requested, and only reimports changed sources. Very large newly
completed campaigns can still require a substantial first indexing pass.
Parquet snapshots are regenerated after the index command completes.
