# Live GAN progress, ETA and results during a large campaign

The recursive leave-one-out GAN campaign **does not require you to wait until
every held-out PCAP has completed**. It saves authoritative batch checkpoints
and complete outcome JSONs as individual comparisons finish. A live TUI
monitor displays overall, group, held-out and current-job progress and gives
direct access to the Results Explorer while the worker continues running.

## Starting a monitored campaign

```bash
python -m pip install -r requirements-results.txt
python run_plan.py --tui
```

Choose **GAN recursive leave-one-out corpus**, select the parent folder and
confirm `RUN`. The Toolbox starts a separately managed process, writes its
stdout/stderr to a dated `.log` alongside the campaign folder, and opens the
live monitor. **q** closes only the monitor, **not** the experiment.

The experiment outputs are timestamped inside your **selected output storage
folder** (which may be on another disk or mounted filesystem). In another SSH
session, reattach with the actual campaign path:

```bash
python monitor_gan_corpus.py /mnt/large-drive/research/gan-corpus/YOUR-RUN-ID
```

You can also choose **Monitor running GAN campaign** from the Toolbox
and browse to the existing run directory, including external mounts (`N`
for `/mnt`, `M` for `/media`, or `e` for an absolute path).

## Large-PCAP streaming generator (October 2026)

The CTGAN **extract** and **sweep** commands use the bounded-memory streaming
engine. It scans one packet record at a time and writes a disk-backed packet
length/replacement plan (approximately 24 bytes per packet), instead of keeping
a full capture and Scapy packet objects in RAM. Training still uses the bundled
CTGAN model format; original dataset files are never changed.

The recursive GAN runner now generates, validates and runs CBR metrics **one
perturbation percentage at a time**. This means the first 0% (self-reference)
and 10% metrics are saved and can be viewed in Results Explorer while later
percentages and held-out cases continue. The packet-progress sub-stage
(scanning/planning/balancing/writing) is reported separately; the overall
comparison-job counter only advances after validation and metric evaluation.

**Run a small pilot before the full study**, with a separate folder containing
two small independent captures:

```bash
python run_perturbation_corpus.py \
  --dataset-root /path/to/two-small-pcaps \
  --output-dir /media/media/GAN_results/streaming-pilot-001 \
  --percentages 0,10 --epochs 10 --device cpu
```

Choose a **new output directory**. Do not resume the earlier memory-exhausted
campaign with the changed generator. Its CTGAN/model and experiment manifest
belong to the previous code version and are preserved for audit.

This fix reduces **peak RAM use**; it does *not* automatically delete generated
captures. The earlier total logical disk-space estimate still applies for
full-capture retention. Performance and metric execution still need validation
on a representative multi-million-packet Spark capture. The generator is
feature-conditioned synthetic packet-template rewriting, **not** complete
end-to-end conversation synthesis.

## Four levels of progress

| Level | Reported | ETA basis |
| --- | --- | --- |
| Entire campaign | Number of finished/failed held-out captures and comparison jobs, total groups, available raw job results | Observed completed held-out durations |
| Current directory group | Group-held-outs completed/total, attempted comparisons, failures | Completed cases in the group, or campaign sample where available |
| Current held-out PCAP | Stage (feature extraction, CTGAN training, GAN capture generation, validation, evaluation), completed comparison jobs, elapsed time | Previous fully completed held-out cases |
| Current comparison job | Seed, GAN percentage, job index, elapsed time, current job result | Duration of earlier completed comparison jobs from this held-out case |

Every **ETA is approximate**. No ETA is shown until at least one appropriate
time sample exists. Earlier jobs in a different group can be quite different
in size/complexity. Failed cases and retries can increase the real finishing
time. CTGAN training shows the planned epoch count and extracted training-file
position, but does not pretend to know per-epoch throughput from a terminal
progress bar.

Use **l** to see the latest training/worker log, and **r** to refresh manually.
The monitor normally refreshes every second.

## View results before the campaign finishes

Press **v** from the live monitor. The system then:

1. Finds all currently saved `experiment.json`, `batch_state.json`, and
   original CBR-Tests outcome JSON files from this running campaign.
2. Indexes available outcomes transactionally into DuckDB; incomplete jobs
   remain visible as jobs even before they have numeric metric results.
3. Opens the paged **GAN Results Explorer**. Drill down into a campaign,
   held-out capture, metric, seed/perturbation percentage and its complete
   nested measurements, or press **t** for comparisons across run dates.
4. Return from the explorer with **q**; the worker has continued independently
   throughout. Press **v** again later to refresh newly completed jobs.

While the campaign is running, you may also index or query from a second
terminal without starting the TUI:

```bash
python results_catalogue.py index --search-root /mnt/large-drive/research/gan-corpus
python results_catalogue.py metric --name ACTUAL_METRIC_ID --limit 100
```

**This does not wait for final exports.** The original complete raw JSONs
are saved per completed job and are the source of truth. The huge consolidated
CSV/JSONL archives are written after the run to avoid expensive repeated
rewrites; DuckDB works on saved partial outcomes in the meantime.

## Inspect without the TUI

A small, atomically rewritten
`<chosen-output-folder>/YOUR-RUN-ID/progress.json` contains:
campaign totals, group/current-heldout/job counts, current stage, elapsed
times, approximate ETAs, and how many actual saved job results are available.

```bash
watch -n 5 'jq "{status, campaign, group, case, job}" /mnt/large-drive/research/gan-corpus/YOUR-RUN-ID/progress.json'
```

The adjacent `campaign.json` and the original per-job `batch_state.json`
remain authoritative for completion and provenance; the progress snapshot
is derived and may be up to approximately 1–2 seconds behind.

For runs launched directly through `run_perturbation_corpus.py`, the regular
terminal still receives its text progress messages, and this monitor can
attach from another SSH session after the campaign directory is created.

## Robustness and limitations

- A failed or interrupted case does not erase completed outcome JSONs.
- Resuming a campaign retries an incomplete held-out case into another
  attempt directory. Progress and cross-date results retain attempt IDs.
- Disconnecting the monitored SSH session after launch does not terminate
  the separately spawned corpus worker. Explicit process management and
  signal handling still apply if the worker itself is stopped.
- No GPU-resource utilisation or reliable wall-clock deadline is claimed.
  The ETAs are observational estimates, and first-run ETAs are unknown until
  a completed case supplies timing data.
- DuckDB/Parquet are derived indexes. Original raw CBR outcomes, PCAP labels,
  checkpoint files and CTGAN checkpoints remain on disk.
