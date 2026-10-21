# GAN trailing-suffix perturbation experiments

This is an **opt-in research workflow**. The normal `run_plan.py`, `run_batch.py`,
existing plans, and existing outcomes are unchanged.

## Design

The comparison is **Dataset 1 vs Dataset 2**, then **Dataset 1 vs Dataset 2-10%**, **Dataset 1 vs Dataset 2-20%**, through 100%. Dataset 1 stays unchanged in every pair; the percentages apply only to Dataset 2.

`run_perturbation.py` orchestrates the companion CTGAN program's `sweep`
command and reuses `runner.fixed_plan_matrix.build_fixed_plan_matrix` plus the
existing `run_batch.py` execution/checkpoint/reporting machinery.

The CTGAN generator **is bundled into this Git feature branch**. Run
`python -m pip install -r gan_generator/requirements.txt` and train its model
using the root `traffic_gan.py` launcher. No Windows transfer or external ZIP
is needed. The generator source archive and its SHA-256 are documented in
[the bundled generator guide](../gan_generator/README.md). Train on a **third,
independent** capture, not Dataset 1 or Dataset 2. Never load an untrusted
pickle checkpoint.

### Example

```bash
# Install CBR-Tests and the bundled GAN dependencies first.
python run_perturbation.py \
  --base /data/dataset1.pcapng \
  --comparison /data/dataset2.pcapng \
  --plan plans/my_frozen_pcap_plan.json \
  --model /data/models/ctgan.pkl \
  --output-dir outcomes/gan-pilot \
  --percentages 0,10,20,30,40,50,60,70,80,90,100 \
  --seeds 42,43,44 \
  --basis packets \
  --display quiet
```

Or launch `python run_plan.py --tui`, select **GAN perturbation experiment**,
and use the same **curses file selector** as other CBR-Tests workflows.
On each file field, press **Enter** to browse directories and select **Dataset 1
(base/reference PCAP)**, **Dataset 2 (comparison PCAP)**, the fixed JSON plan,
trained `.pkl` checkpoint or the preselected repository-root `traffic_gan.py` script. The
browser filters by file type and supports **R** (repository), **H** (home),
**M** (`/media`), **e** (enter a path), Backspace (parent) and **q** (return).
Use **e** on the setup screen to type a path instead. Choose your percentages,
seeds and empty output directory; press **r** to validate and run. Toggle
**Prepare captures only** to generate validated captures and reusable batch
manifests without executing the metrics. The file selector retains your choices
when you return to the setup screen.

## Experimental contract

- **Same number of packets** in each generated capture. The adapter
  independently reads the full output to check the packet count.
- **Two separate capture roles:** Dataset 1 is the **fixed reference** and is never
  perturbed; Dataset 2 is the **candidate** that the GAN modifies. The first
  comparison is Dataset 1 vs original Dataset 2 (0%). The next is Dataset 1 vs
  Dataset 2 with the final 10% replaced, then Dataset 1 vs Dataset 2 with the
  final 20% replaced, and so on. The base remains fixed across all seeds.
- **Last X% of Dataset 2 replaced**: 10% keeps its first 90%, 20% keeps its first 80%.
  The 0% candidate must be byte-for-byte identical to original Dataset 2.
- **Approximately same size**, not exact per-packet bytes. The adapter rejects
  captures outside `--size-tolerance-pct` (default ±5%).
- **One frozen plan** reused for every percentage. The plan **must include
  reference-comparison metrics**. Those metrics are explicitly rebound to Dataset 1
  for each evaluation, using the existing fixed-plan matrix binding machinery;
  intrinsic metrics are still calculated for Dataset 2 candidates.
- **Multiple seeds** use separate batch output directories to prevent the
  existing batch outcome filename convention from colliding.
- **Fail closed** on missing output/provenance, changed source/model hashes,
  mismatched file counts and invalid manifests. Output directory must be new
  or empty; previous experiment outputs will not be overwritten.
- **No implicit training**: training is separate from evaluating. Store
  the training dataset's identity and separation evidence alongside the model
  as part of the experiment record.

## Outputs

```text
outcomes/gan-pilot/
  experiment.json                # base + comparison identities/hashes, plan, GAN and seeds
  perturbation_metrics.csv       # base, comparison, seed, percentage, metric, native value
  seed_42/
    captures/
      gan_000.pcapng
      gan_010.pcapng
      ...
      manifest.csv
      gan_010.packet_labels.csv
      gan_010.flow_labels.csv
      gan_010.json
    fixed_plan_batch.json
    evaluation/
      batch_state.json
      runs/<timestamp>/...
  seed_43/...
```

The `perturbation_metrics.csv` contains the existing metrics' **native scalar
outputs** when available, and statuses otherwise. It deliberately does
**not** invent an aggregate realism score. Use these long-form rows for
plots of metric response versus synthetic packet fraction, and evaluate
seed-to-seed variability.

If a batch fails, `experiment.json` records `needs_attention`. Its
per-seed `batch_state.json` preserves the checkpoint and can be resumed
using `run_batch.py --resume`. This first adapter version does **not**
resume a partially prepared experiment automatically.

## Limitations and controls

The generator should be trained on a third, independent training capture. Do not train it on
Dataset 1 or Dataset 2 if the evaluation is intended to test generalisation.

This GAN version learns **tabular flow features**, then reconstructs
packet templates and pseudo-random opaque payload bytes. It is not a
packet-sequence GAN. It keeps original timestamps and generally keeps
packet endpoints and some transport semantics, so metrics relying on those
attributes may not change. TCP/application-layer semantics may not be
valid. The GAN can fail on unsupported packet layers; the pilot should
begin with an eligible Ethernet/IPv4 TCP/UDP capture.

A suffix perturbation is confounded by temporal drift: the original
last 10% of traffic may differ systematically from the first 90%.
Interpret the results against baseline profiles, and consider repeated
time windows or additional control perturbations for publication.
A fixed total-byte budget also constrains the mean size of replacement
packets, so packet-length results must be interpreted accordingly.

Large captures require substantial disk space and RAM in the companion
prototype. Prepare a representative PCAP for pilot testing before using
the complete research dataset. A trained GAN and real capture are still
required for end-to-end verification.
