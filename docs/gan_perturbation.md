# GAN trailing-suffix perturbation experiments

This is an **opt-in research workflow**. The normal `run_plan.py`, `run_batch.py`,
existing plans, and existing outcomes are unchanged.

## Design

`run_perturbation.py` orchestrates the companion CTGAN program's `sweep`
command and reuses `runner.fixed_plan_matrix.build_fixed_plan_matrix` plus the
existing `run_batch.py` execution/checkpoint/reporting machinery.

The generator is **not bundled into CBR-Tests**. Install the companion
`ctgan-pcap-same-count` project separately, train a checkpoint on **different
captures** from the evaluation PCAP, and provide its trusted `traffic_gan.py`
and `.pkl` checkpoint by path. Never load untrusted pickle checkpoints.

### Example

```bash
# Install CBR-Tests (Scapy) and the companion generator (CTGAN, PyTorch).
python run_perturbation.py \
  --input /data/evaluation.pcapng \
  --plan plans/my_frozen_pcap_plan.json \
  --model /data/models/ctgan.pkl \
  --generator-script /path/to/ctgan-pcap-same-count/traffic_gan.py \
  --output-dir outcomes/gan-pilot \
  --percentages 0,10,20,30,40,50,60,70,80,90,100 \
  --seeds 42,43,44 \
  --basis packets \
  --display quiet
```

Or launch `python run_plan.py --tui`, select **GAN perturbation experiment**,
and follow the setup prompts. Use `--prepare-only` to generate validated
captures and reusable batch manifests without running metrics.

## Experimental contract

- **Same number of packets** in each generated capture. The adapter
  independently reads the full output to check the packet count.
- **Last X% replaced**: 10% keeps the first 90%, 20% keeps the first 80%.
  The 0% candidate must be byte-for-byte identical to the source.
- **Approximately same size**, not exact per-packet bytes. The adapter rejects
  captures outside `--size-tolerance-pct` (default ±5%).
- **One frozen plan** reused for every percentage. If that plan includes
  reference-bound metrics, they are explicitly bound to the unmodified PCAP,
  using the existing fixed-plan matrix binding machinery.
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
  experiment.json                # source, plan, generator and model SHA-256; seeds; status
  perturbation_metrics.csv       # seed, percentage, metric, status, primary value, outcome
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
