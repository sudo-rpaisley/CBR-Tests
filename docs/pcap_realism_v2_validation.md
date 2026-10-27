# Opt-in PCAP realism v2: correctness, controls and temporal GAN

This is an **experimental branch** derived from the exact evaluation software
revision `004560e9549fc2a85d19e1d54b3f15a802c2edca`.
The original 410-outcome campaign and its 32 legacy metric IDs are not rewritten.
**Do not change the running campaign's checkout, plans, or output directory.**

## Scope and interpretation

The new metrics yield **native measurements and applicability evidence**, not
a calibrated global realism label. A high divergence is not automatically a
realism defect; use a scenario hypothesis and a real-for-real reference control.
The original CTGAN does not generate full network conversations or timestamps.

### Existing metric corrections

- Missing-value ratio includes **fully absent configured fields** in the
  denominator and lists missing columns.
- Duplicate signature reports **not applicable** when configured signature
  fields are unavailable, instead of silently shrinking its signature.
- A generic metric with `summary.runnable=false` is now reported as an
  applicable *execution* with `status=not_applicable` for domain reporting.
- `feature_set_mmd_score_from_reference` gains opt-in
  `calibration_mode: reference_fixed`. The default `pooled` mode is retained
  for historical reproduction. Reference-fixed scaling and bandwidth are
  estimated from the same reference sample at every perturbation level.
  Always record estimator, sample size, scaling and bandwidth, and compare
  sensitivity to reference selection.

### New reference-comparison metric IDs

Each opt-in metric requires an unmodified reference PCAP, and reports
`summary.runnable` and `summary.reason_code`.

| Metric ID | Interpretation and caution |
| --- | --- |
| `capture_order_inter_arrival_divergence_from_reference` | Capture-order positive-gap KS plus backwards-gap differences; unlike legacy sorted-time IAT |
| `per_flow_inter_arrival_divergence_from_reference` | Gap distributions within bidirectional 5-tuples; sampled prefix explicitly reported |
| `multiscale_activity_divergence_from_reference` | Elapsed-time relative packet-count histograms over several window sizes |
| `burst_structure_divergence_from_reference` | Burst packet-count and idle-interval KS at declared gap threshold |
| `temporal_autocorrelation_deviation_from_reference` | Log-gap autocorrelations across configured packet lags |
| `flow_population_deviation_from_reference` | 5-tuple packet counts and observed flow durations; bounded-prefix data |
| `bidirectional_flow_balance_deviation_from_reference` | Per-flow packet directional balance; does not claim request-response semantics |
| `port_class_divergence_from_reference` | Coarse port classes rather than exact ephemeral identities |

The bounded-prefix flow tests are explicitly **not** independent population
estimates when they report `truncated=true`. Prefer eligible small complete
captures or a dedicated full PCAP flow export for publication.

### New optional raw-PCAP metrics

| Metric ID | Interpretation |
| --- | --- |
| `exact_packet_duplicate_ratio` | Same raw frame bytes **and** same timestamp (not repeated field signatures) |
| `packet_integrity_profile` | Observable IPv4 header checksums and IP/UDP declared lengths; offloading/truncation confounds |
| `tcp_sequence_continuity_profile` | Directional sequence gaps and retransmission hints; no TCP/application correctness verdict |

These scan the PCAP and are deliberately **not auto-enabled**. Add them to an
explicit PCAP plan using `pcap_metric_template(...)`. They can incur large
I/O costs. For packet-integrity tests, NIC checksum offload and truncated
captures require explicit evidence and should never trigger automatic realism
failure.

### Explicit v2 comparison plan

Create a plan with the exact new metric IDs you intend to validate. The
historical automatic PCAP plan deliberately does **not** include these
potentially expensive v2 metrics.

```python
from pathlib import Path
from runner.plan_builder import build_plan, write_plan

plan, report = build_plan(
    plan_id="pcap-v2-timing-pilot",
    name="Opt-in PCAP temporal realism v2",
    dataset_path=Path("/data/held_out_real.pcap"),
    reference_dataset_path=Path("/data/independent_real.pcap"),
    include_metric_ids=[
        "capture_order_inter_arrival_divergence_from_reference",
        "per_flow_inter_arrival_divergence_from_reference",
        "multiscale_activity_divergence_from_reference",
        "burst_structure_divergence_from_reference",
        "temporal_autocorrelation_deviation_from_reference",
        "flow_population_deviation_from_reference",
        "bidirectional_flow_balance_deviation_from_reference",
        "port_class_divergence_from_reference",
        "exact_packet_duplicate_ratio",
        "packet_integrity_profile",
        "tcp_sequence_continuity_profile",
    ],
)
assert all(v["status"] == "ready" for v in report["metrics"].values())
write_plan(Path("/data/plans/pcap-v2-timing-pilot.json"), plan)
```

Use the original held-out capture as the reference for each of its own
perturbations. The independent real capture shown above is an example
preflight reference; update the frozen evaluation plan to the actual
held-out source where required by the experimental protocol.

## Time-aware generative model (independent of CTGAN)

`gan_generator.temporal_gan` is a small sequential conditional
GRU-generator/GRU-discriminator GAN, not a reproduction of the original TimeGAN
paper. It learns positive inter-arrival sequences, conditioned on packet length.
The saved model is used with `torch.load(..., weights_only=True)` and only
locally trusted training artefacts should be loaded.

Training captures **must not include the held-out evaluation capture**, which
is verified by SHA-256. A different capture (ideally from the same application
scenario) is used for model training.

```bash
# Work in a SEPARATE checkout of this branch and a new output directory.
python -m pip install -r gan_generator/requirements.txt

python -m gan_generator.temporal_gan train \
  --inputs /data/training1.pcap /data/training2.pcap \
  --model /data/temporal-controls/model.pt \
  --epochs 20 --sequence-length 32 --seed 42 --device cpu

python -m gan_generator.temporal_gan sweep \
  --input /data/held_out_real.pcap \
  --model /data/temporal-controls/model.pt \
  --out /data/temporal-controls/sweep \
  --percentages 0,10,20,30,40,50,60,70,80,90,100 \
  --seeds 42,43,44 --pattern distributed --duration-policy match_original
```

This writes timestamp-only PCAPs with accompanying `.pcap.json` manifests,
preserving packet bytes/order/count. It does **not** generate TCP sessions.
The `match_original` duration policy keeps overall capture duration fixed,
but redistributes gaps and may alter generator output statistics. Repeat the
experiment with `--duration-policy free` as a sensitivity check.

The `trailing`, `distributed` and `blocks` replacement patterns test
position-related confounding. Their percentages refer to **inter-arrival
gaps**, unlike the legacy CTGAN's packet positions. Do not compare percentage
labels without declaring the denominator.

## Real-for-real control

```bash
python -m gan_generator.real_for_real_controls \
  --target /data/held_out_real.pcap \
  --donor /data/independent_real.pcap \
  --out /data/real-controls \
  --mode flow --placement distributed --seed 42 \
  --percentages 0,10,25,50,75,100
```

The control records the **actual selected target packet fraction** and both
input hashes. Whole-flow replacement can change packet count, capture duration
and application context. Compare like-for-like traffic, report target
selection criteria and re-validate protocol/link compatibility. The
`--mode packet` option is a deliberately weak control that preserves packet
count but does not preserve complete conversations.

## Recommended staged evaluation

1. Freeze input PCAP hashes, training splits, plan, Git commit, model config
   and packet/exposure definitions.
2. Test identity (0%), constant time shift, positive interval shift, known
   invalid timestamp, loss/duplication, packet-length perturbation.
3. Run content-only CTGAN, timing-only temporal GAN, and real-for-real
   controls at the same declared exposure levels on independent held-outs.
4. Run both trailing and distributed patterns and 3+ generator seeds.
5. Compare native results per field, not mixed-unit average scores.
6. Audit `runnable`, reasons, observed windows, evidence counts and
   `truncated` before statistical aggregation.
7. Calculate capture-level dose-response slopes, null-control false-alert
   behaviour, Spearman trend coefficients, and uncertainty across held-outs.
8. Exclude unavailable cyclic metrics from denominator-based success rates.
9. Keep a subset of unseen captures for final external validation.
10. Only after independent domain validation, propose thresholds or any
    combined score.

### Experimental evidence boundaries

A one-hour or one-day PCAP cannot validate multi-day diurnal regularity.
Global sorted-time IAT ignores capture order; v2 does not replace it silently.
Port-class grouping is not service identification. Flow counts on bounded
prefixes do not measure the whole original capture population. Capture
checksum mismatches may arise from NIC offload. The temporal GAN trains on
global capture-order gaps, not synchronized bidirectional flow dialogues.

5G slice-specific assertions (S-NSSAI/QFI/QoS flow identities, tunnel
policies, subscriber mappings) require **capture-point metadata and supported
protocol decoders**; they cannot be inferred from ordinary application
packets. Do not present these as measured if unavailable.

## Test commands

```bash
python -m compileall -q cbr_tests runner gan_generator
python -m pytest -q tests/test_pcap_realism_v2.py tests/test_pcap_protocol_temporal_gan_v2.py
python -m pytest -q
python scripts/build_reference_documentation.py
```

Do not merge into `main` until the repository CI passes and small
captured-PCAP pilot outputs are independently reviewed.
