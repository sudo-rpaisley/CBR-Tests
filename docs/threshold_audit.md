# Threshold and decision-rule audit

This audit separates the **metric definition** from the **decision policy** used to turn a measured value into PASS/WARN/FAIL. A cited statistic, RFC, standard, or realism paper does not automatically justify a universal numerical cutoff.

## Provenance classes

- `normative` — fixed by a protocol/standard or mathematical value domain.
- `literature-derived` — the cited source explicitly defines or recommends the cutoff for the same construct.
- `empirically-calibrated` — selected from a declared calibration/reference procedure.
- `scenario-configured` — supplied by the experiment, service, slice, capture, or deployment context.
- `framework-default` — a convenience default retained for reproducibility; no universal scientific claim is made.
- `eligibility` — minimum data required to run or interpret a statistic; not a realism threshold.

## Reviewed decision policies

| Area | Measured quantity | Default decision policy | Current provenance | Changes metric equation? | Audit status |
|---|---|---|---|---|---|
| Flow-duration consistency | consistent checked rows / checked rows | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata |
| Packet-byte consistency | consistent checked rows / checked rows | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata |
| TCP-flag consistency | consistent checked rows / checked rows | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata |
| Tabular handshake plausibility | plausible checked TCP rows / checked TCP rows | PASS >= 0.95; WARN >= 0.80 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata |
| Legacy PCAP protocol-validity profile | structurally valid checked packets / checked IP packets | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata; compatibility profile |
| Derived-rate consistency | consistent checked rows / checked rows | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata; post-review metric |
| Service-port consistency | matching applicable rows / checked applicable rows | PASS >= 0.95; WARN >= 0.75 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata; scenario context required |
| Valid slice identifier | valid checked identifiers / checked identifiers | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata; vocabulary is scenario-configured |
| Slice identifier consistency | context-consistent applicable rows / checked applicable rows | PASS >= 0.99; WARN >= 0.95 | `framework-default` unless overridden | No | Explicit `decision_rule` metadata; rules are scenario-configured |
| Valid port range | valid checked ports / checked ports | FAIL when invalid ratio > 0.01; WARN on any remaining invalid or zero port | `framework-default` unless overridden | No | One-sided rule reports provenance |
| Legacy reserved-IP profile | legacy invalid/special-use profile | FAIL when invalid ratio > 0.01; otherwise contextual WARN | `framework-default` unless overridden | No | Compatibility-only rule reports provenance |

The numerical defaults above are retained to preserve historical behaviour. **They are not presented as literature-derived realism constants.** An experiment may override them and supply `threshold_provenance`, or replace them with an empirically calibrated policy before final evaluation.

## Normative and scenario constraints

| Constraint | Classification | Interpretation |
|---|---|---|
| Port integer range 0..65535 | `normative` | Protocol field value domain; it defines validity, not a realism-score cutoff. |
| Non-negative duration, rate, byte, packet, and similar quantities where semantically required | `normative/domain constraint` | Negative values are structurally incompatible with the represented quantity. |
| Observable TCP state/directional contradictions inside captured evidence | `normative/protocol interpretation` | The packet-level handshake evaluator uses protocol-state evidence rather than a universal percentage cutoff. |
| Declared slice identifiers, service populations, expected ports, and mapping rules | `scenario-configured` | These depend on the experiment/network design rather than a universal public vocabulary. |
| Reference population and preprocessing for comparative metrics | `scenario-configured/experimental-design` | Interpretation is only meaningful relative to the declared reference and preprocessing chain. |

## Measurement operationalisation parameters

These values are **not merely verdict thresholds**. They can change whether an observation is counted as consistent and therefore alter the measured ratio itself.

| Metric | Parameter | Default | Current provenance | Scientific treatment |
|---|---|---:|---|---|
| Flow-duration consistency | `tolerance` | 1e-6 | `framework-default` unless overridden | Emitted as `measurement_parameters`; justify against units/exporter precision or calibration. |
| Packet-byte consistency | `tolerance` | 1e-6 | `framework-default` unless overridden | Emitted as `measurement_parameters`; numerical-comparison tolerance. |
| Packet-byte consistency | `variance_tolerance` | 1e-3 | `framework-default` unless overridden | Emitted as `measurement_parameters`; affects variance-vs-standard-deviation consistency. |
| Derived-rate consistency | `relative_tolerance` | 0.02 | `framework-default` unless overridden | Emitted as `measurement_parameters`; should be tied to exporter rounding/precision or calibration. |
| Derived-rate consistency | `absolute_tolerance` | 1e-6 | `framework-default` unless overridden | Emitted as `measurement_parameters`; protects comparisons near zero. |

Each listed tolerance now carries its value, provenance, source, and the marker `measurement_operationalisation_parameter` in metric output. This prevents a tolerance from being mistaken for a universally accepted realism boundary.

## Eligibility rules

Minimum sample sizes, minimum runnable fields, minimum day counts, and lag-pair requirements are treated as **eligibility/reliability conditions**, not realism thresholds. They determine whether sufficient evidence exists to calculate or interpret a metric; they should not be described as PASS/WARN/FAIL realism boundaries.

## Canonical metrics without universal verdict cutoffs

Most statistical, temporal, slice, label, and reference-comparison leaves report a value or distance without embedding a universal realism threshold. This is intentional where the literature establishes a statistic or characteristic of real traffic but does not establish a transferable cutoff. Interpretation should instead use one or more of:

- a declared reference distribution;
- perturbation/calibration experiments;
- scenario or service requirements;
- confidence intervals or empirical reference ranges;
- a separately justified decision model.

## Relationship to the paper audit

The paper-side metric evidence chain remains:

`literature/standard -> realism relationship -> metric definition -> equation -> implementation -> oracle verification -> interpretation`

Threshold provenance is a separate layer attached after the metric definition unless the threshold is genuinely part of the operationalised measurement. The paper must therefore avoid wording that implies a mathematical-definition source, RFC, or real-traffic study endorses a particular PASS/WARN/FAIL cutoff unless that source explicitly does so.

## Before final experiments

1. Preserve current framework defaults only as reproducibility baselines.
2. Decide which experiment-facing cutoffs will be empirically calibrated and document the calibration population and procedure.
3. Declare units/exporter precision for measurement tolerances and override defaults where required.
4. Record every threshold and tolerance, together with its provenance, in run output.
5. Re-run representative bucket datasets and compare outcomes with the pre-overhaul exploratory runs.
6. Freeze the metric contract only after any calibrated thresholds and changed interpretations have been documented.
