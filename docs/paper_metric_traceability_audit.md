# Paper-to-Code Metric Traceability Audit

**Audit date:** 2026-09-13  
**Implementation baseline:** `overhaul/metric-conformance` at `022378d869d6a75827fdc9b17720b2d275f8c838`  
**Paper baseline:** `sudo-rpaisley/paper-draft`, branch `overhaul/metric-conformance`, observed at `54422307d4ec893b8511bd7079b7a5f173823657`  
**Audit/fix branch:** `audit/paper-metric-traceability`

## Purpose

This audit checks whether the metric implementation used by CBR-Tests measures the constructs and equations described by the companion paper. It is intentionally stricter than a conventional software review. A metric is not considered conformant merely because it runs, has unit tests, or produces plausible values. The audit considers the complete chain:

`taxonomy leaf -> construct -> equation -> input population -> denominator -> implementation -> result semantics -> oracle test -> interpretation`

Decision thresholds are treated separately from mathematical definitions. Historical compatibility behaviour is also distinguished from the canonical metric contract intended for the final experiments.

## Executive assessment

The `overhaul/metric-conformance` branch is a substantial improvement over `main` and resolves the major construct mismatches previously present in the codebase. In particular, intrinsic diagnostics are now separated from candidate-versus-reference comparisons, dependency-only profiles are no longer described as reference deviations, zero-denominator cases generally return unavailable evidence rather than artificial perfect scores, and contextual policies are exposed explicitly.

The overhaul is therefore suitable as the basis for the final research implementation, but it should **not yet be frozen for the final experiments**. This audit identified several remaining contract issues. Three have been corrected on the audit branch; four require either an implementation/applicability decision or a paper clarification before the experiment configuration is frozen.

### Current disposition

| Severity | Finding | Status |
| --- | --- | --- |
| High | Intrinsic statistical drift sampled only an early leading window on large datasets | **Fixed on audit branch** |
| High | Canonical Valid Port Range Ratio could redefine the normative port domain | **Fixed on audit branch** |
| High | New slice plans could inherit a legacy missing-as-invalid denominator | **Fixed on audit branch** |
| High | Several automatically adapted PCAP metrics are decoder/self-derived and therefore non-independent | **Open — experiment blocker** |
| Medium/High | Generic Missing Value Ratio on the canonical PCAP view can treat structurally absent fields as data-quality defects | **Open — experiment blocker for generic PCAP use** |
| Medium | Multi-field train/test identifier contamination needs an explicit field-qualified identifier universe in the paper | **Open — paper clarification** |
| Medium | TCP Flag Consistency wording can be read as a full TCP state-machine check, while implementation checks aggregate flag-count invariants | **Open — paper clarification** |
| Medium | Overlapping slice-consistency rules use union semantics, which can make conflicting rules more permissive | **Open — freeze decision required** |

## What is already conformant

The following groups were checked against their equations, implementation contracts, result semantics, and available oracle tests. No experiment-blocking equation/implementation discrepancy was identified in these groups apart from the specific exceptions documented later.

| Metric family | Audit result | Notes |
| --- | --- | --- |
| Address validity | Conformant | Missing/invalid values are separated from checked denominators; reserved-address misuse is context-policy dependent. |
| Temporal consistency | Conformant | Parse success, start/end ordering, and non-negative duration use explicit evaluable populations and return unavailable evidence on empty denominators. |
| Temporal structure diagnostics | Conformant after sampling fix | Intrinsic temporal diagnostics are correctly separated from reference comparison. |
| Statistical internal drift | Conformant after sampling fix | KS, Wasserstein, Energy, and MMD² arithmetic is consistent with the documented estimator; the population/sampling rule required correction. |
| Dependency profiles | Conformant | Pearson, Spearman, and distance-correlation outputs are candidate-only profiles rather than falsely labelled reference deviations. |
| Data quality | Conformant for tabular data | Missingness and duplication equations match the tabular implementation; PCAP applicability is a separate issue. |
| Slice representation | Conformant/contextual | Expected slices/classes and exclusivity assumptions are explicit. |
| Label fidelity | Largely conformant | Split-overlap, label coverage and temporal-label metrics match their stated populations; multi-field identifier notation needs clarification. |
| Reference model comparison | Conformant | Candidate and reference populations are distinct; shared/evaluable fields and pair counts are retained; oracle tests include hand-calculated expected values. |
| Task-based validation | Conformant | Accuracy, precision, recall and F1 use standard formulae; binary class-dependent metrics require an explicit positive label and preserve undefined denominators as unavailable. |
| Result semantics | Conformant | Execution status, applicability, metric value, context and decision/verdict are separated. |
| Threshold provenance | Conformant structurally | Thresholds are represented as decision policy rather than embedded into the mathematical metric definition. Scientific justification remains an experiment-design/documentation obligation. |

## Resolved findings

### F1 — Intrinsic statistical drift did not sample the full ordered population

**Severity:** High  
**Affected metrics:**

- `feature_ks_internal_drift`
- `feature_wasserstein_internal_drift`
- `feature_energy_internal_drift`
- `feature_mmd2_internal_drift`

The canonical construct compares the first and second halves of the complete usable ordered sequence. The previous implementation first truncated the usable feature values to `2 * max_sample_size` and only then split them. On a sufficiently large dataset, a distributional change occurring later in the capture could therefore be completely invisible.

The audit branch now:

1. constructs the two populations from the complete usable ordered sequence;
2. applies deterministic evenly spaced sampling independently within each half when a computational cap is required;
3. records full population counts, sampled counts, the sampling method, and the per-half cap in the result; and
4. includes a regression oracle where the first 2,000 observations are `0` and the second 2,000 are `100`; a capped KS calculation must still return `1.0`.

This change preserves computational control without silently changing the scientific population being compared.

### F2 — Canonical port validity could be changed into a scenario-specific range test

**Severity:** High  
**Affected metric:** `valid_port_range_profile`

The paper defines the canonical Valid Port Range Ratio over the normative 16-bit transport-port namespace:

`0 <= p <= 65535`.

The implementation previously accepted `valid_min_port` and `valid_max_port`, allowing a plan to redefine the canonical concept of a valid port while retaining the same metric ID. For example, a plan could make port 0 or dynamic/private ports mathematically "invalid" even though they remain inside the protocol namespace.

The audit branch now rejects a non-`0..65535` canonical validity range with `noncanonical_metric_configuration`. Scenario/application expectations belong in `service_port_consistency_profile` instead. A dedicated conformance test now verifies both the normative ratio and rejection of sub-range redefinition.

### F3 — Generated plans could inherit the legacy slice missing-value denominator

**Severity:** High  
**Affected metrics:**

- `valid_slice_identifier_profile`
- `slice_identifier_consistency_profile`

The canonical equations exclude missing identifiers from `N_checked`. The runtime intentionally retains `missing_policy: count_invalid` so historical outcomes can be replayed, but old saved plan templates could previously seed newly generated plans with this legacy policy.

The plan catalogue now sanitises new slice templates to:

`missing_policy: exclude_missing`

while leaving historical saved plans and compatibility execution unchanged. A regression test verifies that newly generated templates cannot silently inherit the legacy denominator.

## Open findings that must be addressed before final experiments

### F4 — PCAP decoder-derived validity metrics are not independent evidence

**Severity:** High  
**Status:** Open / experiment blocker

The automatic PCAP adapter creates a canonical tabular packet view from Scapy-decoded protocol objects. Several metrics then test fields that the adapter has already normalised or validated while decoding. This produces measurements that may be mathematically correct but scientifically tautological.

#### Valid IP Address Ratio

The adapter includes rows only for decoded IPv4/IPv6 packets and obtains source/destination address strings directly from the decoded IP layer. Testing those adapter-produced strings for syntactic IP validity does not independently test source-capture realism.

#### Valid Port Range Ratio

Transport ports are obtained from decoded TCP/UDP layer integer fields. A normal decoded packet view cannot provide independent evidence that those decoder-produced values lie inside the 16-bit port namespace.

#### Timestamp Parse Success Ratio

The canonical packet view stores the timestamp as a converted numeric value derived from `packet.time`. Running a parse-success ratio over that already-converted value primarily validates the adapter rather than the original capture timestamp representation.

**Required resolution:** these metrics should not contribute independent PCAP realism evidence when their input is solely the canonical decoder-produced packet view. The preferred solution is to classify them as self-derived/non-independent for automatic PCAP plans while retaining the handlers for tabular datasets and any explicit raw-field workflow where independence can be demonstrated.

Any PCAP report should make this distinction visible rather than returning apparently perfect evidence by construction.

### F5 — Generic missingness on the canonical PCAP view conflates structural absence with data quality

**Severity:** Medium/High  
**Status:** Open / experiment blocker for generic PCAP use

The packet view is heterogeneous. Fields such as transport ports and TCP flags are structurally inapplicable to some packets:

- non-TCP/UDP packets do not have transport ports;
- UDP/non-TCP packets do not have TCP flags;
- the first packet in a sequence has no prior inter-arrival interval.

A generic cell-level Missing Value Ratio therefore risks treating correct protocol structure as a data-quality defect. This violates the intended interpretation that lower missingness represents better data quality unless the expected-cell population has first been restricted to semantically applicable fields/rows.

**Required resolution:** either:

1. define an applicability-aware expected-cell denominator for PCAP fields; or
2. mark the generic missing-value metric as non-independent/not-applicable for the canonical PCAP packet view and use protocol-specific completeness checks instead.

This decision must be fixed before the experiment plans are generated.

### F6 — Multi-field identifier contamination requires a field-qualified mathematical definition

**Severity:** Medium  
**Status:** Open / paper clarification

`train_test_identifier_contamination_ratio` can accept multiple identifier fields. The implementation evaluates corresponding fields and aggregates their counts. The paper notation currently uses a single `I_train` and `I_test`, which can be read as one unqualified value set.

That is ambiguous when the same textual identifier appears in different namespaces, for example a device identifier field and a user/session identifier field.

**Required resolution:** define the identifier universe as field-qualified identifiers, e.g. `(field, value)`, or explicitly define a per-field macro-average/pooled count equation. The implementation can then be checked against the chosen definition without ambiguity.

### F7 — TCP Flag Consistency is an aggregate-flow invariant, not a full TCP state-machine validator

**Severity:** Medium  
**Status:** Open / paper clarification

The implementation checks deterministic consistency of aggregate TCP flag/count fields and deliberately does not classify all unusual packet-level combinations as invalid. This is appropriate for security datasets because SYN+FIN, SYN+RST and similar traffic can be genuine attack/scanning evidence rather than synthetic-data defects.

The paper should therefore describe this leaf as **aggregate TCP flag-count consistency** (for flow/tabular representations) and avoid wording that implies reconstruction and validation of the complete TCP connection state machine. Packet-sequence/state-transition validity should be a separate metric if later added.

### F8 — Overlapping slice-consistency rules currently use union semantics

**Severity:** Medium  
**Status:** Open / experiment-contract decision

When more than one slice-consistency rule matches the same row, the implementation unions all expected slice IDs. This behaviour is documented, so it is not currently a paper/code mismatch, but it can make contradictory overlapping rules more permissive.

Before freezing experiments, choose one explicit policy:

- retain union semantics and state that matching rules represent alternative permitted assignments; or
- reject conflicting overlapping rules as an invalid experiment configuration.

For a research framework, configuration rejection is safer if the rules are intended to encode normative constraints rather than alternative policies.

## PCAP applicability audit

The PCAP adapter already handles an important class correctly: flow-level invariants that are reconstructed from the same packets are marked self-derived/non-independent rather than treated as independent realism evidence. The same scientific principle should now be extended consistently.

### Recommended automatic PCAP evidence policy

| Metric / family | Automatic PCAP status | Rationale |
| --- | --- | --- |
| Direct packet handshake plausibility | Applicable | Uses packet sequence/state evidence directly from the capture. |
| Timestamp coherence direct metric | Applicable | Can inspect source packet timing/coherence rather than a derived flow invariant. |
| Valid IP Address Ratio on canonical packet view | **Non-independent** | Addresses are decoder-produced from already-decoded IP packets. |
| Valid Port Range Ratio on canonical packet view | **Non-independent** | Ports are decoder-produced TCP/UDP integers. |
| Timestamp Parse Success Ratio on canonical packet view | **Non-independent** | Timestamp field is already converted by adapter. |
| Generic Missing Value Ratio | **Do not use without applicability-aware denominator** | Protocol-dependent structural absence is expected. |
| Flow duration / packet-byte / derived-rate invariants reconstructed from same packet view | Non-independent | Already protected by self-derived PCAP policy. |
| Statistical/internal-drift metrics on genuinely observed packet attributes | Applicable with declared ordering/sampling | These describe capture structure rather than re-validating adapter conversions. |
| Reference-comparison metrics | Applicable only with an eligible reference and matched representation | Comparator eligibility and field comparability remain mandatory. |

The final experiment manifest should record whether each result is `independent_observation`, `derived_diagnostic`, `self_derived_nonindependent`, `context_required`, or `not_applicable`.

## Result and decision semantics

The overhaul correctly separates three concepts that must remain separate in the paper and experiment exports:

1. **Execution/applicability:** could the metric be evaluated on this dataset under the declared contract?
2. **Metric value/evidence:** what value and supporting counts were observed?
3. **Decision/verdict:** under an explicitly justified policy, does that value trigger pass/warn/fail?

A successful function call is not evidence that a dataset passed a realism criterion, and `not_applicable`/`insufficient_evidence` must not be coerced into a good score.

## Threshold and tolerance requirements

The implementation now records threshold provenance structurally, but the final experimental protocol must freeze the substantive justification for each threshold/tolerance before results are inspected. Each threshold should be classified as one of:

- normative/standard-derived;
- mathematically necessary;
- measurement/exporter tolerance;
- scenario/deployment policy;
- empirically calibrated on an independent development set; or
- exploratory, in which case it must not be presented as a validated universal boundary.

Thresholds must not be retrofitted to improve perturbation outcomes.

## Canonical versus supporting metrics

The experiment/reporting pipeline must continue to distinguish the canonical paper taxonomy from supporting or post-review diagnostics. In particular, metrics such as `derived_rate_consistency_profile`, `timestamp_coherence_profile`, and `column_quality_profile` may be valuable implementation diagnostics but must not silently become canonical taxonomy leaves unless the paper taxonomy and expert-review record are updated accordingly.

## Required paper updates from this audit

Before the paper is frozen for experiments:

1. document the full-half then within-half deterministic sampling rule for capped intrinsic distribution metrics;
2. state explicitly that Valid Port Range Ratio uses the immutable `0..65535` protocol namespace and that service-specific ranges belong to Service-Port Consistency;
3. define multi-field train/test identifiers using a field-qualified or otherwise explicit aggregation model;
4. tighten TCP Flag Consistency wording to aggregate flag-count invariants where applicable;
5. document the PCAP independence/applicability policy, including decoder-derived and structurally missing fields; and
6. make clear that supporting diagnostics do not automatically count as canonical realism evidence.

## Pre-experiment freeze checklist

The final experiments should not begin until all of the following are true:

- [ ] F4 PCAP decoder-derived metrics are excluded from independent evidence or given a defensible raw-field implementation.
- [ ] F5 PCAP missingness receives an applicability-aware denominator or is excluded from canonical PCAP evidence.
- [ ] F6 identifier-contamination notation is made unambiguous in the paper.
- [ ] F7 TCP flag metric scope is stated precisely in the paper.
- [ ] F8 slice-rule overlap semantics are frozen and tested.
- [ ] The paper metric catalogue and runtime taxonomy have a one-to-one canonical binding for every final leaf.
- [ ] Every canonical metric has at least one independent hand-calculated or trusted-library oracle, plus boundary/undefined-denominator tests where applicable.
- [ ] Every reference metric records reference identity/hash and field mapping/comparability information.
- [ ] Every threshold/tolerance used for a verdict has provenance recorded before experiment execution.
- [ ] Generated experiment plans use canonical policies rather than historical compatibility settings.
- [ ] PCAP, flow and tabular applicability are frozen per metric.
- [ ] The exact CBR-Tests commit and paper commit are tagged/frozen before the first final run.

## Audit conclusion

The metric-conformance overhaul has corrected the largest scientific-contract problems in the original implementation. The remaining defects are narrower and, importantly, identifiable: population sampling, canonical parameter domains, legacy denominator leakage, and representation-dependent PCAP evidence. The first three are corrected on this audit branch. The PCAP independence/missingness issues and the three paper/contract clarifications should be closed before the implementation is tagged for the final perturbation experiments.

Once those items are resolved and CI/oracle tests pass on the resulting commit, the codebase will be in a substantially stronger position for a defensible pre-registered-style experimental freeze.