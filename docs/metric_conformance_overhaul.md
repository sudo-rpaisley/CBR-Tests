# Metric conformance overhaul

This branch is the working branch for bringing the executable CBR-Tests metric suite into explicit agreement with the paper taxonomy and its equations.

## Scientific source of truth

The companion `sudo-rpaisley/paper-draft` branch `overhaul/metric-conformance` contains the metric leaf definitions. For every metric, the audit must preserve a traceable chain:

`literature/standard -> metric definition -> equation -> implementation -> oracle test -> interpretation`

A passing unit test is not sufficient evidence of conformance if the test merely reproduces the implementation. Each metric must have at least one hand-computable oracle case derived from the written equation.

## Conformance states

- `conformant`: implementation and oracle tests match the written equation and interpretation.
- `partial`: implementation captures only part of the defined metric or omits required context.
- `mismatch`: implementation computes a materially different quantity.
- `profile-only`: implementation produces descriptive evidence but does not itself compute the taxonomy leaf named as a score/deviation.
- `not-implemented`: taxonomy leaf has no executable implementation.
- `decision-required`: more than one scientifically defensible definition exists and the taxonomy definition must be settled before code is changed.

## Rules for the overhaul

1. Do not change an equation merely to make existing code pass.
2. Do not change code merely to match an equation if the equation is not well supported by literature or standards.
3. Distinguish intrinsic dataset diagnostics from candidate-versus-reference comparison metrics.
4. Distinguish metric equations from PASS/WARN/FAIL thresholds. Thresholds require their own justification.
5. Preserve raw evidence counts alongside ratios and aggregate scores.
6. Treat scenario-dependent properties as contextual rather than universally good/bad.
7. For metrics with a reference dataset, record the reference identity and any field mapping, sampling, normalisation or preprocessing applied.
8. For classification metrics, make binary/multiclass averaging semantics explicit rather than inferring a positive class silently.

## Initial findings

| Taxonomy leaf | Current implementation | Initial status | Required resolution |
|---|---|---|---|
| Valid IP Address Ratio | `protocol_validity_profile` combines IP validity with ports, packet structure and protocol-layer checks | mismatch | Separate address-validity ratio from broader protocol-validity evidence |
| Reserved-Address Misuse Ratio | Measures configurable special-use categories and also reacts to invalid addresses | partial | Define denominator and policy semantics explicitly; separate invalid-address failure from misuse ratio |
| Valid Port Range Ratio | Valid non-missing integer ports divided by checked ports | conformant candidate | Verify edge cases and port 0 interpretation with oracle tests |
| KS Feature Divergence | Splits one candidate feature into first/second halves | mismatch | Recast as intrinsic stability diagnostic or move comparison definition to reference-model branch |
| Wasserstein Feature Distance | Splits one candidate feature into first/second halves | mismatch | Same resolution as KS |
| Energy Distance | Splits one candidate feature into first/second halves | mismatch | Same resolution as KS |
| Maximum Mean Discrepancy | Splits one candidate feature into first/second halves | mismatch | Same resolution as KS |
| Pearson Correlation Matrix Deviation | Computes candidate Pearson correlation profile only | profile-only | Rename/redefine intrinsic leaf or remove; reference-model branch already computes deviation |
| Spearman Correlation Matrix Deviation | Computes candidate Spearman profile only | profile-only | Same resolution as Pearson |
| Distance Correlation Matrix Deviation | Computes candidate distance-correlation profile only | profile-only | Same resolution as Pearson |
| IAT Distribution Divergence | KS between first and second halves of candidate IAT sequence | decision-required | Decide whether this is temporal stability or reference realism; avoid duplicating reference-model leaf |
| Burstiness Coefficient Deviation | Difference between burstiness of candidate halves | decision-required | Decide whether this is temporal stability or reference realism |
| Hourly Activity Distribution Divergence | Mean pairwise day-to-day total-variation distance within candidate | decision-required | Rename as day-to-day stability unless an external/expected profile is required |
| Diurnal Pattern Similarity Score | Mean pairwise cosine similarity of candidate days | decision-required | Define as intrinsic repeatability or compare against target profile |
| Per-Slice Sample Coverage Ratio | Proportion of expected slice IDs that are present | mismatch with current paper equation | Decide whether coverage means slice presence or minimum sample sufficiency; likely expose both |
| Per-Slice Feature Coverage Ratio | Fraction of expected fields with at least one non-null value in a slice | mismatch with current paper equation | Define cell-level completeness versus field-presence coverage explicitly |
| Attack Window Alignment Score | Overall binary agreement between attack-window state and label state | mismatch with narrow paper equation | Expose alignment accuracy plus attack-window precision/recall components |
| Train-Test Duplicate Overlap Ratio | Jaccard intersection/union of unique keys | mismatch | Decide intended denominator; paper currently uses test-set denominator |
| Train-Test Identifier Contamination Ratio | Intersection/union of identifiers | mismatch | Decide intended denominator; paper currently uses test-set denominator |
| Accuracy / Precision / Recall / F1 | Binary confusion counts; positive class may be inferred lexicographically | partial | Require explicit binary positive label or implement declared multiclass averaging |

## Work sequence

1. Build a 61-leaf conformance inventory linking taxonomy leaf IDs to code handlers and tests.
2. Resolve scientific definitions for all `mismatch`, `profile-only`, and `decision-required` leaves.
3. Add deterministic oracle tests for every equation.
4. Refactor metric implementations to match the approved definitions.
5. Run regression tests and compare behaviour against existing recorded outcomes.
6. Update paper equations, descriptions, adaptation notes and reference-role classifications where the scientific definition changes.
7. Freeze the audited metric contract before final perturbation experiments.
