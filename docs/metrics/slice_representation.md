# Slice representation metrics

This page documents every dispatcher metric in the **Slice representation** category. Return to the [complete metric index](../metric_reference.md).

## `cross_slice_duplicate_overlap_ratio`

Finds records with the same selected key appearing in more than one slice.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`; optional `subset_fields`.
- **Primary output:** Overlap row/group counts and overlap rows divided by all rows.
- **Interpretation:** Lower indicates better slice isolation.
- **Current caveat:** Default subset uses every column except slice, which may be too strict or include volatile fields.

## `cross_slice_identifier_leakage_ratio`

Measures identifiers that appear in multiple slices.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`, `identifier_fields`.
- **Primary output:** Per-field and aggregate leaked/unique identifier counts and ratios.
- **Interpretation:** Lower indicates better separation of entities across slices.
- **Current caveat:** Aggregating multiple identifier fields sums their universes; the result is not a deduplicated cross-field entity ratio.

## `per_slice_class_coverage_ratio`

Checks whether each slice contains expected label classes.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`, `label_field`, optional `expected_classes`.
- **Primary output:** Per-slice missing classes and mean class coverage.
- **Interpretation:** Higher means every slice represents more of the target classes.
- **Current caveat:** If expected classes are omitted they are inferred globally, which can hide classes absent from the entire dataset.

## `per_slice_feature_coverage_ratio`

Checks whether each slice contains at least one non-null value for each candidate feature.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`, `candidate_fields`.
- **Primary output:** Per-slice missing features and mean coverage ratio.
- **Interpretation:** Higher means feature availability is more consistent across slices.
- **Current caveat:** Presence is binary; it does not assess sample count, distribution, or quality within the feature.

## `per_slice_sample_coverage_ratio`

Checks which expected slice IDs are present.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`; optional `expected_slice_ids`.
- **Primary output:** Per-slice sample counts and covered/expected ratio.
- **Interpretation:** Higher means more expected slices are represented.
- **Current caveat:** When no expected list is supplied, observed slices become the target and the ratio is normally 1.

## `slice_distribution_imbalance_score`

Measures the gap between largest and smallest observed slice proportions.

- **Implementation:** `tests/slice_representation_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `secure5g_plan`
- **Inputs:** `slice_field`.
- **Primary output:** Per-slice counts/proportions and max-minus-min imbalance.
- **Interpretation:** Lower is more balanced; 0 means equal observed proportions.
- **Current caveat:** Only observed slices are included; completely absent expected slices require the coverage metric.
