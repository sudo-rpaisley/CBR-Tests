# Label integrity metrics

This page documents every dispatcher metric in the **Label integrity** category. Return to the [complete metric index](../metric_reference.md).

## `attack_window_alignment_score`

Checks whether attack labels agree with configured timestamp windows.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `timestamp_field`, `attack_label_values`, `attack_windows`.
- **Primary output:** Checked/aligned counts and alignment ratio.
- **Interpretation:** Higher means labels agree with the configured temporal ground truth.
- **Current caveat:** A wrong or incomplete window definition can make a correct dataset look bad or vice versa.

## `class_imbalance_score`

Measures the difference between largest and smallest class proportions.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `label_field`, optional `expected_classes`.
- **Primary output:** Class counts/proportions and imbalance score.
- **Interpretation:** Lower is more balanced.
- **Current caveat:** When expected classes are omitted, classes absent from the whole dataset cannot affect the score.

## `label_coverage_ratio`

Measures rows with a nonblank normalized label.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** `deepsecure_plan`, `secure5g_plan`
- **Inputs:** `label_field`, default `label`.
- **Primary output:** Labelled/missing counts and coverage ratio.
- **Interpretation:** Higher is better.
- **Current caveat:** Whitespace-only values are missing; arbitrary strings are accepted as labels.

## `per_slice_label_coverage_ratio`

Computes label coverage separately for each slice and averages it.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `slice_field`.
- **Primary output:** Per-slice counts/ratios and mean ratio.
- **Interpretation:** Higher indicates consistent label availability.
- **Current caveat:** Missing label or slice columns return an empty result with score 0 rather than an execution failure.

## `per_slice_label_entropy_score`

Computes normalized class entropy within each slice.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `slice_field`, optional `expected_classes`.
- **Primary output:** Per-slice entropy and mean entropy.
- **Interpretation:** 1 means balanced across expected classes; 0 means empty or single-class.
- **Current caveat:** High entropy is not always desirable and does not imply labels are correct.

## `pre_post_attack_label_bleed_ratio`

Measures attack labels just outside configured attack windows.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same attack fields; `bleed_window_seconds` default 60.
- **Primary output:** Boundary row/bleed counts and ratio.
- **Interpretation:** Lower suggests cleaner temporal boundaries.
- **Current caveat:** Only rows within the configured boundary distance and outside windows are in the denominator.

## `train_test_duplicate_overlap_ratio`

Computes Jaccard overlap of unique row keys between train and test splits.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `split_field`, train/test values, optional `subset_fields`.
- **Primary output:** Train/test/overlap key counts and intersection-over-union ratio.
- **Interpretation:** Lower means fewer exact duplicate keys cross the split.
- **Current caveat:** Default key uses every column except split; near-duplicates are not detected.

## `train_test_identifier_contamination_ratio`

Measures identifiers present in both train and test sets.

- **Implementation:** `tests/label_fidelity_profile.py` (production implementation awaiting migration)
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `split_field`, train/test values, `identifier_fields`.
- **Primary output:** Per-field and aggregate contaminated/unique identifier ratios.
- **Interpretation:** Lower means stronger entity separation.
- **Current caveat:** Multiple identifier fields are aggregated by summing counts, not by constructing a unified entity identity.
