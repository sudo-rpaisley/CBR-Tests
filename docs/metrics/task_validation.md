# Task-based validation metrics

This page documents every dispatcher metric in the **Task-based validation** category. Return to the [complete metric index](../metric_reference.md).

## `benchmark_model_accuracy`

Computes exact label/prediction agreement.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `prediction_field`.
- **Primary output:** Evaluated/correct counts and accuracy.
- **Interpretation:** Higher is better.
- **Current caveat:** Rows with blank label or prediction are excluded; accuracy can conceal class imbalance.

## `benchmark_model_f1_score`

Computes the harmonic mean of binary precision and recall.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same as precision.
- **Primary output:** Confusion counts and F1 or null.
- **Interpretation:** Higher balances precision and recall.
- **Current caveat:** Binary only; undefined precision/recall or a zero sum yields null.

## `benchmark_model_precision`

Computes binary positive predictive value.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Label/prediction fields; optional `positive_label`.
- **Primary output:** Confusion counts and precision or null.
- **Interpretation:** Higher means fewer predicted positives are false.
- **Current caveat:** If positive label is omitted, exactly two observed classes are required and the lexicographically last becomes positive.

## `benchmark_model_recall`

Computes binary true-positive rate.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same as precision.
- **Primary output:** Confusion counts and recall or null.
- **Interpretation:** Higher means fewer actual positives are missed.
- **Current caveat:** Binary only; zero positive denominator yields null.
