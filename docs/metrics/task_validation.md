# Task-based validation metrics

This page documents every dispatcher metric in the **Task-based validation** category. Return to the [complete metric index](../metric_reference.md).

These metrics are **downstream task evidence**, not direct traffic-realism scores. Their interpretation depends on a fixed benchmark model, evaluation population and split protocol. Rows with blank true labels or predictions are excluded from the task metric and the resulting evaluation coverage must be reported.

## `benchmark_model_accuracy`

Computes exact label/prediction agreement over complete evaluation pairs.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `prediction_field`.
- **Primary output:** Evaluated/correct counts, evaluation coverage and accuracy.
- **Semantics:** Exact label match; naturally supports binary or multiclass labels.
- **Interpretation:** Higher is better for the declared benchmark task, but accuracy is not itself evidence that the traffic is realistic.
- **Caveat:** Accuracy can conceal poor minority-class performance and can be inflated by leakage or unrepresentative test data.

## `benchmark_model_precision`

Computes positive-class precision using an explicitly configured one-vs-rest positive label.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `label_field`, `prediction_field`, and required `positive_label`.
- **Primary output:** Confusion counts and precision or null when undefined.
- **Semantics:** `positive_label` versus all other non-missing labels/predictions. The positive class is never inferred from lexical ordering or from the evaluated data.
- **Interpretation:** Higher means fewer predicted positives are false for the declared positive class.
- **Caveat:** If no positive predictions exist, precision is undefined and the metric returns null rather than an artificial zero.

## `benchmark_model_recall`

Computes positive-class recall using the same explicitly configured one-vs-rest positive label.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same as precision; `positive_label` is required.
- **Primary output:** Confusion counts and recall or null when undefined.
- **Semantics:** `positive_label` versus all other non-missing labels/predictions.
- **Interpretation:** Higher means fewer actual positives are missed for the declared positive class.
- **Caveat:** If no actual positive observations exist, recall is undefined and the metric returns null.

## `benchmark_model_f1_score`

Computes positive-class F1 using the confusion-count form
`2 TP / (2 TP + FP + FN)`.

- **Implementation:** `cbr_tests/metrics/task_validation.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** Same as precision; `positive_label` is required.
- **Primary output:** Confusion counts and F1 or null when the positive-class problem has no support at all.
- **Semantics:** Explicit positive-label one-vs-rest F1. A valid case with `TP=0`, `FP>0` and `FN>0` correctly returns `0.0`.
- **Interpretation:** Higher indicates a better precision/recall balance for the declared positive class.
- **Caveat:** F1 is not a calibrated probability measure, ignores true negatives, and does not establish traffic realism or deployment generalisation.
