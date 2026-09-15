from __future__ import annotations

import pandas as pd


def _normalise(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _fields(metric: dict) -> tuple[str, str]:
    requirements = metric.get("input_requirements", {})
    return (
        requirements.get("label_field", "label"),
        requirements.get("prediction_field", "prediction"),
    )


def _positive_label(metric: dict) -> str | None:
    """Return only an explicitly configured positive label.

    Precision, recall and F1 are positive-class, one-vs-rest measures in the
    canonical taxonomy. The framework must therefore never infer the positive
    class from lexical ordering or from the candidate data itself.
    """

    value = metric.get("input_requirements", {}).get("positive_label")
    return _normalise(value) if value is not None else None


def _confusion_counts(df: pd.DataFrame, metric: dict) -> dict:
    label_field, prediction_field = _fields(metric)
    base = {
        "label_field": label_field,
        "prediction_field": prediction_field,
        "row_count": int(len(df)),
        "evaluated_count": 0,
        "excluded_pair_count": int(len(df)),
        "evaluation_coverage_ratio": 0.0 if len(df) else None,
        "correct_count": 0,
        "true_positive": 0,
        "false_positive": 0,
        "false_negative": 0,
        "true_negative": 0,
        "positive_label": _positive_label(metric),
        "positive_label_configured": _positive_label(metric) is not None,
        "binary_evaluation_mode": "one_vs_rest",
        "observed_label_values": [],
        "observed_prediction_values": [],
    }
    if label_field not in df.columns or prediction_field not in df.columns:
        return base

    pairs: list[tuple[str, str]] = []
    for _, row in df[[label_field, prediction_field]].iterrows():
        label = _normalise(row[label_field])
        prediction = _normalise(row[prediction_field])
        if label is not None and prediction is not None:
            pairs.append((label, prediction))

    evaluated_count = len(pairs)
    positive_label = _positive_label(metric)
    counts = {
        **base,
        "evaluated_count": evaluated_count,
        "excluded_pair_count": int(len(df)) - evaluated_count,
        "evaluation_coverage_ratio": (
            round(evaluated_count / len(df), 6) if len(df) else None
        ),
        "correct_count": sum(label == prediction for label, prediction in pairs),
        "positive_label": positive_label,
        "positive_label_configured": positive_label is not None,
        "observed_label_values": sorted({label for label, _ in pairs}),
        "observed_prediction_values": sorted({prediction for _, prediction in pairs}),
    }
    if positive_label is not None:
        for label, prediction in pairs:
            label_positive = label == positive_label
            prediction_positive = prediction == positive_label
            if label_positive and prediction_positive:
                counts["true_positive"] += 1
            elif not label_positive and prediction_positive:
                counts["false_positive"] += 1
            elif label_positive and not prediction_positive:
                counts["false_negative"] += 1
            else:
                counts["true_negative"] += 1
    return counts


def compute_benchmark_model_accuracy(df: pd.DataFrame, metric: dict) -> dict:
    """Compute exact-match accuracy over complete label/prediction pairs.

    Accuracy is naturally valid for binary or multiclass labels. Rows with a
    missing/blank true label or prediction are excluded from the metric and the
    evaluation coverage is reported separately.
    """

    counts = _confusion_counts(df, metric)
    value = (
        counts["correct_count"] / counts["evaluated_count"]
        if counts["evaluated_count"]
        else None
    )
    counts["benchmark_model_accuracy"] = round(value, 6) if value is not None else None
    counts["evaluation_mode"] = "exact_label_match"
    counts["runnable"] = counts["evaluated_count"] > 0
    counts["unavailable_reason"] = None if counts["runnable"] else "no_complete_label_prediction_pairs"
    return {"summary": counts}


def compute_benchmark_model_precision(df: pd.DataFrame, metric: dict) -> dict:
    """Compute positive-class precision using an explicit one-vs-rest label."""

    counts = _confusion_counts(df, metric)
    counts["evaluation_mode"] = "explicit_positive_label_one_vs_rest"
    if counts["positive_label"] is None:
        counts["benchmark_model_precision"] = None
        counts["runnable"] = False
        counts["unavailable_reason"] = "positive_label_required"
        return {"summary": counts}

    denominator = counts["true_positive"] + counts["false_positive"]
    value = counts["true_positive"] / denominator if denominator else None
    counts["benchmark_model_precision"] = round(value, 6) if value is not None else None
    counts["runnable"] = denominator > 0
    counts["unavailable_reason"] = None if denominator else "no_predicted_positives"
    return {"summary": counts}


def compute_benchmark_model_recall(df: pd.DataFrame, metric: dict) -> dict:
    """Compute positive-class recall using an explicit one-vs-rest label."""

    counts = _confusion_counts(df, metric)
    counts["evaluation_mode"] = "explicit_positive_label_one_vs_rest"
    if counts["positive_label"] is None:
        counts["benchmark_model_recall"] = None
        counts["runnable"] = False
        counts["unavailable_reason"] = "positive_label_required"
        return {"summary": counts}

    denominator = counts["true_positive"] + counts["false_negative"]
    value = counts["true_positive"] / denominator if denominator else None
    counts["benchmark_model_recall"] = round(value, 6) if value is not None else None
    counts["runnable"] = denominator > 0
    counts["unavailable_reason"] = None if denominator else "no_actual_positives"
    return {"summary": counts}


def compute_benchmark_model_f1_score(df: pd.DataFrame, metric: dict) -> dict:
    """Compute positive-class F1 using an explicit one-vs-rest label.

    The confusion-count form is used so a valid case with TP=0, FP>0 and FN>0
    returns F1=0 rather than becoming undefined through a 0/0 precision/recall
    harmonic-mean expression.
    """

    counts = _confusion_counts(df, metric)
    counts["evaluation_mode"] = "explicit_positive_label_one_vs_rest"
    if counts["positive_label"] is None:
        counts["benchmark_model_f1_score"] = None
        counts["runnable"] = False
        counts["unavailable_reason"] = "positive_label_required"
        return {"summary": counts}

    denominator = (
        2 * counts["true_positive"]
        + counts["false_positive"]
        + counts["false_negative"]
    )
    value = 2 * counts["true_positive"] / denominator if denominator else None
    counts["benchmark_model_f1_score"] = round(value, 6) if value is not None else None
    counts["runnable"] = denominator > 0
    counts["unavailable_reason"] = (
        None if denominator else "no_positive_class_observations_or_predictions"
    )
    return {"summary": counts}
