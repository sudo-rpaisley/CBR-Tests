import pandas as pd


def _normalise(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _fields(metric: dict) -> tuple[str, str]:
    requirements = metric.get("input_requirements", {})
    return requirements.get("label_field", "label"), requirements.get("prediction_field", "prediction")


def _positive_label(metric: dict, labels: list[str]) -> str | None:
    value = metric.get("input_requirements", {}).get("positive_label")
    if value is not None:
        return _normalise(value)
    unique = sorted(set(labels))
    return unique[-1] if len(unique) == 2 else None


def _confusion_counts(df: pd.DataFrame, metric: dict) -> dict:
    label_field, prediction_field = _fields(metric)
    if label_field not in df.columns or prediction_field not in df.columns:
        return {
            "label_field": label_field,
            "prediction_field": prediction_field,
            "row_count": int(len(df)),
            "evaluated_count": 0,
            "correct_count": 0,
            "true_positive": 0,
            "false_positive": 0,
            "false_negative": 0,
            "true_negative": 0,
            "positive_label": None,
        }

    pairs = []
    for _, row in df[[label_field, prediction_field]].iterrows():
        label = _normalise(row[label_field])
        prediction = _normalise(row[prediction_field])
        if label is not None and prediction is not None:
            pairs.append((label, prediction))

    labels = [label for label, _ in pairs]
    positive_label = _positive_label(metric, labels)
    counts = {
        "label_field": label_field,
        "prediction_field": prediction_field,
        "row_count": int(len(df)),
        "evaluated_count": len(pairs),
        "correct_count": sum(1 for label, prediction in pairs if label == prediction),
        "true_positive": 0,
        "false_positive": 0,
        "false_negative": 0,
        "true_negative": 0,
        "positive_label": positive_label,
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


def _summary_with_metric(df: pd.DataFrame, metric: dict, metric_id: str, value: float | None) -> dict:
    counts = _confusion_counts(df, metric)
    counts[metric_id] = round(value, 6) if value is not None else None
    return {"summary": counts}


def compute_benchmark_model_accuracy(df: pd.DataFrame, metric: dict) -> dict:
    counts = _confusion_counts(df, metric)
    value = counts["correct_count"] / counts["evaluated_count"] if counts["evaluated_count"] else None
    counts["benchmark_model_accuracy"] = round(value, 6) if value is not None else None
    return {"summary": counts}


def compute_benchmark_model_precision(df: pd.DataFrame, metric: dict) -> dict:
    counts = _confusion_counts(df, metric)
    denominator = counts["true_positive"] + counts["false_positive"]
    value = counts["true_positive"] / denominator if denominator else None
    counts["benchmark_model_precision"] = round(value, 6) if value is not None else None
    return {"summary": counts}


def compute_benchmark_model_recall(df: pd.DataFrame, metric: dict) -> dict:
    counts = _confusion_counts(df, metric)
    denominator = counts["true_positive"] + counts["false_negative"]
    value = counts["true_positive"] / denominator if denominator else None
    counts["benchmark_model_recall"] = round(value, 6) if value is not None else None
    return {"summary": counts}


def compute_benchmark_model_f1_score(df: pd.DataFrame, metric: dict) -> dict:
    counts = _confusion_counts(df, metric)
    precision_denominator = counts["true_positive"] + counts["false_positive"]
    recall_denominator = counts["true_positive"] + counts["false_negative"]
    precision = counts["true_positive"] / precision_denominator if precision_denominator else None
    recall = counts["true_positive"] / recall_denominator if recall_denominator else None
    value = 2 * precision * recall / (precision + recall) if precision is not None and recall is not None and (precision + recall) else None
    counts["benchmark_model_f1_score"] = round(value, 6) if value is not None else None
    return {"summary": counts}
