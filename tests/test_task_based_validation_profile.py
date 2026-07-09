import pandas as pd

from tests.task_based_validation_profile import (
    compute_benchmark_model_accuracy,
    compute_benchmark_model_f1_score,
    compute_benchmark_model_precision,
    compute_benchmark_model_recall,
)


def test_benchmark_model_metrics_from_predictions():
    df = pd.DataFrame({
        "label": ["attack", "attack", "benign", "benign"],
        "prediction": ["attack", "benign", "attack", "benign"],
    })
    metric = {"input_requirements": {"label_field": "label", "prediction_field": "prediction", "positive_label": "attack"}}

    assert compute_benchmark_model_accuracy(df, metric)["summary"]["benchmark_model_accuracy"] == 0.5
    assert compute_benchmark_model_precision(df, metric)["summary"]["benchmark_model_precision"] == 0.5
    assert compute_benchmark_model_recall(df, metric)["summary"]["benchmark_model_recall"] == 0.5
    assert compute_benchmark_model_f1_score(df, metric)["summary"]["benchmark_model_f1_score"] == 0.5


def test_benchmark_model_metrics_ignore_missing_labels_or_predictions():
    df = pd.DataFrame({
        "label": ["attack", None, "benign"],
        "prediction": ["attack", "attack", None],
    })
    metric = {"input_requirements": {"positive_label": "attack"}}

    result = compute_benchmark_model_accuracy(df, metric)

    assert result["summary"]["row_count"] == 3
    assert result["summary"]["evaluated_count"] == 1
    assert result["summary"]["benchmark_model_accuracy"] == 1.0
