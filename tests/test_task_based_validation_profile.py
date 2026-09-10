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
    metric = {
        "input_requirements": {
            "label_field": "label",
            "prediction_field": "prediction",
            "positive_label": "attack",
        }
    }

    assert compute_benchmark_model_accuracy(df, metric)["summary"]["benchmark_model_accuracy"] == 0.5
    assert compute_benchmark_model_precision(df, metric)["summary"]["benchmark_model_precision"] == 0.5
    assert compute_benchmark_model_recall(df, metric)["summary"]["benchmark_model_recall"] == 0.5
    assert compute_benchmark_model_f1_score(df, metric)["summary"]["benchmark_model_f1_score"] == 0.5


def test_benchmark_model_accuracy_is_multiclass_exact_match_and_reports_coverage():
    df = pd.DataFrame({
        "label": ["benign", "dos", "scan", None],
        "prediction": ["benign", "scan", "scan", "dos"],
    })

    result = compute_benchmark_model_accuracy(df, {"input_requirements": {}})["summary"]

    assert result["row_count"] == 4
    assert result["evaluated_count"] == 3
    assert result["excluded_pair_count"] == 1
    assert result["evaluation_coverage_ratio"] == 0.75
    assert result["benchmark_model_accuracy"] == 0.666667
    assert result["evaluation_mode"] == "exact_label_match"
    assert result["runnable"] is True


def test_benchmark_model_metrics_ignore_missing_labels_or_predictions():
    df = pd.DataFrame({
        "label": ["attack", None, "benign"],
        "prediction": ["attack", "attack", None],
    })
    metric = {"input_requirements": {"positive_label": "attack"}}

    result = compute_benchmark_model_accuracy(df, metric)["summary"]

    assert result["row_count"] == 3
    assert result["evaluated_count"] == 1
    assert result["excluded_pair_count"] == 2
    assert result["evaluation_coverage_ratio"] == 0.333333
    assert result["benchmark_model_accuracy"] == 1.0


def test_precision_recall_and_f1_never_infer_the_positive_class():
    df = pd.DataFrame({
        "label": ["attack", "attack", "benign", "benign"],
        "prediction": ["attack", "benign", "attack", "benign"],
    })
    metric = {"input_requirements": {}}

    precision = compute_benchmark_model_precision(df, metric)["summary"]
    recall = compute_benchmark_model_recall(df, metric)["summary"]
    f1 = compute_benchmark_model_f1_score(df, metric)["summary"]

    for result, key in (
        (precision, "benchmark_model_precision"),
        (recall, "benchmark_model_recall"),
        (f1, "benchmark_model_f1_score"),
    ):
        assert result["positive_label"] is None
        assert result[key] is None
        assert result["runnable"] is False
        assert result["unavailable_reason"] == "positive_label_required"


def test_binary_task_metrics_are_explicit_positive_label_one_vs_rest():
    df = pd.DataFrame({
        "label": ["attack", "scan", "benign", "attack"],
        "prediction": ["attack", "attack", "benign", "scan"],
    })
    metric = {"input_requirements": {"positive_label": "attack"}}

    precision = compute_benchmark_model_precision(df, metric)["summary"]
    recall = compute_benchmark_model_recall(df, metric)["summary"]
    f1 = compute_benchmark_model_f1_score(df, metric)["summary"]

    assert precision["true_positive"] == 1
    assert precision["false_positive"] == 1
    assert precision["false_negative"] == 1
    assert precision["true_negative"] == 1
    assert precision["benchmark_model_precision"] == 0.5
    assert recall["benchmark_model_recall"] == 0.5
    assert f1["benchmark_model_f1_score"] == 0.5
    assert f1["evaluation_mode"] == "explicit_positive_label_one_vs_rest"


def test_f1_is_zero_when_precision_and_recall_are_both_zero_but_defined():
    df = pd.DataFrame({
        "label": ["attack", "benign"],
        "prediction": ["benign", "attack"],
    })
    metric = {"input_requirements": {"positive_label": "attack"}}

    precision = compute_benchmark_model_precision(df, metric)["summary"]
    recall = compute_benchmark_model_recall(df, metric)["summary"]
    f1 = compute_benchmark_model_f1_score(df, metric)["summary"]

    assert precision["benchmark_model_precision"] == 0.0
    assert recall["benchmark_model_recall"] == 0.0
    assert f1["benchmark_model_f1_score"] == 0.0
    assert f1["runnable"] is True


def test_positive_class_metrics_are_undefined_when_positive_support_is_absent():
    df = pd.DataFrame({
        "label": ["benign", "benign"],
        "prediction": ["benign", "benign"],
    })
    metric = {"input_requirements": {"positive_label": "attack"}}

    precision = compute_benchmark_model_precision(df, metric)["summary"]
    recall = compute_benchmark_model_recall(df, metric)["summary"]
    f1 = compute_benchmark_model_f1_score(df, metric)["summary"]

    assert precision["benchmark_model_precision"] is None
    assert precision["unavailable_reason"] == "no_predicted_positives"
    assert recall["benchmark_model_recall"] is None
    assert recall["unavailable_reason"] == "no_actual_positives"
    assert f1["benchmark_model_f1_score"] is None
    assert f1["unavailable_reason"] == "no_positive_class_observations_or_predictions"
