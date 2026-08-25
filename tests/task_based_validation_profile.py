"""Compatibility imports for the former metric module location."""

from cbr_tests.metrics.task_validation import (
    compute_benchmark_model_accuracy,
    compute_benchmark_model_f1_score,
    compute_benchmark_model_precision,
    compute_benchmark_model_recall,
)

__all__ = [
    "compute_benchmark_model_accuracy",
    "compute_benchmark_model_f1_score",
    "compute_benchmark_model_precision",
    "compute_benchmark_model_recall",
]
