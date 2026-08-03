from __future__ import annotations

import inspect

import runner.dispatch
from cbr_tests.metrics import (
    column_quality,
    data_quality,
    pearson,
    spearman,
    statistical,
    task_validation,
    temporal,
    timestamp_coherence,
)
from tests import (
    column_quality_profile,
    data_quality_profile,
    pearson_profile,
    spearman_profile,
    statistical_fidelity_profile,
    task_based_validation_profile,
    temporal_metrics_profile,
    timestamp_coherence_profile,
)


def test_legacy_metric_modules_reexport_production_implementations():
    assert pearson_profile.compute_pearson_profile is pearson.compute_pearson_profile
    assert spearman_profile.compute_spearman_profile is spearman.compute_spearman_profile
    assert (
        column_quality_profile.compute_column_quality_profile
        is column_quality.compute_column_quality_profile
    )
    assert data_quality_profile.compute_missing_value_ratio is data_quality.compute_missing_value_ratio
    assert (
        statistical_fidelity_profile.compute_energy_distance
        is statistical.compute_energy_distance
    )
    assert (
        task_based_validation_profile.compute_benchmark_model_accuracy
        is task_validation.compute_benchmark_model_accuracy
    )
    assert (
        temporal_metrics_profile.compute_timestamp_parse_success_ratio
        is temporal.compute_timestamp_parse_success_ratio
    )
    assert (
        timestamp_coherence_profile.run_timestamp_coherence_metric
        is timestamp_coherence.run_timestamp_coherence_metric
    )


def test_dispatch_does_not_import_moved_metric_implementations_from_tests():
    source = inspect.getsource(runner.dispatch)
    moved_imports = (
        "from tests.pearson_profile",
        "from tests.spearman_profile",
        "from tests.column_quality_profile",
        "from tests.data_quality_profile",
        "from tests.task_based_validation_profile",
        "from tests.statistical_fidelity_profile",
        "from tests.temporal_metrics_profile",
        "from tests.timestamp_coherence_profile",
    )
    assert not any(import_line in source for import_line in moved_imports)
