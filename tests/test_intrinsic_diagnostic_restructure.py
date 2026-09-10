import pandas as pd

from cbr_tests.metrics.intrinsic_diagnostics import (
    compute_burstiness_internal_drift,
    compute_day_to_day_diurnal_similarity,
    compute_day_to_day_hourly_activity_divergence,
    compute_distance_correlation_dependency_profile,
    compute_feature_energy_internal_drift,
    compute_feature_ks_internal_drift,
    compute_feature_mmd2_internal_drift,
    compute_feature_wasserstein_internal_drift,
    compute_inter_arrival_internal_drift_ks,
    compute_lagged_periodicity_similarity,
    compute_pearson_dependency_profile,
    compute_spearman_dependency_profile,
)
from runner.dispatch import build_metric_handlers
from runner.metric_catalog import LEGACY_METRIC_ID_ALIASES, available_metric_ids, load_taxonomy_paths


def _feature_metric():
    return {
        "input_requirements": {"candidate_fields": ["feature"]},
        "calculation": {"parameters": {"minimum_sample_size": 2}},
    }


def test_canonical_distribution_diagnostics_preserve_hand_calculated_oracles():
    df = pd.DataFrame({"feature": [1, 2, 10, 11]})
    metric = _feature_metric()

    assert compute_feature_ks_internal_drift(df, metric)["fields"][0]["ks_statistic"] == 1.0
    assert compute_feature_wasserstein_internal_drift(df, metric)["fields"][0]["wasserstein_distance"] == 9.0
    assert compute_feature_energy_internal_drift(df, metric)["fields"][0]["energy_distance"] == 17.0
    assert compute_feature_mmd2_internal_drift(df, metric)["fields"][0]["maximum_mean_discrepancy"] == 0.85085
    assert compute_feature_ks_internal_drift(df, metric)["summary"]["comparison_scope"] == "within_dataset_ordered_half_drift"


def test_canonical_temporal_diagnostic_names_do_not_claim_reference_comparison():
    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=72, freq="h", tz="UTC")})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {"parameters": {"minimum_sample_size": 2, "minimum_day_count": 2, "lags": [24], "minimum_lag_pairs": 2}},
    }

    iat = compute_inter_arrival_internal_drift_ks(df, metric)["summary"]
    burst = compute_burstiness_internal_drift(df, metric)["summary"]
    hourly = compute_day_to_day_hourly_activity_divergence(df, metric)["summary"]
    diurnal = compute_day_to_day_diurnal_similarity(df, metric)["summary"]
    periodic = compute_lagged_periodicity_similarity(df, metric)["summary"]

    assert iat["inter_arrival_internal_drift_ks"] == 0.0
    assert burst["burstiness_internal_drift"] == 0.0
    assert hourly["day_to_day_hourly_activity_divergence"] == 0.0
    assert diurnal["day_to_day_diurnal_similarity"] == 1.0
    assert periodic["lagged_periodicity_similarity"] == 1.0
    assert all(result["interpretation_direction"] == "contextual" for result in (iat, burst, hourly, diurnal, periodic))


def test_dependency_profiles_are_profiles_not_deviations():
    df = pd.DataFrame({
        "x": [-2, -1, 0, 1, 2],
        "y": [-4, -2, 0, 2, 4],
        "x_squared": [4, 1, 0, 1, 4],
    })
    metric = {"input_requirements": {"candidate_fields": ["x", "y", "x_squared"], "minimum_runnable_fields": 2}}

    pearson = compute_pearson_dependency_profile(df, metric)
    spearman = compute_spearman_dependency_profile(df, metric)
    dcor = compute_distance_correlation_dependency_profile(df, metric)

    assert pearson["profile"]["matrix"]["x"]["y"] == 1.0
    assert spearman["profile"]["matrix"]["x"]["y"] == 1.0
    assert dcor["profile"]["matrix"]["x"]["x_squared"] == 0.515923
    assert pearson["summary"]["comparison_scope"] == "within_dataset_dependency_profile"


def test_new_taxonomy_uses_canonical_ids_and_nests_dataset_heuristics():
    paths = load_taxonomy_paths()
    assert paths["inter_arrival_internal_drift_ks"] == [
        "dataset_heuristics",
        "temporal_metrics",
        "temporal_structure_diagnostics",
        "traffic_dynamics",
        "inter_arrival_internal_drift_ks",
    ]
    assert paths["feature_ks_internal_drift"][:3] == [
        "dataset_heuristics",
        "statistical_structure_diagnostics",
        "internal_distribution_drift",
    ]
    assert paths["missing_value_ratio"][:2] == ["dataset_heuristics", "data_quality_and_provenance"]
    assert paths["per_slice_sample_coverage_ratio"][0] == "dataset_heuristics"
    assert paths["label_coverage_ratio"][0] == "dataset_heuristics"


def test_legacy_intrinsic_ids_remain_runtime_compatible_but_not_canonical_catalogue():
    handlers = build_metric_handlers(None, lambda _path: None, {})
    canonical_ids = set(available_metric_ids())

    for legacy_id, canonical_id in LEGACY_METRIC_ID_ALIASES.items():
        assert legacy_id in handlers
        assert canonical_id in handlers
        assert legacy_id not in canonical_ids
        assert canonical_id in canonical_ids
