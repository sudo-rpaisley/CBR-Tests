import pandas as pd

from tests.statistical_fidelity_profile import (
    compute_distance_correlation_profile,
    compute_energy_distance,
    compute_ks_feature_divergence,
    compute_maximum_mean_discrepancy,
    compute_wasserstein_feature_distance,
)


def _metric(*fields):
    return {
        "input_requirements": {"candidate_fields": list(fields)},
        "calculation": {"parameters": {"minimum_sample_size": 2}},
    }


def test_distributional_metrics_report_zero_for_matching_halves():
    df = pd.DataFrame({"feature": [1, 2, 1, 2]})
    metric = _metric("feature")

    assert compute_ks_feature_divergence(df, metric)["fields"][0]["ks_statistic"] == 0.0
    assert compute_wasserstein_feature_distance(df, metric)["fields"][0]["wasserstein_distance"] == 0.0
    assert compute_energy_distance(df, metric)["fields"][0]["energy_distance"] == 0.0
    assert compute_maximum_mean_discrepancy(df, metric)["fields"][0]["maximum_mean_discrepancy"] == 0.0


def test_distributional_metrics_detect_shifted_halves():
    df = pd.DataFrame({"feature": [1, 2, 10, 11]})
    metric = _metric("feature")

    assert compute_ks_feature_divergence(df, metric)["fields"][0]["ks_statistic"] == 1.0
    assert compute_wasserstein_feature_distance(df, metric)["fields"][0]["wasserstein_distance"] == 9.0
    assert compute_energy_distance(df, metric)["fields"][0]["energy_distance"] == 17.0
    assert compute_maximum_mean_discrepancy(df, metric)["fields"][0]["maximum_mean_discrepancy"] > 0.0


def test_distance_correlation_profile_reports_nonlinear_dependency():
    df = pd.DataFrame({
        "x": [-2, -1, 0, 1, 2],
        "x_squared": [4, 1, 0, 1, 4],
        "constant": [1, 1, 1, 1, 1],
    })

    result = compute_distance_correlation_profile(df, ["x", "x_squared", "constant", "missing"])
    profile = result["profile"]

    assert profile["fields"] == ["x", "x_squared"]
    assert profile["matrix"]["x"]["x"] == 1.0
    assert profile["matrix"]["x"]["x_squared"] > 0.0
    assert result["column_validation"][2]["reason"] == "constant_column"
    assert result["column_validation"][3]["reason"] == "missing_column"
