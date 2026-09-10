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


def test_distributional_metrics_match_hand_calculated_shifted_half_oracles():
    df = pd.DataFrame({"feature": [1, 2, 10, 11]})
    metric = _metric("feature")

    # A=[1,2], B=[10,11]. The empirical CDFs are completely separated.
    assert compute_ks_feature_divergence(df, metric)["fields"][0]["ks_statistic"] == 1.0

    # Every matched quantile is displaced by 9.
    assert compute_wasserstein_feature_distance(df, metric)["fields"][0]["wasserstein_distance"] == 9.0

    # 2 E|X-Y| - E|X-X'| - E|Y-Y'| = 18 - 0.5 - 0.5 = 17.
    assert compute_energy_distance(df, metric)["fields"][0]["energy_distance"] == 17.0

    # Median non-zero pairwise distance across [1,2,10,11] is sigma=8.5,
    # gamma=1/(2*sigma^2). The implementation returns the biased empirical
    # squared RBF-kernel MMD (diagonal terms included).
    assert (
        compute_maximum_mean_discrepancy(df, metric)["fields"][0]["maximum_mean_discrepancy"]
        == 0.85085
    )


def test_distance_correlation_profile_matches_nonlinear_dependency_oracle():
    df = pd.DataFrame({
        "x": [-2, -1, 0, 1, 2],
        "x_squared": [4, 1, 0, 1, 4],
        "constant": [1, 1, 1, 1, 1],
    })

    result = compute_distance_correlation_profile(df, ["x", "x_squared", "constant", "missing"])
    profile = result["profile"]

    assert profile["fields"] == ["x", "x_squared"]
    assert profile["matrix"]["x"]["x"] == 1.0
    assert profile["matrix"]["x"]["x_squared"] == 0.515923
    assert result["column_validation"][2]["reason"] == "constant_column"
    assert result["column_validation"][3]["reason"] == "missing_column"
