"""Compatibility imports for the former metric module location."""

from cbr_tests.metrics.statistical import (
    _build_distributional_metric,
    _clean_numeric_values,
    _distance_correlation,
    _distance_matrix,
    _double_center,
    _energy_distance,
    _ks_statistic,
    _mean_pairwise_abs_distance,
    _mean_product,
    _rbf_mmd,
    _split_values,
    _wasserstein_distance,
    compute_distance_correlation_profile,
    compute_energy_distance,
    compute_ks_feature_divergence,
    compute_maximum_mean_discrepancy,
    compute_wasserstein_feature_distance,
)

__all__ = [name for name in globals() if not name.startswith("__")]
