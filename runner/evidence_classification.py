from __future__ import annotations


PCAP_DERIVED_DIAGNOSTICS = {
    "column_quality_profile",
}

PCAP_CONTEXTUAL_DIAGNOSTICS = {
    "duplicate_row_ratio",
    "pearson_dependency_profile",
    "spearman_dependency_profile",
    "distance_correlation_dependency_profile",
    "feature_ks_internal_drift",
    "feature_wasserstein_internal_drift",
    "feature_energy_internal_drift",
    "feature_mmd2_internal_drift",
    "inter_arrival_internal_drift_ks",
    "burstiness_internal_drift",
    "day_to_day_hourly_activity_divergence",
    "day_to_day_diurnal_similarity",
    "lagged_periodicity_similarity",
}


def pcap_evidence_class(metric_id: str) -> str:
    """Return the frozen evidential role for a runnable PCAP metric.

    This classifies how a result may be interpreted in the experiment manifest;
    it is separate from execution success, applicability and any metric verdict.
    """
    if metric_id.endswith("_from_reference"):
        return "reference_dependent"
    if metric_id in PCAP_DERIVED_DIAGNOSTICS:
        return "derived_diagnostic"
    if metric_id in PCAP_CONTEXTUAL_DIAGNOSTICS:
        return "contextual_diagnostic"
    if metric_id in {"service_port_consistency_profile", "reserved_address_misuse_ratio"}:
        return "context_required"
    return "independent_observation"
