from runner.evidence_classification import pcap_evidence_class


def test_pcap_evidence_classes_separate_observations_from_diagnostics():
    assert pcap_evidence_class("handshake_plausibility_profile") == "independent_observation"
    assert pcap_evidence_class("timestamp_coherence_profile") == "independent_observation"
    assert pcap_evidence_class("column_quality_profile") == "derived_diagnostic"
    assert pcap_evidence_class("duplicate_row_ratio") == "contextual_diagnostic"
    assert pcap_evidence_class("feature_ks_internal_drift") == "contextual_diagnostic"
    assert pcap_evidence_class("pearson_dependency_profile") == "contextual_diagnostic"
    assert pcap_evidence_class("feature_wise_ks_statistic_from_reference") == "reference_dependent"
    assert pcap_evidence_class("service_port_consistency_profile") == "context_required"
