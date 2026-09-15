from pathlib import Path

from runner.evidence_classification import pcap_evidence_class
from runner.plan_builder import build_plan


def test_pcap_evidence_classes_separate_observations_from_diagnostics():
    assert pcap_evidence_class("handshake_plausibility_profile") == "independent_observation"
    assert pcap_evidence_class("timestamp_coherence_profile") == "independent_observation"
    assert pcap_evidence_class("column_quality_profile") == "derived_diagnostic"
    assert pcap_evidence_class("duplicate_row_ratio") == "contextual_diagnostic"
    assert pcap_evidence_class("feature_ks_internal_drift") == "contextual_diagnostic"
    assert pcap_evidence_class("pearson_dependency_profile") == "contextual_diagnostic"
    assert pcap_evidence_class("feature_wise_ks_statistic_from_reference") == "reference_dependent"
    assert pcap_evidence_class("service_port_consistency_profile") == "context_required"


def test_automatic_pcap_plan_records_evidence_class(tmp_path: Path):
    capture = tmp_path / "capture.pcap"
    capture.write_bytes(b"placeholder")
    plan, _report = build_plan(plan_id="evidence-class", name="Evidence class", dataset_path=capture)
    metrics = {metric["metric_id"]: metric for metric in plan["metrics"]}

    assert metrics["handshake_plausibility_profile"]["evidence_class"] == "independent_observation"
    assert metrics["column_quality_profile"]["evidence_class"] == "derived_diagnostic"
    assert metrics["feature_ks_internal_drift"]["evidence_class"] == "contextual_diagnostic"
