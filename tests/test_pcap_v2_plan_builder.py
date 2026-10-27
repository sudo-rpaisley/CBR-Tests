from pathlib import Path

from runner.plan_builder import build_plan
from runner.pcap_adapter import PCAP_REFERENCE_METRICS, PCAP_V2_REFERENCE_METRICS


def test_v2_reference_metrics_require_explicit_selection(tmp_path):
    candidate = tmp_path/"candidate.pcap"
    reference = tmp_path/"reference.pcap"
    candidate.write_bytes(b"candidate")
    reference.write_bytes(b"reference")
    legacy, _ = build_plan(plan_id="legacy", name="Legacy", dataset_path=candidate,
                           reference_dataset_path=reference)
    default_ids = {m["metric_id"] for m in legacy["metrics"]}
    assert not (PCAP_V2_REFERENCE_METRICS & default_ids)
    assert PCAP_REFERENCE_METRICS <= default_ids

    include = {
        "capture_order_inter_arrival_divergence_from_reference",
        "multiscale_activity_divergence_from_reference",
        "exact_packet_duplicate_ratio",
        "packet_integrity_profile",
        "tcp_sequence_continuity_profile",
    }
    enhanced, report = build_plan(plan_id="enhanced", name="Enhanced", dataset_path=candidate,
                                  reference_dataset_path=reference,
                                  include_metric_ids=sorted(include))
    enhanced_ids={m["metric_id"] for m in enhanced["metrics"]}
    assert enhanced_ids == include
    assert all(report["metrics"][m]["status"] == "ready" for m in include)
