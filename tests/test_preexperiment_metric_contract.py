from pathlib import Path

import pandas as pd

from cbr_tests.metrics.label_fidelity import compute_train_test_identifier_contamination_ratio
from cbr_tests.metrics.protocol_network.flow_semantics.tcp_flag_consistency_profile import run_tcp_flag_consistency_metric
from cbr_tests.metrics.protocol_network.slice_metadata_integrity.slice_identifier_consistency_profile import run_slice_identifier_consistency_metric
from runner.pcap_adapter import PCAP_AUTOMATIC_EXCLUSIONS, PCAP_SUPPORTED_METRICS
from runner.plan_builder import build_plan


def test_automatic_pcap_plan_excludes_nonindependent_decoder_evidence(tmp_path: Path):
    capture = tmp_path / "capture.pcap"
    capture.write_bytes(b"placeholder")
    plan, report = build_plan(plan_id="pcap-contract", name="PCAP contract", dataset_path=capture)
    included = {metric["metric_id"] for metric in plan["metrics"]}
    assert included == PCAP_SUPPORTED_METRICS
    for metric_id, reason in PCAP_AUTOMATIC_EXCLUSIONS.items():
        assert metric_id not in included
        assert report["metrics"][metric_id]["status"] == "not_applicable"
        assert report["metrics"][metric_id]["reason"] == reason


def test_identifier_contamination_uses_field_qualified_namespaces():
    df = pd.DataFrame({
        "split": ["train", "test"],
        "device_id": ["same-text", "device-2"],
        "user_id": ["user-1", "same-text"],
    })
    metric = {"input_requirements": {
        "split_field": "split",
        "identifier_fields": ["device_id", "user_id"],
        "entity_disjoint_expected": True,
    }}
    summary = compute_train_test_identifier_contamination_ratio(df, metric)["summary"]
    assert summary["identifier_universe"] == "field_qualified_(field,value)"
    assert summary["overlap_identifier_count"] == 0
    assert summary["train_test_identifier_contamination_ratio"] == 0.0


def test_conflicting_overlapping_slice_rules_are_rejected():
    df = pd.DataFrame({"service": ["video"], "slice": ["s1"]})
    metric = {
        "input_requirements": {"slice_field": "slice"},
        "calculation": {"parameters": {"rules": [
            {"when_field": "service", "operator": "equals", "value": "video", "expected_slice_ids": ["s1"]},
            {"when_field": "service", "operator": "contains", "value": "vid", "expected_slice_ids": ["s2"]},
        ]}},
        "_shared_df": df,
    }
    ok, payload = run_slice_identifier_consistency_metric(Path("unused.csv"), metric)
    assert ok is False
    assert payload["reason_code"] == "conflicting_overlapping_slice_rules"
    assert payload["overlap_policy"] == "identical_expected_sets_only"


def test_identical_overlapping_slice_rules_are_allowed():
    df = pd.DataFrame({"service": ["video"], "slice": ["s1"]})
    metric = {
        "input_requirements": {"slice_field": "slice"},
        "calculation": {"parameters": {"rules": [
            {"when_field": "service", "operator": "equals", "value": "video", "expected_slice_ids": ["s1"]},
            {"when_field": "service", "operator": "contains", "value": "vid", "expected_slice_ids": ["s1"]},
        ]}},
        "_shared_df": df,
    }
    ok, payload = run_slice_identifier_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["slice_identifier_consistency_profile"]
    assert ok is True
    assert result["slice_identifier_consistency_ratio"] == 1.0
    assert result["overlap_policy"] == "identical_expected_sets_only"


def test_tcp_flag_metric_declares_aggregate_not_state_machine_scope():
    df = pd.DataFrame({
        "Protocol": [6],
        "Fwd": [1],
        "Bwd": [1],
        "SYN": [1],
        "ACK": [1],
        "FIN": [0],
        "RST": [0],
    })
    metric = {
        "input_requirements": {"field_map": {
            "protocol": "Protocol",
            "total_fwd_packets": "Fwd",
            "total_bwd_packets": "Bwd",
            "syn_flag_count": "SYN",
            "ack_flag_count": "ACK",
            "fin_flag_count": "FIN",
            "rst_flag_count": "RST",
        }},
        "calculation": {"parameters": {}},
        "_shared_df": df,
    }
    ok, payload = run_tcp_flag_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["tcp_flag_consistency_profile"]
    assert ok is True
    assert result["scope"] == "aggregate_tcp_flag_count_invariants"
