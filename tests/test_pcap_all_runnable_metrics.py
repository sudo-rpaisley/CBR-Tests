from pathlib import Path

import pytest
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.dispatch import build_metric_handlers
from runner.pcap_adapter import (
    PCAP_CONTEXT_CONFIGURATION_REASONS,
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_METRICS,
    PCAP_SELF_DERIVED_METRICS,
    PCAP_SUPPORTED_METRICS,
    build_pcap_packet_dataframe,
    pcap_metric_template,
)
from runner.plan_builder import build_plan


def _write_capture(path: Path, packet_count: int = 64) -> None:
    packets = []
    base = 1_700_000_000.0
    for index in range(packet_count):
        if index % 4 == 3:
            packet = (
                IP(src=f"10.0.1.{(index % 20) + 1}", dst="8.8.8.8")
                / UDP(sport=50000 + (index % 1000), dport=53)
                / Raw(b"d" * (20 + index % 11))
            )
        else:
            flags = "S" if index % 8 == 0 else "A"
            packet = (
                IP(src="10.0.0.1", dst="10.0.0.2")
                / TCP(sport=12000 + (index % 17), dport=443, flags=flags)
                / Raw(b"x" * (40 + index % 23))
            )
        packet.time = base + index * (0.05 + (index % 5) * 0.01)
        packets.append(packet)
    wrpcap(str(path), packets)


def test_pcap_supported_set_contains_every_current_automatic_packet_metric():
    assert PCAP_SUPPORTED_METRICS == PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS
    assert len(PCAP_DIRECT_METRICS) == 2
    assert len(PCAP_PACKET_METRICS) == 18
    assert len(PCAP_SUPPORTED_METRICS) == 20


def test_all_packet_view_metrics_execute_on_one_shared_capture(tmp_path):
    capture = tmp_path / "all-metrics.pcap"
    _write_capture(capture)
    dataframe = build_pcap_packet_dataframe(capture)

    assert "Inter Arrival Time" in dataframe.columns
    assert dataframe.iloc[0]["Inter Arrival Time"] != dataframe.iloc[0]["Inter Arrival Time"]
    assert dataframe.iloc[1]["Inter Arrival Time"] > 0

    def forbidden_loader(_path):
        raise AssertionError("PCAP packet-view metric attempted to reload the capture as tabular data")

    handlers = build_metric_handlers(dataframe, forbidden_loader, {})
    for metric_id in sorted(PCAP_PACKET_METRICS):
        metric = pcap_metric_template(metric_id)
        assert metric is not None, metric_id
        ok, payload = handlers[metric_id](capture, metric)
        assert ok is True, (metric_id, payload)
        assert metric_id in payload["test_results"], (metric_id, payload)

    parse_metric = pcap_metric_template("timestamp_parse_success_ratio")
    ok, payload = handlers["timestamp_parse_success_ratio"](capture, parse_metric)
    assert ok is True
    summary = payload["test_results"]["timestamp_parse_success_ratio"]["summary"]
    assert summary["timestamp_parse_success_ratio"] == 1.0


def test_automatic_pcap_plan_contains_all_twenty_currently_runnable_metrics(tmp_path):
    capture = tmp_path / "capture.pcap"
    _write_capture(capture)

    plan, report = build_plan(plan_id="all-pcap", name="All PCAP", dataset_path=capture)
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}

    assert metric_ids == PCAP_SUPPORTED_METRICS
    assert report["runnable_metric_count"] == 20
    assert report["metrics"]["handshake_plausibility_profile"]["status"] == "needs_configuration"
    assert report["metrics"]["handshake_plausibility_profile"]["reason"] == "capture_boundary_policy_required"
    for metric_id in PCAP_SELF_DERIVED_METRICS:
        assert report["metrics"][metric_id]["status"] == "not_applicable"
        assert report["metrics"][metric_id]["reason"] == "self_derived_pcap_invariant_not_independent"


def test_distance_correlation_pcap_template_declares_computational_cap(tmp_path):
    capture = tmp_path / "capture.pcap"
    _write_capture(capture, packet_count=1100)
    dataframe = build_pcap_packet_dataframe(capture)
    handlers = build_metric_handlers(dataframe, lambda _path: None, {})
    metric = pcap_metric_template("distance_correlation_matrix_deviation")

    ok, payload = handlers["distance_correlation_matrix_deviation"](capture, metric)

    assert ok is True
    assert payload["sampling"] == {
        "method": "deterministic_evenly_spaced_rows",
        "original_row_count": 1100,
        "sampled_row_count": 1000,
        "max_sample_size": 1000,
    }


def test_context_configuration_reasons_are_not_silent_exclusions():
    assert PCAP_CONTEXT_CONFIGURATION_REASONS == {
        "handshake_plausibility_profile": "capture_boundary_policy_required"
    }
