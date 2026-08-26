from pathlib import Path

import pytest
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.pcap_adapter import (
    PCAP_FLOW_COLUMNS,
    PCAP_PACKET_COLUMNS,
    PCAP_PACKET_METRICS,
    PCAP_SELF_DERIVED_METRICS,
    build_pcap_flow_dataframe,
    build_pcap_packet_dataframe,
    pcap_metric_template,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.reserved_ip_address_profile import (
    run_reserved_ip_address_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.valid_port_range_profile import (
    run_valid_port_range_metric,
)


def _write_capture(path: Path) -> None:
    packets = [
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="S") / Raw(b"a"),
        IP(src="10.0.0.2", dst="10.0.0.1") / TCP(sport=80, dport=12345, flags="SA") / Raw(b"bb"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="A") / Raw(b"ccc"),
        IP(src="10.0.0.3", dst="8.8.8.8") / UDP(sport=53000, dport=53) / Raw(b"dns"),
    ]
    for packet, timestamp in zip(packets, (1.0, 1.1, 1.2, 2.0)):
        packet.time = timestamp
    wrpcap(str(path), packets)


def test_build_pcap_packet_dataframe_copies_raw_packet_fields(tmp_path):
    capture = tmp_path / "sample.pcap"
    _write_capture(capture)

    dataframe = build_pcap_packet_dataframe(capture)

    assert set(dataframe.columns) == PCAP_PACKET_COLUMNS
    assert len(dataframe) == 4
    assert list(dataframe["Protocol"]) == [6, 6, 6, 17]
    assert dataframe.iloc[0]["Source Port"] == 12345
    assert dataframe.iloc[0]["Destination Port"] == 80
    assert dataframe.iloc[0]["TCP Flags"] == 0x02
    assert dataframe.iloc[3]["TCP Flags"] != dataframe.iloc[3]["TCP Flags"]  # NaN for UDP


def test_build_pcap_flow_dataframe_reconstructs_bidirectional_view(tmp_path):
    capture = tmp_path / "sample.pcap"
    _write_capture(capture)

    dataframe = build_pcap_flow_dataframe(capture)

    assert set(dataframe.columns) == PCAP_FLOW_COLUMNS
    assert len(dataframe) == 2

    tcp_flow = dataframe[dataframe["Protocol"] == 6].iloc[0]
    assert tcp_flow["Source IP"] == "10.0.0.1"
    assert tcp_flow["Destination IP"] == "10.0.0.2"
    assert tcp_flow["Source Port"] == 12345
    assert tcp_flow["Destination Port"] == 80
    assert tcp_flow["Total Fwd Packets"] == 2
    assert tcp_flow["Total Backward Packets"] == 1
    assert tcp_flow["SYN Flag Count"] == 2
    assert tcp_flow["ACK Flag Count"] == 2
    assert tcp_flow["Flow Duration"] == pytest.approx(0.2)
    assert tcp_flow["Flow IAT Min"] == pytest.approx(0.1)
    assert tcp_flow["Flow IAT Mean"] == pytest.approx(0.1)
    assert tcp_flow["Flow IAT Max"] == pytest.approx(0.1)
    assert tcp_flow["Fwd IAT Total"] == pytest.approx(0.2)
    assert tcp_flow["Bwd IAT Total"] == pytest.approx(0.0)


def test_packet_adapted_metrics_run_on_raw_packet_view(tmp_path):
    capture = tmp_path / "sample.pcap"
    _write_capture(capture)
    dataframe = build_pcap_packet_dataframe(capture)

    runners = {
        "reserved_ip_address_profile": run_reserved_ip_address_metric,
        "valid_port_range_profile": run_valid_port_range_metric,
    }

    assert set(runners) == PCAP_PACKET_METRICS
    for metric_id, runner in runners.items():
        metric = pcap_metric_template(metric_id)
        assert metric is not None
        metric["_shared_df"] = dataframe
        ok, payload = runner(capture, metric)
        assert ok is True, (metric_id, payload)
        assert metric_id in payload["test_results"]
        if metric_id == "reserved_ip_address_profile":
            result = payload["test_results"][metric_id]
            assert result["reserved_category_counts"]["private"] > 0
            assert result["reserved_address_count"] == 0
            assert result["status"] == "pass"


def test_self_derived_flow_invariants_are_not_exposed_as_pcap_templates():
    assert PCAP_SELF_DERIVED_METRICS
    for metric_id in PCAP_SELF_DERIVED_METRICS:
        assert pcap_metric_template(metric_id) is None
