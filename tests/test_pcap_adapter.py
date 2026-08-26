from pathlib import Path

import pytest
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.pcap_adapter import (
    PCAP_FLOW_COLUMNS,
    PCAP_FLOW_METRICS,
    build_pcap_flow_dataframe,
    pcap_metric_template,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.flow_duration_consistency_profile import (
    run_flow_duration_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.packet_byte_consistency_profile import (
    run_packet_byte_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.tcp_flag_consistency_profile import (
    run_tcp_flag_consistency_metric,
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


def test_build_pcap_flow_dataframe_reconstructs_bidirectional_flows(tmp_path):
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


def test_all_declared_pcap_flow_metrics_run_on_canonical_flow_table(tmp_path):
    capture = tmp_path / "sample.pcap"
    _write_capture(capture)
    dataframe = build_pcap_flow_dataframe(capture)

    runners = {
        "valid_port_range_profile": run_valid_port_range_metric,
        "tcp_flag_consistency_profile": run_tcp_flag_consistency_metric,
        "flow_duration_consistency_profile": run_flow_duration_consistency_metric,
        "packet_byte_consistency_profile": run_packet_byte_consistency_metric,
    }

    assert set(runners) == PCAP_FLOW_METRICS
    for metric_id, runner in runners.items():
        metric = pcap_metric_template(metric_id)
        assert metric is not None
        metric["_shared_df"] = dataframe
        ok, payload = runner(capture, metric)
        assert ok is True, (metric_id, payload)
        assert metric_id in payload["test_results"]
