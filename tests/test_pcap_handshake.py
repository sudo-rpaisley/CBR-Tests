from pathlib import Path

from scapy.layers.inet import IP, TCP
from scapy.utils import wrpcap

from cbr_tests.metrics.pcap_handshake import run_pcap_handshake_plausibility_metric


def _write(path: Path, packets):
    for index, packet in enumerate(packets):
        packet.time = 1.0 + index * 0.1
    wrpcap(str(path), packets)


def test_pcap_handshake_ignores_boundary_and_incomplete_attempts(tmp_path):
    capture = tmp_path / "handshake.pcap"
    _write(capture, [
        IP(src="10.0.0.9", dst="10.0.0.10") / TCP(sport=40000, dport=443, flags="A"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="S"),
        IP(src="10.0.0.2", dst="10.0.0.1") / TCP(sport=80, dport=12345, flags="SA"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="A"),
        IP(src="10.0.0.3", dst="10.0.0.4") / TCP(sport=23456, dport=22, flags="S"),
    ])
    ok, payload = run_pcap_handshake_plausibility_metric(capture, {})
    assert ok is True
    result = payload["test_results"]["handshake_plausibility_profile"]
    assert result["status"] == "pass"
    assert result["initiated_attempt_count"] == 2
    assert result["completed_handshake_count"] == 1
    assert result["incomplete_attempt_count"] == 1
    assert result["boundary_excluded_flow_count"] >= 1
    assert result["handshake_completion_ratio"] == 0.5
    assert result["handshake_plausibility_ratio"] == 1.0


def test_pcap_handshake_warns_on_direction_contradiction(tmp_path):
    capture = tmp_path / "contradiction.pcap"
    _write(capture, [
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="S"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="SA"),
    ])
    ok, payload = run_pcap_handshake_plausibility_metric(capture, {})
    assert ok is True
    result = payload["test_results"]["handshake_plausibility_profile"]
    assert result["status"] == "warn"
    assert result["contradictory_transition_count"] == 1
    assert result["handshake_plausibility_ratio"] == 0.0


def test_pcap_handshake_not_applicable_without_opening_syn(tmp_path):
    capture = tmp_path / "midstream.pcap"
    _write(capture, [IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=80, flags="A")])
    ok, payload = run_pcap_handshake_plausibility_metric(capture, {})
    assert ok is True
    result = payload["test_results"]["handshake_plausibility_profile"]
    assert result["status"] == "not_applicable"
    assert result["handshake_plausibility_ratio"] is None
