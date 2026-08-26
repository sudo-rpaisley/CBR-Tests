from pathlib import Path

from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.valid_ip_address_profile import (
    classify_ip_value,
    run_protocol_validity_metric,
)


def test_ipv4_values_are_classified():
    assert classify_ip_value("0.0.0.0") == "ipv4"
    assert classify_ip_value("255.255.255.255") == "ipv4"


def test_ipv6_values_are_classified():
    assert classify_ip_value("2001:db8::1") == "ipv6"
    assert classify_ip_value("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") == "ipv6"


def test_invalid_values_are_separate_from_missing():
    assert classify_ip_value("256.1.1.1") == "invalid"
    assert classify_ip_value("gggg::1") == "invalid"
    assert classify_ip_value("") == "missing"
    assert classify_ip_value("   ") == "missing"
    assert classify_ip_value(None) == "missing"


def _write_packets(path: Path, packets) -> None:
    for index, packet in enumerate(packets):
        packet.time = 1.0 + index * 0.1
    wrpcap(str(path), packets)


def test_protocol_validity_checks_ports_protocol_structure_and_addresses(tmp_path):
    capture = tmp_path / "valid.pcap"
    _write_packets(
        capture,
        [
            IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=12345, dport=443, flags="S"),
            IP(src="10.0.0.2", dst="10.0.0.1") / UDP(sport=53, dport=53000) / Raw(b"dns"),
        ],
    )

    ok, payload = run_protocol_validity_metric(capture, {"calculation": {"parameters": {}}})
    result = payload["test_results"]["protocol_validity_profile"]

    assert ok is True
    assert result["checked_packet_count"] == 2
    assert result["valid_packet_count"] == 2
    assert result["structurally_invalid_packet_count"] == 0
    assert result["invalid_port_count"] == 0
    assert result["protocol_mismatch_count"] == 0
    assert result["checked_address_count"] == 4
    assert result["protocol_validity_ratio"] == 1.0
    assert result["status"] == "pass"


def test_protocol_validity_detects_declared_tcp_without_decodable_tcp_header(tmp_path):
    capture = tmp_path / "mismatch.pcap"
    _write_packets(
        capture,
        [IP(src="10.0.0.1", dst="10.0.0.2", proto=6) / Raw(b"x")],
    )

    ok, payload = run_protocol_validity_metric(capture, {"calculation": {"parameters": {}}})
    result = payload["test_results"]["protocol_validity_profile"]

    assert ok is True
    assert result["protocol_mismatch_count"] == 1
    assert result["structurally_invalid_packet_count"] == 1
    assert result["protocol_validity_ratio"] == 0.0
    assert result["status"] == "fail"


def test_suspicious_tcp_flags_are_reported_but_not_invalid_by_default(tmp_path):
    capture = tmp_path / "flags.pcap"
    _write_packets(
        capture,
        [IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=1000, dport=80, flags="SF")],
    )

    ok, payload = run_protocol_validity_metric(capture, {"calculation": {"parameters": {}}})
    result = payload["test_results"]["protocol_validity_profile"]

    assert ok is True
    assert result["suspicious_tcp_flag_count"] == 1
    assert result["suspicious_tcp_flag_reasons"]["syn_fin"] == 1
    assert result["structurally_invalid_packet_count"] == 0
    assert result["protocol_validity_ratio"] == 1.0
    assert result["status"] == "pass"


def test_suspicious_tcp_flags_can_affect_status_only_when_explicitly_configured(tmp_path):
    capture = tmp_path / "flags-policy.pcap"
    _write_packets(
        capture,
        [IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=1000, dport=80, flags="SF")],
    )
    metric = {
        "calculation": {
            "parameters": {
                "suspicious_tcp_flags_affect_status": True,
            }
        }
    }

    ok, payload = run_protocol_validity_metric(capture, metric)
    result = payload["test_results"]["protocol_validity_profile"]

    assert ok is True
    assert result["suspicious_tcp_flag_count"] == 1
    assert result["structurally_invalid_packet_count"] == 1
    assert result["status"] == "fail"
