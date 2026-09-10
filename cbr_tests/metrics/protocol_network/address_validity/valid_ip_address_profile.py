from pathlib import Path
from ipaddress import ip_address


def classify_ip_value(ip_value) -> str:
    """Classify an IP field value as missing, IPv4, IPv6, or invalid."""

    if ip_value is None:
        return "missing"

    text = str(ip_value).strip()
    if text == "":
        return "missing"

    try:
        parsed = ip_address(text)
    except ValueError:
        return "invalid"

    return "ipv4" if parsed.version == 4 else "ipv6"


def _port_is_valid(value) -> bool:
    try:
        port = int(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return 0 <= port <= 65535


def _suspicious_tcp_flags(flags: int) -> list[str]:
    """Return unusual flag combinations without declaring them structurally invalid.

    SYN+FIN and SYN+RST are useful indicators of scans, crafted packets, malformed
    generators, or adversarial traffic. Security datasets can legitimately contain
    them, so they are descriptive evidence unless a plan explicitly opts them into
    the structural validity decision.
    """

    reasons: list[str] = []
    syn = bool(flags & 0x02)
    fin = bool(flags & 0x01)
    rst = bool(flags & 0x04)
    if syn and fin:
        reasons.append("syn_fin")
    if syn and rst:
        reasons.append("syn_rst")
    return reasons


def run_protocol_validity_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Check deterministic packet structure and report non-deterministic anomalies.

    The validity ratio covers decoded IPv4/IPv6 packets only. Non-IP frames are
    reported separately rather than being silently counted as valid IP packets.
    A packet becomes structurally invalid only for deterministic problems such as
    address-family mismatch, impossible decoded ports, an unfragmented IPv4 TCP/
    UDP protocol declaration with no matching transport layer, or an impossibly
    short network-layer packet. Suspicious TCP flag combinations are descriptive by
    default because attack/security traffic may contain them legitimately.
    """

    from scapy.layers.inet import IP, TCP, UDP
    from scapy.layers.inet6 import IPv6
    from scapy.utils import PcapReader

    params = metric.get("calculation", {}).get("parameters", {})
    pass_threshold = float(params.get("pass_threshold", 0.99))
    warn_threshold = float(params.get("warn_threshold", 0.95))
    suspicious_flags_affect_status = bool(
        params.get("suspicious_tcp_flags_affect_status", False)
    )
    if not 0 <= warn_threshold <= pass_threshold <= 1:
        return False, {
            "error": "Protocol validity thresholds must satisfy 0 <= warn_threshold <= pass_threshold <= 1.",
            "reason_code": "invalid_metric_configuration",
        }

    packet_count = 0
    checked_packet_count = 0
    valid_packet_count = 0
    structurally_invalid_packet_count = 0
    non_ip_packet_count = 0

    invalid_ip_count = 0
    invalid_port_count = 0
    protocol_mismatch_count = 0
    zero_length_packet_count = 0
    short_network_layer_count = 0
    suspicious_tcp_flag_count = 0
    fragment_transport_check_skipped_count = 0

    checked_row_count = 0
    checked_address_count = 0
    invalid_address_count = 0
    invalid_row_count = 0
    missing_address_count = 0

    field_counts = {
        "source_ip": {"checked": 0, "invalid": 0, "missing": 0},
        "destination_ip": {"checked": 0, "invalid": 0, "missing": 0},
    }
    address_family_counts = {"ipv4": 0, "ipv6": 0, "unknown": 0}
    transport_counts = {"tcp": 0, "udp": 0, "other": 0}
    suspicious_tcp_flag_reasons = {"syn_fin": 0, "syn_rst": 0}
    issue_examples: list[dict] = []

    def record_issue(packet_index: int, reason: str, **evidence) -> None:
        if len(issue_examples) < 20:
            issue_examples.append(
                {"packet_index": packet_index, "reason": reason, **evidence}
            )

    try:
        with PcapReader(str(dataset_path)) as reader:
            for packet_index, pkt in enumerate(reader):
                packet_count += 1

                try:
                    captured_length = len(pkt)
                except Exception:
                    captured_length = 0
                if captured_length <= 0:
                    zero_length_packet_count += 1

                if IP not in pkt and IPv6 not in pkt:
                    non_ip_packet_count += 1
                    continue

                checked_packet_count += 1
                checked_row_count += 1
                packet_valid = captured_length > 0
                row_invalid = False

                if captured_length <= 0:
                    row_invalid = True
                    record_issue(packet_index, "zero_length_packet")

                if IP in pkt:
                    ip = pkt[IP]
                    expected_family = "ipv4"
                    network_values = (("source_ip", ip.src), ("destination_ip", ip.dst))

                    if int(getattr(ip, "version", 4) or 4) != 4:
                        packet_valid = False
                        row_invalid = True
                        invalid_ip_count += 1
                        record_issue(packet_index, "ipv4_layer_version_mismatch")

                    ihl = int(getattr(ip, "ihl", 0) or 0)
                    if ihl and len(ip) < ihl * 4:
                        packet_valid = False
                        row_invalid = True
                        short_network_layer_count += 1
                        record_issue(
                            packet_index,
                            "ipv4_shorter_than_header",
                            decoded_length=len(ip),
                            header_length=ihl * 4,
                        )

                    protocol = int(getattr(ip, "proto", -1))
                    fragment_offset = int(getattr(ip, "frag", 0) or 0)
                    if protocol == 6:
                        transport_counts["tcp"] += 1
                        if fragment_offset > 0:
                            fragment_transport_check_skipped_count += 1
                        elif TCP not in pkt:
                            packet_valid = False
                            row_invalid = True
                            protocol_mismatch_count += 1
                            record_issue(packet_index, "tcp_protocol_without_tcp_layer")
                    elif protocol == 17:
                        transport_counts["udp"] += 1
                        if fragment_offset > 0:
                            fragment_transport_check_skipped_count += 1
                        elif UDP not in pkt:
                            packet_valid = False
                            row_invalid = True
                            protocol_mismatch_count += 1
                            record_issue(packet_index, "udp_protocol_without_udp_layer")
                    else:
                        transport_counts["other"] += 1
                else:
                    ip6 = pkt[IPv6]
                    expected_family = "ipv6"
                    network_values = (("source_ip", ip6.src), ("destination_ip", ip6.dst))

                    if int(getattr(ip6, "version", 6) or 6) != 6:
                        packet_valid = False
                        row_invalid = True
                        invalid_ip_count += 1
                        record_issue(packet_index, "ipv6_layer_version_mismatch")

                    if len(ip6) < 40:
                        packet_valid = False
                        row_invalid = True
                        short_network_layer_count += 1
                        record_issue(
                            packet_index,
                            "ipv6_shorter_than_base_header",
                            decoded_length=len(ip6),
                        )

                    # If Scapy has decoded TCP/UDP, use that as the final transport
                    # protocol. Only enforce the base next-header value when it
                    # directly names TCP/UDP; extension-header chains are not
                    # misclassified as mismatches.
                    if TCP in pkt:
                        transport_counts["tcp"] += 1
                    elif UDP in pkt:
                        transport_counts["udp"] += 1
                    else:
                        next_header = int(getattr(ip6, "nh", -1))
                        if next_header == 6:
                            packet_valid = False
                            row_invalid = True
                            protocol_mismatch_count += 1
                            transport_counts["tcp"] += 1
                            record_issue(packet_index, "ipv6_tcp_next_header_without_tcp_layer")
                        elif next_header == 17:
                            packet_valid = False
                            row_invalid = True
                            protocol_mismatch_count += 1
                            transport_counts["udp"] += 1
                            record_issue(packet_index, "ipv6_udp_next_header_without_udp_layer")
                        else:
                            transport_counts["other"] += 1

                for field_name, value in network_values:
                    checked_address_count += 1
                    field_counts[field_name]["checked"] += 1
                    cls = classify_ip_value(value)
                    if cls == expected_family:
                        address_family_counts[cls] += 1
                    elif cls == "missing":
                        missing_address_count += 1
                        field_counts[field_name]["missing"] += 1
                        address_family_counts["unknown"] += 1
                        invalid_address_count += 1
                        invalid_ip_count += 1
                        packet_valid = False
                        row_invalid = True
                        record_issue(packet_index, "missing_ip_address", field=field_name)
                    else:
                        invalid_address_count += 1
                        invalid_ip_count += 1
                        field_counts[field_name]["invalid"] += 1
                        address_family_counts["unknown"] += 1
                        packet_valid = False
                        row_invalid = True
                        record_issue(
                            packet_index,
                            "invalid_or_wrong_family_ip_address",
                            field=field_name,
                            value=str(value),
                            observed_class=cls,
                            expected_class=expected_family,
                        )

                if TCP in pkt:
                    tcp = pkt[TCP]
                    for field_name, value in (("source", tcp.sport), ("destination", tcp.dport)):
                        if not _port_is_valid(value):
                            invalid_port_count += 1
                            packet_valid = False
                            row_invalid = True
                            record_issue(
                                packet_index,
                                "invalid_tcp_port",
                                field=field_name,
                                value=str(value),
                            )
                    flag_reasons = _suspicious_tcp_flags(int(tcp.flags))
                    if flag_reasons:
                        suspicious_tcp_flag_count += 1
                        for reason in flag_reasons:
                            suspicious_tcp_flag_reasons[reason] += 1
                        record_issue(
                            packet_index,
                            "suspicious_tcp_flags",
                            flags=str(tcp.flags),
                            flag_reasons=flag_reasons,
                        )
                        if suspicious_flags_affect_status:
                            packet_valid = False
                            row_invalid = True
                elif UDP in pkt:
                    udp = pkt[UDP]
                    for field_name, value in (("source", udp.sport), ("destination", udp.dport)):
                        if not _port_is_valid(value):
                            invalid_port_count += 1
                            packet_valid = False
                            row_invalid = True
                            record_issue(
                                packet_index,
                                "invalid_udp_port",
                                field=field_name,
                                value=str(value),
                            )

                if row_invalid:
                    invalid_row_count += 1

                if packet_valid:
                    valid_packet_count += 1
                else:
                    structurally_invalid_packet_count += 1

    except Exception as exc:
        return False, {
            "error": f"Failed to scan PCAP protocol validity: {exc}",
            "reason_code": "pcap_scan_error",
        }

    if packet_count == 0:
        return False, {"error": "PCAP contains no packets.", "reason_code": "empty_capture"}

    protocol_validity_ratio = (
        round(valid_packet_count / checked_packet_count, 6)
        if checked_packet_count
        else None
    )
    invalid_address_ratio = (
        round(invalid_address_count / checked_address_count, 6)
        if checked_address_count
        else None
    )
    invalid_row_ratio = (
        round(invalid_row_count / checked_row_count, 6) if checked_row_count else None
    )

    if protocol_validity_ratio is None:
        status = "not_applicable"
    elif protocol_validity_ratio >= pass_threshold:
        status = "pass"
    elif protocol_validity_ratio >= warn_threshold:
        status = "warn"
    else:
        status = "fail"

    return True, {
        "test_results": {
            "protocol_validity_profile": {
                "packet_count": packet_count,
                "checked_packet_count": checked_packet_count,
                "non_ip_packet_count": non_ip_packet_count,
                "valid_packet_count": valid_packet_count,
                "structurally_invalid_packet_count": structurally_invalid_packet_count,
                "protocol_validity_ratio": protocol_validity_ratio,
                "invalid_ip_count": invalid_ip_count,
                "invalid_port_count": invalid_port_count,
                "protocol_mismatch_count": protocol_mismatch_count,
                "zero_length_packet_count": zero_length_packet_count,
                "short_network_layer_count": short_network_layer_count,
                "suspicious_tcp_flag_count": suspicious_tcp_flag_count,
                "suspicious_tcp_flag_reasons": suspicious_tcp_flag_reasons,
                "suspicious_tcp_flags_affect_status": suspicious_flags_affect_status,
                "fragment_transport_check_skipped_count": fragment_transport_check_skipped_count,
                "checked_row_count": checked_row_count,
                "checked_address_count": checked_address_count,
                "invalid_address_count": invalid_address_count,
                "invalid_row_count": invalid_row_count,
                "invalid_address_ratio": invalid_address_ratio,
                "invalid_row_ratio": invalid_row_ratio,
                "missing_address_count": missing_address_count,
                "field_counts": field_counts,
                "address_family_counts": address_family_counts,
                "transport_counts": transport_counts,
                "pass_threshold": pass_threshold,
                "warn_threshold": warn_threshold,
                "issue_examples": issue_examples,
                "status": status,
            }
        }
    }
