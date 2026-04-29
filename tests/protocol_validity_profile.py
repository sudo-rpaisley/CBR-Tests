from pathlib import Path
from ipaddress import ip_address


def classify_ip_value(ip_value) -> str:
    """
    Classify an IP field value as one of: missing, ipv4, ipv6, invalid.
    """
    if ip_value is None:
        return "missing"

    text = str(ip_value).strip()
    if text == "":
        return "missing"

    try:
        parsed = ip_address(text)
    except ValueError:
        return "invalid"

    if parsed.version == 4:
        return "ipv4"
    return "ipv6"


def run_protocol_validity_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    from scapy.utils import PcapReader
    from scapy.layers.inet import IP
    from scapy.layers.inet6 import IPv6

    packet_count = 0
    valid_packet_count = 0

    invalid_ip_count = 0
    invalid_port_count = 0
    protocol_mismatch_count = 0
    zero_length_packet_count = 0
    suspicious_tcp_flag_count = 0

    checked_row_count = 0
    checked_address_count = 0
    invalid_address_count = 0
    invalid_row_count = 0
    missing_address_count = 0

    field_counts = {
        "source_ip": {"checked": 0, "invalid": 0, "missing": 0},
        "destination_ip": {"checked": 0, "invalid": 0, "missing": 0}
    }
    address_family_counts = {"ipv4": 0, "ipv6": 0, "unknown": 0}

    try:
        with PcapReader(str(dataset_path)) as reader:
            for pkt in reader:
                packet_count += 1
                packet_valid = True

                try:
                    if len(pkt) <= 0:
                        zero_length_packet_count += 1
                        packet_valid = False
                except Exception:
                    zero_length_packet_count += 1
                    packet_valid = False

                row_checked = False
                row_invalid = False

                if IP in pkt:
                    row_checked = True
                    checked_row_count += 1
                    ip = pkt[IP]
                    for field_name, value in (("source_ip", ip.src), ("destination_ip", ip.dst)):
                        checked_address_count += 1
                        field_counts[field_name]["checked"] += 1
                        cls = classify_ip_value(value)
                        if cls == "ipv4":
                            address_family_counts["ipv4"] += 1
                        elif cls == "ipv6":
                            address_family_counts["ipv6"] += 1
                        elif cls == "missing":
                            missing_address_count += 1
                            field_counts[field_name]["missing"] += 1
                            address_family_counts["unknown"] += 1
                        else:
                            invalid_address_count += 1
                            invalid_ip_count += 1
                            field_counts[field_name]["invalid"] += 1
                            address_family_counts["unknown"] += 1
                            packet_valid = False
                            row_invalid = True

                elif IPv6 in pkt:
                    row_checked = True
                    checked_row_count += 1
                    ip6 = pkt[IPv6]
                    for field_name, value in (("source_ip", ip6.src), ("destination_ip", ip6.dst)):
                        checked_address_count += 1
                        field_counts[field_name]["checked"] += 1
                        cls = classify_ip_value(value)
                        if cls == "ipv4":
                            address_family_counts["ipv4"] += 1
                        elif cls == "ipv6":
                            address_family_counts["ipv6"] += 1
                        elif cls == "missing":
                            missing_address_count += 1
                            field_counts[field_name]["missing"] += 1
                            address_family_counts["unknown"] += 1
                        else:
                            invalid_address_count += 1
                            invalid_ip_count += 1
                            field_counts[field_name]["invalid"] += 1
                            address_family_counts["unknown"] += 1
                            packet_valid = False
                            row_invalid = True

                if row_checked and row_invalid:
                    invalid_row_count += 1

                if packet_valid:
                    valid_packet_count += 1

    except Exception as exc:
        return False, {"error": f"Failed to scan PCAP protocol validity: {exc}"}

    if packet_count == 0:
        return False, {"error": "PCAP contains no packets."}

    protocol_validity_ratio = round(valid_packet_count / packet_count, 6)
    invalid_address_ratio = round(invalid_address_count / checked_address_count, 6) if checked_address_count else 0.0
    invalid_row_ratio = round(invalid_row_count / checked_row_count, 6) if checked_row_count else 0.0

    if protocol_validity_ratio >= 0.99:
        status = "pass"
    elif protocol_validity_ratio >= 0.95:
        status = "warn"
    else:
        status = "fail"

    return True, {
        "test_results": {
            "protocol_validity_profile": {
                "packet_count": packet_count,
                "valid_packet_count": valid_packet_count,
                "protocol_validity_ratio": protocol_validity_ratio,
                "invalid_ip_count": invalid_ip_count,
                "invalid_port_count": invalid_port_count,
                "protocol_mismatch_count": protocol_mismatch_count,
                "zero_length_packet_count": zero_length_packet_count,
                "suspicious_tcp_flag_count": suspicious_tcp_flag_count,
                "checked_row_count": checked_row_count,
                "checked_address_count": checked_address_count,
                "invalid_address_count": invalid_address_count,
                "invalid_row_count": invalid_row_count,
                "invalid_address_ratio": invalid_address_ratio,
                "invalid_row_ratio": invalid_row_ratio,
                "missing_address_count": missing_address_count,
                "field_counts": field_counts,
                "address_family_counts": address_family_counts,
                "status": status
            }
        }
    }
