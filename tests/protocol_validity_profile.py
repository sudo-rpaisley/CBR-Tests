from pathlib import Path
from ipaddress import ip_address

from scapy.utils import PcapReader
from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.inet6 import IPv6


def run_protocol_validity_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """
    Scan a PCAP and assess basic packet/protocol validity.
    """
    packet_count = 0
    valid_packet_count = 0

    invalid_ip_count = 0
    invalid_port_count = 0
    protocol_mismatch_count = 0
    zero_length_packet_count = 0
    suspicious_tcp_flag_count = 0

    try:
        with PcapReader(str(dataset_path)) as reader:
            for pkt in reader:
                packet_count += 1
                packet_valid = True

                try:
                    pkt_len = len(pkt)
                    if pkt_len <= 0:
                        zero_length_packet_count += 1
                        packet_valid = False
                except Exception:
                    zero_length_packet_count += 1
                    packet_valid = False

                if IP in pkt:
                    ip = pkt[IP]
                    try:
                        ip_address(ip.src)
                        ip_address(ip.dst)
                    except Exception:
                        invalid_ip_count += 1
                        packet_valid = False

                    if TCP in pkt and ip.proto != 6:
                        protocol_mismatch_count += 1
                        packet_valid = False

                    if UDP in pkt and ip.proto != 17:
                        protocol_mismatch_count += 1
                        packet_valid = False

                elif IPv6 in pkt:
                    ip6 = pkt[IPv6]
                    try:
                        ip_address(ip6.src)
                        ip_address(ip6.dst)
                    except Exception:
                        invalid_ip_count += 1
                        packet_valid = False

                    if TCP in pkt and ip6.nh != 6:
                        protocol_mismatch_count += 1
                        packet_valid = False

                    if UDP in pkt and ip6.nh != 17:
                        protocol_mismatch_count += 1
                        packet_valid = False

                if TCP in pkt:
                    tcp = pkt[TCP]
                    if not (0 <= int(tcp.sport) <= 65535 and 0 <= int(tcp.dport) <= 65535):
                        invalid_port_count += 1
                        packet_valid = False

                    flags_value = int(tcp.flags)
                    syn_set = bool(flags_value & 0x02)
                    fin_set = bool(flags_value & 0x01)
                    if syn_set and fin_set:
                        suspicious_tcp_flag_count += 1
                        packet_valid = False

                if UDP in pkt:
                    udp = pkt[UDP]
                    if not (0 <= int(udp.sport) <= 65535 and 0 <= int(udp.dport) <= 65535):
                        invalid_port_count += 1
                        packet_valid = False

                if packet_valid:
                    valid_packet_count += 1

    except Exception as exc:
        return False, {
            "error": f"Failed to scan PCAP protocol validity: {exc}"
        }

    if packet_count == 0:
        return False, {
            "error": "PCAP contains no packets."
        }

    protocol_validity_ratio = round(valid_packet_count / packet_count, 6)

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
                "status": status
            }
        }
    }
