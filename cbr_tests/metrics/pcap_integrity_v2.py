"""Opt-in raw-PCAP packet integrity diagnostics.

Capture offloading, truncation, packet loss and mid-flow starts are not assumed
to be realism violations. All results are contextual, with eligibility counts.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.inet6 import IPv6
from scapy.utils import PcapReader


def _params(metric: dict) -> dict:
    return metric.get("calculation", {}).get("parameters", {})


def _read_packets(path: Path):
    with PcapReader(str(path)) as reader:
        for packet in reader:
            yield packet


def run_exact_packet_duplicate_ratio(path: Path, metric: dict) -> tuple[bool, dict]:
    """Byte-for-byte + timestamp repeated frames, distinct from field signatures."""
    max_packets = int(_params(metric).get("max_packets", 500_000))
    if max_packets < 2:
        return False, {"error": "max_packets must be >= 2", "reason_code": "invalid_metric_configuration"}
    checked = duplicate = total = 0
    seen: set[bytes] = set()
    for packet in _read_packets(path):
        total += 1
        if checked >= max_packets:
            continue
        checked += 1
        timestamp = str(packet.time).encode("ascii")
        key = hashlib.blake2b(bytes(packet) + b"\0" + timestamp, digest_size=16).digest()
        if key in seen:
            duplicate += 1
        else:
            seen.add(key)
    return True, {"test_results": {"exact_packet_duplicate_ratio": {
        "checked_packet_count": checked, "total_packet_count": total,
        "duplicate_packet_count": duplicate,
        "exact_packet_duplicate_ratio": round(duplicate / checked, 8) if checked else None,
        "sample_truncated": total > checked, "max_packets": max_packets,
        "signature": "link_frame_bytes_and_capture_timestamp",
        "status": "informational" if checked else "not_applicable",
        "interpretation": "Exact repeated frames may reflect duplicated capture records; no automatic realism verdict.",
    }}}


def _ip_structural_issues(packet) -> list[str]:
    problems = []
    if IP in packet:
        layer = packet[IP]
        if layer.len is not None:
            actual_length = len(bytes(layer))
            if int(layer.len) != actual_length:
                problems.append("ipv4_total_length_mismatch")
        if int(layer.frag) != 0 or bool(int(layer.flags) & 0x1):
            return problems  # transport may be incomplete for fragments
    if UDP in packet:
        layer = packet[UDP]
        if layer.len is not None:
            if int(layer.len) != len(bytes(layer)):
                problems.append("udp_length_mismatch")
    return problems


def run_packet_integrity_profile(path: Path, metric: dict) -> tuple[bool, dict]:
    """Check observable lengths/checksums; do not call offload artefacts 'invalid'."""
    from scapy.compat import raw
    max_packets = int(_params(metric).get("max_packets", 500_000))
    if max_packets < 2:
        return False, {"error": "max_packets must be >= 2", "reason_code": "invalid_metric_configuration"}
    inspected = total = len_mismatch = checksum_mismatch = checksum_checked = truncated = 0
    counts: dict[str, int] = {}
    max_examples = int(_params(metric).get("max_examples", 10))
    examples = []
    for packet in _read_packets(path):
        total += 1
        if inspected >= max_packets:
            continue
        inspected += 1
        problems = _ip_structural_issues(packet)
        for problem in problems:
            counts[problem] = counts.get(problem, 0) + 1
        if problems:
            len_mismatch += 1
            if len(examples) < max_examples:
                examples.append({"packet_index": total-1, "reasons": problems})
        if IP not in packet:
            continue
        ip = packet[IP]
        if int(ip.frag) != 0 or bool(int(ip.flags) & 0x1):
            continue
        if ip.chksum is None:
            continue
        try:
            original_checksum = int(ip.chksum)
            copy = ip.copy()
            del copy.chksum
            computed = IP(raw(copy)).chksum
            checksum_checked += 1
            if int(computed) != original_checksum:
                checksum_mismatch += 1
                if len(examples) < max_examples:
                    examples.append({"packet_index": total-1, "reasons": ["ipv4_header_checksum_mismatch"]})
        except (ValueError, TypeError, AttributeError):
            truncated += 1
    return True, {"test_results": {"packet_integrity_profile": {
        "checked_packet_count": inspected, "total_packet_count": total,
        "length_inconsistency_count": len_mismatch,
        "length_issue_counts": counts,
        "ipv4_header_checksum_checked_count": checksum_checked,
        "ipv4_header_checksum_mismatch_count": checksum_mismatch,
        "ipv4_header_checksum_mismatch_ratio": (
            round(checksum_mismatch/checksum_checked, 8) if checksum_checked else None),
        "undecodable_checksum_count": truncated, "sample_truncated": total > inspected,
        "examples": examples, "status": "informational" if inspected else "not_applicable",
        "interpretation": "Offloading, truncation and virtual interfaces can affect checksum evidence; this diagnostic does not validate application payload or transport checksums.",
    }}}


def run_tcp_sequence_continuity_profile(path: Path, metric: dict) -> tuple[bool, dict]:
    """TCP byte-sequence gap hints; retransmissions and missing starts are contextual."""
    max_packets = int(_params(metric).get("max_packets", 500_000))
    if max_packets < 2:
        return False, {"error": "max_packets must be >= 2", "reason_code": "invalid_metric_configuration"}
    total = tcp_seen = checked = gap = backwards = retransmits = 0
    state: dict[tuple, int] = {}
    max_examples = int(_params(metric).get("max_examples", 10))
    examples = []
    for packet in _read_packets(path):
        total += 1
        if total > max_packets:
            continue
        if TCP not in packet or (IP not in packet and IPv6 not in packet):
            continue
        tcp_seen += 1
        ip = packet[IP] if IP in packet else packet[IPv6]
        tcp = packet[TCP]
        key = (str(ip.src), int(tcp.sport), str(ip.dst), int(tcp.dport))
        seq = int(tcp.seq)
        # TCP sequence consumption: payload bytes + SYN + FIN.
        consumed = len(bytes(tcp.payload)) + bool(int(tcp.flags) & 0x02) + bool(int(tcp.flags) & 0x01)
        last = state.get(key)
        if last is not None and consumed:
            checked += 1
            if seq > last:
                gap += 1
                reason = "forward_sequence_gap"
            elif seq < last:
                backwards += 1
                retransmits += 1
                reason = "retransmission_or_out_of_order"
            else:
                reason = None
            if reason and len(examples) < max_examples:
                examples.append({"packet_index": total-1, "reason": reason})
        end = seq + consumed
        state[key] = max(state.get(key, end), end)
    return True, {"test_results": {"tcp_sequence_continuity_profile": {
        "checked_packet_count": min(total, max_packets),
        "tcp_packet_count": tcp_seen, "observed_tcp_direction_count": len(state),
        "adjacent_sequence_comparison_count": checked,
        "forward_gap_count": gap, "retransmission_or_out_of_order_count": retransmits,
        "backwards_sequence_count": backwards,
        "forward_gap_ratio": round(gap/checked, 8) if checked else None,
        "sample_truncated": total > max_packets, "examples": examples,
        "status": "informational" if checked else "not_applicable",
        "interpretation": "Forward gaps or retransmissions are not definitive defects; off-path capture, segmentation offload, packet loss and reordering are legitimate explanations. TCP ACK consistency is not established by this metric.",
    }}}
