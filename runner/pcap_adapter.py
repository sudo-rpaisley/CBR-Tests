from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Any

import pandas as pd
from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.inet6 import IPv6
from scapy.utils import PcapReader


PCAP_DIRECT_METRICS = {
    "protocol_validity_profile",
    "timestamp_coherence_profile",
}

# Existing tabular metrics that can be evaluated without changing their
# scientific meaning once a PCAP has been reconstructed into canonical flows.
PCAP_FLOW_METRICS = {
    "valid_port_range_profile",
    "tcp_flag_consistency_profile",
    "flow_duration_consistency_profile",
    "packet_byte_consistency_profile",
}

PCAP_FLOW_COLUMNS = {
    "Timestamp",
    "Flow End Timestamp",
    "Source IP",
    "Destination IP",
    "Source Port",
    "Destination Port",
    "Protocol",
    "Flow Duration",
    "Total Fwd Packets",
    "Total Backward Packets",
    "Total Length of Fwd Packets",
    "Total Length of Bwd Packets",
    "Fwd Packet Length Min",
    "Fwd Packet Length Mean",
    "Fwd Packet Length Max",
    "Bwd Packet Length Min",
    "Bwd Packet Length Mean",
    "Bwd Packet Length Max",
    "Packet Length Std",
    "Packet Length Variance",
    "Flow IAT Min",
    "Flow IAT Mean",
    "Flow IAT Max",
    "Flow IAT Std",
    "Fwd IAT Total",
    "Bwd IAT Total",
    "FIN Flag Count",
    "SYN Flag Count",
    "RST Flag Count",
    "PSH Flag Count",
    "ACK Flag Count",
    "URG Flag Count",
    "CWE Flag Count",
    "ECE Flag Count",
}


def is_packet_capture(path: Path) -> bool:
    return Path(path).suffix.lower() in {".pcap", ".pcapng"}


def _endpoint_key(ip: str, port: int | None) -> tuple[str, int]:
    return ip, -1 if port is None else int(port)


def _flow_key(
    protocol: int,
    src_ip: str,
    dst_ip: str,
    src_port: int | None,
    dst_port: int | None,
) -> tuple[Any, ...]:
    left = _endpoint_key(src_ip, src_port)
    right = _endpoint_key(dst_ip, dst_port)
    a, b = sorted((left, right))
    return int(protocol), a, b


def _safe_std_variance(values_sum: float, values_sumsq: float, count: int) -> tuple[float, float]:
    if count <= 0:
        return 0.0, 0.0
    mean = values_sum / count
    variance = max(0.0, (values_sumsq / count) - (mean * mean))
    return math.sqrt(variance), variance


def _series_stats(values: list[float]) -> tuple[float, float, float, float]:
    if not values:
        return 0.0, 0.0, 0.0, 0.0
    total = sum(values)
    count = len(values)
    mean = total / count
    variance = max(0.0, sum((value - mean) ** 2 for value in values) / count)
    return min(values), mean, max(values), math.sqrt(variance)


@dataclass
class _DirectionStats:
    packets: int = 0
    bytes: int = 0
    length_min: int | None = None
    length_max: int | None = None
    length_sum: float = 0.0
    last_timestamp: float | None = None
    iat_total: float = 0.0

    def add(self, timestamp: float, packet_length: int) -> None:
        self.packets += 1
        self.bytes += packet_length
        self.length_sum += packet_length
        self.length_min = packet_length if self.length_min is None else min(self.length_min, packet_length)
        self.length_max = packet_length if self.length_max is None else max(self.length_max, packet_length)
        if self.last_timestamp is not None:
            self.iat_total += timestamp - self.last_timestamp
        self.last_timestamp = timestamp

    def min_length(self) -> float:
        return float(self.length_min or 0)

    def max_length(self) -> float:
        return float(self.length_max or 0)

    def mean_length(self) -> float:
        return self.length_sum / self.packets if self.packets else 0.0


@dataclass
class _FlowState:
    protocol: int
    source_ip: str
    destination_ip: str
    source_port: int | None
    destination_port: int | None
    start_timestamp: float
    end_timestamp: float
    forward: _DirectionStats = field(default_factory=_DirectionStats)
    backward: _DirectionStats = field(default_factory=_DirectionStats)
    last_timestamp: float | None = None
    flow_iats: list[float] = field(default_factory=list)
    length_sum: float = 0.0
    length_sumsq: float = 0.0
    length_count: int = 0
    flag_counts: dict[str, int] = field(
        default_factory=lambda: {
            "FIN": 0,
            "SYN": 0,
            "RST": 0,
            "PSH": 0,
            "ACK": 0,
            "URG": 0,
            "CWE": 0,
            "ECE": 0,
        }
    )

    def is_forward(
        self,
        src_ip: str,
        dst_ip: str,
        src_port: int | None,
        dst_port: int | None,
    ) -> bool:
        return (
            src_ip == self.source_ip
            and dst_ip == self.destination_ip
            and src_port == self.source_port
            and dst_port == self.destination_port
        )

    def add_packet(
        self,
        *,
        timestamp: float,
        packet_length: int,
        src_ip: str,
        dst_ip: str,
        src_port: int | None,
        dst_port: int | None,
        tcp_flags: int | None,
    ) -> None:
        if self.last_timestamp is not None:
            self.flow_iats.append(timestamp - self.last_timestamp)
        self.last_timestamp = timestamp
        self.end_timestamp = timestamp

        direction = self.forward if self.is_forward(src_ip, dst_ip, src_port, dst_port) else self.backward
        direction.add(timestamp, packet_length)

        self.length_sum += packet_length
        self.length_sumsq += packet_length * packet_length
        self.length_count += 1

        if tcp_flags is not None:
            # Scapy follows the conventional FIN,SYN,RST,PSH,ACK,URG,ECE,CWR bit positions.
            masks = {
                "FIN": 0x01,
                "SYN": 0x02,
                "RST": 0x04,
                "PSH": 0x08,
                "ACK": 0x10,
                "URG": 0x20,
                "ECE": 0x40,
                "CWE": 0x80,
            }
            for name, mask in masks.items():
                if tcp_flags & mask:
                    self.flag_counts[name] += 1

    def as_row(self) -> dict[str, Any]:
        flow_iat_min, flow_iat_mean, flow_iat_max, flow_iat_std = _series_stats(self.flow_iats)
        packet_length_std, packet_length_variance = _safe_std_variance(
            self.length_sum,
            self.length_sumsq,
            self.length_count,
        )
        return {
            "Timestamp": self.start_timestamp,
            "Flow End Timestamp": self.end_timestamp,
            "Source IP": self.source_ip,
            "Destination IP": self.destination_ip,
            "Source Port": self.source_port,
            "Destination Port": self.destination_port,
            "Protocol": self.protocol,
            "Flow Duration": self.end_timestamp - self.start_timestamp,
            "Total Fwd Packets": self.forward.packets,
            "Total Backward Packets": self.backward.packets,
            "Total Length of Fwd Packets": self.forward.bytes,
            "Total Length of Bwd Packets": self.backward.bytes,
            "Fwd Packet Length Min": self.forward.min_length(),
            "Fwd Packet Length Mean": self.forward.mean_length(),
            "Fwd Packet Length Max": self.forward.max_length(),
            "Bwd Packet Length Min": self.backward.min_length(),
            "Bwd Packet Length Mean": self.backward.mean_length(),
            "Bwd Packet Length Max": self.backward.max_length(),
            "Packet Length Std": packet_length_std,
            "Packet Length Variance": packet_length_variance,
            "Flow IAT Min": flow_iat_min,
            "Flow IAT Mean": flow_iat_mean,
            "Flow IAT Max": flow_iat_max,
            "Flow IAT Std": flow_iat_std,
            "Fwd IAT Total": self.forward.iat_total,
            "Bwd IAT Total": self.backward.iat_total,
            "FIN Flag Count": self.flag_counts["FIN"],
            "SYN Flag Count": self.flag_counts["SYN"],
            "RST Flag Count": self.flag_counts["RST"],
            "PSH Flag Count": self.flag_counts["PSH"],
            "ACK Flag Count": self.flag_counts["ACK"],
            "URG Flag Count": self.flag_counts["URG"],
            "CWE Flag Count": self.flag_counts["CWE"],
            "ECE Flag Count": self.flag_counts["ECE"],
        }


def build_pcap_flow_dataframe(dataset_path: Path) -> pd.DataFrame:
    """Stream a PCAP/PCAPNG into a canonical bidirectional 5-tuple flow table.

    The first observed packet defines the forward direction. Packet lengths are
    captured frame lengths and durations/IATs are expressed in seconds. The
    adapter deliberately performs no idle-timeout splitting: it reconstructs one
    record per bidirectional 5-tuple for the capture so downstream invariants do
    not depend on an arbitrary exporter timeout.
    """

    path = Path(dataset_path).expanduser().resolve()
    if not is_packet_capture(path):
        raise ValueError(f"Not a PCAP/PCAPNG dataset: {path}")

    flows: dict[tuple[Any, ...], _FlowState] = {}
    with PcapReader(str(path)) as reader:
        for packet in reader:
            if IP in packet:
                ip_layer = packet[IP]
                src_ip = str(ip_layer.src)
                dst_ip = str(ip_layer.dst)
                protocol = int(ip_layer.proto)
            elif IPv6 in packet:
                ip_layer = packet[IPv6]
                src_ip = str(ip_layer.src)
                dst_ip = str(ip_layer.dst)
                if TCP in packet:
                    protocol = 6
                elif UDP in packet:
                    protocol = 17
                else:
                    protocol = int(ip_layer.nh)
            else:
                continue

            src_port: int | None = None
            dst_port: int | None = None
            tcp_flags: int | None = None
            if TCP in packet:
                transport = packet[TCP]
                src_port = int(transport.sport)
                dst_port = int(transport.dport)
                tcp_flags = int(transport.flags)
            elif UDP in packet:
                transport = packet[UDP]
                src_port = int(transport.sport)
                dst_port = int(transport.dport)

            timestamp = float(packet.time)
            packet_length = int(len(packet))
            key = _flow_key(protocol, src_ip, dst_ip, src_port, dst_port)
            state = flows.get(key)
            if state is None:
                state = _FlowState(
                    protocol=protocol,
                    source_ip=src_ip,
                    destination_ip=dst_ip,
                    source_port=src_port,
                    destination_port=dst_port,
                    start_timestamp=timestamp,
                    end_timestamp=timestamp,
                )
                flows[key] = state

            state.add_packet(
                timestamp=timestamp,
                packet_length=packet_length,
                src_ip=src_ip,
                dst_ip=dst_ip,
                src_port=src_port,
                dst_port=dst_port,
                tcp_flags=tcp_flags,
            )

    rows = [state.as_row() for state in flows.values()]
    return pd.DataFrame(rows, columns=sorted(PCAP_FLOW_COLUMNS))


def pcap_metric_template(metric_id: str) -> dict | None:
    templates = {
        "valid_port_range_profile": {
            "metric_id": "valid_port_range_profile",
            "label": "Valid Port Range Profile",
            "input_requirements": {
                "candidate_fields": ["Source Port", "Destination Port"],
            },
            "calculation": {
                "parameters": {
                    "valid_min_port": 0,
                    "valid_max_port": 65535,
                    "invalid_ratio_fail_threshold": 0.01,
                }
            },
        },
        "tcp_flag_consistency_profile": {
            "metric_id": "tcp_flag_consistency_profile",
            "label": "TCP Flag Consistency Profile",
            "input_requirements": {
                "field_map": {
                    "protocol": "Protocol",
                    "total_fwd_packets": "Total Fwd Packets",
                    "total_bwd_packets": "Total Backward Packets",
                    "fin_flag_count": "FIN Flag Count",
                    "syn_flag_count": "SYN Flag Count",
                    "rst_flag_count": "RST Flag Count",
                    "psh_flag_count": "PSH Flag Count",
                    "ack_flag_count": "ACK Flag Count",
                    "urg_flag_count": "URG Flag Count",
                    "cwe_flag_count": "CWE Flag Count",
                    "ece_flag_count": "ECE Flag Count",
                }
            },
            "calculation": {
                "parameters": {
                    "tcp_protocol_values": [6, "6", "TCP", "tcp"],
                    "non_tcp_flags_must_be_zero": True,
                    "max_examples": 10,
                }
            },
        },
        "flow_duration_consistency_profile": {
            "metric_id": "flow_duration_consistency_profile",
            "label": "Flow Duration Consistency Profile",
            "input_requirements": {
                "field_map": {
                    "flow_duration": "Flow Duration",
                    "flow_iat_mean": "Flow IAT Mean",
                    "flow_iat_max": "Flow IAT Max",
                    "flow_iat_min": "Flow IAT Min",
                    "flow_iat_std": "Flow IAT Std",
                    "fwd_iat_total": "Fwd IAT Total",
                    "bwd_iat_total": "Bwd IAT Total",
                }
            },
            "calculation": {"parameters": {"tolerance": 1e-9, "max_examples": 10}},
        },
        "packet_byte_consistency_profile": {
            "metric_id": "packet_byte_consistency_profile",
            "label": "Packet/Byte Consistency Profile",
            "input_requirements": {
                "field_map": {
                    "total_fwd_packets": "Total Fwd Packets",
                    "total_bwd_packets": "Total Backward Packets",
                    "total_len_fwd_packets": "Total Length of Fwd Packets",
                    "total_len_bwd_packets": "Total Length of Bwd Packets",
                    "fwd_pkt_len_min": "Fwd Packet Length Min",
                    "fwd_pkt_len_mean": "Fwd Packet Length Mean",
                    "fwd_pkt_len_max": "Fwd Packet Length Max",
                    "bwd_pkt_len_min": "Bwd Packet Length Min",
                    "bwd_pkt_len_mean": "Bwd Packet Length Mean",
                    "bwd_pkt_len_max": "Bwd Packet Length Max",
                    "packet_length_std": "Packet Length Std",
                    "packet_length_variance": "Packet Length Variance",
                }
            },
            "calculation": {
                "parameters": {
                    "tolerance": 1e-9,
                    "variance_tolerance": 1e-6,
                    "max_examples": 10,
                }
            },
        },
    }
    template = templates.get(metric_id)
    return None if template is None else {**template}
