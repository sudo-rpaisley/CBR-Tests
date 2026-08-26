from __future__ import annotations

from copy import deepcopy
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

# Existing metrics that can consume the canonical decoded-packet view without
# requiring dataset-specific research assumptions.  These are grouped by the
# question they ask so automatic PCAP planning can expose every currently safe
# runnable metric rather than an arbitrary shortlist.
PCAP_PACKET_NETWORK_METRICS = {
    "reserved_ip_address_profile",
    "valid_port_range_profile",
}

PCAP_PACKET_DATA_QUALITY_METRICS = {
    "column_quality_profile",
    "missing_value_ratio",
    "duplicate_row_ratio",
}

PCAP_PACKET_DEPENDENCY_METRICS = {
    "pearson_correlation_profile",
    "spearman_correlation_matrix_deviation",
    "distance_correlation_matrix_deviation",
}

PCAP_PACKET_DISTRIBUTION_METRICS = {
    "kolmogorov_smirnov_feature_divergence",
    "wasserstein_feature_distance",
    "energy_distance",
    "maximum_mean_discrepancy",
}

PCAP_PACKET_TEMPORAL_METRICS = {
    "timestamp_parse_success_ratio",
    "inter_arrival_time_distribution_divergence",
    "burstiness_coefficient_deviation",
    "hourly_activity_distribution_divergence",
    "diurnal_pattern_similarity_score",
    "periodicity_preservation_score",
}

PCAP_PACKET_METRICS = (
    PCAP_PACKET_NETWORK_METRICS
    | PCAP_PACKET_DATA_QUALITY_METRICS
    | PCAP_PACKET_DEPENDENCY_METRICS
    | PCAP_PACKET_DISTRIBUTION_METRICS
    | PCAP_PACKET_TEMPORAL_METRICS
)

PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS

# These metrics are intentionally *not* automatically enabled for raw PCAP.
# Their tabular forms test whether separately exported flow fields agree with one
# another. If CBR-Tests derives both sides of those equations from the same PCAP,
# a pass would mostly validate this adapter rather than the source dataset.
PCAP_SELF_DERIVED_METRICS = {
    "tcp_flag_consistency_profile",
    "flow_duration_consistency_profile",
    "packet_byte_consistency_profile",
    "derived_rate_consistency_profile",
    "start_end_timestamp_consistency_ratio",
    "non_negative_duration_ratio",
}

# These metrics could consume reconstructed information, but their scientific
# interpretation depends on capture-boundary or experiment context that must be
# supplied explicitly rather than guessed by the automatic planner.
PCAP_CONTEXT_CONFIGURATION_REASONS = {
    "handshake_plausibility_profile": "capture_boundary_policy_required",
}

PCAP_PACKET_COLUMNS = {
    "Packet Index",
    "Timestamp",
    "Source IP",
    "Destination IP",
    "Source Port",
    "Destination Port",
    "Protocol",
    "IP Version",
    "Packet Length",
    "TCP Flags",
    "Inter Arrival Time",
}

# Canonical flow view retained for later sequence/reference metrics. It is not
# currently used to manufacture extra self-consistency passes in a PCAP plan.
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


def _packet_fields(packet) -> dict[str, Any] | None:
    if IP in packet:
        ip_layer = packet[IP]
        src_ip = str(ip_layer.src)
        dst_ip = str(ip_layer.dst)
        protocol = int(ip_layer.proto)
        ip_version = 4
    elif IPv6 in packet:
        ip_layer = packet[IPv6]
        src_ip = str(ip_layer.src)
        dst_ip = str(ip_layer.dst)
        ip_version = 6
        # IPv6 extension headers can make the base next-header field differ from
        # the eventual transport protocol. Prefer the decoded transport layer.
        if TCP in packet:
            protocol = 6
        elif UDP in packet:
            protocol = 17
        else:
            protocol = int(ip_layer.nh)
    else:
        return None

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

    return {
        "Timestamp": float(packet.time),
        "Source IP": src_ip,
        "Destination IP": dst_ip,
        "Source Port": src_port,
        "Destination Port": dst_port,
        "Protocol": protocol,
        "IP Version": ip_version,
        "Packet Length": int(len(packet)),
        "TCP Flags": tcp_flags,
    }


def build_pcap_packet_dataframe(dataset_path: Path) -> pd.DataFrame:
    """Return one canonical row per decoded IPv4/IPv6 packet.

    Values in this view are copied from decoded packet fields rather than derived
    from reconstructed flows, so packet-level metrics can operate on raw capture
    evidence without first inventing exporter-specific flow semantics.
    """

    path = Path(dataset_path).expanduser().resolve()
    if not is_packet_capture(path):
        raise ValueError(f"Not a PCAP/PCAPNG dataset: {path}")

    rows: list[dict[str, Any]] = []
    previous_timestamp: float | None = None
    with PcapReader(str(path)) as reader:
        for packet_index, packet in enumerate(reader):
            fields = _packet_fields(packet)
            if fields is None:
                continue
            timestamp = float(fields["Timestamp"])
            inter_arrival_time = (
                None if previous_timestamp is None else timestamp - previous_timestamp
            )
            previous_timestamp = timestamp
            rows.append(
                {
                    "Packet Index": packet_index,
                    **fields,
                    "Inter Arrival Time": inter_arrival_time,
                }
            )

    return pd.DataFrame(rows, columns=sorted(PCAP_PACKET_COLUMNS))


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
    """Stream a PCAP/PCAPNG into a canonical bidirectional 5-tuple view.

    The first observed packet defines the forward direction. Packet lengths are
    captured frame lengths and durations/IATs are expressed in seconds. No idle
    timeout is guessed: one row is produced per bidirectional 5-tuple across the
    capture. Consequently this view is infrastructure for later sequence and
    reference metrics, not evidence that self-derived flow arithmetic is realistic.
    """

    path = Path(dataset_path).expanduser().resolve()
    if not is_packet_capture(path):
        raise ValueError(f"Not a PCAP/PCAPNG dataset: {path}")

    flows: dict[tuple[Any, ...], _FlowState] = {}
    with PcapReader(str(path)) as reader:
        for packet in reader:
            fields = _packet_fields(packet)
            if fields is None:
                continue

            src_ip = fields["Source IP"]
            dst_ip = fields["Destination IP"]
            src_port = fields["Source Port"]
            dst_port = fields["Destination Port"]
            protocol = fields["Protocol"]
            timestamp = fields["Timestamp"]
            packet_length = fields["Packet Length"]
            tcp_flags = fields["TCP Flags"]

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
    """Return a deterministic template for a metric safe on decoded packet evidence.

    The templates deliberately avoid dataset-specific policy such as service
    definitions, allowed slice IDs, reference datasets, attack windows, or model
    configuration.  Numeric dependency/drift metrics use packet length and
    capture-order inter-arrival time because those quantities have meaningful
    continuous scales; TCP flag bitmasks and port identifiers are not treated as
    ordinal measurements for correlation.
    """

    numeric_analysis_fields = ["Packet Length", "Inter Arrival Time"]
    templates = {
        "reserved_ip_address_profile": {
            "metric_id": "reserved_ip_address_profile",
            "label": "Reserved/Special-Use IP Address Profile",
            "input_requirements": {
                "candidate_fields": ["Source IP", "Destination IP"],
            },
            "calculation": {
                "method": "Profile decoded source/destination IP address categories and apply only explicitly configured special-use policy categories.",
                "parameters": {
                    "invalid_ratio_fail_threshold": 0.01,
                },
            },
        },
        "valid_port_range_profile": {
            "metric_id": "valid_port_range_profile",
            "label": "Valid Port Range Profile",
            "input_requirements": {
                "candidate_fields": ["Source Port", "Destination Port"],
            },
            "calculation": {
                "method": "Validate decoded TCP/UDP source and destination ports against the configured integer port range.",
                "parameters": {
                    "valid_min_port": 0,
                    "valid_max_port": 65535,
                    "invalid_ratio_fail_threshold": 0.01,
                },
            },
        },
        "column_quality_profile": {
            "metric_id": "column_quality_profile",
            "label": "Canonical Packet Numeric Field Profile",
            "input_requirements": {
                "candidate_fields": [
                    "Packet Length",
                    "Inter Arrival Time",
                    "Source Port",
                    "Destination Port",
                    "TCP Flags",
                ],
            },
            "calculation": {
                "method": "Profile numeric usability of canonical packet fields; missing transport fields are protocol-conditional and descriptive rather than automatically unrealistic.",
                "parameters": {},
            },
        },
        "missing_value_ratio": {
            "metric_id": "missing_value_ratio",
            "label": "Canonical Packet Field Missingness",
            "input_requirements": {
                "candidate_fields": [
                    "Timestamp",
                    "Source IP",
                    "Destination IP",
                    "Source Port",
                    "Destination Port",
                    "Protocol",
                    "IP Version",
                    "Packet Length",
                    "TCP Flags",
                    "Inter Arrival Time",
                ],
            },
            "calculation": {
                "method": "Profile missingness in canonical packet fields; port/TCP-flag absence is protocol-conditional and the first inter-arrival value is expected to be missing.",
                "parameters": {},
            },
        },
        "duplicate_row_ratio": {
            "metric_id": "duplicate_row_ratio",
            "label": "Repeated Packet-Signature Ratio",
            "input_requirements": {
                "subset_fields": [
                    "Source IP",
                    "Destination IP",
                    "Source Port",
                    "Destination Port",
                    "Protocol",
                    "IP Version",
                    "Packet Length",
                    "TCP Flags",
                ],
            },
            "calculation": {
                "method": "Measure repeated canonical packet signatures while excluding capture index and timestamp; repeats are descriptive because retransmissions and repeated requests may be legitimate.",
                "parameters": {},
            },
        },
        "pearson_correlation_profile": {
            "metric_id": "pearson_correlation_profile",
            "label": "Packet Length/IAT Pearson Profile",
            "input_requirements": {
                "candidate_fields": numeric_analysis_fields,
                "minimum_runnable_fields": 2,
            },
            "calculation": {
                "method": "Compute Pearson dependence between packet length and capture-order inter-arrival time.",
                "parameters": {},
            },
        },
        "spearman_correlation_matrix_deviation": {
            "metric_id": "spearman_correlation_matrix_deviation",
            "label": "Packet Length/IAT Spearman Profile",
            "input_requirements": {
                "candidate_fields": numeric_analysis_fields,
                "minimum_runnable_fields": 2,
            },
            "calculation": {
                "method": "Compute Spearman rank dependence between packet length and capture-order inter-arrival time.",
                "parameters": {},
            },
        },
        "distance_correlation_matrix_deviation": {
            "metric_id": "distance_correlation_matrix_deviation",
            "label": "Packet Length/IAT Distance-Correlation Profile",
            "input_requirements": {
                "candidate_fields": numeric_analysis_fields,
                "minimum_runnable_fields": 2,
            },
            "calculation": {
                "method": "Compute nonlinear distance correlation between packet length and capture-order inter-arrival time using a deterministic evenly spaced computational sample when necessary.",
                "parameters": {
                    "max_sample_size": 1000,
                },
            },
        },
        "kolmogorov_smirnov_feature_divergence": {
            "metric_id": "kolmogorov_smirnov_feature_divergence",
            "label": "Packet Feature KS Internal Drift",
            "input_requirements": {"candidate_fields": numeric_analysis_fields},
            "calculation": {
                "method": "Compare first- and second-half packet-length and inter-arrival distributions with the two-sample KS statistic.",
                "parameters": {"minimum_sample_size": 2, "max_sample_size": 1000},
            },
        },
        "wasserstein_feature_distance": {
            "metric_id": "wasserstein_feature_distance",
            "label": "Packet Feature Wasserstein Internal Drift",
            "input_requirements": {"candidate_fields": numeric_analysis_fields},
            "calculation": {
                "method": "Compare first- and second-half packet-length and inter-arrival distributions with one-dimensional Wasserstein distance.",
                "parameters": {"minimum_sample_size": 2, "max_sample_size": 1000},
            },
        },
        "energy_distance": {
            "metric_id": "energy_distance",
            "label": "Packet Feature Energy-Distance Internal Drift",
            "input_requirements": {"candidate_fields": numeric_analysis_fields},
            "calculation": {
                "method": "Compare first- and second-half packet-length and inter-arrival distributions with the implemented energy-distance expression.",
                "parameters": {"minimum_sample_size": 2, "max_sample_size": 1000},
            },
        },
        "maximum_mean_discrepancy": {
            "metric_id": "maximum_mean_discrepancy",
            "label": "Packet Feature MMD Internal Drift",
            "input_requirements": {"candidate_fields": numeric_analysis_fields},
            "calculation": {
                "method": "Compare first- and second-half packet-length and inter-arrival distributions with RBF-kernel squared MMD.",
                "parameters": {"minimum_sample_size": 2, "max_sample_size": 1000},
            },
        },
        "timestamp_parse_success_ratio": {
            "metric_id": "timestamp_parse_success_ratio",
            "label": "Packet Timestamp Parse Success Ratio",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Parse PCAP epoch timestamps explicitly as seconds since the Unix epoch.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "inter_arrival_time_distribution_divergence": {
            "metric_id": "inter_arrival_time_distribution_divergence",
            "label": "Packet Inter-Arrival Internal Divergence",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare first- and second-half packet inter-arrival distributions using decoded PCAP timestamps.",
                "parameters": {"timestamp_unit": "s", "minimum_sample_size": 2},
            },
        },
        "burstiness_coefficient_deviation": {
            "metric_id": "burstiness_coefficient_deviation",
            "label": "Packet Burstiness Coefficient Deviation",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare burstiness coefficients of first- and second-half packet inter-arrival gaps.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "hourly_activity_distribution_divergence": {
            "metric_id": "hourly_activity_distribution_divergence",
            "label": "Packet Hourly Activity Divergence",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare first- and second-half UTC hourly packet activity distributions.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "diurnal_pattern_similarity_score": {
            "metric_id": "diurnal_pattern_similarity_score",
            "label": "Packet Diurnal Pattern Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare first- and second-half UTC hourly packet-count shapes using cosine similarity.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "periodicity_preservation_score": {
            "metric_id": "periodicity_preservation_score",
            "label": "Packet Periodicity Preservation Score",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare autocorrelation of first- and second-half UTC hourly packet counts at configured lags.",
                "parameters": {"timestamp_unit": "s", "lags": [1, 24]},
            },
        },
    }
    template = templates.get(metric_id)
    return None if template is None else deepcopy(template)
