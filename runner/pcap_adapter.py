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


# Direct metrics read packet captures themselves rather than consuming the
# canonical decoded-packet dataframe. The old protocol_validity_profile remains
# runnable for compatibility/diagnostics but is no longer an automatic taxonomy
# leaf: Valid IP Address Ratio now has its own canonical implementation.
PCAP_DIRECT_METRICS = {
    "timestamp_coherence_profile",
    "handshake_plausibility_profile",
}

# Metrics that can consume the canonical decoded-packet view without requiring
# dataset-specific research assumptions.
PCAP_PACKET_NETWORK_METRICS = {
    "valid_ip_address_ratio",
    "valid_port_range_profile",
}

PCAP_PACKET_DATA_QUALITY_METRICS = {
    "column_quality_profile",
    "missing_value_ratio",
    "duplicate_row_ratio",
}

# The intrinsic diagnostic IDs below are the canonical names used by new plans.
# Historical IDs remain available through pcap_metric_template and the runtime
# dispatcher, but they are deliberately not advertised by PCAP_SUPPORTED_METRICS.
PCAP_PACKET_DEPENDENCY_METRICS = {
    "pearson_dependency_profile",
    "spearman_dependency_profile",
    "distance_correlation_dependency_profile",
}

PCAP_PACKET_DISTRIBUTION_METRICS = {
    "feature_ks_internal_drift",
    "feature_wasserstein_internal_drift",
    "feature_energy_internal_drift",
    "feature_mmd2_internal_drift",
}

PCAP_PACKET_TEMPORAL_METRICS = {
    "timestamp_parse_success_ratio",
    "inter_arrival_internal_drift_ks",
    "burstiness_internal_drift",
    "day_to_day_hourly_activity_divergence",
    "day_to_day_diurnal_similarity",
    "lagged_periodicity_similarity",
}

PCAP_PACKET_METRICS = (
    PCAP_PACKET_NETWORK_METRICS
    | PCAP_PACKET_DATA_QUALITY_METRICS
    | PCAP_PACKET_DEPENDENCY_METRICS
    | PCAP_PACKET_DISTRIBUTION_METRICS
    | PCAP_PACKET_TEMPORAL_METRICS
)

# Canonical IDs reuse the established packet-view configurations. Keeping this
# mapping inside the PCAP adapter makes backwards compatibility local and keeps
# old construct names out of newly generated plans.
PCAP_CANONICAL_TEMPLATE_SOURCE = {
    "pearson_dependency_profile": "pearson_correlation_profile",
    "spearman_dependency_profile": "spearman_correlation_matrix_deviation",
    "distance_correlation_dependency_profile": "distance_correlation_matrix_deviation",
    "feature_ks_internal_drift": "kolmogorov_smirnov_feature_divergence",
    "feature_wasserstein_internal_drift": "wasserstein_feature_distance",
    "feature_energy_internal_drift": "energy_distance",
    "feature_mmd2_internal_drift": "maximum_mean_discrepancy",
    "inter_arrival_internal_drift_ks": "inter_arrival_time_distribution_divergence",
    "burstiness_internal_drift": "burstiness_coefficient_deviation",
    "day_to_day_hourly_activity_divergence": "hourly_activity_distribution_divergence",
    "day_to_day_diurnal_similarity": "diurnal_pattern_similarity_score",
    "lagged_periodicity_similarity": "periodicity_preservation_score",
}

PCAP_CANONICAL_LABELS = {
    "pearson_dependency_profile": "Packet Length/IAT Pearson Dependency Profile",
    "spearman_dependency_profile": "Packet Length/IAT Spearman Dependency Profile",
    "distance_correlation_dependency_profile": "Packet Length/IAT Distance-Correlation Dependency Profile",
    "feature_ks_internal_drift": "Packet Feature KS Internal Drift",
    "feature_wasserstein_internal_drift": "Packet Feature Wasserstein Internal Drift",
    "feature_energy_internal_drift": "Packet Feature Energy Internal Drift",
    "feature_mmd2_internal_drift": "Packet Feature MMD² Internal Drift",
    "inter_arrival_internal_drift_ks": "Packet Inter-Arrival Internal Drift (KS)",
    "burstiness_internal_drift": "Packet Burstiness Internal Drift",
    "day_to_day_hourly_activity_divergence": "Packet Day-to-Day Hourly Activity Divergence",
    "day_to_day_diurnal_similarity": "Packet Day-to-Day Diurnal Similarity",
    "lagged_periodicity_similarity": "Packet Lagged Periodicity Similarity",
}

PCAP_REFERENCE_METRICS = {
    "feature_wise_wasserstein_distance_from_reference",
    "feature_wise_ks_statistic_from_reference",
    "feature_wise_energy_distance_from_reference",
    "feature_set_mmd_score_from_reference",
    "pearson_matrix_deviation_from_reference",
    "spearman_matrix_deviation_from_reference",
    "distance_correlation_matrix_deviation_from_reference",
    "inter_arrival_distribution_divergence_from_reference",
    "burstiness_deviation_from_reference",
    "hourly_activity_divergence_from_reference",
    "protocol_mix_divergence_from_reference",
    "port_use_divergence_from_reference",
}

PCAP_REFERENCE_UNSUPPORTED_REASONS = {
    "slice_proportion_deviation_from_reference": "slice_metadata_required",
    "per_slice_class_divergence_from_reference": "slice_metadata_required",
    "per_slice_feature_distribution_deviation_from_reference": "slice_metadata_required",
    "flow_statistic_deviation_from_reference": "flow_segmentation_policy_required",
}

# Explicit packet-backed metrics require scenario configuration and therefore are
# not part of the configuration-free automatic set.
PCAP_EXPLICIT_PACKET_METRICS = {
    "service_port_consistency_profile",
    "reserved_address_misuse_ratio",
}
PCAP_PACKET_BACKED_METRICS = (
    PCAP_PACKET_METRICS | PCAP_REFERENCE_METRICS | PCAP_EXPLICIT_PACKET_METRICS
)
PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS

PCAP_SELF_DERIVED_METRICS = {
    "tcp_flag_consistency_profile",
    "flow_duration_consistency_profile",
    "packet_byte_consistency_profile",
    "derived_rate_consistency_profile",
    "start_end_timestamp_consistency_ratio",
    "non_negative_duration_ratio",
}

PCAP_CONTEXT_CONFIGURATION_REASONS = {
    "reserved_address_misuse_ratio": "address_policy_required",
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
    """Return one canonical row per decoded IPv4/IPv6 packet."""
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
    """Stream a PCAP/PCAPNG into a canonical bidirectional 5-tuple view."""
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

    Canonical intrinsic diagnostic IDs reuse the established packet-view
    configuration. Historical IDs continue to resolve for replaying old plans.
    """

    numeric_analysis_fields = ["Packet Length", "Inter Arrival Time"]
    templates = {
        "valid_ip_address_ratio": {
            "metric_id": "valid_ip_address_ratio",
            "label": "Valid IP Address Ratio",
            "input_requirements": {
                "candidate_fields": ["Source IP", "Destination IP"],
            },
            "calculation": {
                "method": "Count syntactically valid non-missing decoded source/destination IP values divided by all non-missing candidate IP values.",
                "parameters": {},
            },
        },
        "reserved_address_misuse_ratio": {
            "metric_id": "reserved_address_misuse_ratio",
            "label": "Reserved-Address Misuse Ratio",
            "input_requirements": {
                "candidate_fields": ["Source IP", "Destination IP"],
            },
            "calculation": {
                "method": "Count scenario-policy-inconsistent special-use addresses divided by syntactically valid candidate IP values.",
                "parameters": {"misuse_categories": []},
            },
        },
        "reserved_ip_address_profile": {
            "metric_id": "reserved_ip_address_profile",
            "label": "Reserved/Special-Use IP Address Profile",
            "input_requirements": {
                "candidate_fields": ["Source IP", "Destination IP"],
            },
            "calculation": {
                "method": "Legacy descriptive profile of decoded source/destination IP address categories.",
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
            "label": "Packet Day-to-Day Hourly Activity Divergence",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Measure mean pairwise total-variation divergence between UTC hour-of-day activity profiles from separate observed days.",
                "parameters": {"timestamp_unit": "s", "minimum_day_count": 2},
            },
        },
        "diurnal_pattern_similarity_score": {
            "metric_id": "diurnal_pattern_similarity_score",
            "label": "Packet Day-to-Day Diurnal Pattern Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Measure mean pairwise cosine similarity between UTC hour-of-day packet-count profiles from separate observed days.",
                "parameters": {"timestamp_unit": "s", "minimum_day_count": 2},
            },
        },
        "periodicity_preservation_score": {
            "metric_id": "periodicity_preservation_score",
            "label": "Packet Hourly Periodicity Repeat Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare the continuous UTC hourly packet-count series with lagged copies; lag 24 directly measures one-day repeat similarity.",
                "parameters": {"timestamp_unit": "s", "lags": [24], "minimum_lag_pairs": 2},
            },
        },
    }
    source_id = PCAP_CANONICAL_TEMPLATE_SOURCE.get(metric_id, metric_id)
    template = templates.get(source_id)
    if template is None:
        return None
    template = deepcopy(template)
    if source_id != metric_id:
        template["metric_id"] = metric_id
        template["label"] = PCAP_CANONICAL_LABELS[metric_id]
    return template


def pcap_service_port_template(service_name: str, expected_ports: list[int]) -> dict:
    """Build a service-port metric only for an explicitly single-service capture."""
    name = str(service_name).strip()
    ports = sorted({int(port) for port in expected_ports})
    if not name:
        raise ValueError("service_name must not be empty")
    if not ports or any(port < 0 or port > 65535 for port in ports):
        raise ValueError("expected service ports must contain integers in 0-65535")
    return {
        "metric_id": "service_port_consistency_profile",
        "label": "Service-Port Consistency Profile",
        "input_requirements": {
            "port_fields": ["Source Port", "Destination Port"],
        },
        "calculation": {
            "method": "pcap_single_service_port_consistency",
            "parameters": {
                "service_name": name,
                "expected_ports": ports,
                "match_mode": "any_port",
                "population_mode": "all_rows",
                "pass_threshold": 1.0,
                "warn_threshold": 0.0,
                "max_examples": 10,
                "population_assumption_source": "explicit_plan_configuration",
            },
        },
    }


def pcap_reference_metric_template(metric_id: str, reference_dataset_path: Path) -> dict | None:
    """Return a same-representation packet-level reference metric template."""

    reference_path = str(Path(reference_dataset_path).expanduser().resolve())
    numeric_fields = ["Packet Length", "Inter Arrival Time"]
    templates = {
        "feature_wise_wasserstein_distance_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Feature Wasserstein Distance From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_wasserstein", "parameters": {"max_sample_size": 1000}},
        },
        "feature_wise_ks_statistic_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Feature KS Statistic From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_ks", "parameters": {"max_sample_size": 1000}},
        },
        "feature_wise_energy_distance_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Feature Energy Distance From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_energy_distance", "parameters": {"max_sample_size": 1000}},
        },
        "feature_set_mmd_score_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Feature-Set MMD From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "standardized_multivariate_rbf_mmd", "parameters": {"max_sample_size": 500}},
        },
        "pearson_matrix_deviation_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Pearson Matrix Deviation From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_pearson", "parameters": {}},
        },
        "spearman_matrix_deviation_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Spearman Matrix Deviation From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_spearman", "parameters": {}},
        },
        "distance_correlation_matrix_deviation_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Distance-Correlation Deviation From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "candidate_fields": numeric_fields},
            "calculation": {"method": "packet_reference_distance_correlation", "parameters": {"max_sample_size": 1000}},
        },
        "inter_arrival_distribution_divergence_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Inter-Arrival Divergence From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "timestamp_field": "Timestamp"},
            "calculation": {"method": "packet_reference_inter_arrival", "parameters": {"timestamp_unit": "s"}},
        },
        "burstiness_deviation_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Burstiness Deviation From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "timestamp_field": "Timestamp"},
            "calculation": {"method": "packet_reference_burstiness", "parameters": {"timestamp_unit": "s"}},
        },
        "hourly_activity_divergence_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Hourly Activity Divergence From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "timestamp_field": "Timestamp"},
            "calculation": {"method": "packet_reference_hourly_activity", "parameters": {"timestamp_unit": "s"}},
        },
        "protocol_mix_divergence_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Protocol-Mix Divergence From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "protocol_field": "Protocol"},
            "calculation": {"method": "packet_reference_protocol_mix", "parameters": {}},
        },
        "port_use_divergence_from_reference": {
            "metric_id": metric_id,
            "label": "Packet Port-Use Divergence From Reference",
            "input_requirements": {"reference_dataset_path": reference_path, "port_fields": ["Source Port", "Destination Port"]},
            "calculation": {"method": "packet_reference_port_use", "parameters": {}},
        },
    }
    template = templates.get(metric_id)
    return None if template is None else deepcopy(template)
