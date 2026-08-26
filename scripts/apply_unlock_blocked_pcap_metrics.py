from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    if old not in text:
        raise RuntimeError(f"Expected text not found in {path}: {old[:120]!r}")
    file_path.write_text(text.replace(old, new, 1), encoding="utf-8")


def patch_pcap_adapter() -> None:
    path = Path("runner/pcap_adapter.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        'PCAP_DIRECT_METRICS = {\n    "protocol_validity_profile",\n    "timestamp_coherence_profile",\n}\n',
        'PCAP_DIRECT_METRICS = {\n    "protocol_validity_profile",\n    "timestamp_coherence_profile",\n    "handshake_plausibility_profile",\n}\n',
        1,
    )
    text = text.replace(
        "PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS\n",
        '''PCAP_REFERENCE_METRICS = {
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

PCAP_EXPLICIT_PACKET_METRICS = {"service_port_consistency_profile"}
PCAP_PACKET_BACKED_METRICS = (
    PCAP_PACKET_METRICS | PCAP_REFERENCE_METRICS | PCAP_EXPLICIT_PACKET_METRICS
)
PCAP_SUPPORTED_METRICS = PCAP_DIRECT_METRICS | PCAP_PACKET_BACKED_METRICS
''',
        1,
    )
    text = text.replace(
        '''# These metrics could consume reconstructed information, but their scientific
# interpretation depends on capture-boundary or experiment context that must be
# supplied explicitly rather than guessed by the automatic planner.
PCAP_CONTEXT_CONFIGURATION_REASONS = {
    "handshake_plausibility_profile": "capture_boundary_policy_required",
}
''',
        '''# Context-sensitive raw-PCAP metrics that still require explicit research
# configuration. Handshake plausibility is intentionally absent: its native PCAP
# implementation only evaluates attempts whose opening SYN is actually observed.
PCAP_CONTEXT_CONFIGURATION_REASONS = {}
''',
        1,
    )
    if "def pcap_service_port_template(" not in text:
        text += '''


def pcap_service_port_template(service_name: str, expected_ports: list[int]) -> dict:
    """Build a service-port metric only for an explicitly single-service capture.

    The service population must come from independent experiment knowledge. The
    framework never infers a service from the same ports it is about to test.
    """

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
'''
    path.write_text(text, encoding="utf-8")


def write_handshake_metric() -> None:
    Path("cbr_tests/metrics/pcap_handshake.py").write_text('''from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scapy.layers.inet import IP, TCP
from scapy.layers.inet6 import IPv6
from scapy.utils import PcapReader


@dataclass
class _HandshakeAttempt:
    initiator: tuple[str, int]
    responder: tuple[str, int]
    stage: str = "syn"
    simultaneous_open: bool = False


def _endpoint(packet, source: bool) -> tuple[str, int] | None:
    if TCP not in packet:
        return None
    if IP in packet:
        address = str(packet[IP].src if source else packet[IP].dst)
    elif IPv6 in packet:
        address = str(packet[IPv6].src if source else packet[IPv6].dst)
    else:
        return None
    port = int(packet[TCP].sport if source else packet[TCP].dport)
    return address, port


def _flow_key(left: tuple[str, int], right: tuple[str, int]):
    return tuple(sorted((left, right)))


def run_pcap_handshake_plausibility_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Profile observable TCP handshake order without assuming capture boundaries.

    Only attempts whose opening SYN (without ACK) is captured are evaluated.
    Mid-stream traffic, incomplete attempts, resets and missing handshake packets
    remain descriptive evidence because they are legitimate in security captures.
    """

    max_examples = int(metric.get("calculation", {}).get("parameters", {}).get("max_examples", 10))
    if max_examples < 0:
        return False, {"error": "max_examples must be non-negative.", "reason_code": "invalid_metric_configuration"}

    packet_count = tcp_packet_count = initiated_attempt_count = 0
    completed_handshake_count = reset_before_completion_count = 0
    syn_retransmission_count = syn_ack_retransmission_count = 0
    unpaired_syn_ack_count = ack_without_observed_syn_ack_count = 0
    simultaneous_open_count = contradictory_transition_count = 0
    boundary_flow_keys: set[tuple] = set()
    active: dict[tuple, _HandshakeAttempt] = {}
    examples: list[dict] = []

    def record(packet_index: int, reason: str, **evidence) -> None:
        if len(examples) < max_examples:
            examples.append({"packet_index": packet_index, "reason": reason, **evidence})

    try:
        with PcapReader(str(dataset_path)) as reader:
            for packet_index, packet in enumerate(reader):
                packet_count += 1
                if TCP not in packet or (IP not in packet and IPv6 not in packet):
                    continue
                tcp_packet_count += 1
                source = _endpoint(packet, True)
                destination = _endpoint(packet, False)
                if source is None or destination is None:
                    continue
                key = _flow_key(source, destination)
                flags = int(packet[TCP].flags)
                syn = bool(flags & 0x02)
                ack = bool(flags & 0x10)
                rst = bool(flags & 0x04)
                attempt = active.get(key)

                if syn and not ack:
                    if attempt is None:
                        active[key] = _HandshakeAttempt(source, destination)
                        initiated_attempt_count += 1
                    elif source == attempt.initiator:
                        syn_retransmission_count += 1
                    else:
                        if not attempt.simultaneous_open:
                            simultaneous_open_count += 1
                        attempt.simultaneous_open = True
                    continue

                if syn and ack:
                    if attempt is None:
                        unpaired_syn_ack_count += 1
                        boundary_flow_keys.add(key)
                    elif source == attempt.responder:
                        if attempt.stage == "syn":
                            attempt.stage = "syn_ack"
                        else:
                            syn_ack_retransmission_count += 1
                    elif attempt.simultaneous_open:
                        attempt.stage = "syn_ack"
                    else:
                        contradictory_transition_count += 1
                        record(
                            packet_index,
                            "syn_ack_from_opening_syn_initiator",
                            source=f"{source[0]}:{source[1]}",
                            destination=f"{destination[0]}:{destination[1]}",
                        )
                    continue

                if rst and attempt is not None:
                    reset_before_completion_count += 1
                    del active[key]
                    continue

                if ack and attempt is not None:
                    if attempt.stage == "syn_ack" and source == attempt.initiator:
                        completed_handshake_count += 1
                        del active[key]
                    elif attempt.stage == "syn" and source == attempt.initiator:
                        ack_without_observed_syn_ack_count += 1
                        boundary_flow_keys.add(key)
                        del active[key]
                    continue

                if attempt is None:
                    boundary_flow_keys.add(key)
    except Exception as exc:
        return False, {"error": f"Failed to analyse packet capture: {exc}", "reason_code": "dataset_load_error"}

    incomplete_attempt_count = len(active)
    completion_ratio = (
        round(completed_handshake_count / initiated_attempt_count, 6)
        if initiated_attempt_count else None
    )
    if initiated_attempt_count == 0:
        status = "not_applicable"
        plausibility_ratio = None
        diagnostic = {
            "reason_code": "no_observed_tcp_handshake_initiation",
            "summary": "No TCP opening SYN was observed, so handshake order cannot be evaluated without guessing what occurred before capture began.",
            "evidence": {
                "packet_count": packet_count,
                "tcp_packet_count": tcp_packet_count,
                "unpaired_syn_ack_count": unpaired_syn_ack_count,
                "boundary_excluded_flow_count": len(boundary_flow_keys),
            },
        }
    else:
        plausibility_ratio = round(
            (initiated_attempt_count - contradictory_transition_count) / initiated_attempt_count,
            6,
        )
        if contradictory_transition_count:
            status = "warn"
            diagnostic = {
                "reason_code": "observable_handshake_direction_contradiction",
                "summary": f"Observed {contradictory_transition_count} captured TCP handshake transition(s) whose direction conflicts with the captured opening SYN. This requires inspection rather than being treated as proof of unrealistic traffic.",
                "evidence": {
                    "initiated_attempt_count": initiated_attempt_count,
                    "contradictory_transition_count": contradictory_transition_count,
                    "handshake_plausibility_ratio": plausibility_ratio,
                    "examples": examples,
                },
                "suggestion": "Inspect the cited packets and capture context; spoofing, reordering or adversarial traffic may be intentional.",
            }
        else:
            status = "pass"
            diagnostic = {
                "reason_code": "observable_handshake_sequence_plausible",
                "summary": "No directional contradiction was found among TCP handshake attempts whose opening SYN was captured.",
                "evidence": {
                    "initiated_attempt_count": initiated_attempt_count,
                    "completed_handshake_count": completed_handshake_count,
                    "reset_before_completion_count": reset_before_completion_count,
                    "incomplete_attempt_count": incomplete_attempt_count,
                    "handshake_plausibility_ratio": plausibility_ratio,
                },
            }

    return True, {
        "test_results": {
            "handshake_plausibility_profile": {
                "packet_count": packet_count,
                "tcp_packet_count": tcp_packet_count,
                "initiated_attempt_count": initiated_attempt_count,
                "completed_handshake_count": completed_handshake_count,
                "reset_before_completion_count": reset_before_completion_count,
                "incomplete_attempt_count": incomplete_attempt_count,
                "syn_retransmission_count": syn_retransmission_count,
                "syn_ack_retransmission_count": syn_ack_retransmission_count,
                "unpaired_syn_ack_count": unpaired_syn_ack_count,
                "ack_without_observed_syn_ack_count": ack_without_observed_syn_ack_count,
                "simultaneous_open_count": simultaneous_open_count,
                "boundary_excluded_flow_count": len(boundary_flow_keys),
                "contradictory_transition_count": contradictory_transition_count,
                "handshake_completion_ratio": completion_ratio,
                "handshake_plausibility_ratio": plausibility_ratio,
                "examples": examples,
                "status": status,
                "diagnostic": diagnostic,
                "interpretation": "Completion ratio is descriptive rather than a realism criterion; SYN-only scans, resets, packet loss and capture truncation can all be legitimate security traffic.",
            }
        }
    }
''', encoding="utf-8")


def patch_dispatch() -> None:
    replace_once(
        "runner/dispatch.py",
        "from cbr_tests.metrics.timestamp_coherence import run_timestamp_coherence_metric\nfrom runner.field_translation import translate_metric_fields\n",
        "from cbr_tests.metrics.timestamp_coherence import run_timestamp_coherence_metric\nfrom cbr_tests.metrics.pcap_handshake import run_pcap_handshake_plausibility_metric\nfrom runner.field_translation import translate_metric_fields\nfrom runner.pcap_adapter import is_packet_capture\n",
    )
    replace_once(
        "runner/dispatch.py",
        '@register_metric("handshake_plausibility_profile")\ndef _handshake_metric(dataset_path: Path, metric: dict):\n    return run_handshake_plausibility_metric(dataset_path, metric)\n',
        '@register_metric("handshake_plausibility_profile")\ndef _handshake_metric(dataset_path: Path, metric: dict):\n    if is_packet_capture(dataset_path):\n        return run_pcap_handshake_plausibility_metric(dataset_path, metric)\n    return run_handshake_plausibility_metric(dataset_path, metric)\n',
    )


def patch_reference_metrics() -> None:
    path = Path("tests/reference_model_comparison_profile.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "from math import sqrt\nfrom pathlib import Path\n\nimport pandas as pd\n",
        "from math import sqrt\nfrom pathlib import Path\nfrom threading import Lock\n\nimport numpy as np\nimport pandas as pd\n\nfrom cbr_tests.metrics.temporal import _timestamp_unit\nfrom runner.pcap_adapter import build_pcap_packet_dataframe, is_packet_capture\n",
        1,
    )
    old_loader = '''def _load_reference_df(metric: dict) -> pd.DataFrame:
    path_value = _reference_path(metric)
    if not path_value:
        return pd.DataFrame()
    path = Path(path_value).expanduser()
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        return pd.read_csv(path, sep="\\t" if suffix == ".tsv" else ",", skipinitialspace=True, low_memory=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.DataFrame()
'''
    new_loader = '''_REFERENCE_DF_CACHE: dict[str, pd.DataFrame] = {}
_REFERENCE_DF_CACHE_LOCK = Lock()


def _load_reference_df(metric: dict) -> pd.DataFrame:
    shared = metric.get("_reference_df")
    if isinstance(shared, pd.DataFrame):
        return shared

    path_value = _reference_path(metric)
    if not path_value:
        raise ValueError("reference_dataset_path is required for reference-comparison metrics")
    path = Path(path_value).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Reference dataset does not exist or is not a file: {path}")

    cache_key = str(path)
    with _REFERENCE_DF_CACHE_LOCK:
        cached = _REFERENCE_DF_CACHE.get(cache_key)
        if cached is not None:
            return cached
        suffix = path.suffix.lower()
        if is_packet_capture(path):
            dataframe = build_pcap_packet_dataframe(path)
        elif suffix in {".csv", ".tsv"}:
            dataframe = pd.read_csv(path, sep="\\t" if suffix == ".tsv" else ",", skipinitialspace=True, low_memory=False)
        elif suffix in {".xlsx", ".xls"}:
            dataframe = pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported reference dataset format: {suffix or '<none>'}")
        if dataframe.empty:
            raise ValueError(f"Reference dataset contains no usable rows: {path}")
        _REFERENCE_DF_CACHE[cache_key] = dataframe
        return dataframe
'''
    if old_loader not in text:
        raise RuntimeError("reference loader block not found")
    text = text.replace(old_loader, new_loader, 1)

    old_numeric = '''def _numeric_values(df: pd.DataFrame, field: str, max_sample_size: int) -> list[float]:
    if field not in df.columns:
        return []
    values = pd.to_numeric(df[field], errors="coerce").dropna().tolist()
    return [float(value) for value in values[:max_sample_size]]
'''
    new_numeric = '''def _even_positions(length: int, maximum: int) -> list[int]:
    if length <= maximum:
        return list(range(length))
    if maximum <= 1:
        return [0]
    step = (length - 1) / (maximum - 1)
    return [round(index * step) for index in range(maximum)]


def _numeric_values(df: pd.DataFrame, field: str, max_sample_size: int) -> list[float]:
    if field not in df.columns:
        return []
    series = pd.to_numeric(df[field], errors="coerce").dropna().reset_index(drop=True)
    if series.empty:
        return []
    positions = _even_positions(len(series), max(1, int(max_sample_size)))
    return [float(series.iloc[position]) for position in positions]


def _sample_dataframe(df: pd.DataFrame, max_sample_size: int) -> pd.DataFrame:
    if len(df) <= max_sample_size:
        return df
    return df.iloc[_even_positions(len(df), max_sample_size)].copy()


def _numeric_matrix(df: pd.DataFrame, fields: list[str], max_sample_size: int) -> tuple[np.ndarray, list[str]]:
    usable = [field for field in fields if field in df.columns]
    if not usable:
        return np.empty((0, 0)), []
    numeric = df[usable].apply(pd.to_numeric, errors="coerce").dropna()
    numeric = _sample_dataframe(numeric, max_sample_size)
    return numeric.to_numpy(dtype=float), usable


def _multivariate_rbf_mmd(current: np.ndarray, reference: np.ndarray) -> tuple[float | None, float | None]:
    if current.size == 0 or reference.size == 0 or current.shape[1] != reference.shape[1]:
        return None, None
    pooled = np.vstack([current, reference])
    scale = pooled.std(axis=0)
    scale = np.where(scale > 0, scale, 1.0)
    centre = pooled.mean(axis=0)
    current_z = (current - centre) / scale
    reference_z = (reference - centre) / scale
    pooled_z = np.vstack([current_z, reference_z])

    def squared_distances(left, right):
        values = (
            np.sum(left * left, axis=1)[:, None]
            + np.sum(right * right, axis=1)[None, :]
            - 2.0 * left.dot(right.T)
        )
        return np.maximum(values, 0.0)

    pooled_distances = squared_distances(pooled_z, pooled_z)
    upper = pooled_distances[np.triu_indices(len(pooled_z), 1)]
    positive = upper[upper > 0]
    median_squared_distance = float(np.median(positive)) if positive.size else 1.0
    gamma = 1.0 / (2.0 * median_squared_distance) if median_squared_distance > 0 else 1.0
    k_xx = np.exp(-gamma * squared_distances(current_z, current_z))
    k_yy = np.exp(-gamma * squared_distances(reference_z, reference_z))
    k_xy = np.exp(-gamma * squared_distances(current_z, reference_z))
    mmd_squared = float(k_xx.mean() + k_yy.mean() - 2.0 * k_xy.mean())
    return max(0.0, mmd_squared), sqrt(median_squared_distance)
'''
    if old_numeric not in text:
        raise RuntimeError("numeric helper block not found")
    text = text.replace(old_numeric, new_numeric, 1)

    old_mmd = '''def compute_feature_set_mmd_score_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    values = []
    for field in _candidate_fields(metric):
        values.extend(_numeric_values(df, field, 1000))
    reference_values = []
    for field in _candidate_fields(metric):
        reference_values.extend(_numeric_values(reference_df, field, 1000))
    score = round(_rbf_mmd(values, reference_values), 6) if values and reference_values else None
    return {"summary": {"reference_dataset_path": _reference_path(metric), "current_value_count": len(values), "reference_value_count": len(reference_values), "feature_set_mmd_score_from_reference": score}}
'''
    new_mmd = '''def compute_feature_set_mmd_score_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = [field for field in _candidate_fields(metric) if field in df.columns and field in reference_df.columns]
    max_sample_size = int(metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 500))
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for MMD")
    current_matrix, current_fields = _numeric_matrix(df, fields, max_sample_size)
    reference_matrix, reference_fields = _numeric_matrix(reference_df, fields, max_sample_size)
    common_fields = [field for field in fields if field in current_fields and field in reference_fields]
    score, bandwidth = _multivariate_rbf_mmd(current_matrix, reference_matrix)
    return {"summary": {
        "reference_dataset_path": _reference_path(metric),
        "fields": common_fields,
        "current_row_count": int(current_matrix.shape[0]),
        "reference_row_count": int(reference_matrix.shape[0]),
        "standardization": "pooled_mean_and_standard_deviation",
        "rbf_bandwidth": round(bandwidth, 6) if bandwidth is not None else None,
        "feature_set_mmd_score_from_reference": round(score, 6) if score is not None else None,
        "runnable": score is not None,
    }}
'''
    if old_mmd not in text:
        raise RuntimeError("MMD block not found")
    text = text.replace(old_mmd, new_mmd, 1)

    old_distance = '''def compute_distance_correlation_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    current = compute_distance_correlation_profile(df, fields)["profile"]["matrix"]
    reference = compute_distance_correlation_profile(reference_df, fields)["profile"]["matrix"]
    deviation = _matrix_deviation(current, reference)
    return {"summary": {"reference_dataset_path": _reference_path(metric), "distance_correlation_matrix_deviation_from_reference": deviation["mean_deviation"], "pair_count": deviation["pair_count"]}, "pairs": deviation["pairs"]}
'''
    new_distance = '''def compute_distance_correlation_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    max_sample_size = int(metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000))
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for distance correlation")
    current_df = _sample_dataframe(df, max_sample_size)
    reference_sample = _sample_dataframe(reference_df, max_sample_size)
    current = compute_distance_correlation_profile(current_df, fields)["profile"]["matrix"]
    reference = compute_distance_correlation_profile(reference_sample, fields)["profile"]["matrix"]
    deviation = _matrix_deviation(current, reference)
    return {"summary": {"reference_dataset_path": _reference_path(metric), "distance_correlation_matrix_deviation_from_reference": deviation["mean_deviation"], "pair_count": deviation["pair_count"], "current_sample_size": len(current_df), "reference_sample_size": len(reference_sample)}, "pairs": deviation["pairs"]}
'''
    if old_distance not in text:
        raise RuntimeError("distance-correlation reference block not found")
    text = text.replace(old_distance, new_distance, 1)

    text = text.replace(
        '_parse_timestamp_series(df, _timestamp_field(metric)))',
        '_parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric)))',
    )
    text = text.replace(
        '_parse_timestamp_series(reference_df, _timestamp_field(metric)))',
        '_parse_timestamp_series(reference_df, _timestamp_field(metric), _timestamp_unit(metric)))',
    )
    text = text.replace(
        'return {"summary": {"reference_dataset_path": _reference_path(metric), "current_gap_count": len(current_gaps),',
        'return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_gap_count": len(current_gaps),',
        1,
    )
    text = text.replace(
        'return {"summary": {"reference_dataset_path": _reference_path(metric), "current_burstiness":',
        'return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_burstiness":',
        1,
    )
    old_hourly = '''    current_probs = _probabilities(_hourly_counts(current_ts))
    reference_probs = _probabilities(_hourly_counts(reference_ts))
    divergence = 0.5 * sum(abs(a - b) for a, b in zip(current_probs, reference_probs))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "current_timestamp_count": len(current_ts), "reference_timestamp_count": len(reference_ts), "hourly_activity_divergence_from_reference": round(divergence, 6)}}
'''
    new_hourly = '''    divergence = None
    if current_ts and reference_ts:
        current_probs = _probabilities(_hourly_counts(current_ts))
        reference_probs = _probabilities(_hourly_counts(reference_ts))
        divergence = 0.5 * sum(abs(a - b) for a, b in zip(current_probs, reference_probs))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_timestamp_count": len(current_ts), "reference_timestamp_count": len(reference_ts), "hourly_activity_divergence_from_reference": round(divergence, 6) if divergence is not None else None}}
'''
    if old_hourly not in text:
        raise RuntimeError("hourly reference block not found")
    text = text.replace(old_hourly, new_hourly, 1)
    path.write_text(text, encoding="utf-8")


def patch_plan_builder() -> None:
    path = Path("runner/plan_builder.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        '''from runner.pcap_adapter import (
    PCAP_CONTEXT_CONFIGURATION_REASONS,
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_COLUMNS,
    PCAP_PACKET_METRICS,
    PCAP_SELF_DERIVED_METRICS,
    pcap_metric_template,
)
''',
        '''from runner.pcap_adapter import (
    PCAP_CONTEXT_CONFIGURATION_REASONS,
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_COLUMNS,
    PCAP_PACKET_METRICS,
    PCAP_REFERENCE_METRICS,
    PCAP_REFERENCE_UNSUPPORTED_REASONS,
    PCAP_SELF_DERIVED_METRICS,
    pcap_metric_template,
    pcap_reference_metric_template,
    pcap_service_port_template,
)
''',
        1,
    )
    start = text.index("def _configuration_state(")
    end = text.index("\ndef _metric_from_spec", start)
    new_state = '''def _configuration_state(
    metric_spec: dict,
    dataset: dict,
    reference_dataset: dict | None = None,
) -> tuple[str, str | None, list[str]]:
    metric_id = metric_spec["metric_id"]
    template = metric_spec.get("template")
    manual_reason = metric_spec.get("manual_configuration_reason")
    fmt = dataset.get("format")
    is_pcap = fmt in {"pcap", "pcapng"}

    if is_pcap and metric_id in PCAP_REFERENCE_METRICS:
        if reference_dataset is None:
            return "needs_configuration", "reference_dataset_required", []
        if reference_dataset.get("format") not in {"pcap", "pcapng"}:
            return "needs_configuration", "pcap_reference_representation_mismatch", []
        if template is None:
            return "needs_configuration", "pcap_reference_template_missing", []
        required = required_fields(template)
        available = dataset.get("available_fields", set())
        missing = [field for field in required if field not in available]
        if missing:
            return "needs_mapping", "pcap_adapter_fields_missing", missing
        reference_available = reference_dataset.get("available_fields", set())
        reference_missing = [field for field in required if field not in reference_available]
        if reference_missing:
            return "needs_mapping", "reference_pcap_adapter_fields_missing", reference_missing
        return "ready", None, []

    if is_pcap and metric_id in PCAP_REFERENCE_UNSUPPORTED_REASONS:
        if reference_dataset is None:
            return "needs_configuration", "reference_dataset_required", []
        return "needs_configuration", PCAP_REFERENCE_UNSUPPORTED_REASONS[metric_id], []

    if is_pcap and metric_id == "service_port_consistency_profile":
        if template is None:
            return "needs_configuration", "service_definition_required", []
        params = template.get("calculation", {}).get("parameters", {})
        if not params.get("service_name") or not params.get("expected_ports") or params.get("population_mode") != "all_rows":
            return "needs_configuration", "pcap_service_population_evidence_required", []
        required = required_fields(template)
        available = dataset.get("available_fields", set())
        missing = [field for field in required if field not in available]
        if missing:
            return "needs_mapping", "pcap_adapter_fields_missing", missing
        return "ready", None, []

    if manual_reason:
        return "needs_configuration", manual_reason, []
    if not metric_spec.get("registered_in_taxonomy", False):
        return "needs_configuration", "missing_master_taxonomy_entry", []

    if is_pcap:
        if metric_id in PCAP_DIRECT_METRICS:
            return "ready", None, []
        context_reason = PCAP_CONTEXT_CONFIGURATION_REASONS.get(metric_id)
        if context_reason:
            return "needs_configuration", context_reason, []
        if metric_id in PCAP_SELF_DERIVED_METRICS:
            return "not_applicable", "self_derived_pcap_invariant_not_independent", []
        if metric_id in PCAP_PACKET_METRICS:
            if template is None:
                return "needs_configuration", "pcap_adapter_template_missing", []
            required = required_fields(template)
            available = dataset.get("available_fields", set())
            missing = [field for field in required if field not in available]
            if missing:
                return "needs_mapping", "pcap_adapter_fields_missing", missing
            return "ready", None, []
        return "not_applicable", "pcap_adapter_not_available", []

    if metric_id in PCAP_DIRECT_METRICS:
        return "not_applicable", "packet_capture_metric_on_tabular_dataset", []
    if template is None:
        return "needs_configuration", "no_metric_template_available", []
    required = required_fields(template)
    available = dataset.get("available_fields", set())
    missing = [field for field in required if field not in available]
    if missing:
        return "needs_mapping", "required_fields_not_resolved", missing
    return "ready", None, []
'''
    text = text[:start] + new_state + text[end:]
    text = text.replace(
        '''    include_metric_ids: Iterable[str] | None = None,
    exclude_metric_ids: Iterable[str] | None = None,
) -> tuple[dict, dict]:
''',
        '''    include_metric_ids: Iterable[str] | None = None,
    exclude_metric_ids: Iterable[str] | None = None,
    reference_dataset_path: Path | None = None,
    service_port_configuration: dict | None = None,
) -> tuple[dict, dict]:
''',
        1,
    )
    text = text.replace(
        '''    dataset = inspect_dataset(dataset_path, field_translation_path=field_translation_path)
    catalogue = build_metric_catalog(available_fields=dataset["available_fields"] or None)
''',
        '''    dataset = inspect_dataset(dataset_path, field_translation_path=field_translation_path)
    reference_dataset = None
    if reference_dataset_path is not None:
        reference_path = Path(reference_dataset_path).expanduser().resolve()
        if reference_path == dataset["path"]:
            raise ValueError("Reference dataset must be independent; candidate and reference paths are identical.")
        reference_dataset = inspect_dataset(reference_path)

    catalogue = build_metric_catalog(available_fields=dataset["available_fields"] or None)
''',
        1,
    )
    text = text.replace(
        '''        if dataset["format"] in {"pcap", "pcapng"} and metric_id in PCAP_PACKET_METRICS:
            spec = dict(spec)
            spec["template"] = pcap_metric_template(metric_id)

        state, reason, missing = _configuration_state(spec, dataset)
''',
        '''        if dataset["format"] in {"pcap", "pcapng"}:
            if metric_id in PCAP_PACKET_METRICS:
                spec = dict(spec)
                spec["template"] = pcap_metric_template(metric_id)
            elif metric_id in PCAP_REFERENCE_METRICS and reference_dataset is not None:
                spec = dict(spec)
                spec["template"] = pcap_reference_metric_template(metric_id, reference_dataset["path"])
            elif metric_id == "service_port_consistency_profile" and service_port_configuration:
                spec = dict(spec)
                spec["template"] = pcap_service_port_template(
                    service_port_configuration.get("service_name", ""),
                    service_port_configuration.get("expected_ports", []),
                )

        state, reason, missing = _configuration_state(spec, dataset, reference_dataset)
''',
        1,
    )
    marker = "    validate_plan_schema(plan)\n\n    report = {"
    addition = '''    plan["plan_creation"]["reference_dataset"] = (
        str(reference_dataset["path"]) if reference_dataset is not None else None
    )
    plan["plan_creation"]["reference_dataset_format"] = (
        reference_dataset["format"] if reference_dataset is not None else None
    )
    if service_port_configuration:
        plan["plan_creation"]["service_port_configuration"] = {
            "service_name": str(service_port_configuration.get("service_name", "")),
            "expected_ports": sorted({int(port) for port in service_port_configuration.get("expected_ports", [])}),
            "population_mode": "all_rows",
            "assumption": "candidate capture independently known to represent one service",
        }
    validate_plan_schema(plan)

    report = {'''
    if marker not in text:
        raise RuntimeError("plan validation/report marker not found")
    text = text.replace(marker, addition, 1)
    marker = "    }\n    return plan, report\n\n\ndef write_plan"
    addition = '''    }
    report["reference_dataset"] = str(reference_dataset["path"]) if reference_dataset is not None else None
    report["reference_dataset_format"] = reference_dataset["format"] if reference_dataset is not None else None
    report["service_port_configuration"] = service_port_configuration
    return plan, report


def write_plan'''
    if marker not in text:
        raise RuntimeError("plan report return marker not found")
    text = text.replace(marker, addition, 1)
    path.write_text(text, encoding="utf-8")


def patch_create_plan() -> None:
    path = Path("create_plan.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "def _prompt(value: str | None, label: str, default: str | None = None, *, required: bool = False) -> str | None:\n",
        '''def _parse_expected_ports(value: str | None) -> list[int]:
    if not value:
        return []
    ports = []
    for part in value.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            port = int(text)
        except ValueError as exc:
            raise ValueError(f"Expected service ports must be integers: {text}") from exc
        if port < 0 or port > 65535:
            raise ValueError(f"Expected service port is outside 0-65535: {port}")
        ports.append(port)
    return sorted(set(ports))


def _browse_dataset_file() -> str | None:
    import curses
    from runner.tui import _browse_file
    repo_root = Path.cwd()
    initial = "datasets" if (repo_root / "datasets").is_dir() else ""
    return curses.wrapper(lambda stdscr: _browse_file(stdscr, repo_root, initial))


def _prompt(value: str | None, label: str, default: str | None = None, *, required: bool = False) -> str | None:
''',
        1,
    )
    text = text.replace(
        '''    parser.add_argument(
        "--field-translation",
        help="Optional field translation JSON. If omitted, an existing dataset sidecar is used automatically.",
    )
''',
        '''    parser.add_argument(
        "--field-translation",
        help="Optional field translation JSON. If omitted, an existing dataset sidecar is used automatically.",
    )
    parser.add_argument("--reference-dataset", help="Optional independent reference dataset. Raw-PCAP reference comparison requires a PCAP/PCAPNG reference.")
    parser.add_argument("--single-service", help="Explicitly assert that the entire PCAP represents this one application service.")
    parser.add_argument("--expected-service-ports", help="Comma-separated expected ports for --single-service, e.g. 53 or 80,8080.")
''',
        1,
    )
    old_browser = '''        import curses
        from runner.tui import _browse_file

        repo_root = Path.cwd()
        initial = "datasets" if (repo_root / "datasets").is_dir() else ""
        dataset_value = curses.wrapper(
            lambda stdscr: _browse_file(stdscr, repo_root, initial)
        )
'''
    if old_browser not in text:
        raise RuntimeError("dataset browser block not found")
    text = text.replace(old_browser, "        dataset_value = _browse_dataset_file()\n", 1)
    text = text.replace(
        '''    dataset_path = Path(dataset_value)
    field_translation_path = Path(args.field_translation) if args.field_translation else None
    description = args.description or "Automatically generated CBR-Tests plan."
''',
        '''    dataset_path = Path(dataset_value)
    is_pcap = dataset_path.suffix.lower() in {".pcap", ".pcapng"}

    reference_value = getattr(args, "reference_dataset", None)
    if is_pcap and not reference_value and sys.stdin.isatty() and sys.stdout.isatty():
        answer = input("\nAdd an independent reference PCAP for reference-comparison metrics? [y/N] ").strip().lower()
        if answer in {"y", "yes"}:
            reference_value = _browse_dataset_file()
            if reference_value is None:
                print("Reference selection cancelled; reference-comparison metrics remain excluded.")
    reference_dataset_path = Path(reference_value) if reference_value else None

    single_service = getattr(args, "single_service", None)
    expected_ports = _parse_expected_ports(getattr(args, "expected_service_ports", None))
    if bool(single_service) != bool(expected_ports):
        raise ValueError("--single-service and --expected-service-ports must be supplied together.")
    if is_pcap and not single_service and sys.stdin.isatty() and sys.stdout.isatty():
        answer = input("\nIs the entire capture independently known to represent one application service? [y/N] ").strip().lower()
        if answer in {"y", "yes"}:
            single_service = _prompt(None, "Service name", required=True)
            expected_ports = _parse_expected_ports(_prompt(None, "Expected service port(s), comma-separated", required=True))
            if not expected_ports:
                raise ValueError("At least one expected service port is required.")
    service_port_configuration = None
    if single_service and expected_ports:
        service_port_configuration = {"service_name": single_service, "expected_ports": expected_ports, "population_mode": "all_rows"}

    field_translation_path = Path(args.field_translation) if args.field_translation else None
    description = args.description or "Automatically generated CBR-Tests plan."
''',
        1,
    )
    text = text.replace(
        '''        print(f"Dataset:     {dataset_path}")
        print(f"Output path: {output_path}")
''',
        '''        print(f"Dataset:     {dataset_path}")
        if reference_dataset_path:
            print(f"Reference:   {reference_dataset_path}")
        if service_port_configuration:
            print(f"Service:     {service_port_configuration['service_name']} on {service_port_configuration['expected_ports']} (explicit single-service capture)")
        print(f"Output path: {output_path}")
''',
        1,
    )
    text = text.replace(
        '''        include_metric_ids=_split_metric_args(args.include),
        exclude_metric_ids=_split_metric_args(args.exclude),
    )
''',
        '''        include_metric_ids=_split_metric_args(args.include),
        exclude_metric_ids=_split_metric_args(args.exclude),
        reference_dataset_path=reference_dataset_path,
        service_port_configuration=service_port_configuration,
    )
''',
        1,
    )
    path.write_text(text, encoding="utf-8")


def patch_run_plan() -> None:
    replace_once(
        "run_plan.py",
        "from runner.pcap_adapter import PCAP_PACKET_METRICS, build_pcap_packet_dataframe, is_packet_capture\n",
        "from runner.pcap_adapter import PCAP_PACKET_BACKED_METRICS, build_pcap_packet_dataframe, is_packet_capture\n",
    )
    replace_once(
        "run_plan.py",
        '        metric["metric_id"] in PCAP_PACKET_METRICS for metric in metrics\n',
        '        metric["metric_id"] in PCAP_PACKET_BACKED_METRICS for metric in metrics\n',
    )


def write_tests() -> None:
    Path("tests/test_pcap_handshake.py").write_text('''from pathlib import Path

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
''', encoding="utf-8")

    path = Path("tests/test_plan_builder.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "from runner.pcap_adapter import PCAP_DIRECT_METRICS, PCAP_PACKET_METRICS, PCAP_SELF_DERIVED_METRICS\n",
        '''from runner.pcap_adapter import (
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_METRICS,
    PCAP_REFERENCE_METRICS,
    PCAP_REFERENCE_UNSUPPORTED_REASONS,
    PCAP_SELF_DERIVED_METRICS,
)
''',
        1,
    )
    text += '''


def test_pcap_handshake_is_automatically_runnable_without_boundary_policy(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"pcap-placeholder")
    plan, report = build_plan(plan_id="handshake", name="Handshake", dataset_path=dataset)
    assert "handshake_plausibility_profile" in {metric["metric_id"] for metric in plan["metrics"]}
    assert report["metrics"]["handshake_plausibility_profile"]["status"] == "ready"


def test_pcap_service_port_requires_explicit_single_service_assertion(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"pcap-placeholder")
    plan, report = build_plan(plan_id="service-blocked", name="Service blocked", dataset_path=dataset)
    assert "service_port_consistency_profile" not in {metric["metric_id"] for metric in plan["metrics"]}
    assert report["metrics"]["service_port_consistency_profile"]["reason"] == "service_definition_required"

    configured_plan, configured_report = build_plan(
        plan_id="service-ready",
        name="Service ready",
        dataset_path=dataset,
        service_port_configuration={"service_name": "dns", "expected_ports": [53], "population_mode": "all_rows"},
    )
    configured = {metric["metric_id"]: metric for metric in configured_plan["metrics"]}
    assert configured_report["metrics"]["service_port_consistency_profile"]["status"] == "ready"
    params = configured["service_port_consistency_profile"]["calculation"]["parameters"]
    assert params["population_mode"] == "all_rows"
    assert params["pass_threshold"] == 1.0
    assert params["warn_threshold"] == 0.0


def test_pcap_reference_metrics_unlock_only_with_independent_reference_pcap(tmp_path):
    candidate = tmp_path / "candidate.pcap"
    reference = tmp_path / "reference.pcap"
    candidate.write_bytes(b"candidate-placeholder")
    reference.write_bytes(b"reference-placeholder")
    plan, report = build_plan(
        plan_id="reference-pcap", name="Reference PCAP", dataset_path=candidate, reference_dataset_path=reference
    )
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    assert PCAP_REFERENCE_METRICS.issubset(metric_ids)
    assert report["reference_dataset"] == str(reference.resolve())
    for metric_id in PCAP_REFERENCE_UNSUPPORTED_REASONS:
        assert metric_id not in metric_ids
        assert report["metrics"][metric_id]["reason"] == PCAP_REFERENCE_UNSUPPORTED_REASONS[metric_id]


def test_pcap_reference_rejects_self_comparison_and_representation_mismatch(tmp_path):
    candidate = tmp_path / "candidate.pcap"
    candidate.write_bytes(b"candidate-placeholder")
    with pytest.raises(ValueError, match="must be independent"):
        build_plan(plan_id="self-reference", name="Self reference", dataset_path=candidate, reference_dataset_path=candidate)

    reference_csv = tmp_path / "reference.csv"
    pd.DataFrame({"Packet Length": [1, 2], "Inter Arrival Time": [0.1, 0.2]}).to_csv(reference_csv, index=False)
    plan, report = build_plan(
        plan_id="mismatched-reference", name="Mismatched reference", dataset_path=candidate, reference_dataset_path=reference_csv
    )
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    assert not (PCAP_REFERENCE_METRICS & metric_ids)
    for metric_id in PCAP_REFERENCE_METRICS:
        assert report["metrics"][metric_id]["reason"] == "pcap_reference_representation_mismatch"
'''
    path.write_text(text, encoding="utf-8")

    path = Path("tests/test_reference_model_comparison_profile.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "import pandas as pd\n",
        '''import pandas as pd
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.pcap_adapter import build_pcap_packet_dataframe, pcap_reference_metric_template
''',
        1,
    )
    text += '''


def test_reference_metrics_load_raw_pcap_with_explicit_epoch_units(tmp_path):
    candidate_path = tmp_path / "candidate.pcap"
    reference_path = tmp_path / "reference.pcap"
    candidate_packets = [
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=10000, dport=80, flags="S") / Raw(b"a"),
        IP(src="10.0.0.2", dst="10.0.0.1") / TCP(sport=80, dport=10000, flags="SA") / Raw(b"bb"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=10000, dport=80, flags="A") / Raw(b"ccc"),
        IP(src="10.0.0.3", dst="10.0.0.4") / UDP(sport=20000, dport=53) / Raw(b"dddd"),
    ]
    reference_packets = [
        IP(src="10.1.0.1", dst="10.1.0.2") / TCP(sport=11000, dport=80, flags="S") / Raw(b"aa"),
        IP(src="10.1.0.2", dst="10.1.0.1") / TCP(sport=80, dport=11000, flags="SA") / Raw(b"bbbb"),
        IP(src="10.1.0.1", dst="10.1.0.2") / TCP(sport=11000, dport=80, flags="A") / Raw(b"cccccc"),
        IP(src="10.1.0.3", dst="10.1.0.4") / UDP(sport=21000, dport=53) / Raw(b"dddddddd"),
    ]
    for packets, path, timestamps in (
        (candidate_packets, candidate_path, (1.0, 2.0, 4.0, 8.0)),
        (reference_packets, reference_path, (1.0, 3.0, 6.0, 10.0)),
    ):
        for packet, timestamp in zip(packets, timestamps):
            packet.time = timestamp
        wrpcap(str(path), packets)

    current = build_pcap_packet_dataframe(candidate_path)
    from tests.reference_model_comparison_profile import (
        compute_feature_set_mmd_score_from_reference,
        compute_inter_arrival_distribution_divergence_from_reference,
    )
    temporal_metric = pcap_reference_metric_template("inter_arrival_distribution_divergence_from_reference", reference_path)
    temporal = compute_inter_arrival_distribution_divergence_from_reference(current, temporal_metric)
    assert temporal["summary"]["timestamp_unit"] == "s"
    assert temporal["summary"]["current_gap_count"] == 3
    assert temporal["summary"]["reference_gap_count"] == 3
    assert temporal["summary"]["inter_arrival_distribution_divergence_from_reference"] is not None

    mmd_metric = pcap_reference_metric_template("feature_set_mmd_score_from_reference", reference_path)
    mmd = compute_feature_set_mmd_score_from_reference(current, mmd_metric)["summary"]
    assert mmd["runnable"] is True
    assert mmd["fields"] == ["Packet Length", "Inter Arrival Time"]
    assert mmd["feature_set_mmd_score_from_reference"] is not None
'''
    path.write_text(text, encoding="utf-8")


def patch_docs() -> None:
    path = Path("README.md")
    text = path.read_text(encoding="utf-8")
    old = '''Automatic plans for `.pcap`/`.pcapng` include **all 20 existing metrics that are currently runnable from raw packet evidence without inventing research configuration**. This covers the two direct packet-capture checks, IP/port validation, packet-field data quality, packet-length/inter-arrival dependency profiles, internal distribution-drift metrics, and packet-timestamp temporal metrics.

Metrics that need a reference dataset, service definition, labels, slices, attack windows, train/test information, benchmark configuration, or an explicit capture-boundary policy remain visible in preflight but are not inserted into the plan. Flow self-consistency checks are also excluded where both sides of the comparison would be calculated by CBR-Tests itself, because that would test the adapter rather than provide independent realism evidence.
'''
    new = '''Automatic plans for `.pcap`/`.pcapng` include **21 existing metrics that are currently runnable from raw packet evidence without inventing research configuration**. This includes a capture-boundary-safe raw TCP handshake profile in addition to the existing protocol/timestamp, address/port, data-quality, dependency, distribution-drift and temporal checks.

The handshake profile evaluates only attempts whose opening SYN is actually observed. Mid-stream connections, SYN-only attempts, resets and missing handshake packets are evidence categories rather than automatic realism failures. An independent reference PCAP supplied with `--reference-dataset` can add 12 packet-level reference-comparison metrics, and an explicitly single-service capture can add service-port consistency with `--single-service` plus `--expected-service-ports`.

Metrics that still need labels, slices, attack windows, train/test information, benchmark configuration, or flow-segmentation/exporter semantics remain visible in preflight but are not inserted into the plan. Flow self-consistency checks are also excluded where both sides of the comparison would be calculated by CBR-Tests itself, because that would test the adapter rather than provide independent realism evidence.
'''
    if old not in text:
        raise RuntimeError("README PCAP paragraph not found")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")

    path = Path("docs/plan_creation.md")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "The current automatic PCAP set contains 20 metrics: the two direct PCAP checks plus packet-address/port checks, three data-quality profiles, three dependency profiles, four internal distribution-drift metrics, and six packet-timestamp temporal metrics.",
        "The current configuration-free PCAP set contains 21 metrics: protocol/timestamp checks, a native capture-boundary-safe handshake profile, packet-address/port checks, three data-quality profiles, three dependency profiles, four internal distribution-drift metrics, and six packet-timestamp temporal metrics.",
        1,
    )
    old = '''The planner deliberately does **not** enable flow self-consistency metrics merely because CBR-Tests can reconstruct equivalent fields from the same capture. Checks such as start/end-versus-duration arithmetic, aggregate packet/byte arithmetic and derived-rate arithmetic would otherwise validate values calculated by the adapter against other values calculated by the adapter. Those exclusions are reported as `self_derived_pcap_invariant_not_independent`. `handshake_plausibility_profile` is reported as `needs_configuration` with `capture_boundary_policy_required`, because an arbitrary capture may begin in the middle of established TCP sessions and the planner must not guess that capture-boundary assumption.

A canonical bidirectional 5-tuple flow view is still available internally for later sequence-aware and reference-comparison metrics. It does not assume an exporter idle timeout, so it is not silently treated as equivalent to CICFlowMeter or another exporter.
'''
    new = '''The planner deliberately does **not** enable flow self-consistency metrics merely because CBR-Tests can reconstruct equivalent fields from the same capture. Checks such as start/end-versus-duration arithmetic, aggregate packet/byte arithmetic and derived-rate arithmetic would otherwise validate values calculated by the adapter against other values calculated by the adapter. Those exclusions are reported as `self_derived_pcap_invariant_not_independent`.

`handshake_plausibility_profile` is safe to include automatically because the raw-PCAP implementation evaluates only attempts whose opening SYN is observed. Connections already in progress when capture starts, incomplete/SYN-only attempts, resets and missing SYN+ACK evidence are reported separately and are not treated as realism failures. Completion ratio is descriptive; the metric warns only when an observable handshake transition contradicts the direction established by the captured opening SYN.

### Optional independent reference PCAP

Supply a genuinely independent reference capture to unlock packet-level reference comparisons:

```bash
python create_plan.py \\
  --name "Candidate against real reference" \\
  --dataset datasets/candidate.pcap \\
  --reference-dataset datasets/reference-real.pcap
```

Candidate and reference PCAPs are decoded through the same canonical packet representation. This unlocks 12 metrics: feature-wise Wasserstein/KS/energy distance, standardized multivariate RBF MMD, Pearson/Spearman/distance-correlation deviation, inter-arrival/burstiness/hourly-activity deviation, protocol-mix divergence and port-use divergence. PCAP timestamps use explicit seconds; distance correlation is deterministically bounded; MMD uses pooled feature standardisation so packet length does not dominate inter-arrival time merely because of units.

Self-comparison is rejected and a raw PCAP is not automatically compared with a tabular reference because representation semantics can differ. Slice-reference metrics remain blocked without slice metadata, and flow-statistic reference comparison remains blocked until flow segmentation/exporter semantics are explicit.

### Optional service-port profile

Service identity cannot be inferred from a port and then validated against that same port without circular reasoning. Service-port consistency therefore remains blocked by default for raw captures. If independent experiment knowledge establishes that the **entire capture represents one service**, configure it explicitly:

```bash
python create_plan.py \\
  --name "DNS capture" \\
  --dataset datasets/dns-only.pcap \\
  --single-service dns \\
  --expected-service-ports 53
```

Non-standard ports produce a warning for investigation rather than an automatic realism failure, because legitimate services can use non-standard ports.

A canonical bidirectional 5-tuple flow view remains available internally for later flow-aware work. It does not assume an exporter idle timeout, so it is not silently treated as equivalent to CICFlowMeter or another exporter.
'''
    if old not in text:
        raise RuntimeError("plan creation PCAP blocker paragraph not found")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def main() -> None:
    patch_pcap_adapter()
    write_handshake_metric()
    patch_dispatch()
    patch_reference_metrics()
    patch_plan_builder()
    patch_create_plan()
    patch_run_plan()
    write_tests()
    patch_docs()


if __name__ == "__main__":
    main()
