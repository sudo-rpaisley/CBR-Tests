from __future__ import annotations

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
