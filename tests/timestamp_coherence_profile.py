from pathlib import Path
from scapy.utils import PcapReader


def run_timestamp_coherence_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """
    Scan a PCAP and assess whether packet timestamps are coherent.
    """
    params = metric.get("calculation", {}).get("parameters", {})
    large_gap_threshold_seconds = float(params.get("large_gap_threshold_seconds", 1.0))

    packet_count = 0
    first_timestamp = None
    last_timestamp = None
    previous_timestamp = None

    backwards_jump_count = 0
    zero_delta_count = 0
    large_gap_count = 0

    gap_count = 0
    total_gap_seconds = 0.0
    max_gap_seconds = 0.0

    try:
        with PcapReader(str(dataset_path)) as reader:
            for pkt in reader:
                ts = float(pkt.time)
                packet_count += 1

                if first_timestamp is None:
                    first_timestamp = ts
                last_timestamp = ts

                if previous_timestamp is not None:
                    delta = ts - previous_timestamp

                    if delta < 0:
                        backwards_jump_count += 1
                    else:
                        gap_count += 1
                        total_gap_seconds += delta

                        if delta == 0:
                            zero_delta_count += 1

                        if delta > large_gap_threshold_seconds:
                            large_gap_count += 1

                        if delta > max_gap_seconds:
                            max_gap_seconds = delta

                previous_timestamp = ts

    except Exception as exc:
        return False, {
            "error": f"Failed to scan PCAP timestamps: {exc}"
        }

    if packet_count == 0:
        return False, {
            "error": "PCAP contains no packets."
        }

    mean_gap_seconds = round(total_gap_seconds / gap_count, 6) if gap_count else None
    capture_duration_seconds = round(last_timestamp - first_timestamp, 6) if packet_count > 1 else 0.0

    if backwards_jump_count > 0:
        status = "warn"
    else:
        status = "pass"

    return True, {
        "test_results": {
            "timestamp_coherence_profile": {
                "packet_count": packet_count,
                "first_timestamp": first_timestamp,
                "last_timestamp": last_timestamp,
                "capture_duration_seconds": capture_duration_seconds,
                "backwards_jump_count": backwards_jump_count,
                "zero_delta_count": zero_delta_count,
                "large_gap_count": large_gap_count,
                "large_gap_threshold_seconds": large_gap_threshold_seconds,
                "gap_count": gap_count,
                "mean_gap_seconds": mean_gap_seconds,
                "max_gap_seconds": round(max_gap_seconds, 6),
                "status": status
            }
        }
    }
