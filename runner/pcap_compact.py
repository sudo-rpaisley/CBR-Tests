from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scapy.utils import PcapReader

from runner.pcap_adapter import PCAP_PACKET_COLUMNS, _packet_fields, is_packet_capture


def _intern(value: str, cache: dict[str, str]) -> str:
    """Reuse repeated endpoint strings without relying on process-global interning."""
    cached = cache.get(value)
    if cached is not None:
        return cached
    cache[value] = value
    return value


def build_compact_pcap_packet_dataframe(dataset_path: Path) -> pd.DataFrame:
    """Decode a PCAP/PCAPNG into the canonical packet view with bounded overhead.

    This preserves the same full-population packet semantics as
    ``build_pcap_packet_dataframe`` while avoiding one Python dictionary per
    decoded packet.  Repeated IP strings are shared during decoding and compact
    numeric/category dtypes are used for the finished dataframe.

    The function still intentionally materialises the canonical packet view:
    several metrics depend on the complete ordered packet population.  The aim
    here is to remove avoidable representation overhead, not to introduce an
    implicit sample that would change the experiment.
    """
    path = Path(dataset_path).expanduser().resolve()
    if not is_packet_capture(path):
        raise ValueError(f"Not a PCAP/PCAPNG dataset: {path}")

    packet_indices: list[int] = []
    timestamps: list[float] = []
    source_ips: list[str] = []
    destination_ips: list[str] = []
    source_ports: list[int | None] = []
    destination_ports: list[int | None] = []
    protocols: list[int] = []
    ip_versions: list[int] = []
    packet_lengths: list[int] = []
    tcp_flags: list[int | None] = []
    inter_arrival_times: list[float] = []

    ip_cache: dict[str, str] = {}
    previous_timestamp: float | None = None

    with PcapReader(str(path)) as reader:
        for packet_index, packet in enumerate(reader):
            fields: dict[str, Any] | None = _packet_fields(packet)
            if fields is None:
                continue

            timestamp = float(fields["Timestamp"])
            packet_indices.append(packet_index)
            timestamps.append(timestamp)
            source_ips.append(_intern(str(fields["Source IP"]), ip_cache))
            destination_ips.append(_intern(str(fields["Destination IP"]), ip_cache))
            source_ports.append(fields["Source Port"])
            destination_ports.append(fields["Destination Port"])
            protocols.append(int(fields["Protocol"]))
            ip_versions.append(int(fields["IP Version"]))
            packet_lengths.append(int(fields["Packet Length"]))
            tcp_flags.append(fields["TCP Flags"])
            inter_arrival_times.append(
                np.nan if previous_timestamp is None else timestamp - previous_timestamp
            )
            previous_timestamp = timestamp

    # Construct compact arrays one field at a time.  The port/flag columns remain
    # float64 so missing transport values retain the historical NaN behaviour
    # expected by existing metric code and outcomes.
    data = {
        "Packet Index": np.asarray(packet_indices, dtype=np.int64),
        "Timestamp": np.asarray(timestamps, dtype=np.float64),
        "Source IP": pd.Categorical(source_ips),
        "Destination IP": pd.Categorical(destination_ips),
        "Source Port": np.asarray(
            [np.nan if value is None else value for value in source_ports],
            dtype=np.float64,
        ),
        "Destination Port": np.asarray(
            [np.nan if value is None else value for value in destination_ports],
            dtype=np.float64,
        ),
        "Protocol": np.asarray(protocols, dtype=np.uint8),
        "IP Version": np.asarray(ip_versions, dtype=np.uint8),
        "Packet Length": np.asarray(packet_lengths, dtype=np.uint32),
        "TCP Flags": np.asarray(
            [np.nan if value is None else value for value in tcp_flags],
            dtype=np.float64,
        ),
        "Inter Arrival Time": np.asarray(inter_arrival_times, dtype=np.float64),
    }
    return pd.DataFrame(data, columns=sorted(PCAP_PACKET_COLUMNS))


def dataframe_memory_bytes(dataframe: pd.DataFrame | None) -> int | None:
    """Return pandas' deep in-memory estimate for provenance/resource policy."""
    if dataframe is None:
        return None
    return int(dataframe.memory_usage(index=True, deep=True).sum())
