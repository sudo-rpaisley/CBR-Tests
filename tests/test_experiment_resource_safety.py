from __future__ import annotations

from pathlib import Path

import pandas as pd
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.pcap_adapter import build_pcap_packet_dataframe
from runner.pcap_compact import build_compact_pcap_packet_dataframe
from runner.resource_policy import choose_worker_policy, dataframe_memory_bytes
from runner.tabular import load_tabular_dataset


def _write_repetitive_capture(path: Path, packet_count: int = 2000) -> None:
    packets = []
    for index in range(packet_count):
        if index % 5:
            packet = (
                IP(src="10.0.0.1", dst="10.0.0.2")
                / TCP(sport=12000 + (index % 16), dport=443, flags="A")
                / Raw(b"x" * 32)
            )
        else:
            packet = (
                IP(src="10.0.0.3", dst="8.8.8.8")
                / UDP(sport=53000, dport=53)
                / Raw(b"d" * 16)
            )
        packet.time = 1_700_000_000.0 + index * 0.01
        packets.append(packet)
    wrpcap(str(path), packets)


def test_compact_pcap_view_preserves_canonical_values_and_reduces_memory(tmp_path: Path):
    capture = tmp_path / "capture.pcap"
    _write_repetitive_capture(capture)

    canonical = build_pcap_packet_dataframe(capture)
    compact = build_compact_pcap_packet_dataframe(capture)

    assert list(compact.columns) == list(canonical.columns)
    for column in compact.columns:
        if column in {"Source IP", "Destination IP"}:
            assert compact[column].astype(str).tolist() == canonical[column].astype(str).tolist()
        else:
            pd.testing.assert_series_equal(
                compact[column],
                canonical[column],
                check_dtype=False,
                check_names=True,
            )
    assert dataframe_memory_bytes(compact) < dataframe_memory_bytes(canonical)


def test_memory_policy_caps_workers_relative_to_loaded_dataframe():
    dataframe = pd.DataFrame(
        {
            "Source IP": ["10.0.0.1"] * 1000,
            "value": list(range(1000)),
        }
    )
    frame_bytes = dataframe_memory_bytes(dataframe)
    assert frame_bytes and frame_bytes > 0

    constrained = choose_worker_policy(
        requested_workers=4,
        shared_dataframe=dataframe,
        available_bytes=frame_bytes * 3,
    )
    assert constrained["effective_workers"] == 1
    assert constrained["cap_reason"] == "shared_dataframe_memory_budget"
    assert constrained["shared_dataframe_bytes"] == frame_bytes

    comfortable = choose_worker_policy(
        requested_workers=4,
        shared_dataframe=dataframe,
        available_bytes=frame_bytes * 20,
    )
    assert comfortable["effective_workers"] == 4
    assert comfortable["cap_reason"] is None


def test_memory_policy_does_not_change_runs_without_shared_dataframe():
    policy = choose_worker_policy(
        requested_workers=7,
        shared_dataframe=None,
        available_bytes=1024,
    )
    assert policy["effective_workers"] == 7
    assert policy["worker_cap"] is None


def test_csv_loader_avoids_chunk_collection_concat_peak(monkeypatch, tmp_path: Path):
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n", encoding="utf-8")
    calls = []

    def fake_read_csv(path, **kwargs):
        calls.append((path, kwargs))
        return pd.DataFrame({" value ": [1, 2, 3]})

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)
    progress = []
    dataframe = load_tabular_dataset(
        dataset,
        progress_callback=lambda chunk_index, row_count: progress.append(
            (chunk_index, row_count)
        ),
    )

    assert list(dataframe.columns) == ["value"]
    assert progress == [(1, 3)]
    assert len(calls) == 1
    assert "chunksize" not in calls[0][1]
    assert calls[0][1]["low_memory"] is False
    assert calls[0][1]["memory_map"] is True
