from __future__ import annotations

from pathlib import Path

import pandas as pd
from scapy.layers.inet import IP, TCP
from scapy.packet import Raw
from scapy.utils import wrpcap

from cbr_tests.metrics.reference_comparison import (
    _REFERENCE_DF_CACHE,
    _correlation_profile,
    _load_reference_df,
    compute_pearson_matrix_deviation_from_reference,
)
from runner.pcap_adapter import build_pcap_packet_dataframe
from runner.resource_policy import dataframe_memory_bytes


def test_shared_reference_dataframe_is_reused_without_full_copy():
    reference = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]})
    metric = {"_reference_df": reference}

    loaded = _load_reference_df(metric)

    assert loaded is reference


def test_cached_csv_reference_dataframe_is_reused_without_full_copy(tmp_path: Path):
    reference_path = tmp_path / "reference.csv"
    pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]}).to_csv(
        reference_path, index=False
    )
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path)}}
    _REFERENCE_DF_CACHE.clear()

    first = _load_reference_df(metric)
    second = _load_reference_df(metric)

    assert second is first
    assert _REFERENCE_DF_CACHE[str(reference_path.resolve())] is first


def test_reference_field_mapping_does_not_mutate_cached_columns(tmp_path: Path):
    reference_path = tmp_path / "reference.csv"
    pd.DataFrame({"source_x": [1.0, 2.0], "y": [3.0, 4.0]}).to_csv(
        reference_path, index=False
    )
    _REFERENCE_DF_CACHE.clear()
    metric = {
        "input_requirements": {"reference_dataset_path": str(reference_path)},
        "reference_field_map": {"source_x": "x"},
    }

    mapped = _load_reference_df(metric)
    cached = _REFERENCE_DF_CACHE[str(reference_path.resolve())]

    assert list(mapped.columns) == ["x", "y"]
    assert list(cached.columns) == ["source_x", "y"]


def test_correlation_profile_only_copies_requested_fields_and_preserves_input():
    dataframe = pd.DataFrame(
        {
            "x": [1.0, 2.0, 3.0],
            "y": [3.0, 2.0, 1.0],
            "large_unrelated_payload": ["z" * 1000] * 3,
        }
    )
    before = dataframe.copy(deep=True)

    result = _correlation_profile(dataframe, ["x", "y"], "pearson")

    assert result["x"]["y"] == -1.0
    pd.testing.assert_frame_equal(dataframe, before)


def test_reference_metric_does_not_mutate_shared_reference_dataframe():
    current = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [1.0, 2.0, 3.0]})
    reference = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]})
    before = reference.copy(deep=True)
    metric = {
        "_reference_df": reference,
        "input_requirements": {"candidate_fields": ["x", "y"]},
    }

    result = compute_pearson_matrix_deviation_from_reference(current, metric)

    assert result["summary"]["pearson_matrix_deviation_from_reference"] == 2.0
    pd.testing.assert_frame_equal(reference, before)


def test_raw_pcap_reference_uses_compact_packet_storage(tmp_path: Path):
    reference_path = tmp_path / "reference.pcap"
    packets = []
    for index in range(256):
        packet = (
            IP(src="10.0.0.1", dst="10.0.0.2")
            / TCP(sport=12000 + (index % 8), dport=443, flags="A")
            / Raw(b"x" * 32)
        )
        packet.time = 1_700_000_000.0 + index * 0.01
        packets.append(packet)
    wrpcap(str(reference_path), packets)
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path)}}
    _REFERENCE_DF_CACHE.clear()

    compact = _load_reference_df(metric)
    canonical = build_pcap_packet_dataframe(reference_path)

    assert dataframe_memory_bytes(compact) < dataframe_memory_bytes(canonical)
    assert compact["Source IP"].astype(str).tolist() == canonical["Source IP"].astype(str).tolist()
