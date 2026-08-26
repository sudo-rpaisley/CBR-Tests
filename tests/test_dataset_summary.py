from __future__ import annotations

from pathlib import Path

import pandas as pd

import runner.dataset_summary as dataset_summary
from runner.dataset_summary import default_dataset_summary_path, ensure_dataset_summary
from runner.provenance import sha256_file


def test_default_dataset_summary_path_preserves_dataset_suffix(tmp_path: Path):
    dataset = tmp_path / "capture.pcap"
    assert default_dataset_summary_path(dataset) == tmp_path / "capture.pcap.summary.md"


def test_dataset_summary_is_created_reused_and_refreshed_by_hash(tmp_path: Path, monkeypatch):
    dataset = tmp_path / "traffic.csv"
    dataset.write_text("Timestamp,Source IP,Destination IP,Protocol\n2026-01-01T00:00:00Z,10.0.0.1,10.0.0.2,6\n", encoding="utf-8")
    first_hash = sha256_file(dataset)
    frame = pd.DataFrame(
        {
            "Timestamp": ["2026-01-01T00:00:00Z", "2026-01-01T01:30:00Z"],
            "Source IP": ["10.0.0.1", "10.0.0.2"],
            "Destination IP": ["10.0.0.2", "10.0.0.1"],
            "Source Port": [12345, 443],
            "Destination Port": [443, 12345],
            "Protocol": [6, 6],
            "Packet Length": [100, 200],
        }
    )

    created = ensure_dataset_summary(dataset, dataset_sha256=first_hash, dataframe=frame)
    assert created["status"] == "created"
    summary_path = Path(created["path"])
    text = summary_path.read_text(encoding="utf-8")
    assert first_hash in text
    assert "**Records:** 2 rows" in text
    assert "2026-01-01T00:00:00Z" in text
    assert "1h 30m" in text
    assert "**Unique IP endpoints:** 2" in text

    def _must_not_scan(_path):
        raise AssertionError("cache hit must not rescan the dataset")

    monkeypatch.setattr(dataset_summary, "_load_dataframe_for_summary", _must_not_scan)
    reused = ensure_dataset_summary(dataset, dataset_sha256=first_hash)
    assert reused["status"] == "reused"

    dataset.write_text(dataset.read_text(encoding="utf-8") + "2026-01-01T02:00:00Z,10.0.0.3,10.0.0.1,17\n", encoding="utf-8")
    second_hash = sha256_file(dataset)
    refreshed = ensure_dataset_summary(dataset, dataset_sha256=second_hash, dataframe=frame)
    assert refreshed["status"] == "refreshed"
    refreshed_text = summary_path.read_text(encoding="utf-8")
    assert second_hash in refreshed_text
    assert first_hash not in refreshed_text


def test_dataset_summary_schema_mismatch_forces_refresh(tmp_path: Path):
    dataset = tmp_path / "traffic.csv"
    dataset.write_text("Timestamp,value\n2026-01-01T00:00:00Z,1\n2026-01-02T00:00:00Z,2\n", encoding="utf-8")
    digest = sha256_file(dataset)
    frame = pd.DataFrame(
        {
            "Timestamp": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"],
            "value": [1, 2],
        }
    )
    created = ensure_dataset_summary(dataset, dataset_sha256=digest, dataframe=frame)
    summary_path = Path(created["path"])
    text = summary_path.read_text(encoding="utf-8")
    summary_path.write_text(text.replace('"schema_version":1', '"schema_version":999', 1), encoding="utf-8")

    refreshed = ensure_dataset_summary(dataset, dataset_sha256=digest, dataframe=frame)
    assert refreshed["status"] == "refreshed"
    assert '"schema_version":1' in summary_path.read_text(encoding="utf-8")


def test_tabular_numeric_timestamp_unit_is_not_guessed(tmp_path: Path):
    dataset = tmp_path / "numeric-time.csv"
    dataset.write_text("Timestamp,value\n1660786909.1,1\n1660786910.1,2\n", encoding="utf-8")
    frame = pd.DataFrame({"Timestamp": [1660786909.1, 1660786910.1], "value": [1, 2]})

    result = ensure_dataset_summary(dataset, dataset_sha256=sha256_file(dataset), dataframe=frame)
    text = Path(result["path"]).read_text(encoding="utf-8")
    assert "**Timestamp field:** not determined safely" in text
    assert "will not guess" not in text.lower()


def test_pcap_summary_uses_epoch_seconds_and_decoded_packet_language(tmp_path: Path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"not-used-because-a-canonical-frame-is-supplied")
    frame = pd.DataFrame(
        {
            "Timestamp": [1660786909.0, 1660786969.0],
            "Source IP": ["192.0.2.1", "192.0.2.2"],
            "Destination IP": ["192.0.2.2", "192.0.2.1"],
            "Source Port": [12345, 443],
            "Destination Port": [443, 12345],
            "Protocol": [6, 6],
            "IP Version": [4, 4],
            "Packet Length": [60, 100],
            "TCP Flags": [2, 18],
            "Inter Arrival Time": [None, 60.0],
        }
    )

    result = ensure_dataset_summary(dataset, dataset_sha256=sha256_file(dataset), dataframe=frame)
    text = Path(result["path"]).read_text(encoding="utf-8")
    assert "2 decoded IPv4/IPv6 packets" in text
    assert "Unix epoch seconds from the packet capture" in text
    assert "1m 00.000s" in text
    assert "non-IP frames are not represented" in text
