from pathlib import Path

import pandas as pd

from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.derived_rate_consistency_profile import (
    run_derived_rate_consistency_metric,
)


def _metric() -> dict:
    return {
        "input_requirements": {
            "field_map": {
                "flow_duration": "Flow Duration",
                "total_fwd_packets": "Total Fwd Packets",
                "total_bwd_packets": "Total Bwd Packets",
                "total_len_fwd_packets": "Total Length of Fwd Packets",
                "total_len_bwd_packets": "Total Length of Bwd Packets",
                "flow_packets_per_second": "Flow Packets/s",
                "flow_bytes_per_second": "Flow Bytes/s",
            }
        },
        "calculation": {
            "parameters": {
                "duration_unit": "microseconds",
                "relative_tolerance": 0.001,
                "absolute_tolerance": 1e-9,
            }
        },
    }


def _write_dataset(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "flows.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_accepts_rates_consistent_with_counts_bytes_and_duration(tmp_path: Path):
    path = _write_dataset(
        tmp_path,
        [
            {
                "Flow Duration": 1_000_000,
                "Total Fwd Packets": 4,
                "Total Bwd Packets": 6,
                "Total Length of Fwd Packets": 400,
                "Total Length of Bwd Packets": 600,
                "Flow Packets/s": 10,
                "Flow Bytes/s": 1000,
            },
            {
                "Flow Duration": 0,
                "Total Fwd Packets": 0,
                "Total Bwd Packets": 0,
                "Total Length of Fwd Packets": 0,
                "Total Length of Bwd Packets": 0,
                "Flow Packets/s": 0,
                "Flow Bytes/s": 0,
            },
        ],
    )

    ok, payload = run_derived_rate_consistency_metric(path, _metric())

    assert ok is True
    result = payload["test_results"]["derived_rate_consistency_profile"]
    assert result["checked_row_count"] == 2
    assert result["inconsistent_row_count"] == 0
    assert result["derived_rate_consistency_ratio"] == 1.0
    assert result["status"] == "pass"


def test_reports_rate_mismatches_and_zero_duration_volume(tmp_path: Path):
    path = _write_dataset(
        tmp_path,
        [
            {
                "Flow Duration": 2_000_000,
                "Total Fwd Packets": 4,
                "Total Bwd Packets": 6,
                "Total Length of Fwd Packets": 400,
                "Total Length of Bwd Packets": 600,
                "Flow Packets/s": 50,
                "Flow Bytes/s": 100,
            },
            {
                "Flow Duration": 0,
                "Total Fwd Packets": 1,
                "Total Bwd Packets": 0,
                "Total Length of Fwd Packets": 100,
                "Total Length of Bwd Packets": 0,
                "Flow Packets/s": 0,
                "Flow Bytes/s": 0,
            },
        ],
    )

    ok, payload = run_derived_rate_consistency_metric(path, _metric())

    assert ok is True
    result = payload["test_results"]["derived_rate_consistency_profile"]
    assert result["inconsistent_row_count"] == 2
    assert result["packet_rate_mismatch_count"] == 1
    assert result["byte_rate_mismatch_count"] == 1
    assert result["zero_duration_nonzero_volume_count"] == 1
    assert result["derived_rate_consistency_ratio"] == 0.0
    assert result["status"] == "fail"


def test_requires_an_explicit_duration_unit(tmp_path: Path):
    path = _write_dataset(
        tmp_path,
        [
            {
                "Flow Duration": 1,
                "Total Fwd Packets": 1,
                "Total Bwd Packets": 1,
                "Total Length of Fwd Packets": 10,
                "Total Length of Bwd Packets": 10,
                "Flow Packets/s": 2,
                "Flow Bytes/s": 20,
            }
        ],
    )
    metric = _metric()
    del metric["calculation"]["parameters"]["duration_unit"]

    ok, payload = run_derived_rate_consistency_metric(path, metric)

    assert ok is False
    assert "duration_unit" in payload["error"]


def test_requires_at_least_one_reported_rate_field(tmp_path: Path):
    path = _write_dataset(
        tmp_path,
        [
            {
                "Flow Duration": 1_000_000,
                "Total Fwd Packets": 1,
                "Total Bwd Packets": 1,
                "Total Length of Fwd Packets": 10,
                "Total Length of Bwd Packets": 10,
            }
        ],
    )
    metric = _metric()
    del metric["input_requirements"]["field_map"]["flow_packets_per_second"]
    del metric["input_requirements"]["field_map"]["flow_bytes_per_second"]

    ok, payload = run_derived_rate_consistency_metric(path, metric)

    assert ok is False
    assert "reported" in payload["error"]
