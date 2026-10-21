"""Contract tests for the stand-alone CTGAN experiment adapter (no GAN dependency)."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from runner.perturbation_experiment import (
    collect_metric_rows,
    file_sha256,
    parse_integer_set,
    validate_generation,
)
from run_perturbation import parse_args


def _fixture(tmp_path: Path):
    source = tmp_path / "original.pcap"
    model = tmp_path / "model.pkl"
    out = tmp_path / "captures"
    out.mkdir()
    source.write_bytes(b"example-source")
    model.write_bytes(b"model-checkpoint")
    rows = []
    for pct in (0, 10):
        output = out / f"gan_{pct:03d}.pcap"
        output.write_bytes(source.read_bytes() if pct == 0 else b"example-sourcE")
        for suffix in ("packet_labels.csv", "flow_labels.csv"):
            (out / f"gan_{pct:03d}.{suffix}").write_text("header\n", encoding="utf-8")
        (out / f"gan_{pct:03d}.json").write_text("{}", encoding="utf-8")
        rows.append({
            "requested_pct": pct,
            "actual_packet_pct": float(pct),
            "output": output.name,
            "input_sha256": file_sha256(source),
            "model_sha256": file_sha256(model),
            "seed": 42,
            "source_file_bytes": source.stat().st_size,
            "output_file_bytes": output.stat().st_size,
            "output_sha256": file_sha256(output),
            "total_packets": 10,
            "synthetic_packets": pct // 10,
            "real_packets": 10 - pct // 10,
            "packet_labels": f"gan_{pct:03d}.packet_labels.csv",
            "flow_labels": f"gan_{pct:03d}.flow_labels.csv",
            "file_size_error_pct": 0,
        })
    with (out / "manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return source, model, out


def _validate(source, model, out, **kwargs):
    return validate_generation(
        seed_dir=out, source_path=source, model_path=model,
        percentages=(0, 10), seed=42, size_tolerance_pct=5,
        verify_packets=False, **kwargs,
    )


def test_integer_list_rejects_duplicates_and_bad_values():
    assert parse_integer_set("30,0,10", low=0, high=100) == (0, 10, 30)
    with pytest.raises(ValueError, match="Repeated"):
        parse_integer_set("10,10", low=0, high=100)
    with pytest.raises(ValueError):
        parse_integer_set("110", low=0, high=100)


def test_cli_requires_explicit_zero_baseline():
    with pytest.raises(SystemExit):
        parse_args([
            "--input", "test.pcap", "--plan", "test.json",
            "--model", "test.pkl", "--generator-script", "traffic_gan.py",
            "--output-dir", "out", "--percentages", "10,20",
        ])


def test_manifest_sweep_validates_and_orders(tmp_path):
    source, model, out = _fixture(tmp_path)
    validated = _validate(source, model, out)
    assert [row["requested_pct"] for row in validated] == [0, 10]
    assert [row["synthetic_packets"] for row in validated] == [0, 1]


def test_manifest_rejects_corrupted_hash(tmp_path):
    source, model, out = _fixture(tmp_path)
    changed = out / "gan_010.pcap"
    changed.write_bytes(b"modified-source")
    with pytest.raises(ValueError, match="SHA-256"):
        _validate(source, model, out)


def test_manifest_rejects_missing_provenance(tmp_path):
    source, model, out = _fixture(tmp_path)
    (out / "gan_010.packet_labels.csv").unlink()
    with pytest.raises(FileNotFoundError):
        _validate(source, model, out)


def test_independent_packet_count_must_match(tmp_path, monkeypatch):
    source, model, out = _fixture(tmp_path)
    import runner.perturbation_experiment as adapter
    monkeypatch.setattr(adapter, "packet_count", lambda p: 9 if p.name == "gan_010.pcap" else 10)
    with pytest.raises(ValueError, match="Independent packet-count"):
        adapter.validate_generation(
            seed_dir=out, source_path=source, model_path=model,
            percentages=(0, 10), seed=42, size_tolerance_pct=5,
            verify_packets=True,
        )


def test_outcome_metric_values_are_joined_to_percentages(tmp_path):
    source, model, out = _fixture(tmp_path)
    captures = _validate(source, model, out)
    result_path = tmp_path / "run.json"
    result_path.write_text(json.dumps({
        "metric_results": [{"metric_id": "foo", "status": "success", "result_status": "warn"}],
        "test_results": {"foo": 0.7},
    }), encoding="utf-8")
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "results": [{"dataset_path": captures[1]["dataset_path"], "output_path": str(result_path)}],
    }), encoding="utf-8")
    rows = collect_metric_rows(state, captures)
    assert len(rows) == 1
    assert rows[0]["requested_pct"] == 10
    assert rows[0]["primary_value"] == 0.7
    assert rows[0]["domain_status"] == "warn"
