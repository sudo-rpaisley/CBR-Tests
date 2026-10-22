"""Regression tests for DuckDB/Parquet import and paged experiment explorer."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from runner import results_catalogue as catalog
from runner import results_explorer_tui as explorer


def _json(path: Path, obj: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    return path


def _archive(root: Path, *, name: str = "2026-10-22") -> tuple[Path, Path]:
    """Minimal realistic corpus archive with nested metric distributions."""
    source = root / name
    case_root = source / "cases" / "game" / "holdout_a" / "attempt_001"
    eval_dir = case_root / "seed_42" / "evaluation"
    capture = case_root / "seed_42" / "captures" / "gan_010.pcap"
    capture.parent.mkdir(parents=True, exist_ok=True)
    capture.write_bytes(b"fake-generated")
    raw = _json(eval_dir / "outcomes" / "run.json", {
        "schema_version": 2,
        "metric_results": [
            {"metric_id": "ks_statistic_from_reference",
             "status": "success", "result_status": "warn", "runtime_seconds": 0.1},
            {"metric_id": "entropy_profile", "status": "success",
             "result_status": "pass"},
        ],
        "test_results": {
            "ks_statistic_from_reference": {
                "summary": {"mean_ks_statistic_from_reference": 0.45},
                "per_field": {"ttl": {"quantiles": [0.15, 0.25, 0.5]}},
                "missing": None, "empty": [], "status": "warn",
            },
            "entropy_profile": {"summary": {"entropy": 6.1},
                                "other": {"h": 2.0}},
        },
        "test_results_taxonomy": {"full": "kept in original JSON"},
    })
    _json(eval_dir / "batch_state.json", {
        "status": "completed",
        "results": [{
            "dataset_path": str(capture),
            "output_path": str(raw),
            "status": "completed",
            "job_id": "job1",
        }],
    })
    _json(case_root / "experiment.json", {
        "status": "completed",
        "started_at": name + "T00:00:00+00:00",
        "model_sha256": "abc",
        "plan_sha256": "pqr",
        "runs": [{
            "seed": 42,
            "batch_output_dir": str(eval_dir),
            "captures": [{
                "dataset_path": str(capture),
                "requested_pct": 10,
                "actual_packet_pct": 10.0,
            }],
        }],
    })
    manifest = _json(source / "campaign.json", {
        "campaign_id": "test-" + name,
        "started_at": name + "T10:00:00+00:00",
        "status": "completed",
        "config_sha256": "cfg",
        "settings": {"dataset_root": str(root)},
        "cases": [{
            "case_id": "game/holdout_a",
            "folder": "Game/TFT",
            "held_out": str(root / "dataset" / "a.pcap"),
            "held_out_sha256": "heldouthash",
            "training": [{"path": str(root / "dataset" / "b.pcap"), "sha256": "other"}],
            "status": "completed",
            "attempts": [{"path": str(case_root), "status": "completed"}],
            "output_dir": str(case_root),
        }],
    })
    return manifest, raw


def test_import_is_lossless_idempotent_and_writes_parquet(tmp_path):
    _archive(tmp_path / "outcomes")
    db = tmp_path / "catalogue.duckdb"
    conn = catalog.open_catalogue(db)
    try:
        report = catalog.index_roots(conn, [tmp_path / "outcomes"])
        assert report["indexed"] == 1
        assert report["failed"] == 0
        assert catalog.summary(conn) == {
            "campaigns": 1, "cases": 1, "jobs": 1,
            "metrics": 2, "measurements": 9,
        }
        rows = conn.execute("""
            SELECT metric_id, payload_json, execution_json
            FROM metrics WHERE metric_id = 'ks_statistic_from_reference'
        """).fetchall()
        assert len(rows) == 1
        payload = json.loads(rows[0][1])
        assert payload["per_field"]["ttl"]["quantiles"] == [0.15, 0.25, 0.5]
        assert json.loads(rows[0][2])["runtime_seconds"] == 0.1
        found = conn.execute(
            "SELECT json_pointer, value_json, numeric_value "
            "FROM measurements WHERE json_pointer = '/per_field/ttl/quantiles/2'"
        ).fetchone()
        assert found == ("/per_field/ttl/quantiles/2", "0.5", 0.5)
        assert conn.execute("SELECT COUNT(*) FROM measurements WHERE value_type = 'array'").fetchone() == (1,)
        repeat = catalog.index_roots(conn, [tmp_path / "outcomes"])
        assert repeat["unchanged"] == 1
        assert catalog.summary(conn)["measurements"] == 9
        result = catalog.export_parquet(conn, tmp_path / "parquet")
        assert result["rows"]["metrics"] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(tmp_path / "parquet" / "measurements.parquet")],
        ).fetchone()[0] == 9
        for table in catalog.TABLES:
            assert (tmp_path / "parquet" / (table + ".parquet")).is_file()
    finally:
        conn.close()


def test_reindex_changed_outcome_replaces_old_measurements(tmp_path):
    source, raw = _archive(tmp_path)
    conn = catalog.open_catalogue(tmp_path / "catalogue.duckdb")
    try:
        catalog.index_roots(conn, [tmp_path])
        payload = json.loads(raw.read_text(encoding="utf-8"))
        payload["test_results"]["ks_statistic_from_reference"]["per_field"]["ttl"]["quantiles"] = [0.9]
        _json(raw, payload)
        changed = catalog.index_roots(conn, [tmp_path])
        assert changed["indexed"] == 1
        assert catalog.summary(conn)["measurements"] == 7
        assert conn.execute(
            "SELECT COUNT(*) FROM measurements WHERE json_pointer = '/per_field/ttl/quantiles/2'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM campaigns").fetchone() == (1,)
    finally:
        conn.close()


def test_single_older_experiment_imports_without_parent_campaign(tmp_path):
    folder = tmp_path / "old-run"
    outcome = _json(folder / "results" / "outcome.json", {
        "metric_results": [{"metric_id": "jensen", "status": "success"}],
        "test_results": {"jensen": {"nested": {"measure": 0.8}}},
    })
    dataset = folder / "captures" / "gan_020.pcap"
    dataset.parent.mkdir(parents=True)
    dataset.write_bytes(b"fake")
    _json(folder / "seed_42" / "evaluation" / "batch_state.json", {
        "status": "completed", "results": [{
            "dataset_path": str(dataset), "output_path": str(outcome),
            "status": "completed",
        }],
    })
    _json(folder / "experiment.json", {
        "started_at": "2026-09-01T12:00:00+00:00",
        "status": "completed", "comparison_dataset_path": "/data/b.pcap",
        "base_dataset_path": "/data/a.pcap",
        "runs": [{
            "seed": 42, "batch_output_dir": str(folder / "seed_42" / "evaluation"),
            "captures": [{
                "dataset_path": str(dataset), "requested_pct": 20,
                "actual_packet_pct": 20.0,
            }],
        }],
    })
    conn = catalog.open_catalogue(tmp_path / "results.duckdb")
    try:
        imported = catalog.index_roots(conn, [tmp_path])
        assert imported["indexed"] == 1 and imported["failed"] == 0
        assert catalog.summary(conn)["measurements"] == 1
        assert conn.execute("SELECT source_kind FROM campaigns").fetchone() == ("single",)
        assert conn.execute("SELECT reference_path FROM jobs").fetchone() == ("/data/a.pcap",)
    finally:
        conn.close()


def test_index_two_dates_and_query_filtered_metric(tmp_path):
    root = tmp_path / "outcomes"
    _archive(root, name="2026-10-20")
    _archive(root, name="2026-10-22")
    conn = catalog.open_catalogue(tmp_path / "catalogue.duckdb")
    try:
        assert catalog.index_roots(conn, [root])["indexed"] == 2
        response = catalog.compare_metric(
            conn, "ks_statistic_from_reference",
            held_out="a.pcap", date_from="2026-10-21",
        )
        assert len(response) == 1
        assert response[0]["campaign_id"] == "test-2026-10-22"
        assert response[0]["perturbation_pct"] == 10
        assert response[0]["primary_value"] == 0.45
        with pytest.raises(ValueError, match="Limit"):
            catalog.compare_metric(conn, "ks_statistic_from_reference", limit=0)
    finally:
        conn.close()


def test_browser_queries_are_paged_not_full_scans(tmp_path):
    _archive(tmp_path)
    conn = catalog.open_catalogue(tmp_path / "results.duckdb")
    try:
        catalog.index_roots(conn, [tmp_path])
        campaigns = explorer._rows(
            conn, {"view": "campaigns", "offset": 0, "search": ""}, 1
        )
        assert len(campaigns) == 1
        cases = explorer._rows(conn, {
            "view": "cases", "campaign_key": campaigns[0]["id"],
            "offset": 0, "search": "a.pcap",
        }, 10)
        assert len(cases) == 1
        metrics = explorer._rows(conn, {
            "view": "metrics", "case_key": cases[0]["id"],
            "offset": 0, "search": "ks_",
        }, 10)
        assert [m["id"] for m in metrics] == ["ks_statistic_from_reference"]
        observations = explorer._rows(conn, {
            "view": "observations", "case_key": cases[0]["id"],
            "metric_id": "ks_statistic_from_reference", "offset": 0
        }, 10)
        assert len(observations) == 1
        leaves = explorer._rows(conn, {
            "view": "measurements", "metric_key": observations[0]["id"],
            "offset": 0,
        }, 3)
        assert len(leaves) == 3
    finally:
        conn.close()
