from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from runner.execution import run_metrics_parallel
from runner.parallel_results import collect_parallel_metric_results
from runner.run_plan_helpers import write_outcome
from runner.schema import validate_plan_schema


def _metric(metric_id: str) -> dict:
    return {
        "metric_id": metric_id,
        "taxonomy_path": ["test", metric_id],
        "input_requirements": {},
        "calculation": {"method": metric_id, "parameters": {}},
    }


def test_parallel_fail_fast_marks_unsubmitted_metrics(tmp_path: Path):
    metrics = [_metric("first"), _metric("second"), _metric("third")]
    called: list[str] = []

    def _failed(_dataset, _metric_config):
        called.append("first")
        return False, {"error": "boom"}

    def _unexpected(_dataset, metric_config):
        called.append(metric_config["metric_id"])
        return True, {"test_results": {metric_config["metric_id"]: {"ok": True}}}

    results = run_metrics_parallel(
        tmp_path / "dataset.csv",
        metrics,
        {"first": _failed, "second": _unexpected, "third": _unexpected},
        workers=1,
        fail_fast=True,
    )

    assert called == ["first"]
    assert [item[0] for item in results] == [0, 1, 2]
    assert results[0][1] is False
    assert results[1][2]["status"] == "not_run_fail_fast"
    assert results[2][2]["status"] == "not_run_fail_fast"


def test_parallel_payload_contains_real_metric_timestamps(tmp_path: Path):
    metrics = [_metric("only")]

    results = run_metrics_parallel(
        tmp_path / "dataset.csv",
        metrics,
        {"only": lambda _dataset, _metric_config: (True, {"test_results": {"only": {"ok": True}}})},
        workers=1,
    )

    payload = results[0][2]
    started = datetime.fromisoformat(payload["started_at"])
    finished = datetime.fromisoformat(payload["finished_at"])
    assert started.tzinfo is not None
    assert finished >= started
    assert payload["elapsed_seconds"] >= 0


def test_parallel_collector_preserves_completed_and_not_run_records():
    metrics = [_metric("first"), _metric("second")]
    run_started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    parallel_out = [
        (
            0,
            False,
            {
                "error": "boom",
                "started_at": "2026-01-01T00:00:01+00:00",
                "finished_at": "2026-01-01T00:00:02+00:00",
                "elapsed_seconds": 1.0,
            },
        ),
        (
            1,
            False,
            {
                "status": "not_run_fail_fast",
                "reason": "previous_metric_failed",
                "started_at": "2026-01-01T00:00:02+00:00",
                "finished_at": "2026-01-01T00:00:02+00:00",
                "elapsed_seconds": 0.0,
            },
        ),
    ]
    completed_statuses: dict[str, str] = {}
    completed_durations: dict[str, float] = {}

    status, _test_results, metric_results, _column_validations = collect_parallel_metric_results(
        parallel_out=parallel_out,
        metrics=metrics,
        run_started_at=run_started_at,
        fail_fast=True,
        completed_statuses=completed_statuses,
        completed_durations=completed_durations,
    )

    assert status == "failed"
    assert [record["status"] for record in metric_results] == ["failed", "not_run_fail_fast"]
    assert metric_results[0]["started_at"] == "2026-01-01T00:00:01+00:00"
    assert completed_statuses == {"first": "failed", "second": "not_run_fail_fast"}


def test_write_outcome_creates_parent_and_replaces_file(tmp_path: Path):
    destination = tmp_path / "nested" / "outcome.json"
    write_outcome(destination, {"status": "success", "value": 1})
    write_outcome(destination, {"status": "success", "value": 2})

    assert json.loads(destination.read_text(encoding="utf-8"))["value"] == 2
    assert not list(destination.parent.glob(f".{destination.name}.*.tmp"))


def test_plan_schema_rejects_duplicate_metric_ids():
    plan = {
        "plan_meta": {"plan_id": "plan-1", "name": "Plan"},
        "execution_policy": {"fail_fast": True, "allow_skips": False, "sample_mode": "full"},
        "metrics": [_metric("duplicate"), _metric("duplicate")],
    }

    with pytest.raises(ValueError, match="Duplicate metric_id"):
        validate_plan_schema(plan)


def test_plan_schema_rejects_required_optional_overlap():
    metric = _metric("m1")
    metric["field_requirements"] = {
        "required": ["Source IP"],
        "optional": ["Source IP"],
    }
    plan = {
        "plan_meta": {"plan_id": "plan-1"},
        "metrics": [metric],
    }

    with pytest.raises(ValueError, match="both required and optional"):
        validate_plan_schema(plan)
