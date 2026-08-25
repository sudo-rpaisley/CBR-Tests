from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from runner.execution import run_metrics_parallel
from runner.live_rendering import render_live_taxonomy
from runner.parallel_results import collect_parallel_metric_results
from runner.telemetry import RunState
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.reserved_ip_address_profile import (
    run_reserved_ip_address_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.service_port_consistency_profile import (
    run_service_port_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.valid_port_range_profile import (
    run_valid_port_range_metric,
)


def _base_metric(metric_id: str) -> dict:
    return {
        "metric_id": metric_id,
        "taxonomy_path": ["dataset_heuristics", "network"],
        "input_requirements": {},
        "calculation": {"method": metric_id, "parameters": {}},
    }


def test_reserved_ip_profile_respects_disabled_private_category():
    metric = _base_metric("reserved_ip_address_profile")
    metric["input_requirements"] = {"candidate_fields": ["Source IP", "Destination IP"]}
    metric["calculation"]["parameters"] = {"count_private_as_reserved": False}
    metric["_shared_df"] = pd.DataFrame({
        "Source IP": ["10.0.0.1"],
        "Destination IP": ["8.8.8.8"],
    })

    ok, payload = run_reserved_ip_address_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["reserved_ip_address_profile"]

    assert ok is True
    assert result["reserved_address_count"] == 0
    assert result["status"] == "pass"
    assert result["diagnostic"]["reason_code"] == "ip_profile_within_policy"


def test_valid_port_range_uses_configured_bounds_and_explains_failure():
    metric = _base_metric("valid_port_range_profile")
    metric["input_requirements"] = {"candidate_fields": ["Source Port", "Destination Port"]}
    metric["calculation"]["parameters"] = {
        "valid_min_port": 0,
        "valid_max_port": 1023,
        "invalid_ratio_fail_threshold": 0.10,
    }
    metric["_shared_df"] = pd.DataFrame({
        "Source Port": [80, 9000],
        "Destination Port": [443, 53],
    })

    ok, payload = run_valid_port_range_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["valid_port_range_profile"]

    assert ok is True
    assert result["invalid_port_count"] == 1
    assert result["invalid_port_ratio"] == 0.25
    assert result["status"] == "fail"
    assert result["diagnostic"]["reason_code"] == "invalid_port_ratio_exceeded"
    assert "9000" in str(result["diagnostic"]["evidence"]["examples"])


def test_valid_port_range_is_not_applicable_when_all_ports_are_missing():
    metric = _base_metric("valid_port_range_profile")
    metric["input_requirements"] = {"candidate_fields": ["Source Port", "Destination Port"]}
    metric["_shared_df"] = pd.DataFrame({
        "Source Port": [None, None],
        "Destination Port": [None, None],
    })

    ok, payload = run_valid_port_range_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["valid_port_range_profile"]

    assert ok is True
    assert result["status"] == "not_applicable"
    assert result["diagnostic"]["reason_code"] == "no_non_missing_ports"


def _service_metric() -> dict:
    metric = _base_metric("service_port_consistency_profile")
    metric["input_requirements"] = {"port_fields": ["Source Port", "Destination Port"]}
    metric["calculation"]["parameters"] = {
        "service_name": "dns",
        "expected_ports": [53],
        "match_mode": "any_port",
        "pass_threshold": 0.95,
        "warn_threshold": 0.75,
    }
    return metric


def test_service_port_consistency_does_not_treat_every_mixed_row_as_dns():
    metric = _service_metric()
    metric["_shared_df"] = pd.DataFrame({
        "Source Port": [51000, 51001, 51002],
        "Destination Port": [80, 443, 22],
    })

    ok, payload = run_service_port_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["service_port_consistency_profile"]

    assert ok is True
    assert result["status"] == "not_applicable"
    assert result["diagnostic"]["reason_code"] == "service_population_unavailable"
    assert "every row" in result["diagnostic"]["summary"]


def test_service_port_consistency_filters_to_service_population():
    metric = _service_metric()
    metric["_shared_df"] = pd.DataFrame({
        "Service": ["dns", "DNS", "http"],
        "Source Port": [51000, 53, 52000],
        "Destination Port": [53, 9999, 80],
    })

    ok, payload = run_service_port_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["service_port_consistency_profile"]

    assert ok is True
    assert result["population_row_count"] == 2
    assert result["checked_row_count"] == 2
    assert result["matching_row_count"] == 2
    assert result["service_port_match_ratio"] == 1.0
    assert result["status"] == "pass"


def test_service_port_consistency_failure_contains_threshold_and_examples():
    metric = _service_metric()
    metric["_shared_df"] = pd.DataFrame({
        "Service": ["dns", "dns"],
        "Source Port": [51000, 51001],
        "Destination Port": [9999, 9998],
    })

    ok, payload = run_service_port_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["service_port_consistency_profile"]

    assert ok is True
    assert result["status"] == "fail"
    assert result["diagnostic"]["reason_code"] == "service_port_match_below_warn_threshold"
    assert result["diagnostic"]["evidence"]["warn_threshold"] == 0.75
    assert result["mismatch_examples"]


def test_parallel_collector_keeps_execution_and_domain_status_separate():
    metrics = [_base_metric("m1")]
    parallel_out = [(
        0,
        True,
        {
            "started_at": "2026-08-07T12:00:00+00:00",
            "finished_at": "2026-08-07T12:00:01+00:00",
            "elapsed_seconds": 1.0,
            "test_results": {
                "m1": {
                    "status": "fail",
                    "diagnostic": {
                        "reason_code": "threshold_exceeded",
                        "summary": "Observed value exceeded the threshold.",
                    },
                }
            },
        },
    )]
    completed_statuses = {}
    completed_durations = {}

    overall, _results, records, _validation = collect_parallel_metric_results(
        parallel_out=parallel_out,
        metrics=metrics,
        run_started_at=datetime(2026, 8, 7, tzinfo=timezone.utc),
        fail_fast=False,
        completed_statuses=completed_statuses,
        completed_durations=completed_durations,
    )

    assert overall == "success"
    assert records[0]["status"] == "success"
    assert records[0]["result_status"] == "fail"
    assert records[0]["diagnostic"]["reason_code"] == "threshold_exceeded"
    assert completed_statuses["m1"] == "fail"


def test_compact_live_state_shows_error_reason_not_bare_failed():
    plan = {"plan_meta": {"plan_id": "p1", "name": "Plan"}}
    metrics = [{"metric_id": "m1", "taxonomy_path": ["network"]}]
    state = RunState.from_plan(
        case_id="case1",
        plan=plan,
        metrics=metrics,
        dataset_path=Path("dataset.csv"),
        output_path=Path("out.json"),
        started_at=datetime.now(timezone.utc),
    )
    state.mark_running("m1")
    state.mark_completed(
        "m1",
        "failed",
        elapsed_seconds=0.01,
        error="No requested port fields exist in the dataset.",
        diagnostic={
            "reason_code": "missing_required_fields",
            "summary": "No requested port fields exist in the dataset.",
        },
    )

    text = render_live_taxonomy(
        metrics=metrics,
        current_metric_id="m1",
        completed_statuses={"m1": "error"},
        completed_durations={"m1": 0.01},
        default_predictions={},
        predicted_metric_total=1.0,
        display_mode="compact",
        run_state=state,
    )

    assert "error: 1" in text
    assert "missing_required_fields" in text
    assert "No requested port fields exist" in text
    assert "[failed" not in text


def test_parallel_progress_callback_remains_backward_compatible(tmp_path: Path):
    metrics = [_base_metric("only")]
    events = []

    def old_callback(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds):
        events.append((event, metric_id, ok))

    results = run_metrics_parallel(
        tmp_path / "dataset.csv",
        metrics,
        {"only": lambda _dataset, _metric: (True, {"test_results": {"only": {"status": "pass"}}})},
        workers=1,
        progress_callback=old_callback,
    )

    assert results[0][1] is True
    assert any(event == "completed" for event, _metric_id, _ok in events)
