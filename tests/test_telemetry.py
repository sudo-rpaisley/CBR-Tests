from datetime import datetime, timezone
from pathlib import Path

from runner.telemetry import RunState
from runner.live_rendering import render_live_taxonomy


def _state():
    plan = {"plan_meta": {"plan_id": "p1", "name": "Plan 1"}}
    metrics = [
        {"metric_id": "m1", "taxonomy_path": ["quality"]},
        {"metric_id": "m2", "taxonomy_path": ["quality"]},
        {"metric_id": "m3", "taxonomy_path": ["network"]},
    ]
    return RunState.from_plan(
        case_id="case1",
        plan=plan,
        metrics=metrics,
        dataset_path=Path("dataset.csv"),
        output_path=Path("out.json"),
        started_at=datetime.now(timezone.utc),
    )


def test_run_state_tracks_metric_statuses_and_events():
    state = _state()
    state.mark_running("m1")
    state.mark_completed("m1", "success", elapsed_seconds=1.25)
    state.mark_skipped("m2", ["Source IP"])

    assert state.status_counts()["success"] == 1
    assert state.status_counts()["skipped"] == 1
    assert state.status_counts()["pending"] == 1
    assert state.branch_summaries()["quality"]["success"] == 1
    assert state.branch_summaries()["quality"]["skipped"] == 1
    assert state.completed_statuses()["m1"] == "success"
    assert state.completed_durations()["m1"] == 1.25
    assert any(event.event_type == "metric_skipped" for event in state.events)


def test_compact_renderer_can_read_run_state():
    state = _state()
    state.mark_running("m3")
    state.mark_skipped("m2", ["Source IP"])

    text = render_live_taxonomy(
        metrics=[],
        current_metric_id="m3",
        completed_statuses={},
        completed_durations={},
        default_predictions={"m3": 20.0},
        predicted_metric_total=20.0,
        display_mode="compact",
        run_state=state,
    )

    assert "Taxonomy summary" in text
    assert "running: 1" in text
    assert "skipped: 1" in text
    assert "missing: Source IP" in text
    assert "Recent events" in text
