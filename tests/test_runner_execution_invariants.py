from pathlib import Path

from runner.execution import render_live_taxonomy, run_metric_with_heartbeat


def test_render_live_taxonomy_status_labels():
    metrics = [
        {"metric_id": "m1", "taxonomy_path": ["a"]},
        {"metric_id": "m2", "taxonomy_path": ["a"]},
    ]
    txt = render_live_taxonomy(
        metrics=metrics,
        current_metric_id="m2",
        completed_statuses={"m1": "success"},
        completed_durations={"m1": 1.2},
        default_predictions={"m2": 20.0},
        predicted_metric_total=20.0,
        elapsed=5.0,
        completed=False,
    )
    assert "m1 [success | run time 1.2s]" in txt
    assert "m2 [running | 5.0/20s ]" in txt


def test_run_metric_with_heartbeat_executes_handler():
    metrics = [{"metric_id": "m1", "taxonomy_path": ["a"]}]

    def handler(_dataset, _metric):
        return True, {"test_results": {"m1": {"ok": True}}}

    ok, payload = run_metric_with_heartbeat(
        dataset_path=Path("dummy.csv"),
        metric=metrics[0],
        metrics=metrics,
        completed_statuses={},
        completed_durations={},
        current=1,
        total=1,
        shutdown_requested={"requested": False},
        run_start_perf=None,
        metric_handlers={"m1": handler},
        default_predictions={"m1": 5.0},
    )
    assert ok is True
    assert payload["test_results"]["m1"]["ok"] is True
