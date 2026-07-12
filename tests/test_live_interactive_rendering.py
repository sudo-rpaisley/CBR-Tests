from datetime import datetime, timezone
from pathlib import Path

from runner.live_rendering import _INTERACTIVE_STATE, render_interactive_run_state
from runner.telemetry import RunState


def test_interactive_dashboard_renders_branch_controls_and_expanded_metrics(monkeypatch):
    plan = {
        "plan_meta": {"plan_id": "plan_1", "name": "Demo Plan"},
        "metrics": [
            {"metric_id": "metric_a", "taxonomy_path": ["branch_a"]},
            {"metric_id": "metric_b", "taxonomy_path": ["branch_b"]},
        ],
    }
    state = RunState.from_plan(
        case_id="case_1",
        plan=plan,
        metrics=plan["metrics"],
        dataset_path=Path("data.csv"),
        output_path=Path("out.json"),
        started_at=datetime.now(timezone.utc),
    )
    state.mark_running("metric_a")
    _INTERACTIVE_STATE["selected_branch"] = 0
    _INTERACTIVE_STATE["expanded_branches"] = {"branch_a"}
    _INTERACTIVE_STATE["metric_offsets"] = {}
    monkeypatch.setattr("runner.live_rendering._KEYBOARD_ENABLED", True)

    output = render_interactive_run_state(state, {}, max_lines=40)

    assert "Controls: ↑/↓ select branch" in output
    assert "> ▾ branch_a" in output
    assert "metric_a [running" in output


def test_interactive_keyboard_reads_full_arrow_escape_sequence(monkeypatch):
    import os
    import runner.live_rendering as live_rendering

    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"\x1b[B")
        monkeypatch.setattr(live_rendering, "_KEYBOARD_ENABLED", True)
        monkeypatch.setattr(live_rendering, "_KEYBOARD_FD", read_fd)

        assert live_rendering._read_key() == "down"
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_interactive_dashboard_shows_keyboard_unavailable_message(monkeypatch):
    plan = {"plan_meta": {"plan_id": "plan_1", "name": "Demo Plan"}, "metrics": [{"metric_id": "metric_a", "taxonomy_path": ["branch_a"]}]}
    state = RunState.from_plan(
        case_id="case_1",
        plan=plan,
        metrics=plan["metrics"],
        dataset_path=Path("data.csv"),
        output_path=Path("out.json"),
        started_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("runner.live_rendering._KEYBOARD_ENABLED", False)

    output = render_interactive_run_state(state, {}, max_lines=40)

    assert "Controls unavailable: terminal input was not captured" in output


def test_interactive_dashboard_scrolls_expanded_branch_metrics(monkeypatch):
    metrics = [{"metric_id": f"metric_{index}", "taxonomy_path": ["branch_a"]} for index in range(12)]
    plan = {"plan_meta": {"plan_id": "plan_1", "name": "Demo Plan"}, "metrics": metrics}
    state = RunState.from_plan(
        case_id="case_1",
        plan=plan,
        metrics=metrics,
        dataset_path=Path("data.csv"),
        output_path=Path("out.json"),
        started_at=datetime.now(timezone.utc),
    )
    _INTERACTIVE_STATE["selected_branch"] = 0
    _INTERACTIVE_STATE["expanded_branches"] = {"branch_a"}
    _INTERACTIVE_STATE["metric_offsets"] = {"branch_a": 2}
    monkeypatch.setattr("runner.live_rendering._KEYBOARD_ENABLED", True)

    output = render_interactive_run_state(state, {}, max_lines=80)

    assert "… 2 earlier metrics hidden" in output
    assert "metric_2 [pending]" in output
    assert "metric_0 [pending]" not in output


def test_interactive_keyboard_reads_page_down_escape_sequence(monkeypatch):
    import os
    import runner.live_rendering as live_rendering

    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, b"\x1b[6~")
        monkeypatch.setattr(live_rendering, "_KEYBOARD_ENABLED", True)
        monkeypatch.setattr(live_rendering, "_KEYBOARD_FD", read_fd)

        assert live_rendering._read_key() == "metric_page_down"
    finally:
        os.close(read_fd)
        os.close(write_fd)
