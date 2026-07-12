from datetime import datetime, timezone
from pathlib import Path

from runner.live_rendering import _INTERACTIVE_STATE, render_interactive_run_state
from runner.telemetry import RunState


def test_interactive_dashboard_renders_branch_controls_and_expanded_metrics():
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
