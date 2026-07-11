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
