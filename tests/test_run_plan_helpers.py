from datetime import datetime, timezone
from pathlib import Path

from runner.run_plan_helpers import (
    build_base_header_lines,
    build_outcome,
    build_title_box_lines,
    configure_signal_handlers,
    detect_ip_fields,
)


class _DummyFrame:
    def __init__(self, columns):
        self.columns = columns


def test_detect_ip_fields_prefers_first_matching_candidates():
    df = _DummyFrame(columns=["Src IP", "Dst IP", "src_ip", "dst_ip"])
    source_field, destination_field = detect_ip_fields(df)
    assert source_field == "Src IP"
    assert destination_field == "Dst IP"


def test_detect_ip_fields_defaults_to_na_when_missing():
    df = _DummyFrame(columns=["foo", "bar"])
    source_field, destination_field = detect_ip_fields(df)
    assert source_field == "n/a"
    assert destination_field == "n/a"


def test_build_title_box_lines_adds_frame_and_status_separator():
    lines = build_title_box_lines(["Header"], ["Status: Running"], width=20)
    assert lines[0] == "=" * 20
    assert lines[-1] == "=" * 20
    assert any("Status: Running" in line for line in lines)
    assert any("-" * 16 in line for line in lines)


def test_build_base_header_lines_with_dataset_size(tmp_path: Path):
    dataset = tmp_path / "data.csv"
    dataset.write_text("a,b\n1,2\n", encoding="utf-8")
    output = tmp_path / "out.json"
    plan = {"plan_meta": {"name": "Demo", "plan_id": "plan_1"}}

    lines = build_base_header_lines(plan, "case_1", dataset, output, include_dataset_size=True)
    assert lines[0] == "Run Title: Demo (plan_1)"
    assert lines[1] == "Case ID: case_1"
    assert any(line.startswith("Source Dataset: data.csv (") for line in lines)


def test_build_outcome_contains_expected_fields():
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    outcome = build_outcome(
        status="success",
        case_id="case_1",
        plan_id="plan_1",
        metrics=[{"metric_id": "m1"}],
        dataset_path=Path("/tmp/data.csv"),
        metric_results=[{"metric_id": "m1", "status": "success"}],
        test_results={"m1": {"passed": True}},
        run_started_at=started,
        run_start_perf=0.0,
        column_validations={"m1": {"ok": True}},
    )

    assert outcome["status"] == "success"
    assert outcome["case_id"] == "case_1"
    assert outcome["plan_id"] == "plan_1"
    assert outcome["metric_ids"] == ["m1"]
    assert outcome["column_validations"]["m1"]["ok"] is True


def test_configure_signal_handlers_registers_sigint(monkeypatch):
    calls = []

    def _fake_signal(sig, handler):
        calls.append((sig, handler))

    monkeypatch.setattr("runner.run_plan_helpers.signal.signal", _fake_signal)
    control_state = {"pause_requested": False, "cancel_requested": False}
    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    configure_signal_handlers(control_state, shutdown_requested)
    assert any(call[0].name == "SIGINT" for call in calls)
