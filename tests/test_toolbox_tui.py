from __future__ import annotations

from pathlib import Path

from runner.toolbox_tui import (
    _automatic_plan_output,
    initial_plan_builder_state,
    plan_builder_issues,
    plan_builder_review_lines,
    plan_builder_visible_fields,
    toolbox_items,
)


def test_toolbox_exposes_experiment_preparation_and_result_tools():
    keys = {item.key for item in toolbox_items()}

    assert {"single", "batch", "build_plan"} <= keys
    assert {"validate_plan", "migrate_plan"} <= keys
    assert {"compare_outcomes", "rerun_compare", "export_graphs"} <= keys
    assert {"list_metrics", "docs_check", "docs_inventory"} <= keys


def test_plan_builder_defaults_to_essential_fields_only():
    assert plan_builder_visible_fields(False) == (
        "name",
        "datasets",
        "references",
        "metric_policy",
    )
    advanced = plan_builder_visible_fields(True)
    assert "include_metrics" in advanced
    assert "exclude_metrics" in advanced
    assert "single_service" in advanced
    assert "output" in advanced


def test_plan_builder_automatic_output_uses_plan_or_batch_suffix():
    state = initial_plan_builder_state()
    state["name"] = "Final Bucket Experiment"
    state["datasets"] = ["datasets/a.csv"]
    assert _automatic_plan_output(state) == "plans/final-bucket-experiment_plan.json"

    state["datasets"].append("datasets/b.csv")
    assert _automatic_plan_output(state) == "plans/final-bucket-experiment_batch.json"


def test_plan_builder_requires_name_dataset_and_output(tmp_path):
    state = initial_plan_builder_state()
    issues = plan_builder_issues(state, tmp_path)

    assert "Enter a plan name." in issues
    assert "Select at least one candidate dataset." in issues


def test_plan_builder_accepts_valid_single_dataset_setup(tmp_path):
    dataset = tmp_path / "candidate.csv"
    dataset.write_text("a,b\n1,2\n", encoding="utf-8")
    state = initial_plan_builder_state()
    state.update(
        {
            "name": "Candidate plan",
            "datasets": [str(dataset)],
            "output": "plans/candidate-plan_plan.json",
        }
    )

    assert plan_builder_issues(state, tmp_path) == []
    review = plan_builder_review_lines(state)
    assert "Name: Candidate plan" in review
    assert "Generated jobs: 1" in review
    assert "Include filter: All runnable metrics" in review


def test_plan_builder_rejects_mixed_raw_and_tabular_candidates(tmp_path):
    csv_path = tmp_path / "candidate.csv"
    pcap_path = tmp_path / "capture.pcap"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    pcap_path.write_bytes(b"")
    state = initial_plan_builder_state()
    state.update(
        {
            "name": "Mixed",
            "datasets": [str(csv_path), str(pcap_path)],
            "references": [str(csv_path)],
            "output": "plans/mixed_batch.json",
        }
    )

    assert "Do not mix raw PCAP and tabular candidate datasets in one plan batch." in plan_builder_issues(state, tmp_path)


def test_plan_builder_requires_service_name_and_ports_together(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"")
    state = initial_plan_builder_state()
    state.update(
        {
            "name": "Service capture",
            "datasets": [str(dataset)],
            "single_service": "DNS",
            "output": "plans/service_plan.json",
        }
    )

    assert "Single-service name and expected ports must be supplied together." in plan_builder_issues(state, tmp_path)
