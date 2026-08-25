import json

import pandas as pd
import pytest

from runner.metric_catalog import available_metric_ids, build_metric_catalog
from runner.plan_builder import build_plan, write_plan
from runner.schema import validate_plan_schema


def test_metric_catalog_covers_runtime_dispatcher():
    runtime_ids = set(available_metric_ids())
    catalogue_ids = {entry["metric_id"] for entry in build_metric_catalog()}
    assert catalogue_ids == runtime_ids
    assert runtime_ids


def test_generated_plan_includes_every_available_metric_by_default():
    plan, report = build_plan(plan_id="all-tests", name="All Tests")
    configured_ids = {metric["metric_id"] for metric in plan["metrics"]}

    assert configured_ids == set(available_metric_ids())
    assert report["selected_metric_count"] == len(configured_ids)
    assert plan["execution_policy"] == {
        "fail_fast": False,
        "allow_skips": True,
        "sample_mode": "full",
    }
    validate_plan_schema(plan)


def test_dataset_preflight_enables_ready_port_range_but_not_service_rule(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame(
        {
            "Source Port": [12345, 443],
            "Destination Port": [53, 443],
        }
    ).to_csv(dataset, index=False)

    plan, report = build_plan(plan_id="ports", name="Ports", dataset_path=dataset)
    metrics = {metric["metric_id"]: metric for metric in plan["metrics"]}

    assert report["metrics"]["valid_port_range_profile"]["status"] == "ready"
    assert metrics["valid_port_range_profile"]["enabled"] is True
    assert report["metrics"]["service_port_consistency_profile"]["status"] == "needs_configuration"
    assert metrics["service_port_consistency_profile"]["enabled"] is False
    assert metrics["service_port_consistency_profile"]["calculation"]["parameters"]["expected_ports"] == []


def test_reference_metrics_never_inherit_reference_dataset_paths():
    plan, report = build_plan(plan_id="reference", name="Reference")
    reference_metrics = [
        metric for metric in plan["metrics"] if metric["metric_id"].endswith("_from_reference")
    ]
    assert reference_metrics
    for metric in reference_metrics:
        assert metric["enabled"] is False
        assert report["metrics"][metric["metric_id"]]["reason"] == "reference_dataset_required"
        inputs = metric.get("input_requirements", {})
        params = metric.get("calculation", {}).get("parameters", {})
        for container in (inputs, params):
            for key, value in container.items():
                if "reference" in key and "path" in key:
                    assert value == ""


def test_include_exclude_rejects_unknown_metric_ids():
    with pytest.raises(ValueError, match="Unknown metric IDs"):
        build_plan(
            plan_id="bad",
            name="Bad",
            include_metric_ids=["not_a_real_metric"],
        )


def test_write_plan_is_valid_and_requires_force_for_overwrite(tmp_path):
    plan, _ = build_plan(plan_id="write-test", name="Write Test")
    output = tmp_path / "plan.json"

    write_plan(output, plan)
    with open(output, "r", encoding="utf-8") as handle:
        persisted = json.load(handle)
    validate_plan_schema(persisted)

    with pytest.raises(FileExistsError):
        write_plan(output, plan)

    write_plan(output, plan, overwrite=True)
