import json
from pathlib import Path

import pandas as pd

from create_plan import _create_batch
from run_batch import load_batch
from runner.schema import validate_plan_schema


def _metric_ids(plan: dict) -> set[str]:
    return {metric["metric_id"] for metric in plan["metrics"]}


def test_create_batch_generates_sequential_manifest_and_common_metric_plans(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    for path, offset in ((first, 0), (second, 10)):
        pd.DataFrame(
            {
                "Source IP": ["192.0.2.1", "198.51.100.2"],
                "Destination IP": ["198.51.100.2", "192.0.2.1"],
                "Source Port": [12345 + offset, 443],
                "Destination Port": [53, 443],
                "Protocol": [17, 6],
            }
        ).to_csv(path, index=False)

    manifest_path = tmp_path / "plans" / "comparison_batch.json"
    written = _create_batch(
        plan_id="comparison",
        name="Comparison",
        description="Batch test",
        dataset_paths=[first, second],
        field_translation_path=None,
        include_metric_ids=None,
        exclude_metric_ids=None,
        reference_dataset_path=None,
        service_port_configuration=None,
        output_path=manifest_path,
        force=False,
        per_dataset_metrics=False,
        interactive=False,
    )

    assert written == manifest_path.resolve()
    batch = load_batch(written)
    assert batch["batch_meta"]["execution_mode"] == "sequential"
    assert batch["batch_meta"]["dataset_count"] == 2
    assert batch["batch_meta"]["metric_policy"] == "common_across_all_datasets"
    assert len(batch["jobs"]) == 2
    assert batch["common_metric_ids"]

    plans = []
    for job in batch["jobs"]:
        plan_path = Path(job["plan_path"])
        if not plan_path.is_absolute():
            plan_path = (Path.cwd() / plan_path).resolve()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        validate_plan_schema(plan)
        plans.append(plan)
        assert job["runnable_metric_count"] == len(plan["metrics"])
        assert plan["plan_creation"]["batch"]["job_count"] == 2

    assert _metric_ids(plans[0]) == _metric_ids(plans[1])
    assert _metric_ids(plans[0]) == set(batch["common_metric_ids"])


def test_batch_can_keep_dataset_specific_metric_sets(tmp_path):
    first = tmp_path / "richer.csv"
    second = tmp_path / "ports_only.csv"
    pd.DataFrame(
        {
            "Source IP": ["192.0.2.1", "198.51.100.2"],
            "Destination IP": ["198.51.100.2", "192.0.2.1"],
            "Source Port": [12345, 443],
            "Destination Port": [53, 443],
            "Protocol": [17, 6],
        }
    ).to_csv(first, index=False)
    pd.DataFrame(
        {
            "Source Port": [12345, 443],
            "Destination Port": [53, 443],
        }
    ).to_csv(second, index=False)

    manifest_path = tmp_path / "plans" / "per_dataset_batch.json"
    _create_batch(
        plan_id="per-dataset",
        name="Per Dataset",
        description="Batch test",
        dataset_paths=[first, second],
        field_translation_path=None,
        include_metric_ids=None,
        exclude_metric_ids=None,
        reference_dataset_path=None,
        service_port_configuration=None,
        output_path=manifest_path,
        force=False,
        per_dataset_metrics=True,
        interactive=False,
    )

    batch = load_batch(manifest_path.resolve())
    assert batch["batch_meta"]["metric_policy"] == "per_dataset"
    assert batch["common_metric_ids"] is None
    assert len(batch["jobs"]) == 2


def test_load_batch_rejects_invalid_manifest(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"schema_version": 1, "batch_meta": {"batch_id": "x"}, "jobs": []}), encoding="utf-8")

    try:
        load_batch(path)
    except ValueError as exc:
        assert "non-empty jobs list" in str(exc)
    else:
        raise AssertionError("Expected invalid empty batch manifest to be rejected")
