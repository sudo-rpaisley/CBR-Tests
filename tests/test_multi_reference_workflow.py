from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import create_plan
from runner.field_translation import load_field_translation, translate_metric_fields
from runner.plan_builder import build_plan
from tests.reference_model_comparison_profile import compute_feature_wise_ks_statistic_from_reference


def _write_flow_csv(path: Path, *, offset: float = 0.0) -> None:
    pd.DataFrame(
        {
            "f1": [1.0 + offset, 2.0 + offset, 3.0 + offset, 4.0 + offset],
            "f2": [2.0 + offset, 4.0 + offset, 6.0 + offset, 8.0 + offset],
            "timestamp": [
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T03:00:00Z",
            ],
            "Protocol": [6, 6, 17, 17],
            "Source Port": [80, 443, 53, 12345],
            "Destination Port": [50000, 50001, 53, 443],
        }
    ).to_csv(path, index=False)


def test_tabular_reference_plan_unlocks_reference_metrics(tmp_path):
    candidate = tmp_path / "candidate.csv"
    reference = tmp_path / "reference.csv"
    _write_flow_csv(candidate)
    _write_flow_csv(reference, offset=0.5)

    requested = [
        "feature_wise_ks_statistic_from_reference",
        "pearson_matrix_deviation_from_reference",
        "hourly_activity_divergence_from_reference",
        "protocol_mix_divergence_from_reference",
        "port_use_divergence_from_reference",
    ]
    plan, report = build_plan(
        plan_id="tabular-reference",
        name="Tabular reference",
        dataset_path=candidate,
        reference_dataset_path=reference,
        include_metric_ids=requested,
    )

    assert {metric["metric_id"] for metric in plan["metrics"]} == set(requested)
    assert report["runnable_metric_count"] == len(requested)
    assert plan["plan_creation"]["reference_dataset"] == str(reference.resolve())


def test_reference_sidecar_maps_different_raw_column_names(tmp_path):
    candidate = tmp_path / "candidate.csv"
    reference = tmp_path / "reference.csv"
    candidate_translation = tmp_path / "candidate.translation.json"
    reference_translation = tmp_path / "reference.field_translation.json"

    pd.DataFrame(
        {
            "candidate_a": [1, 2, 3, 4],
            "candidate_b": [2, 4, 6, 8],
        }
    ).to_csv(candidate, index=False)
    pd.DataFrame(
        {
            "reference_a": [1, 2, 4, 5],
            "reference_b": [2, 4, 8, 10],
        }
    ).to_csv(reference, index=False)

    candidate_translation.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "test_to_dataset_fields": {"f1": "candidate_a", "f2": "candidate_b"},
            }
        ),
        encoding="utf-8",
    )
    reference_translation.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "test_to_dataset_fields": {"f1": "reference_a", "f2": "reference_b"},
            }
        ),
        encoding="utf-8",
    )

    plan, _report = build_plan(
        plan_id="translated-reference",
        name="Translated reference",
        dataset_path=candidate,
        field_translation_path=candidate_translation,
        reference_dataset_path=reference,
        include_metric_ids=["feature_wise_ks_statistic_from_reference"],
    )
    metric = plan["metrics"][0]
    assert metric["reference_field_map"] == {
        "reference_a": "candidate_a",
        "reference_b": "candidate_b",
    }

    candidate_df = pd.read_csv(candidate)
    translated_metric = translate_metric_fields(
        metric,
        load_field_translation(candidate_translation),
        candidate_df.columns,
    )
    result = compute_feature_wise_ks_statistic_from_reference(candidate_df, translated_metric)
    assert result["summary"]["runnable_field_count"] == 2


def test_batch_builds_candidate_reference_cross_product(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    candidates = [tmp_path / "candidate1.csv", tmp_path / "candidate2.csv"]
    references = [tmp_path / "reference1.csv", tmp_path / "reference2.csv"]
    for index, path in enumerate([*candidates, *references]):
        _write_flow_csv(path, offset=float(index))

    output = tmp_path / "plans" / "matrix_batch.json"
    create_plan._create_batch(
        plan_id="matrix",
        name="Matrix",
        description="matrix test",
        dataset_paths=candidates,
        field_translation_path=None,
        include_metric_ids=["feature_wise_ks_statistic_from_reference"],
        exclude_metric_ids=None,
        reference_dataset_paths=references,
        service_port_configuration=None,
        output_path=output,
        force=False,
        per_dataset_metrics=False,
        interactive=False,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["batch_meta"]["dataset_count"] == 2
    assert payload["batch_meta"]["reference_dataset_count"] == 2
    assert payload["batch_meta"]["job_count"] == 4
    assert len(payload["jobs"]) == 4
    assert {
        (Path(job["dataset_path"]).name, Path(job["reference_dataset_path"]).name)
        for job in payload["jobs"]
    } == {
        ("candidate1.csv", "reference1.csv"),
        ("candidate1.csv", "reference2.csv"),
        ("candidate2.csv", "reference1.csv"),
        ("candidate2.csv", "reference2.csv"),
    }


def test_reference_browser_collects_multiple_files(monkeypatch):
    selected = iter(["datasets/reference-a.csv", "datasets/reference-b.csv"])
    answers = iter(["y", "n"])
    monkeypatch.setattr(create_plan, "_browse_dataset_file", lambda: next(selected))
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))

    assert create_plan._browse_reference_files() == [
        "datasets/reference-a.csv",
        "datasets/reference-b.csv",
    ]
