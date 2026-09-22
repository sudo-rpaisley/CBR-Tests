from __future__ import annotations

import json
from pathlib import Path

from scripts.migrate_campaign_plans_to_canonical_ids import _campaign_plan_paths, assess_plan


def _plan(*metric_ids: str, allow_skips: bool = False) -> dict:
    return {
        "plan_meta": {"plan_id": "test-plan", "name": "Test plan"},
        "execution_policy": {
            "fail_fast": False,
            "allow_skips": allow_skips,
            "sample_mode": "full",
        },
        "metrics": [
            {
                "metric_id": metric_id,
                "label": metric_id,
                "taxonomy_path": ["legacy", metric_id],
                "enabled": True,
                "input_requirements": {"candidate_fields": ["Packet Length"]},
                "calculation": {"method": "test", "parameters": {}},
            }
            for metric_id in metric_ids
        ],
    }


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_assessment_marks_safe_legacy_ids_ready_after_migration(tmp_path):
    path = _write_json(
        tmp_path / "legacy_plan.json",
        _plan("burstiness_coefficient_deviation", "pearson_correlation_profile"),
    )

    assessment = assess_plan(path)

    assert assessment.changed is True
    assert assessment.ready_after_safe_migration is True
    assert assessment.compatibility_ids == []
    assert [change["to_metric_id"] for change in assessment.changes] == [
        "burstiness_internal_drift",
        "pearson_dependency_profile",
    ]


def test_assessment_refuses_compatibility_only_profile(tmp_path):
    path = _write_json(
        tmp_path / "compatibility_plan.json",
        _plan("protocol_validity_profile"),
    )

    assessment = assess_plan(path)

    assert assessment.changed is False
    assert assessment.ready_after_safe_migration is False
    assert assessment.compatibility_ids == ["protocol_validity_profile"]
    assert "compatibility-only" in str(assessment.contract_error)


def test_assessment_reports_other_strict_contract_blockers(tmp_path):
    path = _write_json(
        tmp_path / "skipping_plan.json",
        _plan("timestamp_parse_success_ratio", allow_skips=True),
    )

    assessment = assess_plan(path)

    assert assessment.changed is False
    assert assessment.ready_after_safe_migration is False
    assert "allow_skips=false" in str(assessment.contract_error)


def test_campaign_plan_collection_deduplicates_reused_plans(tmp_path):
    plan_a = _write_json(tmp_path / "plans" / "a.json", _plan("timestamp_parse_success_ratio"))
    plan_b = _write_json(tmp_path / "plans" / "b.json", _plan("timestamp_parse_success_ratio"))

    batch = {
        "schema_version": 1,
        "batch_meta": {"batch_id": "matrix-a", "name": "Matrix A"},
        "jobs": [
            {
                "job_id": "job-1",
                "dataset_path": "datasets/a.pcapng",
                "plan_path": "plans/a.json",
            },
            {
                "job_id": "job-2",
                "dataset_path": "datasets/b.pcapng",
                "plan_path": "plans/a.json",
            },
            {
                "job_id": "job-3",
                "dataset_path": "datasets/c.pcapng",
                "plan_path": "plans/b.json",
            },
        ],
    }
    batch_path = _write_json(tmp_path / "plans" / "matrix-a_batch.json", batch)
    campaign = {
        "schema_version": 1,
        "campaign_meta": {"campaign_id": "campaign-a", "name": "Campaign A"},
        "matrices": [
            {
                "queue_id": "matrix-001-matrix-a",
                "position": 1,
                "batch_id": "matrix-a",
                "batch_path": str(batch_path.relative_to(tmp_path)),
            }
        ],
    }
    campaign_path = _write_json(tmp_path / "campaigns" / "campaign-a_campaign.json", campaign)

    loaded, paths = _campaign_plan_paths(tmp_path, campaign_path)

    assert loaded["campaign_meta"]["campaign_id"] == "campaign-a"
    assert paths == [plan_a.resolve(), plan_b.resolve()]
