import json

from cbr_tests.plan_migration import (
    LEGACY_INTRINSIC_METRIC_MIGRATIONS,
    legacy_intrinsic_metric_ids,
    migrate_plan_to_canonical_ids,
)
from runner.schema import validate_plan_schema


def _plan(*metric_ids):
    return {
        "plan_meta": {"plan_id": "legacy-plan", "name": "Legacy plan"},
        "execution_policy": {"fail_fast": False, "allow_skips": False, "sample_mode": "full"},
        "metrics": [
            {
                "metric_id": metric_id,
                "label": metric_id,
                "taxonomy_path": ["old", metric_id],
                "enabled": True,
                "input_requirements": {"candidate_fields": ["Packet Length", "Inter Arrival Time"]},
                "calculation": {"method": "legacy_method", "parameters": {"max_sample_size": 321}},
            }
            for metric_id in metric_ids
        ],
    }


def test_migration_replaces_all_legacy_intrinsic_ids_and_preserves_parameters():
    source = _plan(*LEGACY_INTRINSIC_METRIC_MIGRATIONS)
    migrated, changes = migrate_plan_to_canonical_ids(source)

    assert len(changes) == len(LEGACY_INTRINSIC_METRIC_MIGRATIONS) == 12
    assert legacy_intrinsic_metric_ids(migrated) == []
    assert source["metrics"][0]["metric_id"] in LEGACY_INTRINSIC_METRIC_MIGRATIONS
    assert all(metric["calculation"]["parameters"]["max_sample_size"] == 321 for metric in migrated["metrics"])
    assert migrated["plan_meta"]["metric_id_contract"] == "metric-conformance-canonical-v1"
    validate_plan_schema(migrated)


def test_migration_updates_label_and_taxonomy_path_to_canonical_construct():
    source = _plan("kolmogorov_smirnov_feature_divergence")
    migrated, changes = migrate_plan_to_canonical_ids(source)
    metric = migrated["metrics"][0]

    assert changes == [
        {
            "from_metric_id": "kolmogorov_smirnov_feature_divergence",
            "to_metric_id": "feature_ks_internal_drift",
        }
    ]
    assert metric["metric_id"] == "feature_ks_internal_drift"
    assert metric["label"] == "Feature KS Internal Drift"
    assert metric["taxonomy_path"][-1] == "feature_ks_internal_drift"
    assert "statistical_structure_diagnostics" in metric["taxonomy_path"]


def test_migration_leaves_nonlegacy_metrics_unchanged():
    source = _plan("timestamp_parse_success_ratio")
    migrated, changes = migrate_plan_to_canonical_ids(source)

    assert changes == []
    assert migrated == source


def test_migrated_plan_round_trips_as_json_and_schema_validates():
    source = _plan("pearson_correlation_profile", "energy_distance")
    migrated, _changes = migrate_plan_to_canonical_ids(source)
    payload = json.loads(json.dumps(migrated))
    validate_plan_schema(payload)
