from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy


def test_validate_plan_schema_rejects_missing_plan_id():
    bad = {"plan_meta": {}, "metrics": [{"metric_id": "m1"}]}
    try:
        validate_plan_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        assert True


def test_taxonomy_builders_basic_shape():
    metrics = [{"metric_id": "m1", "taxonomy_path": ["a", "b"]}]
    plan_tax = build_plan_taxonomy(metrics)
    assert "a" in plan_tax
    result_tax = build_result_taxonomy(metrics, [{"metric_id": "m1", "status": "success"}], {"m1": {"ok": True}})
    assert result_tax["a"]["b"]["_metrics"][0]["status"] == "success"
