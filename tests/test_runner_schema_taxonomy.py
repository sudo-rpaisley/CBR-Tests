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


def test_validate_plan_schema_requires_taxonomy_and_calculation_method():
    bad_taxonomy = {
        "plan_meta": {"plan_id": "p1"},
        "metrics": [{"metric_id": "m1", "taxonomy_path": []}],
    }
    try:
        validate_plan_schema(bad_taxonomy)
        assert False, "expected ValueError for empty taxonomy_path"
    except ValueError:
        assert True

    bad_calc = {
        "plan_meta": {"plan_id": "p1"},
        "metrics": [{"metric_id": "m1", "taxonomy_path": ["x"], "calculation": {}}],
    }
    try:
        validate_plan_schema(bad_calc)
        assert False, "expected ValueError for missing calculation.method"
    except ValueError:
        assert True
