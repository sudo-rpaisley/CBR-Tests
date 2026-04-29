from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, build_test_results_taxonomy
from runner.dispatch import METRIC_REGISTRY


def test_validate_plan_schema_rejects_missing_plan_id():
    bad = {"plan_meta": {}, "metrics": [{"metric_id": "m1"}]}
    try:
        validate_plan_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        assert True


def test_metric_registry_contains_core_metrics():
    assert "valid_port_range_profile" in METRIC_REGISTRY
    assert "reserved_ip_address_profile" in METRIC_REGISTRY


def test_taxonomy_builders_basic_shape():
    metrics = [{"metric_id": "m1", "taxonomy_path": ["a", "b"]}]
    plan_tax = build_plan_taxonomy(metrics)
    assert "a" in plan_tax
    result_tax = build_result_taxonomy(metrics, [{"metric_id": "m1", "status": "success"}], {"m1": {"ok": True}})
    assert result_tax["a"]["b"]["_metrics"][0]["status"] == "success"
    results_tax = build_test_results_taxonomy(metrics, {"m1": {"ok": True}})
    assert results_tax["a"]["b"]["_results"][0]["metric_id"] == "m1"


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
