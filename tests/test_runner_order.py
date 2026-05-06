from runner.order import order_metrics_by_taxonomy


def test_order_metrics_by_taxonomy_basic():
    metrics = [{"metric_id": "b"}, {"metric_id": "a"}, {"metric_id": "c"}]
    ranks = {"a": 0, "b": 1}
    ordered = order_metrics_by_taxonomy(metrics, ranks)
    assert [m["metric_id"] for m in ordered] == ["a", "b", "c"]


def test_order_metrics_by_taxonomy_strict_raises():
    metrics = [{"metric_id": "x"}]
    try:
        order_metrics_by_taxonomy(metrics, {}, strict=True)
        assert False, "expected ValueError"
    except ValueError:
        assert True
