from runner.result_semantics import metric_result_semantics, scientific_status_fragments


def test_execution_success_can_still_be_scientifically_not_runnable():
    payload = {
        "summary": {
            "observed_day_count": 1,
            "minimum_day_count": 2,
            "runnable": False,
            "interpretation_direction": "contextual",
        }
    }
    semantics = metric_result_semantics(payload)

    assert semantics["runnable"] is False
    assert semantics["applicability"] == "not_runnable"
    assert semantics["verdict"] is None
    assert semantics["contextual"] is True
    assert scientific_status_fragments(payload) == [
        "runnable=no",
        "interpretation=contextual",
    ]


def test_verdict_is_reported_separately_from_runnable_state():
    payload = {"summary": {"runnable": True, "status": "warn", "ratio": 0.96}}
    assert scientific_status_fragments(payload) == ["runnable=yes", "verdict=warn"]


def test_not_applicable_verdict_forces_non_runnable_semantics():
    payload = {"status": "not_applicable"}
    semantics = metric_result_semantics(payload)
    assert semantics["runnable"] is False
    assert semantics["verdict"] == "not_applicable"


def test_metric_without_explicit_applicability_does_not_invent_it():
    payload = {"summary": {"mean_wasserstein_distance": 1.25}}
    semantics = metric_result_semantics(payload)
    assert semantics["runnable"] is None
    assert semantics["applicability"] == "not_reported"
    assert scientific_status_fragments(payload) == []
