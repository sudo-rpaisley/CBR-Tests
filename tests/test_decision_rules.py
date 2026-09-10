import pytest

from cbr_tests.metrics.decision_rules import classify_ratio, resolve_ratio_decision_rule


def test_ratio_decision_rule_marks_unoverridden_cutoffs_as_framework_defaults():
    rule = resolve_ratio_decision_rule({}, default_pass=0.99, default_warn=0.95)
    assert rule["pass_threshold"] == 0.99
    assert rule["warn_threshold"] == 0.95
    assert rule["provenance"] == "framework-default"
    assert rule["scientific_role"] == "decision_policy_not_metric_definition"


def test_ratio_decision_rule_marks_threshold_overrides_as_scenario_configured():
    rule = resolve_ratio_decision_rule(
        {"pass_threshold": 0.9, "warn_threshold": 0.8},
        default_pass=0.99,
        default_warn=0.95,
    )
    assert rule["provenance"] == "scenario-configured"
    assert classify_ratio(0.95, rule) == "pass"
    assert classify_ratio(0.85, rule) == "warn"
    assert classify_ratio(0.5, rule) == "fail"


def test_ratio_decision_rule_allows_explicit_provenance_label():
    rule = resolve_ratio_decision_rule(
        {
            "pass_threshold": 0.9,
            "warn_threshold": 0.8,
            "threshold_provenance": "empirically-calibrated",
        },
        default_pass=0.99,
        default_warn=0.95,
    )
    assert rule["provenance"] == "empirically-calibrated"


def test_ratio_decision_rule_rejects_invalid_ordering():
    with pytest.raises(ValueError):
        resolve_ratio_decision_rule(
            {"pass_threshold": 0.8, "warn_threshold": 0.9},
            default_pass=0.99,
            default_warn=0.95,
        )


def test_none_ratio_is_not_applicable_not_failure():
    rule = resolve_ratio_decision_rule({}, default_pass=0.99, default_warn=0.95)
    assert classify_ratio(None, rule) == "not_applicable"
