from cbr_tests.metrics.decision_rules import (
    describe_measurement_parameter,
    resolve_maximum_ratio_failure_rule,
)


def test_maximum_ratio_failure_rule_records_framework_default():
    rule = resolve_maximum_ratio_failure_rule(
        {}, parameter_name="invalid_ratio_fail_threshold", default=0.01
    )
    assert rule["failure_threshold"] == 0.01
    assert rule["provenance"] == "framework-default"
    assert rule["scientific_role"] == "decision_policy_not_metric_definition"


def test_maximum_ratio_failure_rule_records_explicit_provenance():
    rule = resolve_maximum_ratio_failure_rule(
        {
            "invalid_ratio_fail_threshold": 0.05,
            "invalid_ratio_fail_threshold_provenance": "empirically-calibrated",
        },
        parameter_name="invalid_ratio_fail_threshold",
        default=0.01,
    )
    assert rule["failure_threshold"] == 0.05
    assert rule["provenance"] == "empirically-calibrated"
    assert rule["source"] == "metric.calculation.parameters"


def test_measurement_parameter_records_framework_default():
    result = describe_measurement_parameter(
        {}, parameter_name="tolerance", default=1e-6, value=1e-6
    )
    assert result["value"] == 1e-6
    assert result["provenance"] == "framework-default"
    assert result["scientific_role"] == "measurement_operationalisation_parameter"


def test_measurement_parameter_records_configured_provenance():
    result = describe_measurement_parameter(
        {"tolerance": 0.01, "tolerance_provenance": "empirically-calibrated"},
        parameter_name="tolerance",
        default=1e-6,
        value=0.01,
    )
    assert result["value"] == 0.01
    assert result["provenance"] == "empirically-calibrated"
    assert result["source"] == "metric.calculation.parameters"
