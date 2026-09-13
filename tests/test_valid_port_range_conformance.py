import pandas as pd

from cbr_tests.metrics.protocol_network.port_validity.valid_port_range_profile import (
    run_valid_port_range_metric,
)


def _metric(parameters=None):
    return {
        "_shared_df": pd.DataFrame({"Source Port": [0, 53, 65535, 70000, None]}),
        "input_requirements": {"candidate_fields": ["Source Port"]},
        "calculation": {"parameters": parameters or {}},
    }


def test_canonical_port_ratio_uses_full_normative_16_bit_domain():
    ok, payload = run_valid_port_range_metric(None, _metric())
    result = payload["test_results"]["valid_port_range_profile"]

    assert ok is True
    assert result["checked_port_count"] == 4
    assert result["valid_port_count"] == 3
    assert result["out_of_range_port_count"] == 1
    assert result["valid_port_range_ratio"] == 0.75
    assert result["normative_domain"] == {
        "port_namespace_min": 0,
        "port_namespace_max": 65535,
        "scientific_role": "normative_value_domain",
    }


def test_canonical_port_ratio_rejects_scenario_specific_subranges():
    ok, payload = run_valid_port_range_metric(
        None,
        _metric({"valid_min_port": 1, "valid_max_port": 49151}),
    )

    assert ok is False
    assert payload["reason_code"] == "noncanonical_metric_configuration"
    assert "0-65535" in payload["error"]
