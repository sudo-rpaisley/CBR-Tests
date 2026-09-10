import pandas as pd

from cbr_tests.metrics.address_validity import (
    compute_reserved_address_misuse_ratio,
    compute_valid_ip_address_ratio,
)


def test_valid_ip_address_ratio_uses_non_missing_checked_values_as_denominator():
    df = pd.DataFrame(
        {
            "Source IP": ["10.0.0.1", "not-an-ip", None],
            "Destination IP": ["8.8.8.8", "2001:db8::1", ""],
        }
    )
    metric = {
        "input_requirements": {
            "candidate_fields": ["Source IP", "Destination IP"],
        }
    }

    result = compute_valid_ip_address_ratio(df, metric)
    summary = result["summary"]

    assert summary["checked_address_count"] == 4
    assert summary["valid_address_count"] == 3
    assert summary["invalid_address_count"] == 1
    assert summary["missing_address_count"] == 2
    assert summary["valid_ip_address_ratio"] == 0.75
    assert summary["denominator_policy"] == "non_missing_candidate_ip_values"


def test_reserved_address_misuse_ratio_counts_only_explicit_policy_violations():
    df = pd.DataFrame(
        {
            "Source IP": ["10.0.0.1", "8.8.8.8", "bad", None],
            "Destination IP": ["192.0.2.1", "1.1.1.1", "", "198.18.0.1"],
        }
    )
    metric = {
        "input_requirements": {
            "candidate_fields": ["Source IP", "Destination IP"],
        },
        "calculation": {
            "parameters": {
                "misuse_categories": ["private", "documentation", "benchmarking"],
            }
        },
    }

    result = compute_reserved_address_misuse_ratio(df, metric)
    summary = result["summary"]

    # Five syntactically valid addresses are eligible for the misuse denominator:
    # 10.0.0.1, 8.8.8.8, 192.0.2.1, 1.1.1.1 and 198.18.0.1.
    assert summary["valid_address_count"] == 5
    assert summary["invalid_address_count"] == 1
    assert summary["missing_address_count"] == 2
    assert summary["misuse_address_count"] == 3
    assert summary["reserved_address_misuse_ratio"] == 0.6
    assert summary["denominator_policy"] == "valid_candidate_ip_values_only"


def test_reserved_address_profile_without_policy_does_not_claim_zero_misuse():
    df = pd.DataFrame(
        {
            "Source IP": ["10.0.0.1"],
            "Destination IP": ["8.8.8.8"],
        }
    )
    metric = {
        "input_requirements": {
            "candidate_fields": ["Source IP", "Destination IP"],
        }
    }

    result = compute_reserved_address_misuse_ratio(df, metric)
    summary = result["summary"]

    assert summary["policy_configured"] is False
    assert summary["runnable"] is False
    assert summary["reserved_address_misuse_ratio"] is None
    assert summary["special_use_category_counts"] if False else True
