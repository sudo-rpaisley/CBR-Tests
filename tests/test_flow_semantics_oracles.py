from pathlib import Path

import pandas as pd

from cbr_tests.metrics.protocol_network.flow_semantics.flow_duration_consistency_profile import (
    run_flow_duration_consistency_metric,
)
from cbr_tests.metrics.protocol_network.flow_semantics.packet_byte_consistency_profile import (
    run_packet_byte_consistency_metric,
)
from cbr_tests.metrics.protocol_network.flow_semantics.tcp_flag_consistency_profile import (
    run_tcp_flag_consistency_metric,
)


def test_flow_duration_consistency_profile_hand_calculated_oracle():
    """Hand-calculated oracle: one of five fully numeric rows is consistent."""
    df = pd.DataFrame(
        {
            "duration": [10.0, -1.0, 10.0, 2.0, 10.0],
            "iat_min": [1.0, 0.0, 3.0, 0.5, 1.0],
            "iat_mean": [2.0, 0.0, 2.0, 1.0, 2.0],
            "iat_max": [3.0, 0.0, 4.0, 3.0, 3.0],
            "iat_std": [1.0, 0.0, 1.0, 1.0, 1.0],
            "fwd_iat_total": [4.0, 0.0, 4.0, 1.5, 11.0],
            "bwd_iat_total": [5.0, 0.0, 4.0, 1.5, 5.0],
        }
    )
    metric = {
        "_shared_df": df,
        "input_requirements": {
            "field_map": {
                "flow_duration": "duration",
                "flow_iat_min": "iat_min",
                "flow_iat_mean": "iat_mean",
                "flow_iat_max": "iat_max",
                "flow_iat_std": "iat_std",
                "fwd_iat_total": "fwd_iat_total",
                "bwd_iat_total": "bwd_iat_total",
            }
        },
        "calculation": {"parameters": {}},
    }

    ok, payload = run_flow_duration_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["flow_duration_consistency_profile"]

    assert ok is True
    assert result["checked_row_count"] == 5
    assert result["consistent_row_count"] == 1
    assert result["inconsistent_row_count"] == 4
    assert result["negative_duration_count"] == 1
    assert result["iat_order_violation_count"] == 1
    assert result["iat_exceeds_duration_count"] == 1
    assert result["direction_iat_exceeds_duration_count"] == 1
    assert result["flow_duration_consistency_ratio"] == 0.2


def test_packet_byte_consistency_profile_hand_calculated_oracle():
    """Hand-calculated oracle: one of five fully numeric rows is consistent."""
    df = pd.DataFrame(
        {
            "fwd_packets": [2, -1, 0, 2, 2],
            "bwd_packets": [1, 0, 0, 0, 0],
            "fwd_bytes": [200, 0, 100, 120, 200],
            "bwd_bytes": [100, 0, 0, 0, 0],
            "fwd_min": [50, 0, 0, 40, 100],
            "fwd_mean": [100, 0, 0, 45, 50],
            "fwd_max": [150, 0, 0, 50, 150],
            "bwd_min": [100, 0, 0, 0, 0],
            "bwd_mean": [100, 0, 0, 0, 0],
            "bwd_max": [100, 0, 0, 0, 0],
        }
    )
    metric = {
        "_shared_df": df,
        "input_requirements": {
            "field_map": {
                "total_fwd_packets": "fwd_packets",
                "total_bwd_packets": "bwd_packets",
                "total_len_fwd_packets": "fwd_bytes",
                "total_len_bwd_packets": "bwd_bytes",
                "fwd_pkt_len_min": "fwd_min",
                "fwd_pkt_len_mean": "fwd_mean",
                "fwd_pkt_len_max": "fwd_max",
                "bwd_pkt_len_min": "bwd_min",
                "bwd_pkt_len_mean": "bwd_mean",
                "bwd_pkt_len_max": "bwd_max",
            }
        },
        "calculation": {"parameters": {}},
    }

    ok, payload = run_packet_byte_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["packet_byte_consistency_profile"]

    assert ok is True
    assert result["checked_row_count"] == 5
    assert result["consistent_row_count"] == 1
    assert result["inconsistent_row_count"] == 4
    assert result["negative_packet_count"] == 1
    assert result["zero_packet_nonzero_byte_count"] == 1
    assert result["byte_total_exceeds_max_possible_count"] == 1
    assert result["length_order_violation_count"] == 1
    assert result["packet_byte_consistency_ratio"] == 0.2


def test_tcp_flag_consistency_profile_hand_calculated_oracle():
    """Hand-calculated oracle for aggregate TCP flag-count invariants."""
    df = pd.DataFrame(
        {
            "protocol": [6, 6, 17, 17],
            "fwd_packets": [1, 1, 1, 1],
            "bwd_packets": [1, 0, 1, 1],
            "syn": [1, 2, 0, 1],
            "ack": [1, 0, 0, 0],
            "fin": [0, 0, 0, 0],
            "rst": [0, 0, 0, 0],
        }
    )
    metric = {
        "_shared_df": df,
        "input_requirements": {
            "field_map": {
                "protocol": "protocol",
                "total_fwd_packets": "fwd_packets",
                "total_bwd_packets": "bwd_packets",
                "syn_flag_count": "syn",
                "ack_flag_count": "ack",
                "fin_flag_count": "fin",
                "rst_flag_count": "rst",
            }
        },
        "calculation": {"parameters": {}},
    }

    ok, payload = run_tcp_flag_consistency_metric(Path("unused.csv"), metric)
    result = payload["test_results"]["tcp_flag_consistency_profile"]

    assert ok is True
    assert result["checked_row_count"] == 4
    assert result["consistent_row_count"] == 2
    assert result["inconsistent_row_count"] == 2
    assert result["flag_exceeds_packet_count"] == 1
    assert result["non_tcp_with_tcp_flags_count"] == 1
    assert result["tcp_flag_consistency_ratio"] == 0.5
    assert result["scope"] == "aggregate_tcp_flag_count_invariants"
