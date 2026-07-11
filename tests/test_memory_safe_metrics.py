from pathlib import Path

import pandas as pd

from runner.dispatch import run_distance_correlation_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics import handshake_plausibility_profile
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.handshake_plausibility_profile import run_handshake_plausibility_metric


def test_handshake_metric_uses_shared_dataframe_without_reloading(monkeypatch):
    def _fail_load(_path):
        raise AssertionError("should not reload when _shared_df is provided")

    monkeypatch.setattr(handshake_plausibility_profile, "load_tabular_dataset", _fail_load)
    df = pd.DataFrame(
        {
            "Protocol": ["TCP"],
            "Fwd Packets": [1],
            "Bwd Packets": [1],
            "SYN": [1],
            "ACK": [1],
            "RST": [0],
            "FIN": [0],
        }
    )
    metric = {
        "_shared_df": df,
        "input_requirements": {
            "field_map": {
                "protocol": "Protocol",
                "total_fwd_packets": "Fwd Packets",
                "total_bwd_packets": "Bwd Packets",
                "syn_flag_count": "SYN",
                "ack_flag_count": "ACK",
                "rst_flag_count": "RST",
                "fin_flag_count": "FIN",
            }
        },
    }

    ok, payload = run_handshake_plausibility_metric(Path("unused.csv"), metric)

    assert ok is True
    assert payload["test_results"]["handshake_plausibility_profile"]["checked_tcp_row_count"] == 1


def test_distance_correlation_metric_samples_large_shared_dataframe():
    df = pd.DataFrame({"a": range(20), "b": range(20), "c": [value * value for value in range(20)]})
    metric = {
        "input_requirements": {"candidate_fields": ["a", "b", "c"], "minimum_runnable_fields": 2},
        "calculation": {"parameters": {"max_rows": 5, "random_state": 1}},
    }

    ok, payload = run_distance_correlation_metric(Path("unused.csv"), metric, lambda _path: df, shared_df=df)

    assert ok is True
    summary = payload["test_results"]["distance_correlation_matrix_deviation"]["summary"]
    assert summary["source_row_count"] == 20
    assert summary["sampled_row_count"] == 5
