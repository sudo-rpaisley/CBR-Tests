"""Equivalence checks for fused packet diagnostics and vectorised temporal bins."""
from __future__ import annotations

import pandas as pd
from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.l2 import ARP, Ether
from scapy.utils import wrpcap

from cbr_tests.metrics.pcap_handshake import run_pcap_handshake_plausibility_metric
from cbr_tests.metrics.timestamp_coherence import run_timestamp_coherence_metric
from cbr_tests.metrics.temporal import _continuous_hourly_counts, compute_periodicity_preservation_score
from runner.dispatch import build_metric_handlers
from runner.pcap_compact import build_compact_pcap_packet_dataframe


def test_fused_scanners_match_independent_metrics_and_keep_non_ip_frames(tmp_path, monkeypatch):
    path = tmp_path / "mixed.pcap"
    packets = [
        Ether() / ARP(),
        Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=1234, dport=443, flags="S"),
        Ether() / IP(src="10.0.0.2", dst="10.0.0.1") / TCP(sport=443, dport=1234, flags="SA"),
        Ether() / IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=1234, dport=443, flags="A"),
        Ether() / IP(src="10.0.0.3", dst="10.0.0.4") / UDP(sport=5000, dport=53),
        Ether() / IP(src="10.0.0.5", dst="10.0.0.6") / TCP(sport=5001, dport=25, flags="S"),
    ]
    for packet, ts in zip(packets, [3.0, 3.1, 3.1, 3.0, 4.5, 4.5]):
        packet.time = ts
    wrpcap(str(path), packets)
    metrics = {
        "handshake_plausibility_profile": {"calculation": {"parameters": {"max_examples": 4}}},
        "timestamp_coherence_profile": {"calculation": {"parameters": {"large_gap_threshold_seconds": 0.75}}},
    }
    expected = {
        "handshake_plausibility_profile": run_pcap_handshake_plausibility_metric(path, metrics["handshake_plausibility_profile"]),
        "timestamp_coherence_profile": run_timestamp_coherence_metric(path, metrics["timestamp_coherence_profile"]),
    }
    frame = build_compact_pcap_packet_dataframe(path, precompute_metrics=metrics)
    cached = frame.attrs["_precomputed_packet_metrics"]
    assert cached == expected
    assert len(frame) == len(packets) - 1
    assert cached["timestamp_coherence_profile"][1]["test_results"]["timestamp_coherence_profile"]["packet_count"] == len(packets)

    # These handlers must use the precomputed results, not open the PCAP again.
    def refuse_reread(*_args, **_kwargs):
        raise AssertionError("A second PCAP scan was attempted")

    monkeypatch.setattr("runner.dispatch.run_timestamp_coherence_metric", refuse_reread)
    monkeypatch.setattr("runner.dispatch.run_pcap_handshake_plausibility_metric", refuse_reread)
    handlers = build_metric_handlers(frame, lambda path: None, {})
    for metric_id, metric in metrics.items():
        assert handlers[metric_id](path, metric) == expected[metric_id]


def test_unfused_scanner_behaviour_unchanged(tmp_path):
    path = tmp_path / "packets.pcap"
    packets = [
        IP(src="192.0.2.1", dst="192.0.2.2") / TCP(sport=12345, dport=80, flags="S"),
        IP(src="192.0.2.1", dst="192.0.2.2") / TCP(sport=12345, dport=80, flags="SA"),
    ]
    for i, packet in enumerate(packets):
        packet.time = 5.0 + i
    wrpcap(str(path), packets)
    ok, result = run_pcap_handshake_plausibility_metric(path, {})
    assert ok and result["test_results"]["handshake_plausibility_profile"]["contradictory_transition_count"] == 1
    ok, result = run_timestamp_coherence_metric(path, {})
    assert ok and result["test_results"]["timestamp_coherence_profile"]["packet_count"] == 2


def test_vectorised_hour_bins_match_historical_timestamp_loop():
    timestamps = pd.to_datetime([
        "2026-10-23T23:40:00Z",
        "2026-10-24T00:01:00Z",
        "2026-10-24T00:59:00Z",
        "2026-10-24T04:01:00Z",
        "2026-10-25T02:10:00Z",
    ], utc=True)
    series = pd.Series(timestamps).sort_values()
    historical = _continuous_hourly_counts(series.tolist())
    vectorised = _continuous_hourly_counts(series)
    assert vectorised == historical
    frame = pd.DataFrame({"Timestamp": series.astype("int64") / 1_000_000_000})
    metric = {
        "input_requirements": {"timestamp_field": "Timestamp"},
        "calculation": {"parameters": {"timestamp_unit": "s", "lags": [1, 24], "minimum_lag_pairs": 1}},
    }
    result = compute_periodicity_preservation_score(frame, metric)
    assert result["summary"]["hourly_bin_count"] == len(historical[0])
    assert result["lags"][0]["lag_hours"] == 1
