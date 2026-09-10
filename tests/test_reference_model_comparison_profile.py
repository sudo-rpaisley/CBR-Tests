import pandas as pd
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw
from scapy.utils import wrpcap

from runner.pcap_adapter import build_pcap_packet_dataframe, pcap_reference_metric_template

from tests.reference_model_comparison_profile import (
    compute_burstiness_deviation_from_reference,
    compute_distance_correlation_matrix_deviation_from_reference,
    compute_feature_set_mmd_score_from_reference,
    compute_feature_wise_energy_distance_from_reference,
    compute_feature_wise_ks_statistic_from_reference,
    compute_feature_wise_wasserstein_distance_from_reference,
    compute_flow_statistic_deviation_from_reference,
    compute_hourly_activity_divergence_from_reference,
    compute_inter_arrival_distribution_divergence_from_reference,
    compute_pearson_matrix_deviation_from_reference,
    compute_per_slice_class_divergence_from_reference,
    compute_per_slice_feature_distribution_deviation_from_reference,
    compute_port_use_divergence_from_reference,
    compute_protocol_mix_divergence_from_reference,
    compute_slice_proportion_deviation_from_reference,
    compute_spearman_matrix_deviation_from_reference,
)


def test_reference_distribution_dependency_and_temporal_metrics(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "f1": [1, 2, 3, 4],
        "f2": [1, 2, 3, 4],
        "timestamp": pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h"),
    })
    reference = pd.DataFrame({
        "f1": [1, 2, 10, 11],
        "f2": [1, 2, 10, 11],
        "timestamp": pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h"),
    })
    reference.to_csv(reference_path, index=False)
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path), "candidate_fields": ["f1", "f2"], "timestamp_field": "timestamp"}}

    assert compute_feature_wise_ks_statistic_from_reference(current, metric)["summary"]["runnable_field_count"] == 2
    assert compute_pearson_matrix_deviation_from_reference(current, metric)["summary"]["pair_count"] == 1
    assert compute_hourly_activity_divergence_from_reference(current, metric)["summary"]["hourly_activity_divergence_from_reference"] == 0.0


def test_reference_distributional_metric_oracles(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({"f": [0.0, 1.0]})
    pd.DataFrame({"f": [0.0, 2.0]}).to_csv(reference_path, index=False)
    metric = {
        "input_requirements": {
            "reference_dataset_path": str(reference_path),
            "candidate_fields": ["f"],
        },
        "calculation": {"parameters": {"max_sample_size": 100}},
    }

    wasserstein = compute_feature_wise_wasserstein_distance_from_reference(current, metric)
    assert wasserstein["fields"][0]["feature_wise_wasserstein_distance_from_reference"] == 0.5

    ks = compute_feature_wise_ks_statistic_from_reference(current, metric)
    assert ks["fields"][0]["feature_wise_ks_statistic_from_reference"] == 0.5

    energy = compute_feature_wise_energy_distance_from_reference(current, metric)
    assert energy["fields"][0]["feature_wise_energy_distance_from_reference"] == 0.5

    mmd = compute_feature_set_mmd_score_from_reference(current, metric)["summary"]
    assert mmd["standardization"] == "pooled_mean_and_standard_deviation"
    assert mmd["estimator"] == "biased_empirical_mmd_squared"
    assert mmd["rbf_bandwidth"] == 1.206045
    assert mmd["feature_set_mmd_score_from_reference"] == 0.196735


def test_reference_correlation_matrix_deviation_oracles(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "x": [1.0, 2.0, 3.0, 4.0],
        "y": [1.0, 2.0, 3.0, 4.0],
    })
    pd.DataFrame({
        "x": [1.0, 2.0, 3.0, 4.0],
        "y": [4.0, 3.0, 2.0, 1.0],
    }).to_csv(reference_path, index=False)
    metric = {
        "input_requirements": {
            "reference_dataset_path": str(reference_path),
            "candidate_fields": ["x", "y"],
        }
    }

    pearson = compute_pearson_matrix_deviation_from_reference(current, metric)["summary"]
    assert pearson["pair_count"] == 1
    assert pearson["pearson_matrix_deviation_from_reference"] == 2.0

    spearman = compute_spearman_matrix_deviation_from_reference(current, metric)["summary"]
    assert spearman["pair_count"] == 1
    assert spearman["spearman_matrix_deviation_from_reference"] == 2.0


def test_reference_distance_correlation_matrix_deviation_oracle(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "x": [0.0, 1.0, 2.0, 3.0],
        "y": [0.0, 1.0, 2.0, 3.0],
    })
    pd.DataFrame({
        "x": [0.0, 1.0, 2.0, 3.0],
        "y": [0.0, 1.0, 0.0, 1.0],
    }).to_csv(reference_path, index=False)
    metric = {
        "input_requirements": {
            "reference_dataset_path": str(reference_path),
            "candidate_fields": ["x", "y"],
        },
        "calculation": {"parameters": {"max_sample_size": 100}},
    }

    result = compute_distance_correlation_matrix_deviation_from_reference(current, metric)["summary"]
    assert result["pair_count"] == 1
    assert result["distance_correlation_matrix_deviation_from_reference"] == 0.47336


def test_reference_temporal_oracles_use_declared_timestamp_units_and_timezone():
    current = pd.DataFrame({"timestamp": [0, 1, 3, 6]})
    reference = pd.DataFrame({"timestamp": [0, 1, 2, 3]})
    metric = {
        "_reference_df": reference,
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {"parameters": {"timestamp_unit": "s"}},
    }

    iat = compute_inter_arrival_distribution_divergence_from_reference(current, metric)["summary"]
    assert iat["comparison_scope"] == "global_sorted_timestamp_sequence"
    assert iat["divergence"] == "two_sample_ks_statistic"
    assert iat["inter_arrival_distribution_divergence_from_reference"] == 0.666667

    burst = compute_burstiness_deviation_from_reference(current, metric)["summary"]
    assert burst["burstiness_deviation_from_reference"] == 0.579796

    hourly_metric = {
        "_reference_df": pd.DataFrame({"timestamp": [0, 7200]}),
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {"parameters": {"timestamp_unit": "s", "activity_timezone": "UTC"}},
    }
    hourly = compute_hourly_activity_divergence_from_reference(
        pd.DataFrame({"timestamp": [0, 3600]}), hourly_metric
    )["summary"]
    assert hourly["timestamp_unit"] == "s"
    assert hourly["activity_timezone"] == "UTC"
    assert hourly["divergence"] == "total_variation_distance_over_24_hour_bins"
    assert hourly["hourly_activity_divergence_from_reference"] == 0.5


def test_reference_slice_metrics_exclude_missing_and_do_not_score_unshared_conditionals():
    current = pd.DataFrame({
        "slice": ["s1", "s1", "s2", None],
        "label": ["a", "a", "a", "a"],
        "f": [0.0, 0.0, 5.0, 99.0],
    })
    reference = pd.DataFrame({
        "slice": ["s1", "s1", "s3", None],
        "label": ["a", "b", "b", "b"],
        "f": [0.0, 1.0, 7.0, 99.0],
    })
    metric = {
        "_reference_df": reference,
        "input_requirements": {
            "slice_field": "slice",
            "label_field": "label",
            "candidate_fields": ["f"],
        },
    }

    slice_prop = compute_slice_proportion_deviation_from_reference(current, metric)["summary"]
    assert slice_prop["current_valid_slice_count"] == 3
    assert slice_prop["reference_valid_slice_count"] == 3
    assert slice_prop["slice_proportion_deviation_from_reference"] == 0.333333

    classes = compute_per_slice_class_divergence_from_reference(current, metric)
    assert classes["summary"]["shared_slice_count"] == 1
    assert classes["summary"]["candidate_only_slices"] == ["s2"]
    assert classes["summary"]["reference_only_slices"] == ["s3"]
    assert classes["summary"]["per_slice_class_divergence_from_reference"] == 0.5

    features = compute_per_slice_feature_distribution_deviation_from_reference(current, metric)
    assert features["summary"]["shared_slice_count"] == 1
    assert features["summary"]["comparison_count"] == 1
    assert features["summary"]["per_slice_feature_distribution_deviation_from_reference"] == 0.5


def test_reference_protocol_and_port_metrics_exclude_missing_categories():
    current = pd.DataFrame({
        "Protocol": [6, 6, 17, None],
        "Source Port": [80, 80, None, None],
        "Destination Port": [1000, 1001, None, None],
    })
    reference = pd.DataFrame({
        "Protocol": [6, 17, 17, None],
        "Source Port": [80, 53, None, None],
        "Destination Port": [1000, 1000, None, None],
    })
    metric = {
        "_reference_df": reference,
        "input_requirements": {
            "protocol_field": "Protocol",
            "port_fields": ["Source Port", "Destination Port"],
        },
    }

    protocol = compute_protocol_mix_divergence_from_reference(current, metric)["summary"]
    assert protocol["current_valid_protocol_count"] == 3
    assert protocol["reference_valid_protocol_count"] == 3
    assert protocol["protocol_mix_divergence_from_reference"] == 0.333333

    ports = compute_port_use_divergence_from_reference(current, metric)
    assert ports["summary"]["runnable_field_count"] == 2
    assert ports["summary"]["port_use_divergence_from_reference"] == 0.5
    assert [row["total_variation_distance"] for row in ports["fields"]] == [0.5, 0.5]


def test_flow_statistic_reference_distance_requires_matching_flow_definition_for_interpretation():
    current = pd.DataFrame({"Flow Duration": [1.0, 2.0]})
    reference = pd.DataFrame({"Flow Duration": [1.0, 3.0]})
    metric = {
        "_reference_df": reference,
        "input_requirements": {
            "candidate_fields": ["Flow Duration"],
            "flow_definition_id": "bidirectional-5tuple-timeout60",
            "reference_flow_definition_id": "bidirectional-5tuple-timeout60",
        },
    }
    result = compute_flow_statistic_deviation_from_reference(current, metric)
    assert result["fields"][0]["flow_statistic_deviation_from_reference"] == 0.5
    assert result["summary"]["distance"] == "feature_wise_wasserstein_1"
    assert result["summary"]["flow_segmentation_comparability"] == "matched"
    assert result["summary"]["interpretation_ready"] is True

    mismatched = {
        **metric,
        "input_requirements": {
            **metric["input_requirements"],
            "reference_flow_definition_id": "unidirectional-5tuple-timeout30",
        },
    }
    mismatch_result = compute_flow_statistic_deviation_from_reference(current, mismatched)["summary"]
    assert mismatch_result["flow_segmentation_comparability"] == "mismatched"
    assert mismatch_result["interpretation_ready"] is False


def test_reference_slice_and_protocol_metrics(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "slice": ["s1", "s1", "s2"],
        "label": ["benign", "attack", "benign"],
        "Protocol": [6, 6, 17],
        "Source Port": [80, 80, 53],
        "Destination Port": [1000, 1001, 53],
    })
    reference = pd.DataFrame({
        "slice": ["s1", "s2", "s2"],
        "label": ["benign", "attack", "attack"],
        "Protocol": [6, 17, 17],
        "Source Port": [80, 53, 53],
        "Destination Port": [1000, 53, 53],
    })
    reference.to_csv(reference_path, index=False)
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path), "slice_field": "slice", "label_field": "label", "protocol_field": "Protocol", "port_fields": ["Source Port", "Destination Port"]}}

    assert compute_slice_proportion_deviation_from_reference(current, metric)["summary"]["slice_proportion_deviation_from_reference"] == 0.333333
    assert compute_per_slice_class_divergence_from_reference(current, metric)["summary"]["per_slice_class_divergence_from_reference"] > 0.0
    assert compute_protocol_mix_divergence_from_reference(current, metric)["summary"]["protocol_mix_divergence_from_reference"] == 0.333333
    assert compute_port_use_divergence_from_reference(current, metric)["summary"]["port_use_divergence_from_reference"] > 0.0


def test_reference_metrics_load_raw_pcap_with_explicit_epoch_units(tmp_path):
    candidate_path = tmp_path / "candidate.pcap"
    reference_path = tmp_path / "reference.pcap"
    candidate_packets = [
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=10000, dport=80, flags="S") / Raw(b"a"),
        IP(src="10.0.0.2", dst="10.0.0.1") / TCP(sport=80, dport=10000, flags="SA") / Raw(b"bb"),
        IP(src="10.0.0.1", dst="10.0.0.2") / TCP(sport=10000, dport=80, flags="A") / Raw(b"ccc"),
        IP(src="10.0.0.3", dst="10.0.0.4") / UDP(sport=20000, dport=53) / Raw(b"dddd"),
    ]
    reference_packets = [
        IP(src="10.1.0.1", dst="10.1.0.2") / TCP(sport=11000, dport=80, flags="S") / Raw(b"aa"),
        IP(src="10.1.0.2", dst="10.1.0.1") / TCP(sport=80, dport=11000, flags="SA") / Raw(b"bbbb"),
        IP(src="10.1.0.1", dst="10.1.0.2") / TCP(sport=11000, dport=80, flags="A") / Raw(b"cccccc"),
        IP(src="10.1.0.3", dst="10.1.0.4") / UDP(sport=21000, dport=53) / Raw(b"dddddddd"),
    ]
    for packets, path, timestamps in (
        (candidate_packets, candidate_path, (1.0, 2.0, 4.0, 8.0)),
        (reference_packets, reference_path, (1.0, 3.0, 6.0, 10.0)),
    ):
        for packet, timestamp in zip(packets, timestamps):
            packet.time = timestamp
        wrpcap(str(path), packets)

    current = build_pcap_packet_dataframe(candidate_path)
    temporal_metric = pcap_reference_metric_template("inter_arrival_distribution_divergence_from_reference", reference_path)
    temporal = compute_inter_arrival_distribution_divergence_from_reference(current, temporal_metric)
    assert temporal["summary"]["timestamp_unit"] == "s"
    assert temporal["summary"]["current_gap_count"] == 3
    assert temporal["summary"]["reference_gap_count"] == 3
    assert temporal["summary"]["inter_arrival_distribution_divergence_from_reference"] is not None

    hourly_metric = pcap_reference_metric_template("hourly_activity_divergence_from_reference", reference_path)
    hourly = compute_hourly_activity_divergence_from_reference(current, hourly_metric)["summary"]
    assert hourly["timestamp_unit"] == "s"
    assert hourly["activity_timezone"] == "UTC"

    mmd_metric = pcap_reference_metric_template("feature_set_mmd_score_from_reference", reference_path)
    mmd = compute_feature_set_mmd_score_from_reference(current, mmd_metric)["summary"]
    assert mmd["runnable"] is True
    assert mmd["fields"] == ["Packet Length", "Inter Arrival Time"]
    assert mmd["feature_set_mmd_score_from_reference"] is not None
