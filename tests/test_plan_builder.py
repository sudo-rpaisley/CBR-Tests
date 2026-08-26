import json

import pandas as pd
import pytest

from create_plan import _slug
from runner.metric_catalog import available_metric_ids, build_metric_catalog
from runner.pcap_adapter import (
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_METRICS,
    PCAP_REFERENCE_METRICS,
    PCAP_REFERENCE_UNSUPPORTED_REASONS,
    PCAP_SELF_DERIVED_METRICS,
)
from runner.plan_builder import build_plan, write_plan
from runner.schema import validate_plan_schema


def test_metric_catalog_covers_runtime_dispatcher():
    runtime_ids = set(available_metric_ids())
    catalogue_ids = {entry["metric_id"] for entry in build_metric_catalog()}
    assert catalogue_ids == runtime_ids
    assert runtime_ids
    assert _slug("DeepSecure DrDoS DNS") == "deepsecure-drdos-dns"
    assert _slug("  My Plan 2026!  ") == "my-plan-2026"


def test_automatic_plan_requires_dataset():
    with pytest.raises(ValueError, match="dataset is required"):
        build_plan(plan_id="missing-dataset", name="Missing Dataset", dataset_path=None)


def test_generated_plan_contains_only_ready_enabled_metrics(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame(
        {
            "Source IP": ["192.0.2.1", "198.51.100.2"],
            "Destination IP": ["198.51.100.2", "192.0.2.1"],
            "Source Port": [12345, 443],
            "Destination Port": [53, 443],
            "Protocol": [17, 6],
        }
    ).to_csv(dataset, index=False)

    plan, report = build_plan(plan_id="runnable", name="Runnable", dataset_path=dataset)

    assert plan["metrics"]
    assert all(metric["enabled"] is True for metric in plan["metrics"])
    assert all(metric["configuration"]["status"] == "ready" for metric in plan["metrics"])
    assert report["runnable_metric_count"] == len(plan["metrics"])
    assert report["excluded_metric_count"] > 0
    assert plan["execution_policy"] == {
        "fail_fast": False,
        "allow_skips": False,
        "sample_mode": "full",
    }
    validate_plan_schema(plan)


def test_dataset_preflight_includes_ready_port_range_and_excludes_service_rule(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame(
        {
            "Source Port": [12345, 443],
            "Destination Port": [53, 443],
        }
    ).to_csv(dataset, index=False)

    plan, report = build_plan(plan_id="ports", name="Ports", dataset_path=dataset)
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}

    assert report["metrics"]["valid_port_range_profile"]["status"] == "ready"
    assert "valid_port_range_profile" in metric_ids
    assert report["metrics"]["service_port_consistency_profile"]["status"] == "needs_configuration"
    assert "service_port_consistency_profile" not in metric_ids


def test_reference_metrics_are_reported_but_never_written_without_reference_configuration(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame(
        {
            "Source Port": [12345, 443, 53],
            "Destination Port": [53, 443, 12345],
            "feature": [1.0, 2.0, 3.0],
        }
    ).to_csv(dataset, index=False)

    plan, report = build_plan(plan_id="reference", name="Reference", dataset_path=dataset)
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    reference_ids = [metric_id for metric_id in report["metrics"] if metric_id.endswith("_from_reference")]

    assert "valid_port_range_profile" in metric_ids
    assert reference_ids
    for metric_id in reference_ids:
        assert metric_id not in metric_ids
        assert report["metrics"][metric_id]["included"] is False
        assert report["metrics"][metric_id]["reason"] == "reference_dataset_required"


def test_existing_field_translation_sidecar_can_make_metric_runnable(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame(
        {
            "src_port_custom": [12345, 443],
            "dst_port_custom": [53, 443],
        }
    ).to_csv(dataset, index=False)
    sidecar = tmp_path / "flows.field_translation.json"
    sidecar.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "test_to_dataset_fields": {
                    "Source Port": "src_port_custom",
                    "Destination Port": "dst_port_custom",
                },
            }
        ),
        encoding="utf-8",
    )

    plan, report = build_plan(plan_id="mapped", name="Mapped", dataset_path=dataset)
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}

    assert "valid_port_range_profile" in metric_ids
    assert report["field_translation_path"] == str(sidecar.resolve())


def test_pcap_plan_includes_direct_and_independent_packet_adapter_metrics(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"pcap-placeholder")

    plan, report = build_plan(plan_id="pcap", name="PCAP", dataset_path=dataset)
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    expected = PCAP_DIRECT_METRICS | PCAP_PACKET_METRICS

    assert metric_ids == expected
    assert report["runnable_metric_count"] == len(expected)
    assert all(metric["enabled"] is True for metric in plan["metrics"])
    for metric_id in PCAP_SELF_DERIVED_METRICS:
        assert metric_id not in metric_ids
        assert report["metrics"][metric_id]["reason"] == "self_derived_pcap_invariant_not_independent"


def test_include_exclude_rejects_unknown_metric_ids(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame({"Source Port": [1], "Destination Port": [2]}).to_csv(dataset, index=False)

    with pytest.raises(ValueError, match="Unknown metric IDs"):
        build_plan(
            plan_id="bad",
            name="Bad",
            dataset_path=dataset,
            include_metric_ids=["not_a_real_metric"],
        )


def test_write_plan_is_valid_and_requires_force_for_overwrite(tmp_path):
    dataset = tmp_path / "flows.csv"
    pd.DataFrame({"Source Port": [1], "Destination Port": [2]}).to_csv(dataset, index=False)
    plan, _ = build_plan(plan_id="write-test", name="Write Test", dataset_path=dataset)
    output = tmp_path / "plan.json"

    write_plan(output, plan)
    with open(output, "r", encoding="utf-8") as handle:
        persisted = json.load(handle)
    validate_plan_schema(persisted)

    with pytest.raises(FileExistsError):
        write_plan(output, plan)

    write_plan(output, plan, overwrite=True)



def test_pcap_handshake_is_automatically_runnable_without_boundary_policy(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"pcap-placeholder")
    plan, report = build_plan(plan_id="handshake", name="Handshake", dataset_path=dataset)
    assert "handshake_plausibility_profile" in {metric["metric_id"] for metric in plan["metrics"]}
    assert report["metrics"]["handshake_plausibility_profile"]["status"] == "ready"


def test_pcap_service_port_requires_explicit_single_service_assertion(tmp_path):
    dataset = tmp_path / "capture.pcap"
    dataset.write_bytes(b"pcap-placeholder")
    plan, report = build_plan(plan_id="service-blocked", name="Service blocked", dataset_path=dataset)
    assert "service_port_consistency_profile" not in {metric["metric_id"] for metric in plan["metrics"]}
    assert report["metrics"]["service_port_consistency_profile"]["reason"] == "service_definition_required"

    configured_plan, configured_report = build_plan(
        plan_id="service-ready",
        name="Service ready",
        dataset_path=dataset,
        service_port_configuration={"service_name": "dns", "expected_ports": [53], "population_mode": "all_rows"},
    )
    configured = {metric["metric_id"]: metric for metric in configured_plan["metrics"]}
    assert configured_report["metrics"]["service_port_consistency_profile"]["status"] == "ready"
    params = configured["service_port_consistency_profile"]["calculation"]["parameters"]
    assert params["population_mode"] == "all_rows"
    assert params["pass_threshold"] == 1.0
    assert params["warn_threshold"] == 0.0


def test_pcap_reference_metrics_unlock_only_with_independent_reference_pcap(tmp_path):
    candidate = tmp_path / "candidate.pcap"
    reference = tmp_path / "reference.pcap"
    candidate.write_bytes(b"candidate-placeholder")
    reference.write_bytes(b"reference-placeholder")
    plan, report = build_plan(
        plan_id="reference-pcap", name="Reference PCAP", dataset_path=candidate, reference_dataset_path=reference
    )
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    assert PCAP_REFERENCE_METRICS.issubset(metric_ids)
    assert report["reference_dataset"] == str(reference.resolve())
    for metric_id in PCAP_REFERENCE_UNSUPPORTED_REASONS:
        assert metric_id not in metric_ids
        assert report["metrics"][metric_id]["reason"] == PCAP_REFERENCE_UNSUPPORTED_REASONS[metric_id]


def test_pcap_reference_rejects_self_comparison_and_representation_mismatch(tmp_path):
    candidate = tmp_path / "candidate.pcap"
    candidate.write_bytes(b"candidate-placeholder")
    with pytest.raises(ValueError, match="must be independent"):
        build_plan(plan_id="self-reference", name="Self reference", dataset_path=candidate, reference_dataset_path=candidate)

    reference_csv = tmp_path / "reference.csv"
    pd.DataFrame({"Packet Length": [1, 2], "Inter Arrival Time": [0.1, 0.2]}).to_csv(reference_csv, index=False)
    plan, report = build_plan(
        plan_id="mismatched-reference", name="Mismatched reference", dataset_path=candidate, reference_dataset_path=reference_csv
    )
    metric_ids = {metric["metric_id"] for metric in plan["metrics"]}
    assert not (PCAP_REFERENCE_METRICS & metric_ids)
    for metric_id in PCAP_REFERENCE_METRICS:
        assert report["metrics"][metric_id]["reason"] == "pcap_reference_representation_mismatch"
