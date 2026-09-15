from __future__ import annotations

import json
from pathlib import Path

import pytest

from runner.fixed_plan_matrix import build_fixed_plan_matrix
from runner.plan_builder import build_plan, write_plan
from runner.reference_binding import bind_runtime_reference, reference_bound_metric_ids
from runner.unified_tui import _menu_items


REFERENCE_METRIC = "feature_wise_ks_statistic_from_reference"
INTRINSIC_METRIC = "valid_port_range_profile"


def _pcap(path: Path) -> Path:
    path.write_bytes(b"")
    return path


def _reference_plan(tmp_path: Path) -> Path:
    candidate = _pcap(tmp_path / "seed_candidate.pcapng")
    reference = _pcap(tmp_path / "seed_reference.pcapng")
    plan, _ = build_plan(
        plan_id="fixed-pcap-plan",
        name="Fixed PCAP plan",
        dataset_path=candidate,
        reference_dataset_path=reference,
        include_metric_ids=[REFERENCE_METRIC, INTRINSIC_METRIC],
    )
    path = tmp_path / "fixed_plan.json"
    write_plan(path, plan)
    return path


def test_runtime_reference_binding_changes_only_configured_reference_path(tmp_path):
    plan_path = _reference_plan(tmp_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    new_reference = _pcap(tmp_path / "new_reference.pcapng")

    bound = bind_runtime_reference(plan, new_reference)

    assert reference_bound_metric_ids(bound) == [REFERENCE_METRIC]
    reference_metric = next(metric for metric in bound["metrics"] if metric["metric_id"] == REFERENCE_METRIC)
    intrinsic_metric = next(metric for metric in bound["metrics"] if metric["metric_id"] == INTRINSIC_METRIC)
    assert reference_metric["input_requirements"]["reference_dataset_path"] == str(new_reference.resolve())
    assert "reference_dataset_path" not in intrinsic_metric["input_requirements"]
    assert bound["plan_meta"] == plan["plan_meta"]


def test_fixed_plan_matrix_reuses_same_scientific_plan_across_candidates(tmp_path):
    plan_path = _reference_plan(tmp_path)
    candidate_a = _pcap(tmp_path / "candidate_a.pcapng")
    candidate_b = _pcap(tmp_path / "candidate_b.pcap")
    reference_a = _pcap(tmp_path / "reference_a.pcapng")
    reference_b = _pcap(tmp_path / "reference_b.pcapng")
    output = tmp_path / "mapped_batch.json"

    written = build_fixed_plan_matrix(
        name="Mapped PCAP experiment",
        plan_path=plan_path,
        candidate_paths=[candidate_a, candidate_b],
        reference_paths=[reference_a, reference_b],
        output_path=output,
        repo_root=tmp_path,
    )

    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["batch_meta"]["metric_policy"] == "fixed_plan"
    assert payload["batch_meta"]["plan_binding_mode"] == "fixed_pcap_plan"
    assert payload["batch_meta"]["job_count"] == 4
    assert len(payload["jobs"]) == 4
    assert len({tuple(job["metric_ids"]) for job in payload["jobs"]}) == 1
    # One binding plan per reference, not one regenerated plan per candidate/reference job.
    assert len({job["plan_path"] for job in payload["jobs"]}) == 2


def test_fixed_plan_matrix_without_references_points_every_job_at_source_plan(tmp_path):
    plan_path = _reference_plan(tmp_path)
    candidate_a = _pcap(tmp_path / "candidate_a.pcapng")
    candidate_b = _pcap(tmp_path / "candidate_b.pcapng")
    output = tmp_path / "candidate_batch.json"

    build_fixed_plan_matrix(
        name="Candidate only",
        plan_path=plan_path,
        candidate_paths=[candidate_a, candidate_b],
        output_path=output,
        repo_root=tmp_path,
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["batch_meta"]["job_count"] == 2
    assert {job["plan_path"] for job in payload["jobs"]} == {plan_path.name}


def test_references_require_reference_enabled_plan(tmp_path):
    candidate = _pcap(tmp_path / "seed.pcapng")
    reference = _pcap(tmp_path / "reference.pcapng")
    plan, _ = build_plan(
        plan_id="intrinsic-only",
        name="Intrinsic only",
        dataset_path=candidate,
        include_metric_ids=[INTRINSIC_METRIC],
    )
    plan_path = tmp_path / "intrinsic_plan.json"
    write_plan(plan_path, plan)

    with pytest.raises(ValueError, match="no reference-enabled metrics"):
        build_fixed_plan_matrix(
            name="Bad matrix",
            plan_path=plan_path,
            candidate_paths=[candidate],
            reference_paths=[reference],
            output_path=tmp_path / "bad.json",
            repo_root=tmp_path,
        )


def test_toolbox_exposes_fixed_pcap_plan_mapping():
    keys = [item.key for item in _menu_items()]
    assert "fixed_pcap_matrix" in keys
    assert keys.index("build_plan") < keys.index("fixed_pcap_matrix") < keys.index("campaign_build")
