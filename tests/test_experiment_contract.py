from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from runner.contract import (
    enforce_skip_policy,
    validate_dataset_format_applicability,
    validate_loaded_dataset_applicability,
    validate_output_path_safety,
)
from runner.provenance import build_provenance_manifest, resolve_plan_source_path, sha256_json
from runner.run_plan_helpers import build_outcome, write_outcome
from runner.schema import validate_plan_schema


def _plan(*, sample_mode: str = "full", allow_skips: bool = False) -> dict:
    return {
        "plan_meta": {"plan_id": "plan-1", "name": "Plan", "version": "1.0.0"},
        "applicability": {
            "dataset_formats": ["csv"],
            "dataset_family": ["flow_feature"],
            "requires_numeric_fields": True,
            "minimum_numeric_fields": 2,
        },
        "execution_policy": {
            "fail_fast": True,
            "allow_skips": allow_skips,
            "sample_mode": sample_mode,
        },
        "metrics": [
            {
                "metric_id": "m1",
                "taxonomy_path": ["test", "m1"],
                "input_requirements": {},
                "calculation": {"method": "m1", "parameters": {}},
            }
        ],
    }


def test_plan_schema_rejects_unimplemented_sample_mode():
    with pytest.raises(ValueError, match="sample_mode 'random' is not implemented"):
        validate_plan_schema(_plan(sample_mode="random"))


def test_plan_schema_validates_applicability_types():
    plan = _plan()
    plan["applicability"]["minimum_numeric_fields"] = True
    with pytest.raises(ValueError, match="minimum_numeric_fields"):
        validate_plan_schema(plan)


def test_dataset_format_applicability_is_enforced(tmp_path: Path):
    with pytest.raises(ValueError, match="not permitted"):
        validate_dataset_format_applicability(_plan(), tmp_path / "capture.pcap")


def test_numeric_applicability_is_enforced():
    dataframe = pd.DataFrame({"numeric": [1, 2], "text": ["a", "b"]})
    with pytest.raises(ValueError, match="requires at least 2"):
        validate_loaded_dataset_applicability(_plan(), dataframe)


def test_allow_skips_false_is_enforced():
    with pytest.raises(ValueError, match="allow_skips is false"):
        enforce_skip_policy(_plan(allow_skips=False), {"m1": ["Source IP"]})


def test_allow_skips_true_and_dry_run_are_permitted():
    enforce_skip_policy(_plan(allow_skips=True), {"m1": ["Source IP"]})
    enforce_skip_policy(_plan(allow_skips=False), {"m1": ["Source IP"]}, dry_run=True)


def test_output_path_cannot_collide_with_input(tmp_path: Path):
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("x\n1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="collides"):
        validate_output_path_safety(dataset, protected_paths=[dataset])


def test_existing_output_requires_explicit_overwrite(tmp_path: Path):
    output = tmp_path / "outcome.json"
    output.write_text("{}\n", encoding="utf-8")
    with pytest.raises(FileExistsError, match="--force-output"):
        validate_output_path_safety(output, protected_paths=[], allow_overwrite=False)
    validate_output_path_safety(output, protected_paths=[], allow_overwrite=True)


def test_provenance_identifies_exact_inputs(tmp_path: Path):
    dataset = tmp_path / "dataset.csv"
    dataset.write_bytes(b"value\n1\n2\n")
    plan = _plan()
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan), encoding="utf-8")

    manifest = build_provenance_manifest(
        plan=plan,
        dataset_path=dataset,
        case_file=plan_file,
        plan_source_path=plan_file,
        field_translation={"Src IP": "Source IP"},
        translation_path=None,
        taxonomy_path=None,
        cli_arguments={"case": str(plan_file), "workers": 1},
    )

    assert manifest["run_id"]
    assert manifest["dataset"]["sha256"] == hashlib.sha256(dataset.read_bytes()).hexdigest()
    assert manifest["plan"]["sha256"] == sha256_json(plan)
    assert manifest["plan"]["snapshot"] == plan
    assert manifest["field_translation"]["effective_mapping"] == {"Src IP": "Source IP"}
    assert "python" in manifest["software"]
    assert "dependencies" in manifest["software"]


def test_case_provenance_resolves_referenced_plan(tmp_path: Path):
    plan_dir = tmp_path / "plans"
    plan_dir.mkdir()
    plan_file = plan_dir / "p.json"
    plan_file.write_text("{}", encoding="utf-8")
    case_file = tmp_path / "case.json"
    case_file.write_text(json.dumps({"test_plan": {"path": "plans/p.json"}}), encoding="utf-8")

    assert resolve_plan_source_path(case_file) == plan_file.resolve()


def test_outcome_schema_v2_embeds_provenance(tmp_path: Path):
    plan = _plan()
    started = datetime.now(timezone.utc)
    start_perf = time.perf_counter()
    provenance = {"run_id": "run-123", "schema_version": 1}
    outcome = build_outcome(
        "success",
        "case-1",
        "plan-1",
        plan["metrics"],
        tmp_path / "dataset.csv",
        [],
        {},
        started,
        start_perf,
        {},
        provenance=provenance,
    )

    assert outcome["schema_version"] == 2
    assert outcome["run_id"] == "run-123"
    assert outcome["provenance"] == provenance


def test_outcome_writer_rejects_non_finite_json_without_replacing_previous_file(tmp_path: Path):
    output = tmp_path / "outcome.json"
    write_outcome(output, {"status": "success", "value": 1})

    with pytest.raises(ValueError):
        write_outcome(output, {"status": "success", "value": float("nan")})

    assert json.loads(output.read_text(encoding="utf-8"))["value"] == 1
