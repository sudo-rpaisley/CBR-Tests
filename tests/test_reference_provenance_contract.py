from __future__ import annotations

import hashlib
import json
from pathlib import Path

from runner.provenance import build_provenance_manifest, sha256_json


def test_reference_dataset_identity_hash_and_mapping_are_frozen_in_provenance(tmp_path: Path):
    """Final-experiment outcomes must identify the exact comparator and mapping used."""
    candidate = tmp_path / "candidate.csv"
    reference = tmp_path / "reference.csv"
    candidate.write_bytes(b"candidate_field\n1\n2\n")
    reference.write_bytes(b"reference_field\n1\n2\n")

    metric = {
        "metric_id": "feature_wise_ks_statistic_from_reference",
        "taxonomy_path": ["reference_model_comparison", "statistical_fidelity"],
        "input_requirements": {
            "candidate_fields": ["candidate_field"],
            "reference_dataset_path": str(reference.resolve()),
        },
        "reference_field_map": {"reference_field": "candidate_field"},
        "calculation": {"method": "feature_wise_ks_statistic_from_reference", "parameters": {}},
    }
    plan = {
        "plan_meta": {"plan_id": "reference-freeze", "name": "Reference freeze", "version": "1.0.0"},
        "metrics": [metric],
    }
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan), encoding="utf-8")

    manifest = build_provenance_manifest(
        plan=plan,
        dataset_path=candidate,
        case_file=plan_file,
        plan_source_path=plan_file,
        field_translation={},
        translation_path=None,
        taxonomy_path=None,
        cli_arguments={"case": str(plan_file)},
    )

    assert manifest["dataset"]["path"] == str(candidate.resolve())
    assert manifest["dataset"]["sha256"] == hashlib.sha256(candidate.read_bytes()).hexdigest()
    assert manifest["plan"]["sha256"] == sha256_json(plan)
    assert manifest["plan"]["snapshot"]["metrics"][0]["reference_field_map"] == {
        "reference_field": "candidate_field"
    }
    assert manifest["plan"]["snapshot"]["metrics"][0]["input_requirements"]["reference_dataset_path"] == str(reference.resolve())

    assert len(manifest["reference_datasets"]) == 1
    reference_manifest = manifest["reference_datasets"][0]
    assert reference_manifest["path"] == str(reference.resolve())
    assert reference_manifest["size_bytes"] == reference.stat().st_size
    assert reference_manifest["sha256"] == hashlib.sha256(reference.read_bytes()).hexdigest()
