from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from cbr_tests.plan_migration import migrate_plan_to_canonical_ids
from runner.experiment_contract import (
    ExperimentContractError,
    FINAL_EXPERIMENT_CONTRACT,
    validate_final_experiment_plan,
)


def _canonical_plan() -> dict:
    source = json.loads(
        Path("examples/quickstart/plan.json").read_text(encoding="utf-8")
    )
    migrated, _changes = migrate_plan_to_canonical_ids(source)
    migrated["execution_policy"]["allow_skips"] = False
    return migrated


def test_final_experiment_contract_accepts_canonical_full_plan_without_skips():
    report = validate_final_experiment_plan(_canonical_plan())

    assert report["contract"] == FINAL_EXPERIMENT_CONTRACT
    assert report["canonical_metric_ids"] is True
    assert report["compatibility_only_profiles"] is False
    assert report["sample_mode"] == "full"
    assert report["allow_skips"] is False
    assert report["enabled_metric_count"] == 4


def test_final_experiment_contract_rejects_legacy_metric_ids():
    plan = _canonical_plan()
    plan["metrics"][-1]["metric_id"] = "pearson_correlation_profile"

    with pytest.raises(ExperimentContractError, match="Legacy metric IDs present"):
        validate_final_experiment_plan(plan)


def test_final_experiment_contract_rejects_compatibility_only_profiles():
    plan = _canonical_plan()
    compatibility_metric = deepcopy(plan["metrics"][0])
    compatibility_metric["metric_id"] = "reserved_ip_address_profile"
    compatibility_metric["label"] = "Reserved IP Address Profile"
    compatibility_metric["taxonomy_path"] = [
        "dataset_heuristics",
        "protocol_and_network_realism",
        "address_validity",
        "reserved_ip_address_profile",
    ]
    plan["metrics"].append(compatibility_metric)

    with pytest.raises(ExperimentContractError, match="compatibility-only"):
        validate_final_experiment_plan(plan)


def test_final_experiment_contract_rejects_non_full_sample_mode_at_schema_boundary():
    plan = _canonical_plan()
    plan["execution_policy"]["sample_mode"] = "head"

    # The base plan schema currently supports only full-population execution, so
    # this invalid mode is rejected before the stricter experiment checks run.
    with pytest.raises(ValueError, match="sample_mode 'head' is not implemented"):
        validate_final_experiment_plan(plan)


def test_final_experiment_contract_rejects_skippable_metrics():
    plan = _canonical_plan()
    plan["execution_policy"]["allow_skips"] = True

    with pytest.raises(ExperimentContractError, match="allow_skips=false"):
        validate_final_experiment_plan(plan)


def test_final_experiment_contract_rejects_plan_with_no_enabled_metrics():
    plan = _canonical_plan()
    for metric in plan["metrics"]:
        metric["enabled"] = False

    with pytest.raises(ExperimentContractError, match="no enabled metrics"):
        validate_final_experiment_plan(plan)
