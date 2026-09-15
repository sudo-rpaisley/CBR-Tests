import json

import pytest

from scripts.rerun_and_compare import _reject_legacy_representative_plan, _resolved_plan_payload


def _plan(metric_id: str) -> dict:
    return {
        "plan_meta": {"plan_id": "example", "name": "Example"},
        "metrics": [
            {
                "metric_id": metric_id,
                "label": metric_id,
                "taxonomy_path": ["example", metric_id],
            }
        ],
    }


def test_representative_rerun_rejects_legacy_intrinsic_plan(tmp_path):
    plan = tmp_path / "legacy.json"
    plan.write_text(json.dumps(_plan("wasserstein_feature_distance")), encoding="utf-8")

    with pytest.raises(SystemExit, match="canonical metric-conformance contract") as exc:
        _reject_legacy_representative_plan(plan)
    assert "migrate_plan_to_canonical_ids.py" in str(exc.value)


def test_representative_rerun_rejects_compatibility_only_protocol_profile(tmp_path):
    plan = tmp_path / "legacy_protocol.json"
    plan.write_text(json.dumps(_plan("protocol_validity_profile")), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        _reject_legacy_representative_plan(plan)
    message = str(exc.value)
    assert "Compatibility-only profile IDs (1)" in message
    assert "regenerate this plan" in message
    assert "Valid IP Address Ratio" in message


def test_representative_rerun_rejects_legacy_reserved_address_profile(tmp_path):
    plan = tmp_path / "legacy_reserved.json"
    plan.write_text(json.dumps(_plan("reserved_ip_address_profile")), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        _reject_legacy_representative_plan(plan)
    assert "requires an explicit address-use policy" in str(exc.value)


def test_representative_rerun_accepts_canonical_plan(tmp_path):
    plan = tmp_path / "canonical.json"
    plan.write_text(json.dumps(_plan("feature_wasserstein_internal_drift")), encoding="utf-8")
    _reject_legacy_representative_plan(plan)


def test_case_guard_resolves_referenced_plan(tmp_path):
    plans = tmp_path / "plans"
    cases = tmp_path / "cases"
    plans.mkdir()
    cases.mkdir()
    plan = plans / "legacy.json"
    plan.write_text(json.dumps(_plan("pearson_correlation_profile")), encoding="utf-8")
    case = cases / "case.json"
    case.write_text(json.dumps({"test_plan": {"path": "../plans/legacy.json"}}), encoding="utf-8")

    resolved_path, payload = _resolved_plan_payload(case)
    assert resolved_path == plan.resolve()
    assert payload["metrics"][0]["metric_id"] == "pearson_correlation_profile"
    with pytest.raises(SystemExit):
        _reject_legacy_representative_plan(case)
